"""SearchEngine — orchestrates the full search pipeline."""
import logging

from src.config.settings import settings
from src.utils.llm import LLMClient
from .contact_extractor import ContactExtractor
from .cross_reference import CrossReferenceEngine
from .google import WebSearcher
from .models import SearchRequest, SearchStats
from .query_generator import QueryGenerator
from .ranker import Ranker
from .site_parser import SiteParser
from .writer import SearchWriter

logger = logging.getLogger(__name__)


class SearchEngine:
    """
    Main search engine orchestrator.

    Pipeline: query_gen -> google -> site_parser -> contact_extractor -> ranker -> writer

    Usage:
        engine = SearchEngine()
        stats = await engine.search(SearchRequest(article="6754-21-3110", ...))
        await engine.close()
    """

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.llm = LLMClient()
        self.query_gen = QueryGenerator(self.llm)
        self.google = WebSearcher()
        self.site_parser = SiteParser()
        self.contact_extractor = ContactExtractor(self.llm)
        self.cross_ref = CrossReferenceEngine(self.llm)
        self.ranker = Ranker()

    async def search(self, request: SearchRequest) -> SearchStats:
        """Execute full search pipeline."""
        stats = SearchStats(article=request.article)

        # Stage 1: Generate queries
        logger.info(f"=== Search: {request.article} (depth={request.depth.value}) ===")
        queries = await self.query_gen.generate(request)
        stats.queries_generated = len(queries)
        logger.info(f"Generated {len(queries)} queries")

        if self.dry_run:
            for q in queries:
                logger.info(f"  [{q.strategy}/{q.language}] {q.query}")
            return stats

        # Stage 2: Google search
        google_results = await self.google.search_all(
            queries, max_per_query=settings.search_max_results_per_query,
        )
        stats.google_results = len(google_results)
        logger.info(f"Found {len(google_results)} unique URLs from Google")

        if not google_results:
            logger.warning("No Google results found")
            return stats

        # Stage 3: Parse sites and extract contacts
        max_sites = settings.search_max_sites_to_parse
        contacts = []

        for i, gr in enumerate(google_results[:max_sites]):
            logger.debug(f"Parsing site {i + 1}/{min(len(google_results), max_sites)}: {gr.url}")
            try:
                pages = await self.site_parser.load(gr.url)
                if not pages:
                    continue
                stats.sites_parsed += 1

                contact = await self.contact_extractor.extract(pages, article=request.article)
                if contact.emails or contact.phones or contact.price is not None:
                    contact.source_url = gr.url
                    contacts.append(contact)
                    stats.contacts_extracted += 1
                    logger.debug(
                        f"  Extracted: emails={contact.emails}, "
                        f"phones={contact.phones}, price={contact.price}"
                    )
            except Exception as e:
                stats.errors.append(f"{gr.url}: {e}")
                logger.warning(f"Error processing {gr.url}: {e}")

        logger.info(f"Extracted contacts from {stats.contacts_extracted}/{stats.sites_parsed} sites")

        # Stage 4: Cross-reference if too few results
        if len(contacts) < 3 and request.include_analogs and not self.google.is_blocked:
            logger.info("Few results found, trying cross-references...")
            alt_articles = await self.cross_ref.find_alternatives(request)

            for alt in alt_articles[:3]:
                logger.info(f"Cross-ref search: {alt}")
                alt_request = request.model_copy(
                    update={"article": alt, "include_analogs": False}
                )
                alt_queries = await self.query_gen.generate(alt_request)
                # Only use direct queries for cross-refs (faster)
                alt_queries = [q for q in alt_queries if q.strategy == "direct"][:3]

                alt_results = await self.google.search_all(alt_queries)
                for gr in alt_results[:5]:
                    try:
                        pages = await self.site_parser.load(gr.url)
                        if pages:
                            contact = await self.contact_extractor.extract(pages, article=alt)
                            if contact.emails or contact.phones:
                                contact.source_url = gr.url
                                contacts.append(contact)
                    except Exception as e:
                        stats.errors.append(f"xref {gr.url}: {e}")

        # Stage 5: Rank and deduplicate
        ranked = self.ranker.rank(contacts)
        stats.results_after_dedup = len(ranked)
        logger.info(f"After dedup: {len(ranked)} unique suppliers")

        # Log top results
        for i, r in enumerate(ranked[:5]):
            logger.info(
                f"  #{i + 1}: {r.company_name or r.website} "
                f"| email={r.email} | phone={r.phone} "
                f"| price={r.price} {r.currency} "
                f"| conf={r.confidence:.2f}"
            )

        # Stage 6: Write to DB (sync writer in executor to avoid blocking event loop)
        if ranked:
            import asyncio

            def _write_to_db():
                writer = SearchWriter()
                try:
                    return writer.write(
                        ranked,
                        position_id=request.position_id,
                        task_id=request.task_id,
                        article=request.article,
                    )
                finally:
                    writer.close()

            loop = asyncio.get_event_loop()
            write_stats = await loop.run_in_executor(None, _write_to_db)
            stats.suppliers_created = write_stats["suppliers_created"]
            stats.suppliers_updated = write_stats["suppliers_updated"]
            stats.results_linked = write_stats.get("results_linked", 0)

        return stats

    async def close(self):
        """Release all resources."""
        await self.google.close()
        await self.site_parser.close()
        await self.llm.close()

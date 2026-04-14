"""Web search scraper — DuckDuckGo (primary) + Google (fallback)."""
import logging
from urllib.parse import urlparse, parse_qs, unquote

from bs4 import BeautifulSoup

from src.config.settings import settings
from src.utils.http import HttpClient
from .models import GoogleResult, SearchQuery

logger = logging.getLogger(__name__)

# Domains to skip (not supplier sites)
SKIP_DOMAINS = {
    "google.com", "google.ru", "google.kz",
    "duckduckgo.com",
    "youtube.com", "wikipedia.org",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "tiktok.com", "reddit.com", "linkedin.com",
    "vk.com", "ok.ru", "pinterest.com",
    "amazon.com", "ebay.com",
}


class WebSearcher:
    """Web search using DuckDuckGo HTML (no JS required)."""

    def __init__(self):
        self.http = HttpClient(
            delay_min=settings.search_google_delay_min,
            delay_max=settings.search_google_delay_max,
        )
        self._blocked = False

    async def search(self, query: SearchQuery, max_results: int = 10) -> list[GoogleResult]:
        """Search DuckDuckGo and return parsed results."""
        if self._blocked:
            logger.warning("Search blocked (previous error), skipping")
            return []

        try:
            response = await self.http.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query.query},
            )

            if response.status_code == 429:
                logger.warning("DuckDuckGo 429 — stopping all searches")
                self._blocked = True
                return []

            if response.status_code != 200:
                logger.warning(f"DuckDuckGo returned {response.status_code} for '{query.query}'")
                return []

            return self._parse_ddg_results(response.text, query, max_results)

        except Exception as e:
            logger.error(f"Search error for '{query.query}': {e}")
            return []

    def _parse_ddg_results(
        self, html: str, query: SearchQuery, max_results: int,
    ) -> list[GoogleResult]:
        """Parse DuckDuckGo HTML search results."""
        soup = BeautifulSoup(html, "lxml")
        results = []

        for a in soup.select("a.result__a"):
            if len(results) >= max_results:
                break

            href = a.get("href", "")
            url = self._clean_ddg_url(href)
            if not url or self._should_skip(url):
                continue

            title = a.get_text(strip=True)

            # Snippet is in sibling element
            snippet = ""
            parent = a.find_parent("div", class_="result")
            if parent:
                snippet_el = parent.select_one("a.result__snippet")
                if snippet_el:
                    snippet = snippet_el.get_text(strip=True)

            results.append(GoogleResult(
                url=url,
                title=title,
                snippet=snippet,
                query=query.query,
                strategy=query.strategy,
            ))

        logger.debug(f"Parsed {len(results)} results for '{query.query}'")
        return results

    def _clean_ddg_url(self, raw_url: str) -> str:
        """Extract actual URL from DuckDuckGo redirect."""
        if "//duckduckgo.com/l/?uddg=" in raw_url:
            parsed = parse_qs(urlparse(raw_url).query)
            urls = parsed.get("uddg", [])
            return unquote(urls[0]) if urls else ""
        if raw_url.startswith("http"):
            return raw_url
        return ""

    def _should_skip(self, url: str) -> bool:
        """Skip non-supplier domains."""
        try:
            domain = urlparse(url).netloc.lower().replace("www.", "")
            return any(skip in domain for skip in SKIP_DOMAINS)
        except Exception:
            return True

    async def search_all(
        self, queries: list[SearchQuery], max_per_query: int = 10,
    ) -> list[GoogleResult]:
        """Run all queries sequentially, deduplicate URLs."""
        all_results = []
        seen_urls = set()

        for q in queries:
            if self._blocked:
                logger.info("Search blocked, stopping remaining queries")
                break

            results = await self.search(q, max_per_query)
            for r in results:
                if r.url not in seen_urls:
                    seen_urls.add(r.url)
                    all_results.append(r)

        logger.info(f"Total unique search results: {len(all_results)}")
        return all_results

    @property
    def is_blocked(self) -> bool:
        return self._blocked

    async def close(self):
        await self.http.close()


# Backward-compatible alias
GoogleSearcher = WebSearcher

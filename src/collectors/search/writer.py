"""Write search results to DB.

- public.parsed_suppliers: supplier contact data (via existing SupplierWriter dedup)
- crm.search_results: N:M link between CRM position and supplier

Offers are NOT written directly. Manager reviews results in CRM UI and
explicitly converts chosen suppliers into supplier_requests + offers.
"""
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.config.database import SessionLocal
from src.models.supplier import ParsedSupplier
from src.pipeline.normalizer import extract_domain, normalize_email, normalize_phone
from src.pipeline.writer import SupplierWriter
from .models import SearchResultItem

logger = logging.getLogger(__name__)


class SearchWriter:
    def __init__(self, db: Optional[Session] = None):
        self.db = db or SessionLocal()
        self._owns_session = db is None
        self.supplier_writer = SupplierWriter(self.db)

    def write(
        self,
        results: list[SearchResultItem],
        position_id: Optional[int] = None,
        task_id: Optional[int] = None,
        article: str = "",
    ) -> dict:
        """
        Save results into parsed_suppliers (with dedup) + crm.search_results (link).
        Returns {"suppliers_created", "suppliers_updated", "results_linked", "skipped"}.
        """
        stats = {
            "suppliers_created": 0,
            "suppliers_updated": 0,
            "results_linked": 0,
            "skipped": 0,
        }

        supplier_items = []
        # Remember order — we need to match ranked results back to supplier rows
        keep_indices = []
        for idx, r in enumerate(results):
            if not r.company_name and not r.email and not r.phone:
                stats["skipped"] += 1
                continue
            keep_indices.append(idx)
            supplier_items.append({
                "name": r.company_name or r.website or "Unknown",
                "phone": r.phone,
                "email": r.email,
                "website": r.website,
                "city": r.city,
                "source": "search_engine",
                "source_url": r.source_url,
                "raw_data": {
                    "search_query": r.search_query,
                    "search_strategy": r.search_strategy,
                    "confidence": r.confidence,
                    "part_number": r.part_number,
                },
            })

        if supplier_items:
            import_result = self.supplier_writer.batch_import(supplier_items)
            stats["suppliers_created"] = import_result.created
            stats["suppliers_updated"] = import_result.updated
            logger.info(
                f"Suppliers: {import_result.created} created, "
                f"{import_result.updated} updated, "
                f"{import_result.skipped} skipped"
            )

        # Link each result to the position in crm.search_results
        if position_id is not None and keep_indices:
            for idx in keep_indices:
                r = results[idx]
                try:
                    supplier_id = self._find_supplier_id(r)
                    if not supplier_id:
                        continue
                    self._upsert_search_result(
                        position_id=position_id,
                        supplier_id=supplier_id,
                        task_id=task_id,
                        result=r,
                    )
                    self.db.commit()
                    stats["results_linked"] += 1
                except Exception as e:
                    self.db.rollback()
                    logger.warning(f"Failed to link result for {r.company_name}: {e}")

        return stats

    def _find_supplier_id(self, result: SearchResultItem):
        """Find supplier_id by email/phone/domain match (after dedup via SupplierWriter)."""
        email = normalize_email(result.email) if result.email else None
        phone = normalize_phone(result.phone) if result.phone else None

        if email:
            row = self.db.query(ParsedSupplier.id).filter(ParsedSupplier.email == email).first()
            if row:
                return row[0]
        if phone:
            row = self.db.query(ParsedSupplier.id).filter(ParsedSupplier.phone == phone).first()
            if row:
                return row[0]
        if result.website:
            domain = extract_domain(result.website)
            if domain:
                row = (
                    self.db.query(ParsedSupplier.id)
                    .filter(ParsedSupplier.website.ilike(f"%{domain}%"))
                    .first()
                )
                if row:
                    return row[0]
        return None

    def _upsert_search_result(
        self,
        position_id: int,
        supplier_id,
        task_id: Optional[int],
        result: SearchResultItem,
    ):
        """Insert or update (position_id, supplier_id) in crm.search_results.

        On conflict: keep the higher confidence score.
        """
        self.db.execute(
            text("""
                INSERT INTO crm.search_results
                    (position_id, supplier_id, task_id, confidence,
                     search_query, search_strategy, source_url)
                VALUES
                    (:pos_id, CAST(:sup_id AS uuid), :task_id, :confidence,
                     :query, :strategy, :url)
                ON CONFLICT (position_id, supplier_id) DO UPDATE SET
                    confidence = GREATEST(crm.search_results.confidence, EXCLUDED.confidence),
                    task_id = COALESCE(EXCLUDED.task_id, crm.search_results.task_id),
                    search_query = COALESCE(EXCLUDED.search_query, crm.search_results.search_query),
                    search_strategy = COALESCE(EXCLUDED.search_strategy, crm.search_results.search_strategy),
                    source_url = COALESCE(EXCLUDED.source_url, crm.search_results.source_url)
            """),
            {
                "pos_id": position_id,
                "sup_id": str(supplier_id),
                "task_id": task_id,
                "confidence": result.confidence,
                "query": result.search_query or None,
                "strategy": result.search_strategy or None,
                "url": result.source_url or None,
            },
        )

    def close(self):
        if self._owns_session:
            self.db.close()

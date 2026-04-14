"""Write search results to database (parsed_suppliers + crm.offers)."""
import json
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.config.database import SessionLocal
from src.models.supplier import ParsedSupplier
from src.pipeline.normalizer import extract_domain
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
        article: str = "",
    ) -> dict:
        """
        Write results to DB.
        Returns {"suppliers_created", "suppliers_updated", "offers_created", "skipped"}.
        """
        stats = {
            "suppliers_created": 0,
            "suppliers_updated": 0,
            "offers_created": 0,
            "skipped": 0,
        }

        # Step 1: Write suppliers via SupplierWriter
        supplier_items = []
        for r in results:
            if not r.company_name and not r.email and not r.phone:
                stats["skipped"] += 1
                continue
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
                    "notes": r.notes,
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

        # Step 2: Write offers to crm.offers (if position_id provided)
        if position_id:
            for r in results:
                try:
                    supplier_id = self._find_supplier_id(r)
                    if not supplier_id:
                        continue
                    self._insert_offer(position_id, supplier_id, r)
                    self.db.commit()
                    stats["offers_created"] += 1
                except Exception as e:
                    self.db.rollback()
                    logger.warning(f"Failed to write offer for {r.company_name}: {e}")

        return stats

    def _find_supplier_id(self, result: SearchResultItem):
        """Find supplier_id by email/phone/domain match."""
        if result.email:
            row = (
                self.db.query(ParsedSupplier.id)
                .filter(ParsedSupplier.email == result.email)
                .first()
            )
            if row:
                return row[0]

        if result.phone:
            row = (
                self.db.query(ParsedSupplier.id)
                .filter(ParsedSupplier.phone == result.phone)
                .first()
            )
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

    def _insert_offer(self, position_id: int, supplier_id, result: SearchResultItem):
        """Insert offer into crm.offers."""
        details = json.dumps({
            "source": "search_engine",
            "source_url": result.source_url,
            "confidence": result.confidence,
            "search_strategy": result.search_strategy,
        })

        self.db.execute(
            text("""
                INSERT INTO crm.offers
                    (position_id, supplier_id, price, price_currency,
                     in_stock, condition, status, details)
                SELECT :pos_id, CAST(:sup_id AS uuid), :price, :currency,
                       :in_stock, :condition, 'pending', CAST(:details AS jsonb)
                WHERE NOT EXISTS (
                    SELECT 1 FROM crm.offers
                    WHERE position_id = :pos_id AND supplier_id = CAST(:sup_id AS uuid)
                )
            """),
            {
                "pos_id": position_id,
                "sup_id": str(supplier_id),
                "price": result.price,
                "currency": result.currency or "USD",
                "in_stock": result.in_stock,
                "condition": result.condition or "unknown",
                "details": details,
            },
        )

    def close(self):
        if self._owns_session:
            self.db.close()

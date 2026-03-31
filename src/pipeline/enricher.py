"""
Enricher: cross-source data enrichment for suppliers.

When we have the same supplier from multiple sources (e.g. 2GIS + Exkavator),
merge their data to build a more complete profile.
"""
import logging
from typing import Optional

from sqlalchemy.orm import Session

from src.models.supplier import Supplier
from src.pipeline.normalizer import normalize_phone, normalize_email, normalize_website, extract_domain

logger = logging.getLogger(__name__)


class Enricher:
    """Enrich suppliers with cross-source data"""

    def __init__(self, db: Session):
        self.db = db

    def enrich_supplier(self, supplier: Supplier, new_data: dict) -> bool:
        """
        Fill in missing fields from new_data.
        Only fills empty fields — never overwrites existing data.
        Returns True if any field was updated.
        """
        updated = False

        fill_fields = [
            ("phone", normalize_phone),
            ("email", normalize_email),
            ("website", normalize_website),
            ("inn", None),
            ("telegram", None),
            ("whatsapp", None),
            ("vk", None),
            ("city", None),
            ("region", None),
            ("address", None),
            ("supplier_type", None),
        ]

        for field_name, normalizer in fill_fields:
            current = getattr(supplier, field_name, None)
            new_value = new_data.get(field_name)

            if not current and new_value:
                if normalizer:
                    new_value = normalizer(new_value)
                if new_value:
                    setattr(supplier, field_name, new_value)
                    updated = True

        # Update rating if better
        new_rating = new_data.get("rating_2gis")
        if new_rating and (not supplier.rating_2gis or new_rating > supplier.rating_2gis):
            supplier.rating_2gis = new_rating
            updated = True

        new_reviews = new_data.get("reviews_count")
        if new_reviews and (not supplier.reviews_count or new_reviews > supplier.reviews_count):
            supplier.reviews_count = new_reviews
            updated = True

        if updated:
            self.db.flush()

        return updated

    def merge_raw_data(self, supplier: Supplier, source: str, raw_data: dict):
        """Merge raw_data from a new source into existing raw_data"""
        if not raw_data:
            return

        current = supplier.raw_data or {}
        if not isinstance(current, dict):
            current = {}

        # Store per-source raw data
        current[f"_source_{source}"] = raw_data
        supplier.raw_data = current
        self.db.flush()

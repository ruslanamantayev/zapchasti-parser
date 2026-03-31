"""
Writer: saves parsed data to database with deduplication.
Ported from zapchasti-platform/backend/app/domains/suppliers/service.py (batch_import)
"""
import logging
from typing import Optional

from sqlalchemy.orm import Session

from src.models.supplier import Supplier, SupplierBrand, SupplierEquipmentType, SupplierPartCategory
from src.pipeline.normalizer import (
    normalize_phone, normalize_email, normalize_website,
    normalize_name, extract_domain, slugify,
)

logger = logging.getLogger(__name__)


class ImportResult:
    def __init__(self):
        self.created = 0
        self.updated = 0
        self.skipped = 0
        self.errors = 0
        self.details: list[str] = []


class SupplierWriter:
    """Writes supplier data to DB with deduplication"""

    def __init__(self, db: Session):
        self.db = db

    def batch_import(self, items: list[dict]) -> ImportResult:
        """
        Import a list of supplier dicts into the database.

        Expected dict keys: name, phone, email, website, city, region, address,
        source, source_url, source_id, raw_data, supplier_type,
        rating_2gis, reviews_count, inn, telegram, whatsapp, vk,
        brand_ids, equipment_type_ids, part_category_ids
        """
        result = ImportResult()

        for item in items:
            try:
                name = normalize_name(item.get("name")) or item.get("name", "")
                if not name:
                    result.skipped += 1
                    continue

                phone = normalize_phone(item.get("phone"))
                email = normalize_email(item.get("email"))
                website = normalize_website(item.get("website"))

                existing = self._find_duplicate(
                    inn=item.get("inn"),
                    phone=phone,
                    email=email,
                    website=website,
                    source=item.get("source"),
                    source_id=item.get("source_id"),
                )

                if existing:
                    updated = self._update_existing(existing, item, phone, email, website)
                    if updated:
                        result.updated += 1
                    else:
                        result.skipped += 1
                    continue

                slug = self._generate_unique_slug(name)
                supplier = Supplier(
                    name=name,
                    slug=slug,
                    inn=item.get("inn"),
                    supplier_type=item.get("supplier_type"),
                    phone=phone,
                    email=email,
                    website=website,
                    telegram=item.get("telegram"),
                    whatsapp=item.get("whatsapp"),
                    vk=item.get("vk"),
                    city=item.get("city"),
                    region=item.get("region"),
                    address=item.get("address"),
                    source=item.get("source"),
                    source_url=item.get("source_url"),
                    source_id=item.get("source_id"),
                    raw_data=item.get("raw_data") or {},
                    rating_2gis=item.get("rating_2gis"),
                    reviews_count=item.get("reviews_count"),
                )
                self.db.add(supplier)
                self.db.flush()

                self._set_m2m(supplier.id, item)
                result.created += 1

            except Exception as e:
                self.db.rollback()
                result.errors += 1
                result.details.append(f"{item.get('name', '?')}: {e}")
                logger.warning(f"Import error: {e}")

        try:
            self.db.commit()
        except Exception:
            self.db.rollback()
        return result

    def _update_existing(self, existing: Supplier, item: dict,
                         phone: Optional[str], email: Optional[str],
                         website: Optional[str]) -> bool:
        """Update existing supplier with new data if available. Returns True if updated."""
        updated = False
        if phone and not existing.phone:
            existing.phone = phone
            updated = True
        if email and not existing.email:
            existing.email = email
            updated = True
        if website and not existing.website:
            existing.website = website
            updated = True
        if item.get("rating_2gis") and (not existing.rating_2gis or item["rating_2gis"] != existing.rating_2gis):
            existing.rating_2gis = item["rating_2gis"]
            updated = True
        if item.get("reviews_count") and (not existing.reviews_count or item["reviews_count"] != existing.reviews_count):
            existing.reviews_count = item["reviews_count"]
            updated = True
        if item.get("raw_data"):
            existing.raw_data = item["raw_data"]
            updated = True
        return updated

    def _find_duplicate(
        self,
        inn: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        website: Optional[str] = None,
        source: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> Optional[Supplier]:
        """5-level deduplication chain"""
        # 1. source + source_id
        if source and source_id:
            existing = (
                self.db.query(Supplier)
                .filter(Supplier.source == source, Supplier.source_id == source_id)
                .first()
            )
            if existing:
                return existing

        # 2. INN
        if inn:
            existing = self.db.query(Supplier).filter(Supplier.inn == inn).first()
            if existing:
                return existing

        # 3. Phone
        if phone:
            existing = self.db.query(Supplier).filter(Supplier.phone == phone).first()
            if existing:
                return existing

        # 4. Email
        if email:
            existing = self.db.query(Supplier).filter(Supplier.email == email).first()
            if existing:
                return existing

        # 5. Domain
        if website:
            domain = extract_domain(website)
            if domain:
                existing = (
                    self.db.query(Supplier)
                    .filter(Supplier.website.ilike(f"%{domain}%"))
                    .first()
                )
                if existing:
                    return existing

        return None

    def _set_m2m(self, supplier_id, item: dict):
        """Set M2M relationships from item dict"""
        for bid in set(item.get("brand_ids") or []):
            self.db.add(SupplierBrand(supplier_id=supplier_id, brand_id=bid))
        for eid in set(item.get("equipment_type_ids") or []):
            self.db.add(SupplierEquipmentType(supplier_id=supplier_id, equipment_type_id=eid))
        for pid in set(item.get("part_category_ids") or []):
            self.db.add(SupplierPartCategory(supplier_id=supplier_id, part_category_id=pid))

    def _generate_unique_slug(self, name: str) -> str:
        base_slug = slugify(name)
        slug = base_slug
        counter = 1
        while self.db.query(Supplier).filter(Supplier.slug == slug).first():
            slug = f"{base_slug}-{counter}"
            counter += 1
        return slug

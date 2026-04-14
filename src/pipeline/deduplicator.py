"""
6-level deduplication chain for suppliers.

Chain order (most reliable → fuzzy):
1. source + source_id (exact match within source)
2. INN (tax ID — unique per legal entity)
3. Phone (normalized +7XXXXXXXXXX)
4. Email
5. Domain (website domain)
6. Fuzzy name match (normalized name + city)
"""
import logging
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import func

from src.models.supplier import ParsedSupplier as Supplier
from src.pipeline.normalizer import (
    normalize_phone, normalize_email, normalize_website,
    normalize_name, extract_domain,
)

logger = logging.getLogger(__name__)


class Deduplicator:
    """Finds duplicate suppliers using a 6-level chain"""

    def __init__(self, db: Session):
        self.db = db
        self._stats = {"source_id": 0, "inn": 0, "phone": 0, "email": 0, "domain": 0, "fuzzy": 0}

    def find_duplicate(
        self,
        name: Optional[str] = None,
        inn: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        website: Optional[str] = None,
        city: Optional[str] = None,
        source: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> Optional[Supplier]:
        """
        Find existing supplier matching any of the given identifiers.
        Returns the first match found in the chain.
        """
        # Normalize inputs
        phone = normalize_phone(phone)
        email = normalize_email(email)
        website = normalize_website(website)
        name = normalize_name(name)

        # Level 1: source + source_id
        if source and source_id:
            existing = (
                self.db.query(Supplier)
                .filter(Supplier.source == source, Supplier.source_id == source_id)
                .first()
            )
            if existing:
                self._stats["source_id"] += 1
                return existing

        # Level 2: INN
        if inn:
            existing = self.db.query(Supplier).filter(Supplier.inn == inn).first()
            if existing:
                self._stats["inn"] += 1
                return existing

        # Level 3: Phone
        if phone:
            existing = self.db.query(Supplier).filter(Supplier.phone == phone).first()
            if existing:
                self._stats["phone"] += 1
                return existing

        # Level 4: Email
        if email:
            existing = self.db.query(Supplier).filter(Supplier.email == email).first()
            if existing:
                self._stats["email"] += 1
                return existing

        # Level 5: Domain
        if website:
            domain = extract_domain(website)
            if domain:
                existing = (
                    self.db.query(Supplier)
                    .filter(Supplier.website.ilike(f"%{domain}%"))
                    .first()
                )
                if existing:
                    self._stats["domain"] += 1
                    return existing

        # Level 6: Fuzzy name + city match
        if name and city:
            normalized = name.lower().strip()
            existing = (
                self.db.query(Supplier)
                .filter(
                    func.lower(Supplier.name) == normalized,
                    func.lower(Supplier.city) == city.lower().strip(),
                )
                .first()
            )
            if existing:
                self._stats["fuzzy"] += 1
                return existing

        return None

    def mark_duplicate(self, duplicate: Supplier, original: Supplier):
        """Mark a supplier as duplicate of another"""
        duplicate.is_duplicate = True
        duplicate.duplicate_of = original.id
        self.db.flush()

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    def log_stats(self):
        total = sum(self._stats.values())
        if total > 0:
            logger.info(
                f"Dedup stats: {total} duplicates found — "
                f"source_id={self._stats['source_id']}, "
                f"inn={self._stats['inn']}, "
                f"phone={self._stats['phone']}, "
                f"email={self._stats['email']}, "
                f"domain={self._stats['domain']}, "
                f"fuzzy={self._stats['fuzzy']}"
            )

"""Deduplicate and rank search results by quality/confidence."""
import logging

from src.pipeline.normalizer import normalize_email, normalize_phone, extract_domain
from .models import ExtractedContact, SearchResultItem

logger = logging.getLogger(__name__)

# Scoring weights
SCORE_EMAIL = 50
SCORE_PRICE = 40
SCORE_IN_STOCK = 30
SCORE_INDUSTRY_SITE = 20
SCORE_PHONE = 15
PENALTY_FORUM = -30
PENALTY_NO_CONTACTS = -50

FORUM_INDICATORS = {
    "forum", "board", "discuss", "community", "reddit",
    "stackexchange", "quora", "otvet", "answers",
}

INDUSTRY_INDICATORS = {
    "parts", "spare", "запчаст", "деталь", "catalog",
    "каталог", "shop", "store", "магазин", "склад",
    "warehouse", "dealer", "дилер", "поставщик", "supplier",
}


class Ranker:
    def rank(self, contacts: list[ExtractedContact]) -> list[SearchResultItem]:
        """Deduplicate and score extracted contacts. Returns sorted list."""
        seen_domains: set[str] = set()
        seen_emails: set[str] = set()
        seen_phones: set[str] = set()
        unique: list[SearchResultItem] = []

        for c in contacts:
            domain = extract_domain(c.website or c.source_url) or ""
            primary_email = normalize_email(c.emails[0]) if c.emails else ""
            primary_phone = normalize_phone(c.phones[0]) if c.phones else ""

            # Check duplicates
            is_dup = False
            if domain and domain in seen_domains:
                is_dup = True
            if primary_email and primary_email in seen_emails:
                is_dup = True
            if primary_phone and primary_phone in seen_phones:
                is_dup = True

            if is_dup:
                continue

            if domain:
                seen_domains.add(domain)
            if primary_email:
                seen_emails.add(primary_email)
            if primary_phone:
                seen_phones.add(primary_phone)

            score = self._score(c)
            confidence = max(0.0, min(1.0, score / 100.0))

            unique.append(SearchResultItem(
                company_name=c.company_name,
                email=primary_email,
                phone=primary_phone,
                website=c.website or domain,
                city=c.city,
                country=c.country,
                price=c.price,
                currency=c.currency,
                in_stock=c.in_stock,
                part_number=c.part_number,
                source_url=c.source_url,
                confidence=confidence,
            ))

        unique.sort(key=lambda x: x.confidence, reverse=True)
        logger.info(f"Ranked {len(unique)} unique results from {len(contacts)} contacts")
        return unique

    def _score(self, contact: ExtractedContact) -> float:
        """Calculate raw score for a contact."""
        score = 0.0

        if contact.emails:
            score += SCORE_EMAIL
        if contact.phones:
            score += SCORE_PHONE
        if contact.price is not None:
            score += SCORE_PRICE
        if contact.in_stock is True:
            score += SCORE_IN_STOCK

        # Check domain for industry relevance
        domain = extract_domain(contact.source_url) or ""
        url_lower = (contact.source_url + " " + domain).lower()

        if any(ind in url_lower for ind in FORUM_INDICATORS):
            score += PENALTY_FORUM
        elif any(ind in url_lower for ind in INDUSTRY_INDICATORS):
            score += SCORE_INDUSTRY_SITE

        if not contact.emails and not contact.phones:
            score += PENALTY_NO_CONTACTS

        return score

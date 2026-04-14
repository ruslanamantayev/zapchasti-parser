"""Extract contacts (email, phone, price) from HTML content."""
import logging
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from src.pipeline.normalizer import normalize_phone, normalize_email
from src.utils.llm import LLMClient
from .models import SitePage, ExtractedContact

logger = logging.getLogger(__name__)

# Email regex — standard pattern
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# Prefixes to skip (not useful for outreach)
SKIP_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "support", "newsletter",
    "unsubscribe", "mailer-daemon", "postmaster", "webmaster",
    "abuse", "spam", "bounce",
}

# Phone regex — matches international formats
PHONE_RE = re.compile(
    r"(?:\+?\d{1,3}[\s.\-]?)?"
    r"(?:\(?\d{2,5}\)?[\s.\-]?)?"
    r"\d{2,4}[\s.\-]?\d{2,4}[\s.\-]?\d{2,4}"
)

# Price patterns
PRICE_RE = re.compile(
    r"(?:(?:USD|EUR|RUB|KZT|₽|\$|€|руб\.?|тенге|тг\.?)\s*)"
    r"([\d][\d\s.,]*[\d])"
    r"|([\d][\d\s.,]*[\d])\s*"
    r"(?:USD|EUR|RUB|KZT|₽|\$|€|руб\.?|тенге|тг\.?)",
    re.IGNORECASE,
)

CURRENCY_MAP = {
    "$": "USD", "usd": "USD",
    "€": "EUR", "eur": "EUR",
    "₽": "RUB", "руб": "RUB", "rub": "RUB",
    "тенге": "KZT", "тг": "KZT", "kzt": "KZT",
}

LLM_SYSTEM = """You are extracting supplier contact information from a webpage.
Return JSON with these fields:
{
  "company_name": "string",
  "emails": ["list of business email addresses"],
  "phones": ["list of phone numbers"],
  "city": "string",
  "country": "string",
  "price": null or number,
  "currency": "USD/EUR/RUB/KZT or empty",
  "in_stock": null or true/false,
  "part_number": "string"
}
Rules:
- Only include real business contacts, not generic/noreply addresses
- Phones in original format (we normalize later)
- price as a number without currency symbol
- If information is not found, use empty string or null"""


class ContactExtractor:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    async def extract(self, pages: list[SitePage], article: str = "") -> ExtractedContact:
        """Extract contacts from one or more pages of the same site."""
        all_emails: set[str] = set()
        all_phones: set[str] = set()
        company_name = ""
        price = None
        currency = ""
        in_stock = None
        source_url = pages[0].url if pages else ""

        for page in pages:
            soup = BeautifulSoup(page.html, "lxml")
            text = soup.get_text(separator=" ", strip=True)

            # Company name from <title>
            if not company_name:
                title_tag = soup.find("title")
                if title_tag:
                    company_name = self._clean_company_name(title_tag.get_text(strip=True))

            # Emails
            for email in EMAIL_RE.findall(text):
                email_lower = email.lower()
                prefix = email_lower.split("@")[0]
                if prefix not in SKIP_EMAIL_PREFIXES:
                    normalized = normalize_email(email_lower)
                    if normalized:
                        all_emails.add(normalized)

            # Also check href="mailto:..." links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("mailto:"):
                    email = href[7:].split("?")[0].strip().lower()
                    prefix = email.split("@")[0]
                    if prefix not in SKIP_EMAIL_PREFIXES:
                        normalized = normalize_email(email)
                        if normalized:
                            all_emails.add(normalized)

            # Phones
            for match in PHONE_RE.finditer(text):
                phone_raw = match.group()
                # Filter out unlikely phone numbers (too short or just numbers in text)
                digits = re.sub(r"\D", "", phone_raw)
                if len(digits) < 10 or len(digits) > 15:
                    continue
                normalized = normalize_phone(phone_raw)
                if normalized:
                    all_phones.add(normalized)

            # Also check href="tel:..." links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("tel:"):
                    phone_raw = href[4:].strip()
                    normalized = normalize_phone(phone_raw)
                    if normalized:
                        all_phones.add(normalized)

            # Price — only if article mentioned on page
            if article and article.lower() in text.lower() and price is None:
                price, currency = self._extract_price(text)
                # In stock detection
                if re.search(r"в\s*наличии|in\s*stock|есть на складе|available", text, re.IGNORECASE):
                    in_stock = True
                elif re.search(r"под\s*заказ|out\s*of\s*stock|нет в наличии|unavailable", text, re.IGNORECASE):
                    in_stock = False

        # LLM fallback: if regex found nothing useful
        if not all_emails and not all_phones:
            llm_result = await self._llm_extract(pages, article)
            if llm_result:
                return llm_result

        return ExtractedContact(
            source_url=source_url,
            company_name=company_name,
            emails=sorted(all_emails),
            phones=sorted(all_phones),
            website=self._extract_domain(source_url),
            price=price,
            currency=currency,
            in_stock=in_stock,
            part_number=article,
        )

    async def _llm_extract(self, pages: list[SitePage], article: str) -> ExtractedContact | None:
        """LLM fallback when regex finds nothing."""
        try:
            combined = ""
            for page in pages:
                soup = BeautifulSoup(page.html, "lxml")
                combined += soup.get_text(separator=" ", strip=True) + "\n\n"
            combined = combined[:5000]

            prompt = f"Part article being searched: {article}\n\nWebpage text:\n{combined}"
            data = await self.llm.complete_json(prompt, system=LLM_SYSTEM)

            phones = []
            for p in data.get("phones", []):
                normalized = normalize_phone(p)
                if normalized:
                    phones.append(normalized)

            emails = []
            for e in data.get("emails", []):
                normalized = normalize_email(e)
                if normalized:
                    emails.append(normalized)

            return ExtractedContact(
                source_url=pages[0].url if pages else "",
                company_name=data.get("company_name", ""),
                emails=emails,
                phones=phones,
                website=self._extract_domain(pages[0].url) if pages else "",
                price=data.get("price"),
                currency=data.get("currency", ""),
                in_stock=data.get("in_stock"),
                part_number=data.get("part_number", article),
            )
        except Exception as e:
            logger.debug(f"LLM extraction failed: {e}")
            return None

    def _extract_price(self, text: str) -> tuple[float | None, str]:
        """Extract first price and currency from text."""
        match = PRICE_RE.search(text)
        if not match:
            return None, ""

        # Find currency near the match
        context = text[max(0, match.start() - 15):match.end() + 15].lower()
        currency = ""
        for key, val in CURRENCY_MAP.items():
            if key in context:
                currency = val
                break

        # Parse number
        num_str = match.group(1) or match.group(2)
        if not num_str:
            return None, ""
        num_str = num_str.replace(" ", "").replace(",", ".")
        # Handle cases like "1.234.56" -> "1234.56"
        parts = num_str.split(".")
        if len(parts) > 2:
            num_str = "".join(parts[:-1]) + "." + parts[-1]

        try:
            return float(num_str), currency
        except ValueError:
            return None, ""

    def _clean_company_name(self, title: str) -> str:
        """Extract company name from page title."""
        for sep in [" - ", " | ", " — ", " :: ", " >> ", " « ", " » "]:
            if sep in title:
                title = title.split(sep)[0]
        return title.strip()[:200]

    def _extract_domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.replace("www.", "")
        except Exception:
            return ""

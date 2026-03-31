"""
Data normalization: phones, emails, company names, websites.
Ported from zapchasti-platform/backend/app/domains/suppliers/normalizer.py
"""
import re


def normalize_phone(raw: str | None) -> str | None:
    """
    Normalize phone: "+7 (999) 123-45-67" -> "+79991234567"
    """
    if not raw:
        return None

    digits = re.sub(r"[^\d+]", "", raw)

    if digits.startswith("+"):
        digits_only = digits[1:]
    else:
        digits_only = digits

    if digits_only.startswith("8") and len(digits_only) == 11:
        digits_only = "7" + digits_only[1:]

    if digits_only.startswith("7") and len(digits_only) == 11:
        return "+" + digits_only

    if len(digits_only) == 10:
        return "+7" + digits_only

    return raw.strip() if raw else None


def normalize_email(raw: str | None) -> str | None:
    """Lowercase, trim"""
    if not raw:
        return None
    return raw.strip().lower()


def normalize_website(raw: str | None) -> str | None:
    """
    Strip protocol, www, trailing slash: "https://www.company.ru/" -> "company.ru"
    """
    if not raw:
        return None

    url = raw.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.rstrip("/")

    return url if url else None


def normalize_name(raw: str | None) -> str | None:
    """Trim, collapse spaces"""
    if not raw:
        return None
    return re.sub(r"\s+", " ", raw.strip())


def extract_domain(website: str | None) -> str | None:
    """Extract domain from URL for deduplication"""
    normalized = normalize_website(website)
    if not normalized:
        return None
    return normalized.split("/")[0]


# Transliteration map (Cyrillic -> Latin, compatible with WordPress Cyr-to-Lat)
TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'j', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'x', 'ц': 'c', 'ч': 'ch', 'ш': 'sh', 'щ': 'shh',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
}


def slugify(text: str) -> str:
    """Generate slug from text with Cyrillic transliteration"""
    if not text:
        return ""

    slug = ''.join(
        c for c in text
        if c.isalnum() or c.isspace() or c == '-' or ord(c) < 128 or '\u0400' <= c <= '\u04FF'
    )

    slug = slug.lower()

    for ru, en in TRANSLIT_MAP.items():
        slug = slug.replace(ru, en)

    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    slug = slug.strip('-')[:200]

    return slug

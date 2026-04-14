"""Download supplier website pages and find contact pages."""
import logging
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from src.config.settings import settings
from src.utils.http import HttpClient
from .models import SitePage

logger = logging.getLogger(__name__)

CONTACT_PAGE_RE = re.compile(
    r"(contact|kontakt|about|о.?нас|контакт|связь|обратная|feedback|impressum)",
    re.IGNORECASE,
)


class SiteParser:
    def __init__(self):
        self.http = HttpClient(
            delay_min=settings.search_site_delay_min,
            delay_max=settings.search_site_delay_max,
            verify_ssl=False,  # many supplier sites have bad certs
        )

    async def load(self, url: str) -> list[SitePage]:
        """Load main page + contact page if found. Returns 1-2 SitePage objects."""
        pages = []

        try:
            html = await self.http.get_text(url)
            pages.append(SitePage(url=url, html=html, is_contact_page=False))

            # Try to find and load contact page
            contact_url = self._find_contact_link(html, url)
            if contact_url:
                try:
                    contact_html = await self.http.get_text(contact_url)
                    pages.append(SitePage(url=contact_url, html=contact_html, is_contact_page=True))
                except Exception as e:
                    logger.debug(f"Failed to load contact page {contact_url}: {e}")

        except Exception as e:
            logger.warning(f"Failed to load {url}: {e}")

        return pages

    def _find_contact_link(self, html: str, base_url: str) -> str | None:
        """Find a link to /contacts or /about page."""
        soup = BeautifulSoup(html, "lxml")
        base_domain = urlparse(base_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            href_lower = href.lower()

            if CONTACT_PAGE_RE.search(text) or CONTACT_PAGE_RE.search(href_lower):
                full_url = urljoin(base_url, href)
                # Only follow links on the same domain
                if urlparse(full_url).netloc == base_domain:
                    return full_url

        return None

    async def close(self):
        await self.http.close()

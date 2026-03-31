"""
HTTP client with anti-ban measures: User-Agent rotation, delays, retry on 429.
"""
import asyncio
import logging
import random
from typing import Optional

import httpx

from src.config.settings import settings

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
]


class HttpClient:
    """
    Async HTTP client with built-in rate limiting and retry logic.

    Usage:
        async with HttpClient() as client:
            html = await client.get_text("https://example.com")
            data = await client.get_json("https://api.example.com/items")
    """

    def __init__(
        self,
        delay_min: Optional[float] = None,
        delay_max: Optional[float] = None,
        max_retries: int = 3,
        timeout: float = 30.0,
        headers: Optional[dict] = None,
        verify_ssl: bool = True,
    ):
        self.delay_min = delay_min or settings.default_delay_min
        self.delay_max = delay_max or settings.default_delay_max
        self.max_retries = max_retries
        self.timeout = timeout
        self.extra_headers = headers or {}
        self.verify_ssl = verify_ssl
        self._client: Optional[httpx.AsyncClient] = None
        self._request_count = 0

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/json",
                    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                    **self.extra_headers,
                },
                follow_redirects=True,
                verify=self.verify_ssl,
            )
        return self._client

    async def _delay(self):
        """Random delay between requests"""
        delay = random.uniform(self.delay_min, self.delay_max)
        await asyncio.sleep(delay)

    def _rotate_ua(self, client: httpx.AsyncClient):
        """Rotate User-Agent for each request"""
        client.headers["User-Agent"] = random.choice(USER_AGENTS)

    async def get(self, url: str, params: Optional[dict] = None) -> httpx.Response:
        """
        GET request with delay, UA rotation, and retry on 429.
        """
        client = await self._get_client()

        for attempt in range(1, self.max_retries + 1):
            self._rotate_ua(client)
            await self._delay()

            try:
                response = await client.get(url, params=params)
                self._request_count += 1

                if response.status_code == 429:
                    wait = min(15 * attempt, 60)
                    logger.warning(f"429 rate limited, waiting {wait}s (attempt {attempt})")
                    await asyncio.sleep(wait)
                    continue

                return response

            except httpx.HTTPError as e:
                logger.warning(f"HTTP error on {url}: {e} (attempt {attempt})")
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(5 * attempt)

        raise httpx.HTTPError(f"Max retries ({self.max_retries}) exceeded for {url}")

    async def get_text(self, url: str, params: Optional[dict] = None) -> str:
        """GET and return response text"""
        response = await self.get(url, params=params)
        response.raise_for_status()
        return response.text

    async def get_json(self, url: str, params: Optional[dict] = None) -> dict:
        """GET and return parsed JSON"""
        response = await self.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    @property
    def request_count(self) -> int:
        return self._request_count

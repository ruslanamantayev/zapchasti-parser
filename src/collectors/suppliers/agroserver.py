"""
Agroserver.ru collector — agricultural equipment suppliers and parts.
Priority: P1, estimated 1-3K unique suppliers.

TODO: Implement parsing logic
- Company directory: /companies/
- Ads: /ads/parts/
- Extract: company name, phone, email, city, product categories
"""
import logging
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)


class AgroserverCollector(BaseCollector):
    source_name = "agroserver"

    async def collect_bulk(self, **kwargs) -> CollectorStats:
        logger.warning("Agroserver collector not yet implemented")
        return CollectorStats(collector_name="agroserver")

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */7 * *"

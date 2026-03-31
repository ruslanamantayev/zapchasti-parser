"""
Della.ru / ATI.su collector — freight and equipment companies.
Priority: P1, estimated 5-15K unique companies.

TODO: Implement parsing logic
- Company directory
- Vehicle/equipment parks
- Extract: company name, INN, phone, city, fleet info
"""
import logging
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)


class DellaCollector(BaseCollector):
    source_name = "della"

    async def collect_bulk(self, **kwargs) -> CollectorStats:
        logger.warning("Della collector not yet implemented")
        return CollectorStats(collector_name="della")

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */7 * *"

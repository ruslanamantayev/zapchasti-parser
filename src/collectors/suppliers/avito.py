"""
Avito.ru collector — suppliers from ads for parts/equipment.
Priority: P1, estimated 3-5K unique suppliers.

TODO: Implement parsing logic
- Search categories: "запчасти спецтехника", "запчасти экскаватор"
- Extract seller profiles: name, phone, city, rating
- Separate: parts sellers vs equipment sellers
- Rate limiting: strict, use proxies
"""
import logging
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)


class AvitoCollector(BaseCollector):
    source_name = "avito"

    async def collect_bulk(self, **kwargs) -> CollectorStats:
        logger.warning("Avito collector not yet implemented")
        return CollectorStats(collector_name="avito")

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */3 * *"

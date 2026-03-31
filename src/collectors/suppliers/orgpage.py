"""
OrgPage.ru collector — company directory.
Priority: P1, estimated 2-5K unique suppliers.

TODO: Implement parsing logic
- Search by queries: "запчасти спецтехника", "запчасти экскаватор", etc.
- Extract: company name, phone, email, website, city, address
- Deduplicate with existing suppliers
"""
import logging
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)


class OrgPageCollector(BaseCollector):
    source_name = "orgpage"

    async def collect_bulk(self, **kwargs) -> CollectorStats:
        logger.warning("OrgPage collector not yet implemented")
        return CollectorStats(collector_name="orgpage")

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */7 * *"

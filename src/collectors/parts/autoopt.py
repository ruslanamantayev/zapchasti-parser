"""
AutoOpt.ru collector — parts catalog for KAMAZ, BELAZ, MTZ.
Priority: P0, estimated ~60K parts.

TODO: Implement parsing logic
- Parse catalog structure (categories → subcategories → parts)
- Extract: article, name, brand, compatible models, price
- Save to parts_catalog table / CSV+JSONL
"""
import logging
from datetime import datetime
from typing import Optional

from src.collectors.base import BaseCollector, CollectorStats

logger = logging.getLogger(__name__)


class AutooptCollector(BaseCollector):
    source_name = "autoopt"

    async def collect_bulk(self, **kwargs) -> CollectorStats:
        logger.warning("AutoOpt collector not yet implemented")
        return CollectorStats(collector_name="autoopt")

    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        return await self.collect_bulk(**kwargs)

    def get_schedule(self) -> str:
        return "0 0 */14 * *"

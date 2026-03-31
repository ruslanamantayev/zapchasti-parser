"""
Base collector interface — all data source parsers inherit from this.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CollectorResult(BaseModel):
    """Result of a single collected item"""
    source: str
    source_id: Optional[str] = None
    entity_type: str  # supplier / part / equipment_owner / service_center / ...
    raw_data: dict
    parsed_at: datetime = None

    def model_post_init(self, __context):
        if self.parsed_at is None:
            self.parsed_at = datetime.utcnow()


class CollectorStats(BaseModel):
    """Aggregate stats for a collector run"""
    collector_name: str
    total_found: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    started_at: datetime = None
    finished_at: Optional[datetime] = None

    def model_post_init(self, __context):
        if self.started_at is None:
            self.started_at = datetime.utcnow()

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.

    Each collector must implement:
    - collect_bulk() — gather all historical data
    - collect_new(since) — gather new data since timestamp
    - get_schedule() — cron schedule for continuous mode
    """
    source_name: str = "unknown"

    @abstractmethod
    async def collect_bulk(self, **kwargs) -> CollectorStats:
        """Gather all historical data from this source"""
        ...

    @abstractmethod
    async def collect_new(self, since: datetime, **kwargs) -> CollectorStats:
        """Gather new data since the given timestamp"""
        ...

    def get_schedule(self) -> Optional[str]:
        """
        Cron schedule for continuous mode.
        Return None to disable continuous mode.
        Example: "0 */6 * * *" (every 6 hours)
        """
        return None

    async def close(self):
        """Cleanup resources (http clients, etc.)"""
        pass

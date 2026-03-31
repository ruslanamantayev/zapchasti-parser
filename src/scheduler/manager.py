"""
Scheduler manager: runs collectors on configured schedules using APScheduler.
"""
import asyncio
import importlib
import logging
from pathlib import Path
from typing import Optional

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

SOURCES_FILE = Path(__file__).parent.parent / "config" / "sources.yaml"


def load_sources_config() -> dict:
    """Load sources configuration from YAML"""
    if not SOURCES_FILE.exists():
        logger.warning(f"Sources config not found: {SOURCES_FILE}")
        return {}

    with open(SOURCES_FILE) as f:
        config = yaml.safe_load(f)

    return config.get("sources", {})


def import_collector_class(class_path: str):
    """Dynamically import a collector class from dotted path"""
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


async def run_collector_job(name: str, collector_class):
    """Job wrapper: instantiate and run a collector"""
    logger.info(f"[Scheduler] Starting: {name}")
    collector = collector_class()
    try:
        stats = await collector.collect_bulk()
        logger.info(
            f"[Scheduler] {name} done: "
            f"found={stats.total_found}, created={stats.created}, "
            f"updated={stats.updated}, errors={stats.errors}"
        )
    except Exception as e:
        logger.error(f"[Scheduler] {name} failed: {e}")
    finally:
        await collector.close()


class SchedulerManager:
    """Manages scheduled collector jobs"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._sources = load_sources_config()

    def setup(self):
        """Register all enabled sources as scheduled jobs"""
        for name, config in self._sources.items():
            if not config.get("enabled", False):
                logger.info(f"[Scheduler] Skipping disabled source: {name}")
                continue

            schedule = config.get("schedule")
            collector_path = config.get("collector")

            if not schedule or not collector_path:
                logger.warning(f"[Scheduler] Invalid config for {name}")
                continue

            try:
                collector_class = import_collector_class(collector_path)
            except (ImportError, AttributeError) as e:
                logger.error(f"[Scheduler] Cannot import {collector_path}: {e}")
                continue

            # Parse cron schedule
            parts = schedule.split()
            if len(parts) != 5:
                logger.error(f"[Scheduler] Invalid cron '{schedule}' for {name}")
                continue

            trigger = CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4],
            )

            self.scheduler.add_job(
                run_collector_job,
                trigger=trigger,
                args=[name, collector_class],
                id=name,
                name=f"Collector: {name}",
                replace_existing=True,
            )

            desc = config.get("description", "")
            logger.info(f"[Scheduler] Registered: {name} ({schedule}) — {desc}")

    def start(self):
        """Start the scheduler"""
        self.setup()
        jobs = self.scheduler.get_jobs()
        logger.info(f"[Scheduler] Starting with {len(jobs)} jobs")
        self.scheduler.start()

    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()
        logger.info("[Scheduler] Stopped")

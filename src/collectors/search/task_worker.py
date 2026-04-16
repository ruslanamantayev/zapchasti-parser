"""
Polls crm.search_tasks for pending jobs and executes SearchEngine on them.

Designed to run as a background loop alongside the APScheduler collectors.
"""
import asyncio
import logging
from datetime import datetime

from sqlalchemy import text

from src.config.database import SessionLocal
from .engine import SearchEngine
from .models import SearchDepth, SearchRequest

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 15


class SearchTaskWorker:
    """Background loop that drains crm.search_tasks queue."""

    def __init__(self, poll_interval: int = POLL_INTERVAL_SECONDS):
        self.poll_interval = poll_interval
        self._stop_event: asyncio.Event | None = None

    async def run(self):
        """Main loop — picks up tasks until stopped."""
        self._stop_event = asyncio.Event()
        logger.info(f"[SearchTaskWorker] Started (poll every {self.poll_interval}s)")

        while not self._stop_event.is_set():
            try:
                await self._process_next()
            except Exception as e:
                logger.exception(f"[SearchTaskWorker] Unhandled error: {e}")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.poll_interval,
                )
            except asyncio.TimeoutError:
                pass

        logger.info("[SearchTaskWorker] Stopped")

    def stop(self):
        if self._stop_event:
            self._stop_event.set()

    async def _process_next(self):
        """Claim one pending task (atomic) and execute it."""
        db = SessionLocal()
        try:
            # Atomically claim oldest pending task
            claimed = db.execute(
                text("""
                    UPDATE crm.search_tasks
                    SET status = 'running', started_at = NOW()
                    WHERE id = (
                        SELECT id FROM crm.search_tasks
                        WHERE status = 'pending'
                        ORDER BY created_at
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    RETURNING id, position_id, depth
                """)
            ).fetchone()
            db.commit()

            if not claimed:
                return  # nothing to do

            task_id = claimed.id
            position_id = claimed.position_id
            depth_str = claimed.depth

            # Load position context for the search query
            position = db.execute(
                text("""
                    SELECT id, name, name_en, article,
                           equipment_brand, equipment_model, equipment_type,
                           request_id
                    FROM crm.positions WHERE id = :pid
                """),
                {"pid": position_id},
            ).fetchone()

            if not position:
                self._fail(db, task_id, "Position not found")
                return

            if not position.article:
                self._fail(db, task_id, "Position has no article number")
                return

            logger.info(
                f"[SearchTaskWorker] Processing task {task_id} "
                f"(position {position_id}, depth={depth_str}, article={position.article})"
            )

            try:
                depth = SearchDepth(depth_str)
            except ValueError:
                depth = SearchDepth.QUICK

            search_request = SearchRequest(
                article=position.article,
                name=position.name or "",
                name_en=position.name_en or "",
                equipment_brand=position.equipment_brand or "",
                equipment_model=position.equipment_model or "",
                equipment_type=position.equipment_type or "",
                depth=depth,
                position_id=position_id,
                request_id=position.request_id,
                task_id=task_id,
            )

            db.close()
            db = None

            engine = SearchEngine()
            try:
                stats = await engine.search(search_request)
            finally:
                await engine.close()

            # Persist outcome
            db2 = SessionLocal()
            try:
                db2.execute(
                    text("""
                        UPDATE crm.search_tasks
                        SET status = 'completed',
                            completed_at = NOW(),
                            queries_generated = :q,
                            sites_parsed = :s,
                            results_count = :r
                        WHERE id = :tid
                    """),
                    {
                        "tid": task_id,
                        "q": stats.queries_generated,
                        "s": stats.sites_parsed,
                        "r": stats.results_linked or stats.results_after_dedup,
                    },
                )
                db2.commit()
            finally:
                db2.close()

            logger.info(
                f"[SearchTaskWorker] Task {task_id} completed: "
                f"queries={stats.queries_generated}, sites={stats.sites_parsed}, "
                f"linked={stats.results_linked}"
            )

        except Exception as e:
            logger.exception(f"[SearchTaskWorker] Failed task: {e}")
            if db is not None:
                try:
                    # Mark running tasks as failed
                    self._fail(db, claimed.id if claimed else None, str(e)[:500])
                except Exception:
                    db.rollback()
        finally:
            if db is not None:
                db.close()

    def _fail(self, db, task_id, error: str):
        if not task_id:
            return
        db.execute(
            text("""
                UPDATE crm.search_tasks
                SET status = 'failed', completed_at = NOW(), error = :err
                WHERE id = :tid
            """),
            {"tid": task_id, "err": error},
        )
        db.commit()
        logger.warning(f"[SearchTaskWorker] Task {task_id} failed: {error}")

#!/usr/bin/env python3
"""
CLI: Run continuous monitoring mode (scheduler).
Runs all enabled collectors on their configured schedules.

Usage:
    python scripts/run_continuous.py
"""
import asyncio
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging import setup_logging
from src.scheduler.manager import SchedulerManager
from src.collectors.search.task_worker import SearchTaskWorker


async def run():
    setup_logging()

    manager = SchedulerManager()
    manager.start()

    worker = SearchTaskWorker()
    worker_task = asyncio.create_task(worker.run())

    print("Continuous mode started. Scheduler + SearchTaskWorker running...")

    # Wait forever, handle shutdown via signal
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    print("Shutting down...")
    worker.stop()
    try:
        await asyncio.wait_for(worker_task, timeout=5)
    except asyncio.TimeoutError:
        worker_task.cancel()
    manager.stop()


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

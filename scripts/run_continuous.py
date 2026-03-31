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


def main():
    setup_logging()

    manager = SchedulerManager()
    manager.start()

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()

    def shutdown(sig):
        print(f"\nReceived {sig.name}, shutting down...")
        manager.stop()
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown, sig)

    print("Continuous mode started. Press Ctrl+C to stop.")
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main()

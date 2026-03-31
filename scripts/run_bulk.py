#!/usr/bin/env python3
"""
CLI: Run a collector in bulk mode (historical data gathering)

Usage:
    python scripts/run_bulk.py dgis_suppliers
    python scripts/run_bulk.py hespareparts
    python scripts/run_bulk.py exkavator_companies
    python scripts/run_bulk.py --list
"""
import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging import setup_logging
from src.config.database import engine


# Registry of available collectors
COLLECTORS = {}


def register_collector(name: str, collector_class):
    """Register a collector for CLI access"""
    COLLECTORS[name] = collector_class


def check_db():
    """Verify database connection"""
    try:
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        print("[OK] Database connection successful")
        return True
    except Exception as e:
        print(f"[FAIL] Database connection failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="zapchasti-parser: bulk data collection",
    )
    parser.add_argument(
        "collector",
        nargs="?",
        help="Collector name (e.g. dgis_suppliers, hespareparts)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available collectors",
    )
    parser.add_argument(
        "--check-db",
        action="store_true",
        help="Check database connection",
    )

    args = parser.parse_args()
    setup_logging()

    if args.check_db or (not args.collector and not args.list):
        check_db()
        if not args.collector:
            return

    if args.list:
        if COLLECTORS:
            print("Available collectors:")
            for name in sorted(COLLECTORS):
                print(f"  - {name}")
        else:
            print("No collectors registered yet. Migrate parsers first (iterations 3-5).")
        return

    if args.collector:
        if args.collector not in COLLECTORS:
            print(f"Unknown collector: {args.collector}")
            print(f"Available: {', '.join(sorted(COLLECTORS)) or 'none yet'}")
            sys.exit(1)

        # Will be implemented as collectors are migrated
        collector_cls = COLLECTORS[args.collector]
        print(f"Running bulk collection: {args.collector}")


if __name__ == "__main__":
    main()

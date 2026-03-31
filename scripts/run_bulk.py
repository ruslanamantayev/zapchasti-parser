#!/usr/bin/env python3
"""
CLI: Run a collector in bulk mode (historical data gathering)

Usage:
    python scripts/run_bulk.py dgis_suppliers
    python scripts/run_bulk.py dgis_suppliers --city "Москва"
    python scripts/run_bulk.py dgis_suppliers --all
    python scripts/run_bulk.py --list
"""
import argparse
import asyncio
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging import setup_logging
from src.config.database import engine

# === Collector Registry ===
from src.collectors.suppliers.dgis import DgisSupplierCollector
from src.collectors.parts.hespareparts import HesparepartsCollector

COLLECTORS = {
    "dgis_suppliers": DgisSupplierCollector,
    "hespareparts": HesparepartsCollector,
}


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


async def run_collector(name: str, args):
    """Run a collector with CLI arguments"""
    collector_cls = COLLECTORS[name]
    collector = collector_cls()

    kwargs = {}
    if hasattr(args, "city") and args.city:
        kwargs["city"] = args.city
    if hasattr(args, "cities") and args.cities:
        kwargs["cities"] = [c.strip() for c in args.cities.split(",")]
    if hasattr(args, "all_cities") and args.all_cities:
        pass  # default behavior — all cities

    try:
        stats = await collector.collect_bulk(**kwargs)
        print(f"\n{'='*50}")
        print(f"Collector: {stats.collector_name}")
        print(f"Found: {stats.total_found}")
        print(f"Created: {stats.created}")
        print(f"Updated: {stats.updated}")
        print(f"Skipped: {stats.skipped}")
        print(f"Errors: {stats.errors}")
        if stats.duration_seconds:
            print(f"Duration: {stats.duration_seconds:.1f}s")
        print(f"{'='*50}")
    finally:
        await collector.close()


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
    parser.add_argument(
        "--city",
        help="Single city to parse (e.g. 'Москва')",
    )
    parser.add_argument(
        "--cities",
        help="Comma-separated cities (e.g. 'Москва,СПб,Казань')",
    )
    parser.add_argument(
        "--all",
        dest="all_cities",
        action="store_true",
        help="Parse all cities",
    )

    args = parser.parse_args()
    setup_logging()

    if args.check_db or (not args.collector and not args.list):
        check_db()
        if not args.collector:
            return

    if args.list:
        print("Available collectors:")
        for name in sorted(COLLECTORS):
            print(f"  - {name}")
        return

    if args.collector:
        if args.collector not in COLLECTORS:
            print(f"Unknown collector: {args.collector}")
            print(f"Available: {', '.join(sorted(COLLECTORS))}")
            sys.exit(1)

        asyncio.run(run_collector(args.collector, args))


if __name__ == "__main__":
    main()

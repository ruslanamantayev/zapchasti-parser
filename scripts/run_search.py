#!/usr/bin/env python3
"""
CLI: Search for suppliers by article number.

Usage:
    # Dry run — only generate queries, no Google search
    uv run python scripts/run_search.py "NOV55-1165" \\
        --name "Brake shoes drawworks drum" \\
        --equipment "IRI-100 drilling rig" \\
        --depth quick --dry-run

    # Real search (quick)
    uv run python scripts/run_search.py "BEN286934X" \\
        --name "Bendix AD-2 Air Dryer" \\
        --depth quick

    # Full search with CRM position link
    uv run python scripts/run_search.py "6754-21-3110" \\
        --name "Турбина" \\
        --brand "Komatsu" --model "PC200-8" \\
        --depth normal --position-id 42
"""
import argparse
import asyncio
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.search import SearchDepth, SearchEngine, SearchRequest
from src.utils.logging import setup_logging


async def run_search(args):
    engine = SearchEngine(dry_run=args.dry_run)

    # Parse --equipment "Brand Model" shortcut
    brand = args.brand or ""
    model = args.model or ""
    if args.equipment and not brand:
        parts = args.equipment.split(None, 1)
        brand = parts[0] if parts else ""
        model = parts[1] if len(parts) > 1 else ""

    request = SearchRequest(
        article=args.article,
        name=args.name or "",
        name_en=args.name_en or "",
        equipment_brand=brand,
        equipment_model=model,
        equipment_type=args.equipment_type or "",
        depth=SearchDepth(args.depth),
        position_id=args.position_id,
    )

    try:
        stats = await engine.search(request)

        print(f"\n{'=' * 60}")
        print(f"  Search results: {stats.article}")
        print(f"{'=' * 60}")
        print(f"  Queries generated:  {stats.queries_generated}")

        if not args.dry_run:
            print(f"  Google results:     {stats.google_results}")
            print(f"  Sites parsed:       {stats.sites_parsed}")
            print(f"  Contacts extracted: {stats.contacts_extracted}")
            print(f"  After dedup:        {stats.results_after_dedup}")
            print(f"  Suppliers created:  {stats.suppliers_created}")
            print(f"  Suppliers updated:  {stats.suppliers_updated}")
            print(f"  Offers created:     {stats.offers_created}")

        if stats.errors:
            print(f"  Errors:             {len(stats.errors)}")
            for err in stats.errors[:5]:
                print(f"    - {err[:100]}")

        print(f"{'=' * 60}")

    finally:
        await engine.close()


def main():
    parser = argparse.ArgumentParser(
        description="Search for spare parts suppliers by article number",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("article", help="Part article/number to search")
    parser.add_argument("--name", help="Part name (Russian)")
    parser.add_argument("--name-en", help="Part name (English)")
    parser.add_argument("--brand", help="Equipment brand (e.g. Komatsu, NOV)")
    parser.add_argument("--model", help="Equipment model (e.g. PC200-8, IRI-100)")
    parser.add_argument("--equipment", help="Equipment shortcut: 'Brand Model' (alternative to --brand + --model)")
    parser.add_argument("--equipment-type", help="Equipment type (e.g. drilling rig, excavator)")
    parser.add_argument(
        "--depth",
        choices=["quick", "normal", "deep"],
        default="normal",
        help="Search depth: quick (5 queries), normal (15), deep (25+)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only generate queries, don't search")
    parser.add_argument("--position-id", type=int, help="CRM position ID to link offers")

    args = parser.parse_args()
    setup_logging()
    asyncio.run(run_search(args))


if __name__ == "__main__":
    main()

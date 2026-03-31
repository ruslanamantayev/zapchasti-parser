#!/usr/bin/env python3
"""
CLI: Run continuous monitoring mode (scheduler)

Usage:
    python scripts/run_continuous.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging import setup_logging


def main():
    setup_logging()
    print("Continuous mode not yet implemented (iteration 7).")
    print("Use scripts/run_bulk.py for manual collection.")


if __name__ == "__main__":
    main()

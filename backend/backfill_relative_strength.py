"""
backfill_relative_strength.py — One-time backfill of relative_strength_daily.

Iterates over the last 60 trading days in prices_daily and computes RS for
each date using the existing price history. Run once after the schema is created.

Usage:
    python backend/backfill_relative_strength.py

Requires SUPABASE_DB_URL in .env or environment.
"""

import sys
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from db import get_engine
from compute_relative_strength import compute_rs_for_all_stocks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BACKFILL_DAYS = 60


def get_backfill_dates(engine) -> list:
    """Return the most recent BACKFILL_DAYS distinct trading dates from prices_daily."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT date
            FROM prices_daily
            ORDER BY date DESC
            LIMIT :n
        """), {"n": BACKFILL_DAYS}).fetchall()
    # Return in ascending order so older dates are processed first
    return sorted([r[0] for r in rows])


def run():
    logger.info("=== Relative Strength backfill started ===")
    engine = get_engine()

    dates = get_backfill_dates(engine)
    if not dates:
        logger.error("prices_daily is empty — nothing to backfill.")
        sys.exit(1)

    logger.info(f"  Backfilling {len(dates)} dates: {dates[0]} → {dates[-1]}")

    total_rows = 0
    for i, d in enumerate(dates, 1):
        logger.info(f"  [{i}/{len(dates)}] Backfilling RS for {d}...")
        rows_written = compute_rs_for_all_stocks(engine, d)
        total_rows += rows_written
        logger.info(f"    Done — {rows_written} stocks written")

    logger.info(f"=== Backfill complete: {total_rows:,} total rows across {len(dates)} dates ===")


if __name__ == "__main__":
    run()

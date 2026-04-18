"""
backfill_relative_strength.py — One-time backfill of relative_strength_daily.

Loads ALL price data in a single query, then computes RS for each of the last
60 trading days entirely in memory. Only DB round-trips are the final upserts,
avoiding Supabase connection timeouts.

Usage:
    python backend/backfill_relative_strength.py

Requires SUPABASE_DB_URL in .env or environment.
"""

import sys
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from db import get_engine
from compute_relative_strength import (
    fetch_all_close_prices_bulk,
    compute_rs_for_date_in_memory,
    _batch_upsert,
    _load_active_symbols,
    UPSERT_BATCH_SIZE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BACKFILL_DAYS = 60
# Extra calendar days before the earliest backfill date needed for 1Y return lookback
HISTORY_BUFFER_DAYS = 400


def get_backfill_dates(engine) -> list:
    """Return the most recent BACKFILL_DAYS distinct trading dates from prices_daily."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT date
            FROM prices_daily
            WHERE symbol != '^NSEI'
            ORDER BY date DESC
            LIMIT :n
        """), {"n": BACKFILL_DAYS}).fetchall()
    return sorted([r[0] for r in rows])


def run():
    logger.info("=== Relative Strength backfill started ===")
    engine = get_engine()

    dates = get_backfill_dates(engine)
    if not dates:
        logger.error("prices_daily is empty — nothing to backfill.")
        sys.exit(1)

    logger.info(f"  Backfilling {len(dates)} dates: {dates[0]} → {dates[-1]}")

    # ── Load ALL price data once ───────────────��──────────────────────────────
    # We need history going back HISTORY_BUFFER_DAYS before the earliest date
    # to compute the 1Y timeframe (252 trading days).
    earliest = dates[0]
    cutoff = earliest - timedelta(days=HISTORY_BUFFER_DAYS)
    logger.info(f"  Loading all prices since {cutoff} in one query...")
    prices_asc = fetch_all_close_prices_bulk(engine, cutoff)

    # ── Load active symbols once ──────────────────────────────────────────────
    active_symbols = _load_active_symbols(engine)
    logger.info(f"  {len(active_symbols)} active stocks to process")

    # ── Compute RS for each date in memory ──────────────────────────────���─────
    accumulated = []
    total_rows = 0

    for i, d in enumerate(dates, 1):
        rows = compute_rs_for_date_in_memory(prices_asc, active_symbols, d)
        accumulated.extend(rows)

        if not rows:
            logger.warning(f"  [{i}/{len(dates)}] {d} — skipped (no Nifty data)")
            continue

        logger.info(f"  [{i}/{len(dates)}] {d} — {len(rows)} stocks computed")
        total_rows += len(rows)

        # Upsert in batches to avoid building a huge in-memory list
        if len(accumulated) >= UPSERT_BATCH_SIZE * 10:
            _batch_upsert(accumulated)
            logger.info(f"    Flushed {len(accumulated)} rows to DB")
            accumulated = []

    # Final flush
    if accumulated:
        _batch_upsert(accumulated)
        logger.info(f"  Final flush: {len(accumulated)} rows")

    logger.info(f"=== Backfill complete: {total_rows:,} total rows across {len(dates)} dates ===")


if __name__ == "__main__":
    run()

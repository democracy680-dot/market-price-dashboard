"""
backfill_setup_candidates.py — One-time backfill of setup_candidates_daily.

Runs compute_setups_for_all_stocks for each of the last N trading days.
Run once after deploying schema_breakout_reversal.sql.

Usage:
    python backend/backfill_setup_candidates.py          # last 30 calendar days
    python backend/backfill_setup_candidates.py 14       # last 14 calendar days
"""

import sys
import logging
from datetime import date, timedelta

from compute_setup_candidates import compute_setups_for_all_stocks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 30

    end_date   = date.today()
    start_date = end_date - timedelta(days=days_back)

    # Walk forward day by day (skip weekends)
    current = start_date
    processed = 0
    while current <= end_date:
        if current.weekday() < 5:   # Mon-Fri only
            logger.info(f"━━━ Backfilling {current} ━━━")
            counts = compute_setups_for_all_stocks(current)
            total  = sum(counts.values())
            logger.info(f"  Done — {total} candidates across {len(counts)} patterns")
            processed += 1
        current += timedelta(days=1)

    logger.info(f"Backfill complete — processed {processed} trading days")


if __name__ == "__main__":
    main()

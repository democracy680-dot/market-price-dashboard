"""
backfill_minervini_template.py — Populate minervini_template_daily for past N days.

RS Rank is recomputed across the full universe for each historical date,
so accuracy matches what the daily refresh would have produced on that day.

Usage:
    python backend/backfill_minervini_template.py            # last 30 days
    python backend/backfill_minervini_template.py --days 60  # last 60 days
    python backend/backfill_minervini_template.py --from 2025-01-01  # from specific date
"""

import sys
import logging
import argparse
from datetime import date, timedelta

sys.path.insert(0, __import__("os").path.dirname(__file__))

from compute_minervini_template import compute_minervini_for_all_stocks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _trading_days_between(start: date, end: date) -> list[date]:
    """Return weekdays (Mon-Fri) between start and end inclusive."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=Mon, 4=Fri
            days.append(current)
        current += timedelta(days=1)
    return days


def run_backfill(start_date: date, end_date: date):
    days = _trading_days_between(start_date, end_date)
    logger.info(f"Backfilling Minervini Template for {len(days)} trading days: {start_date} → {end_date}")

    total_pass = 0
    for i, d in enumerate(days, 1):
        logger.info(f"[{i}/{len(days)}] Processing {d} ...")
        try:
            pass_count = compute_minervini_for_all_stocks(d)
            total_pass += pass_count
        except Exception as e:
            logger.error(f"  Failed for {d}: {e}", exc_info=True)

    logger.info(f"Backfill complete. Total template-pass events: {total_pass}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Minervini Trend Template")
    parser.add_argument("--days", type=int, default=30, help="Number of past days to backfill (default: 30)")
    parser.add_argument("--from", dest="from_date", type=str, default=None,
                        help="Start date in YYYY-MM-DD format (overrides --days)")
    args = parser.parse_args()

    end_date = date.today()
    if args.from_date:
        start_date = date.fromisoformat(args.from_date)
    else:
        start_date = end_date - timedelta(days=args.days)

    run_backfill(start_date, end_date)

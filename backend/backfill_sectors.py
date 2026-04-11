"""
backfill_sectors.py — One-time script to populate the sector column in stocks.

Fetches sector info from yfinance for all active stocks that have no sector set,
then writes it back to the stocks table.

Usage:
    python backend/backfill_sectors.py

Run from the repo root. Safe to re-run.
"""

import sys
import logging
import concurrent.futures

import yfinance as yf
from sqlalchemy import text

from db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

WORKERS = 10


def fetch_sector(yahoo_symbol: str, nse_symbol: str) -> dict:
    try:
        info = yf.Ticker(yahoo_symbol).info
        sector = info.get("sector")
        return {"symbol": nse_symbol, "sector": sector}
    except Exception as e:
        logger.debug(f"  {yahoo_symbol}: {e}")
        return {"symbol": nse_symbol, "sector": None}


def run():
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol, yahoo_symbol FROM stocks WHERE is_active = TRUE AND (sector IS NULL OR sector = '')")
        ).fetchall()

    if not rows:
        logger.info("All stocks already have sectors set. Nothing to do.")
        return

    logger.info(f"Fetching sectors for {len(rows)} stocks with missing sector...")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {
            pool.submit(fetch_sector, yahoo_sym, sym): sym
            for sym, yahoo_sym in rows
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            results.append(future.result())
            if i % 50 == 0:
                logger.info(f"  {i}/{len(rows)} done")

    filled = [r for r in results if r["sector"]]
    logger.info(f"Got sector for {len(filled)}/{len(rows)} stocks")

    if not filled:
        logger.warning("No sectors retrieved. Check yfinance connectivity.")
        return

    with engine.begin() as conn:
        for r in filled:
            conn.execute(
                text("UPDATE stocks SET sector = :sector WHERE symbol = :symbol"),
                {"symbol": r["symbol"], "sector": r["sector"]},
            )

    logger.info(f"Updated {len(filled)} stocks with sector data.")
    logger.info("Done. Re-run daily_refresh.py to recompute sector_performance_daily.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(f"Fatal: {e}")
        sys.exit(1)

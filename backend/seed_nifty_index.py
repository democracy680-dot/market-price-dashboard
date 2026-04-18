"""
seed_nifty_index.py — One-time script to add ^NSEI (Nifty 50) to the database.

What it does:
  1. Inserts ^NSEI into the stocks table (if not already present).
  2. Fetches 2 years of daily close prices from yfinance.
  3. Upserts into prices_daily so compute_relative_strength.py has a baseline.
  4. Runs the RS backfill for the last 60 days.

Run once from the project root:
    python backend/seed_nifty_index.py
"""

import sys
import logging
from datetime import date

import yfinance as yf
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

sys.path.insert(0, "backend")
from db import get_engine, get_psycopg2_conn
from backfill_relative_strength import run as run_backfill

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NIFTY_SYMBOL      = "^NSEI"
NIFTY_YAHOO       = "^NSEI"
NIFTY_NAME        = "Nifty 50 Index"


def seed_stocks_row(engine):
    """Insert ^NSEI into stocks table if it isn't there already."""
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT symbol FROM stocks WHERE symbol = :s"),
            {"s": NIFTY_SYMBOL},
        ).fetchone()
        if existing:
            logger.info(f"  {NIFTY_SYMBOL} already in stocks table — skipping insert")
            return
        conn.execute(text("""
            INSERT INTO stocks (symbol, name, yahoo_symbol, is_active)
            VALUES (:symbol, :name, :yahoo_symbol, TRUE)
        """), {
            "symbol":       NIFTY_SYMBOL,
            "name":         NIFTY_NAME,
            "yahoo_symbol": NIFTY_YAHOO,
        })
        logger.info(f"  Inserted {NIFTY_SYMBOL} into stocks table")


def fetch_and_seed_prices(engine):
    """Fetch 2 years of ^NSEI OHLCV from yfinance and upsert into prices_daily."""
    logger.info(f"  Fetching {NIFTY_YAHOO} prices from yfinance...")
    raw = yf.download(
        tickers=NIFTY_YAHOO,
        period="2y",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    if raw.empty:
        logger.error("  yfinance returned no data for ^NSEI — aborting.")
        sys.exit(1)

    # Flatten MultiIndex if present
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    raw.columns = [c.lower() for c in raw.columns]
    if "adj close" in raw.columns:
        raw = raw.drop(columns=["adj close"])

    raw = raw.reset_index().rename(columns={"date": "date"})
    raw.columns = [c.lower() for c in raw.columns]
    raw["date"] = pd.to_datetime(raw["date"]).dt.date
    raw["symbol"] = NIFTY_SYMBOL
    raw = raw[["symbol", "date", "open", "high", "low", "close", "volume"]].dropna(subset=["close"])

    rows = list(raw.itertuples(index=False, name=None))
    logger.info(f"  {len(rows)} rows to upsert into prices_daily")

    sql = """
        INSERT INTO prices_daily (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
        logger.info(f"  Upserted {len(rows)} rows for {NIFTY_SYMBOL} into prices_daily")
    finally:
        conn.close()


def main():
    logger.info("=== Seeding Nifty 50 index data ===")
    engine = get_engine()

    # Step 1: Add to stocks
    seed_stocks_row(engine)

    # Step 2: Seed prices
    fetch_and_seed_prices(engine)

    # Step 3: Run RS backfill now that ^NSEI data exists
    logger.info("=== Running RS backfill ===")
    run_backfill()

    logger.info("=== Done. Nifty 50 seeded and RS backfill complete. ===")


if __name__ == "__main__":
    main()

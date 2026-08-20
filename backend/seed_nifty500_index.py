"""
seed_nifty500_index.py — One-time script to add ^CRSLDX (Nifty 500) to the DB.

  1. Inserts ^CRSLDX into the stocks table (if not present), is_active=TRUE
     so daily_refresh keeps it current.
  2. Fetches ~2 years of ^CRSLDX OHLCV from yfinance and upserts into prices_daily.

Usage:
    python backend/seed_nifty500_index.py
"""

import sys
import logging
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

NIFTY500_SYMBOL = "^CRSLDX"
NIFTY500_YAHOO  = "^CRSLDX"   # confirmed in Task 2 Step 1
NIFTY500_NAME   = "Nifty 500"


def ensure_stock_row(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stocks (symbol, name, yahoo_symbol, sector, is_active)
            VALUES (:sym, :name, :yah, 'Index', TRUE)
            ON CONFLICT (symbol) DO UPDATE SET is_active = TRUE, yahoo_symbol = EXCLUDED.yahoo_symbol
        """), {"sym": NIFTY500_SYMBOL, "name": NIFTY500_NAME, "yah": NIFTY500_YAHOO})
    logger.info("  ^CRSLDX present in stocks (is_active=TRUE)")


def fetch_and_upsert_prices(engine):
    start = date.today() - timedelta(days=730)
    df = yf.download(NIFTY500_YAHOO, start=start.isoformat(), auto_adjust=False, progress=False)
    if df is None or df.empty:
        logger.error("  yfinance returned no data for %s — aborting.", NIFTY500_YAHOO)
        sys.exit(1)
    df = df.reset_index()
    # Flatten possible MultiIndex columns from yfinance
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    rows = []
    for _, r in df.iterrows():
        d = pd.to_datetime(r["Date"]).date()
        rows.append((NIFTY500_SYMBOL, d,
                     float(r["Open"]), float(r["High"]), float(r["Low"]),
                     float(r["Close"]), int(r["Volume"]) if pd.notna(r["Volume"]) else 0))
    sql = """
        INSERT INTO prices_daily (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    logger.info("  Upserted %d ^CRSLDX price rows", len(rows))


def run():
    engine = get_engine()
    ensure_stock_row(engine)
    fetch_and_upsert_prices(engine)
    logger.info("=== Nifty 500 seed complete ===")


if __name__ == "__main__":
    run()

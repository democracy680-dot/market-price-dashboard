"""
backfill_new_stocks.py — Backfill historical price + snapshot data for stocks
that are in the stocks table but have no entries in prices_daily.

This is needed after seed_themes.py (or any script that adds new stocks) is run,
since daily_refresh.py won't retroactively fill missing history.

Usage:
    python backend/backfill_new_stocks.py

Run from the repo root. Safe to re-run — only processes stocks with zero price rows.
"""

import sys
import math
import logging

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn
from fetcher import fetch_prices, fetch_fundamentals
from compute import compute_snapshots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def find_stocks_missing_prices(engine) -> pd.DataFrame:
    """Return active stocks that have zero rows in prices_daily."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.symbol, s.yahoo_symbol, s.sector
            FROM stocks s
            WHERE s.is_active = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM prices_daily p WHERE p.symbol = s.symbol
              )
        """)).fetchall()
    return pd.DataFrame(rows, columns=["symbol", "yahoo_symbol", "sector"])


def upsert_prices(prices_df: pd.DataFrame, symbol_map: dict) -> int:
    if prices_df.empty:
        return 0

    df = prices_df.copy()
    df["symbol"] = df["yahoo_symbol"].map(symbol_map)
    df = df.dropna(subset=["symbol"])
    df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
    df = df.where(pd.notnull(df), None)
    rows = list(df.itertuples(index=False, name=None))

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
            execute_values(cur, sql, rows, page_size=2000)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _clean(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def upsert_snapshots(snapshots_df: pd.DataFrame) -> int:
    if snapshots_df.empty:
        return 0

    cols = [
        "symbol", "date", "cmp",
        "ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d",
        "dma_50", "dma_200", "status_50dma", "status_200dma",
        "market_cap_cr", "pe_ratio",
    ]
    for c in cols:
        if c not in snapshots_df.columns:
            snapshots_df[c] = None

    df = snapshots_df[cols]
    rows = [tuple(_clean(v) for v in row) for row in df.itertuples(index=False, name=None)]

    sql = """
        INSERT INTO snapshots_daily
            (symbol, date, cmp,
             ret_1d, ret_1w, ret_30d, ret_60d, ret_180d, ret_365d,
             dma_50, dma_200, status_50dma, status_200dma,
             market_cap_cr, pe_ratio)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            cmp           = EXCLUDED.cmp,
            ret_1d        = EXCLUDED.ret_1d,
            ret_1w        = EXCLUDED.ret_1w,
            ret_30d       = EXCLUDED.ret_30d,
            ret_60d       = EXCLUDED.ret_60d,
            ret_180d      = EXCLUDED.ret_180d,
            ret_365d      = EXCLUDED.ret_365d,
            dma_50        = EXCLUDED.dma_50,
            dma_200       = EXCLUDED.dma_200,
            status_50dma  = EXCLUDED.status_50dma,
            status_200dma = EXCLUDED.status_200dma,
            market_cap_cr = EXCLUDED.market_cap_cr,
            pe_ratio      = EXCLUDED.pe_ratio
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def run():
    engine = get_engine()

    # 1. Find stocks with no price history
    missing = find_stocks_missing_prices(engine)
    if missing.empty:
        logger.info("No stocks are missing price data. Nothing to backfill.")
        return

    logger.info(f"Found {len(missing)} stocks with no price history — starting backfill...")
    logger.info("Symbols: " + ", ".join(missing["symbol"].tolist()))

    yahoo_symbols = missing["yahoo_symbol"].dropna().tolist()
    symbol_map = dict(zip(missing["yahoo_symbol"], missing["symbol"]))

    # 2. Fetch 2-year history
    logger.info("Fetching 2-year OHLCV history from yfinance...")
    prices_df = fetch_prices(yahoo_symbols)

    if prices_df.empty:
        logger.error("yfinance returned no data. Aborting.")
        sys.exit(1)

    fetched = prices_df["yahoo_symbol"].nunique()
    logger.info(f"Got data for {fetched}/{len(yahoo_symbols)} symbols")

    # 3. Upsert prices_daily
    n_prices = upsert_prices(prices_df, symbol_map)
    logger.info(f"Upserted {n_prices} rows into prices_daily")

    # 4. Compute snapshots
    prices_nse = prices_df.copy()
    prices_nse["symbol"] = prices_nse["yahoo_symbol"].map(symbol_map)
    prices_nse = prices_nse.dropna(subset=["symbol"])

    snapshots = compute_snapshots(prices_nse[["symbol", "date", "close"]])
    logger.info(f"Computed {len(snapshots)} snapshot rows")

    # 5. Fetch fundamentals (market cap, PE)
    logger.info("Fetching fundamentals...")
    fundamentals = fetch_fundamentals(yahoo_symbols)
    fundamentals["symbol"] = fundamentals["yahoo_symbol"].map(symbol_map)
    fundamentals = fundamentals.dropna(subset=["symbol"])
    snapshots = snapshots.merge(
        fundamentals[["symbol", "market_cap_cr", "pe_ratio"]],
        on="symbol",
        how="left",
    )

    # 6. Upsert snapshots_daily
    n_snaps = upsert_snapshots(snapshots)
    logger.info(f"Upserted {n_snaps} rows into snapshots_daily")

    # 7. Update sector in stocks table where available
    sectors_to_update = fundamentals[fundamentals["sector"].notna()][["symbol", "sector"]]
    if not sectors_to_update.empty:
        with engine.begin() as conn:
            for _, row in sectors_to_update.iterrows():
                conn.execute(
                    text("UPDATE stocks SET sector = :sector WHERE symbol = :symbol AND (sector IS NULL OR sector != :sector)"),
                    {"symbol": row["symbol"], "sector": row["sector"]},
                )
        logger.info(f"Updated sector for {len(sectors_to_update)} stocks")

    logger.info("=== Backfill complete ===")


if __name__ == "__main__":
    run()

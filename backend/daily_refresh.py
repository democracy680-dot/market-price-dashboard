"""
daily_refresh.py — The main daily cron target.

What it does:
  1. Reads all active stocks from Supabase
  2. Fetches last 250 days of OHLCV from yfinance (in batches of 50)
  3. Upserts into prices_daily
  4. Computes snapshots (returns, DMAs) and upserts into snapshots_daily
  5. Writes sector aggregations into sector_performance_daily
  6. Logs the run result to refresh_log

Usage:
    python backend/daily_refresh.py

Run from the repo root. Requires SUPABASE_DB_URL in .env or environment.
"""

import sys
import uuid
import math
import logging
from datetime import datetime, timezone

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn
from fetcher import fetch_prices, fetch_fundamentals
from compute import compute_snapshots, compute_sector_performance
from compute_technicals import run_technical_refresh

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_active_stocks(conn) -> pd.DataFrame:
    result = conn.execute(
        text("SELECT symbol, yahoo_symbol, sector FROM stocks WHERE is_active = TRUE")
    )
    rows = result.fetchall()
    return pd.DataFrame(rows, columns=["symbol", "yahoo_symbol", "sector"])


def upsert_prices(prices_df: pd.DataFrame, symbol_map: dict):
    """
    Bulk-upsert rows into prices_daily using psycopg2 execute_values.
    Much faster than SQLAlchemy parametrized inserts for large datasets.
    """
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
    """Convert float NaN to None so psycopg2 sends NULL, not 'nan'."""
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def upsert_snapshots(snapshots_df: pd.DataFrame):
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
            -- Never overwrite a good MCap/PE with NULL — keep existing if new is NULL
            market_cap_cr = COALESCE(EXCLUDED.market_cap_cr, snapshots_daily.market_cap_cr),
            pe_ratio      = COALESCE(EXCLUDED.pe_ratio,      snapshots_daily.pe_ratio)
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def upsert_sector_performance(sector_df: pd.DataFrame):
    if sector_df.empty:
        return 0

    cols = [
        "date", "sector", "num_companies", "advances", "declines",
        "day_change_pct", "week_chg_pct", "month_chg_pct",
        "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct",
    ]
    for c in cols:
        if c not in sector_df.columns:
            sector_df[c] = None

    df = sector_df[cols].where(pd.notnull(sector_df[cols]), None)
    rows = list(df.itertuples(index=False, name=None))

    sql = """
        INSERT INTO sector_performance_daily
            (date, sector, num_companies, advances, declines,
             day_change_pct, week_chg_pct, month_chg_pct,
             qtr_chg_pct, half_yr_chg_pct, year_chg_pct)
        VALUES %s
        ON CONFLICT (date, sector) DO UPDATE SET
            num_companies   = EXCLUDED.num_companies,
            advances        = EXCLUDED.advances,
            declines        = EXCLUDED.declines,
            day_change_pct  = EXCLUDED.day_change_pct,
            week_chg_pct    = EXCLUDED.week_chg_pct,
            month_chg_pct   = EXCLUDED.month_chg_pct,
            qtr_chg_pct     = EXCLUDED.qtr_chg_pct,
            half_yr_chg_pct = EXCLUDED.half_yr_chg_pct,
            year_chg_pct    = EXCLUDED.year_chg_pct
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def log_run(conn, run_id, started_at, status, total, success, failed, error=None):
    conn.execute(
        text("""
            INSERT INTO refresh_log
                (run_id, started_at, finished_at, stocks_total,
                 stocks_success, stocks_failed, status, error_message)
            VALUES
                (:run_id, :started_at, NOW(), :stocks_total,
                 :stocks_success, :stocks_failed, :status, :error_message)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at    = NOW(),
                stocks_total   = EXCLUDED.stocks_total,
                stocks_success = EXCLUDED.stocks_success,
                stocks_failed  = EXCLUDED.stocks_failed,
                status         = EXCLUDED.status,
                error_message  = EXCLUDED.error_message
        """),
        {
            "run_id":         str(run_id),
            "started_at":     started_at,
            "stocks_total":   total,
            "stocks_success": success,
            "stocks_failed":  failed,
            "status":         status,
            "error_message":  error,
        },
    )


def run():
    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    logger.info(f"=== Daily refresh started  run_id={run_id} ===")

    engine = get_engine()

    # ── 1. Load stock list ────────────────────────────────────────────────────
    with engine.connect() as conn:
        stocks = load_active_stocks(conn)

    total = len(stocks)
    logger.info(f"Loaded {total} active stocks")

    if total == 0:
        logger.error("No active stocks found — is the stocks table seeded?")
        sys.exit(1)

    yahoo_symbols = stocks["yahoo_symbol"].dropna().tolist()
    symbol_map = dict(zip(stocks["yahoo_symbol"], stocks["symbol"]))

    # ── 2. Fetch OHLCV ───────────────────────────────────────────────────────
    logger.info("Fetching prices from yfinance...")
    prices_df = fetch_prices(yahoo_symbols)

    success = prices_df["yahoo_symbol"].nunique() if not prices_df.empty else 0
    failed = total - success
    logger.info(f"Fetched data for {success}/{total} symbols ({failed} failed/missing)")

    if prices_df.empty:
        with engine.begin() as conn:
            log_run(conn, run_id, started_at, "failed", total, 0, total,
                    "yfinance returned no data")
        logger.error("No data fetched — aborting.")
        sys.exit(1)

    # ── 3. Upsert prices_daily ───────────────────────────────────────────────
    logger.info("Upserting prices_daily...")
    n_prices = upsert_prices(prices_df, symbol_map)
    logger.info(f"  {n_prices} rows upserted into prices_daily")

    # ── 4. Compute snapshots ─────────────────────────────────────────────────
    logger.info("Computing snapshots...")
    prices_nse = prices_df.copy()
    prices_nse["symbol"] = prices_nse["yahoo_symbol"].map(symbol_map)
    prices_nse = prices_nse.dropna(subset=["symbol"])

    snapshots = compute_snapshots(prices_nse[["symbol", "date", "close"]])
    logger.info(f"  Computed {len(snapshots)} snapshot rows")

    # ── 4b. Fetch fundamentals (market cap, PE, sector) ─────────────────────
    logger.info("Fetching fundamentals...")
    fundamentals = fetch_fundamentals(yahoo_symbols)
    # Map yahoo_symbol → symbol, then merge into snapshots
    fundamentals["symbol"] = fundamentals["yahoo_symbol"].map(symbol_map)
    fundamentals = fundamentals.dropna(subset=["symbol"])
    # Deduplicate fundamentals before merging — yfinance can return multiple rows
    # for the same symbol, which would create duplicate (symbol, date) pairs in
    # snapshots and trigger a CardinalityViolation on upsert.
    fundamentals = fundamentals.drop_duplicates(subset=["symbol"], keep="last")
    snapshots = snapshots.merge(
        fundamentals[["symbol", "market_cap_cr", "pe_ratio"]],
        on="symbol",
        how="left",
    )
    # Safety: drop any remaining duplicates before upsert
    snapshots = snapshots.drop_duplicates(subset=["symbol", "date"], keep="last")

    # ── 4c. Backfill sectors into stocks table ───────────────────────────────
    sectors_to_update = fundamentals[fundamentals["sector"].notna()][["symbol", "sector"]]
    if not sectors_to_update.empty:
        logger.info(f"Updating sectors for {len(sectors_to_update)} stocks...")
        with engine.begin() as conn:
            for _, row in sectors_to_update.iterrows():
                conn.execute(
                    text("UPDATE stocks SET sector = :sector WHERE symbol = :symbol AND (sector IS NULL OR sector != :sector)"),
                    {"symbol": row["symbol"], "sector": row["sector"]},
                )
        logger.info("  Sectors updated.")

    n_snaps = upsert_snapshots(snapshots)
    logger.info(f"  {n_snaps} rows upserted into snapshots_daily")

    # ── 4d. Backfill MCap/PE from previous snapshot for stocks still NULL ────
    logger.info("Backfilling missing MCap/PE from previous snapshots...")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE snapshots_daily sd
            SET
                market_cap_cr = prev.market_cap_cr,
                pe_ratio      = prev.pe_ratio
            FROM (
                SELECT DISTINCT ON (symbol)
                    symbol, market_cap_cr, pe_ratio
                FROM snapshots_daily
                WHERE date < CURRENT_DATE
                  AND (market_cap_cr IS NOT NULL OR pe_ratio IS NOT NULL)
                ORDER BY symbol, date DESC
            ) prev
            WHERE sd.symbol    = prev.symbol
              AND sd.date      = CURRENT_DATE
              AND sd.market_cap_cr IS NULL
              AND sd.pe_ratio  IS NULL
        """))
    logger.info("  MCap/PE backfill complete.")

    # ── 5. Sector aggregations ───────────────────────────────────────────────
    logger.info("Computing sector performance...")
    sector_df = compute_sector_performance(snapshots, stocks[["symbol", "sector"]])

    n_sectors = upsert_sector_performance(sector_df)
    logger.info(f"  {n_sectors} rows upserted into sector_performance_daily")

    # ── 5b. Technical indicators (final step — depends on prices_daily being fresh) ──
    logger.info("Computing technical indicators...")
    try:
        run_technical_refresh()
    except Exception as tech_err:
        # Technical refresh failure must NOT abort the main daily run.
        # Prices and snapshots are already committed above.
        logger.error(f"  Technical refresh failed (non-fatal): {tech_err}", exc_info=True)

    # ── 6. Log the run ───────────────────────────────────────────────────────
    status = "success" if failed == 0 else "partial"
    with engine.begin() as conn:
        log_run(conn, run_id, started_at, status, total, success, failed)

    logger.info(f"=== Refresh complete: {status} | "
                f"{success} ok / {failed} failed | "
                f"{n_prices} price rows | {n_snaps} snapshots ===")


if __name__ == "__main__":
    run()

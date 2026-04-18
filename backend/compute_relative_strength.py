"""
compute_relative_strength.py — Daily RS computation orchestrator.

What it does:
  1. Loads ALL close prices from prices_daily in ONE bulk query (last 400 days).
  2. Computes Nifty 50 (^NSEI) returns for each timeframe from those prices.
  3. For each active stock, computes excess return vs ^NSEI for 6 timeframes.
  4. Classifies each excess return into one of 5 buckets.
  5. Upserts one row per stock into relative_strength_daily.

Timeframes → trading day lookback:
  1W = 5, 2W = 10, 1M = 21, 3M = 63, 6M = 126, 1Y = 252

Usage (standalone):
    python backend/compute_relative_strength.py
"""

import sys
import math
import logging
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta

from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NIFTY_SYMBOL = "^NSEI"
UPSERT_BATCH_SIZE = 500

# Trading day lookback for each timeframe
TIMEFRAME_DAYS = {
    "1w":  5,
    "2w":  10,
    "1m":  21,
    "3m":  63,
    "6m":  126,
    "1y":  252,
}

# Threshold table — single source of truth
RS_THRESHOLDS = {
    "1w":  {"strong_out": 1.5,  "out": 0.5,  "under": -0.5,  "strong_under": -1.5},
    "2w":  {"strong_out": 2.5,  "out": 0.75, "under": -0.75, "strong_under": -2.5},
    "1m":  {"strong_out": 4.0,  "out": 1.0,  "under": -1.0,  "strong_under": -4.0},
    "3m":  {"strong_out": 8.0,  "out": 2.5,  "under": -2.5,  "strong_under": -8.0},
    "6m":  {"strong_out": 12.0, "out": 4.0,  "under": -4.0,  "strong_under": -12.0},
    "1y":  {"strong_out": 15.0, "out": 5.0,  "under": -5.0,  "strong_under": -15.0},
}

_UPSERT_SQL = """
    INSERT INTO relative_strength_daily
        (symbol, date,
         rs_excess_1w, rs_excess_2w, rs_excess_1m, rs_excess_3m, rs_excess_6m, rs_excess_1y,
         rs_bucket_1w, rs_bucket_2w, rs_bucket_1m, rs_bucket_3m, rs_bucket_6m, rs_bucket_1y,
         computed_at)
    VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
        rs_excess_1w = EXCLUDED.rs_excess_1w,
        rs_excess_2w = EXCLUDED.rs_excess_2w,
        rs_excess_1m = EXCLUDED.rs_excess_1m,
        rs_excess_3m = EXCLUDED.rs_excess_3m,
        rs_excess_6m = EXCLUDED.rs_excess_6m,
        rs_excess_1y = EXCLUDED.rs_excess_1y,
        rs_bucket_1w = EXCLUDED.rs_bucket_1w,
        rs_bucket_2w = EXCLUDED.rs_bucket_2w,
        rs_bucket_1m = EXCLUDED.rs_bucket_1m,
        rs_bucket_3m = EXCLUDED.rs_bucket_3m,
        rs_bucket_6m = EXCLUDED.rs_bucket_6m,
        rs_bucket_1y = EXCLUDED.rs_bucket_1y,
        computed_at  = EXCLUDED.computed_at
"""


# ── Pure functions (testable, no side effects) ────────────────────────────────

def classify_rs(excess_return_pct: float | None, timeframe: str) -> str | None:
    """
    Classify an excess return into one of 5 RS buckets.
    Returns None when excess_return_pct is None (insufficient data for that timeframe).
    """
    if excess_return_pct is None:
        return None
    t = RS_THRESHOLDS[timeframe]
    if excess_return_pct > t["strong_out"]:
        return "🚀 Strong Outperformer"
    elif excess_return_pct > t["out"]:
        return "✅ Outperformer"
    elif excess_return_pct >= t["under"]:
        return "⚖️ In-line"
    elif excess_return_pct >= t["strong_under"]:
        return "📉 Underperformer"
    else:
        return "🔻 Strong Underperformer"


def compute_excess_return(
    stock_return_pct: float | None,
    nifty_return_pct: float | None,
) -> float | None:
    """Excess return = stock_return - nifty_return, both as percentages."""
    if stock_return_pct is None or nifty_return_pct is None:
        return None
    return float(stock_return_pct) - float(nifty_return_pct)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _clean(v):
    """Convert NaN/Inf to None so psycopg2 sends NULL."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _return_at_n(prices_desc: list, n: int) -> float | None:
    """
    Compute return over n trading days from a DESC-sorted price list.
    prices_desc[0] = latest close, prices_desc[n] = close n trading days ago.
    Returns (latest / earlier - 1) * 100, or None if there aren't enough rows.
    """
    if len(prices_desc) <= n:
        return None
    latest  = prices_desc[0]
    earlier = prices_desc[n]
    if latest is None or earlier is None or earlier == 0:
        return None
    return (float(latest) / float(earlier) - 1) * 100.0


def _fetch_close_prices(engine, as_of_date: date) -> dict:
    """
    Single query: load close prices for ALL symbols up to as_of_date.
    Returns {symbol: [close_latest, close_day_before, ...]} sorted DESC.
    Includes ^NSEI so Nifty returns are computed the same way as stock returns.
    """
    cutoff = as_of_date - timedelta(days=400)
    sql = text("""
        SELECT symbol, close
        FROM prices_daily
        WHERE date >= :cutoff AND date <= :as_of_date
        ORDER BY symbol, date DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"cutoff": cutoff, "as_of_date": as_of_date}).fetchall()

    prices: dict[str, list] = defaultdict(list)
    for symbol, close in rows:
        prices[symbol].append(float(close) if close is not None else None)

    logger.info(f"  Loaded prices for {len(prices)} symbols ({len(rows):,} rows)")
    return dict(prices)


def fetch_all_close_prices_bulk(engine, cutoff_date: date) -> dict:
    """
    Load close prices for ALL symbols since cutoff_date in one query.
    Returns {symbol: [(date, close), ...]} sorted ASC by date.
    Used by the backfill script to avoid repeated queries.
    """
    sql = text("""
        SELECT symbol, date, close
        FROM prices_daily
        WHERE date >= :cutoff
        ORDER BY symbol, date ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"cutoff": cutoff_date}).fetchall()

    prices: dict[str, list] = defaultdict(list)
    for symbol, dt, close in rows:
        prices[symbol].append((dt, float(close) if close is not None else None))

    logger.info(f"  Bulk loaded {len(rows):,} rows across {len(prices)} symbols")
    return dict(prices)


def compute_rs_for_date_in_memory(
    prices_asc: dict,
    active_symbols: list,
    as_of_date: date,
) -> list:
    """
    Compute RS for all active stocks for a single date using pre-loaded price data.

    prices_asc: {symbol: [(date, close), ...]} sorted ASC by date
    Returns list of upsert tuples (ready for _batch_upsert).
    """
    now = datetime.now(timezone.utc)

    # Build DESC-sorted price lists up to as_of_date for each symbol
    def _prices_desc(symbol: str) -> list:
        all_bars = prices_asc.get(symbol, [])
        relevant = [c for dt, c in all_bars if dt <= as_of_date]
        return list(reversed(relevant))  # DESC: latest first

    nifty_prices = _prices_desc(NIFTY_SYMBOL)
    if not nifty_prices:
        return []

    nifty_returns = {tf: _return_at_n(nifty_prices, n) for tf, n in TIMEFRAME_DAYS.items()}

    rows = []
    for symbol in active_symbols:
        prices = _prices_desc(symbol)
        if not prices:
            continue

        excess, bucket = {}, {}
        for tf, n in TIMEFRAME_DAYS.items():
            exc = compute_excess_return(_return_at_n(prices, n), nifty_returns[tf])
            excess[tf] = _clean(exc)
            bucket[tf] = classify_rs(exc, tf)

        rows.append((
            symbol, as_of_date,
            excess["1w"], excess["2w"], excess["1m"],
            excess["3m"], excess["6m"], excess["1y"],
            bucket["1w"], bucket["2w"], bucket["1m"],
            bucket["3m"], bucket["6m"], bucket["1y"],
            now,
        ))
    return rows


def _load_active_symbols(engine) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol FROM stocks WHERE is_active = TRUE ORDER BY symbol")
        ).fetchall()
    return [r[0] for r in rows]


def _get_latest_prices_date(engine):
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(date) FROM prices_daily")).fetchone()
    return row[0] if row else None


# ── Core computation ──────────────────────────────────────────────────────────

def compute_rs_for_all_stocks(engine, as_of_date: date) -> int:
    """
    Compute RS for every active stock and upsert to relative_strength_daily.
    Returns count of rows written.
    """
    # ── 1. Load all close prices in one query ─────────────────────────────────
    prices_by_symbol = _fetch_close_prices(engine, as_of_date)

    # ── 2. Get Nifty 50 prices — required for every stock's excess return ──────
    nifty_prices = prices_by_symbol.get(NIFTY_SYMBOL)
    if not nifty_prices:
        logger.warning(
            f"^NSEI has no price data on or before {as_of_date} — "
            "skipping RS computation for this date."
        )
        return 0

    # Nifty returns for each timeframe (used as baseline for every stock)
    nifty_returns = {
        tf: _return_at_n(nifty_prices, n)
        for tf, n in TIMEFRAME_DAYS.items()
    }
    logger.info(
        f"  Nifty returns on {as_of_date}: "
        + ", ".join(f"{tf}={v:+.2f}%" if v is not None else f"{tf}=None"
                    for tf, v in nifty_returns.items())
    )

    # ── 3. Compute RS per stock ───────────────────────────────────────────────
    active_symbols = _load_active_symbols(engine)
    now = datetime.now(timezone.utc)
    rows_to_upsert = []

    for symbol in active_symbols:
        prices = prices_by_symbol.get(symbol)
        if not prices:
            continue

        row_tuple = (
            symbol,
            as_of_date,
        )
        excess = {}
        bucket = {}
        for tf, n in TIMEFRAME_DAYS.items():
            stock_ret  = _return_at_n(prices, n)
            exc        = compute_excess_return(stock_ret, nifty_returns[tf])
            bkt        = classify_rs(exc, tf)
            excess[tf] = _clean(exc)
            bucket[tf] = bkt

        rows_to_upsert.append((
            symbol,
            as_of_date,
            excess["1w"], excess["2w"], excess["1m"],
            excess["3m"], excess["6m"], excess["1y"],
            bucket["1w"], bucket["2w"], bucket["1m"],
            bucket["3m"], bucket["6m"], bucket["1y"],
            now,
        ))

        if len(rows_to_upsert) >= UPSERT_BATCH_SIZE:
            _batch_upsert(rows_to_upsert)
            rows_to_upsert = []

    if rows_to_upsert:
        _batch_upsert(rows_to_upsert)

    total = len(active_symbols)
    logger.info(f"  RS computation complete for {as_of_date}: {total} stocks processed")
    return total


def _batch_upsert(rows: list):
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def run_rs_refresh():
    """
    Main entry point — compute and persist RS for all active stocks.
    Called by daily_refresh.py as a step, or run standalone.
    """
    logger.info("=== RS refresh started ===")
    engine = get_engine()

    as_of_date = _get_latest_prices_date(engine)
    if as_of_date is None:
        logger.error("prices_daily is empty — cannot compute RS.")
        return

    logger.info(f"  Target date: {as_of_date}")
    rows_written = compute_rs_for_all_stocks(engine, as_of_date)

    # Sanity check
    with engine.connect() as conn:
        check = conn.execute(text("""
            SELECT rs_excess_1m, rs_bucket_1m
            FROM relative_strength_daily
            WHERE symbol = 'RELIANCE' AND date = :d
        """), {"d": as_of_date}).fetchone()
    logger.info(f"  Sanity check — RELIANCE 1M RS on {as_of_date}: {check}")

    logger.info(f"=== RS refresh complete: {rows_written} rows written ===")


if __name__ == "__main__":
    run_rs_refresh()

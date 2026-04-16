"""
compute_technicals.py — Daily technical indicator computation orchestrator.

What it does:
  1. Fetches ALL OHLCV for the last 380 calendar days in one query (grouped by symbol in Python).
  2. Determines the target date = latest date in prices_daily.
  3. For each active stock, computes RSI, MACD, ADX, SMA50, SMA200.
  4. Upserts one row per stock into technicals_daily (batch of 200).
  5. Logs the run to technicals_refresh_log.

Performance target: <3 minutes for ~1500 stocks.

Usage (standalone):
    python backend/compute_technicals.py

Also called from daily_refresh.py as the final step:
    from compute_technicals import run_technical_refresh
    run_technical_refresh()
"""

import sys
import uuid
import math
import logging
from collections import defaultdict
from datetime import datetime, timezone

from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn
from indicators import (
    compute_rsi,
    compute_macd,
    compute_adx,
    compute_sma,
    compute_technical_status,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Minimum OHLCV rows required to compute full indicators
MIN_ROWS_REQUIRED = 210
# Batch size for DB upserts
UPSERT_BATCH_SIZE = 200


def _clean(v):
    """Convert float NaN/Inf to None so psycopg2 sends NULL."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _fetch_all_ohlcv(engine) -> dict:
    """
    Single query for all OHLCV data needed to compute indicators.
    Returns {symbol: [(date, open, high, low, close, volume), ...]} sorted ascending by date.

    We fetch 380 calendar days to ensure ~260+ trading days of history.
    """
    logger.info("Fetching bulk OHLCV from prices_daily (last 380 calendar days)...")
    sql = text("""
        SELECT symbol, date, open, high, low, close, volume
        FROM prices_daily
        WHERE date >= CURRENT_DATE - INTERVAL '380 days'
        ORDER BY symbol, date ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    # Group by symbol
    ohlcv_by_symbol = defaultdict(list)
    for row in rows:
        symbol, date, o, h, l, c, vol = row
        ohlcv_by_symbol[symbol].append({
            "date":   date,
            "open":   float(o)   if o   is not None else None,
            "high":   float(h)   if h   is not None else None,
            "low":    float(l)   if l   is not None else None,
            "close":  float(c)   if c   is not None else None,
            "volume": int(vol)   if vol is not None else None,
        })

    logger.info(f"  Loaded {len(rows):,} OHLCV rows across {len(ohlcv_by_symbol)} symbols")
    return dict(ohlcv_by_symbol)


def _load_active_symbols(engine) -> list:
    """Return all active NSE symbols."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol FROM stocks WHERE is_active = TRUE ORDER BY symbol")
        ).fetchall()
    return [r[0] for r in rows]


def _get_latest_prices_date(engine):
    """Return the latest date present in prices_daily (= today's trading close)."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT MAX(date) FROM prices_daily")
        ).fetchone()
    return row[0] if row else None


def _compute_for_symbol(symbol: str, bars: list, target_date) -> dict:
    """
    Given sorted OHLCV bars for one symbol, compute all indicators for target_date.
    Always returns a dict — nulls out indicators if insufficient data.
    """
    base = {
        "symbol":           symbol,
        "date":             target_date,
        "cmp":              None,
        "volume":           None,
        "rsi_14":           None,
        "macd_line":        None,
        "macd_signal":      None,
        "macd_histogram":   None,
        "adx_14":           None,
        "plus_di_14":       None,
        "minus_di_14":      None,
        "sma_50":           None,
        "sma_200":          None,
        "signal_score":     0,
        "technical_status": "⚪ Insufficient Data",
    }

    if not bars:
        return base

    # Use bars up to and including target_date
    relevant = [b for b in bars if b["date"] <= target_date]
    if not relevant:
        return base

    last_bar = relevant[-1]
    base["cmp"]    = _clean(last_bar["close"])
    base["volume"] = last_bar["volume"]

    if len(relevant) < MIN_ROWS_REQUIRED:
        # Not enough history — return the row with only CMP/Volume filled
        return base

    # Extract series (all valid bars)
    closes = [b["close"] for b in relevant if b["close"] is not None]
    highs  = [b["high"]  for b in relevant if b["high"]  is not None]
    lows   = [b["low"]   for b in relevant if b["low"]   is not None]

    # Trim to the same length (handle rare missing H/L entries)
    min_len = min(len(closes), len(highs), len(lows))
    closes_trim = closes[-min_len:]
    highs_trim  = highs[-min_len:]
    lows_trim   = lows[-min_len:]

    # ── Compute indicators ────────────────────────────────────────────────────
    rsi   = compute_rsi(closes_trim)
    macd  = compute_macd(closes_trim)
    adx_r = compute_adx(highs_trim, lows_trim, closes_trim)
    sma50  = compute_sma(closes_trim, 50)
    sma200 = compute_sma(closes_trim, 200)

    macd_line = macd["line"]       if macd else None
    macd_sig  = macd["signal"]     if macd else None
    macd_hist = macd["histogram"]  if macd else None
    adx_val   = adx_r["adx"]      if adx_r else None
    plus_di   = adx_r["plus_di"]  if adx_r else None
    minus_di  = adx_r["minus_di"] if adx_r else None

    score, status = compute_technical_status(
        cmp=base["cmp"], rsi=rsi,
        sma_50=sma50, sma_200=sma200,
        macd_line=macd_line, macd_signal=macd_sig, macd_histogram=macd_hist,
        adx=adx_val,
    )

    return {
        "symbol":           symbol,
        "date":             target_date,
        "cmp":              _clean(base["cmp"]),
        "volume":           base["volume"],
        "rsi_14":           _clean(rsi),
        "macd_line":        _clean(macd_line),
        "macd_signal":      _clean(macd_sig),
        "macd_histogram":   _clean(macd_hist),
        "adx_14":           _clean(adx_val),
        "plus_di_14":       _clean(plus_di),
        "minus_di_14":      _clean(minus_di),
        "sma_50":           _clean(sma50),
        "sma_200":          _clean(sma200),
        "signal_score":     score,
        "technical_status": status,
    }


_UPSERT_COLS = [
    "symbol", "date", "cmp", "volume",
    "rsi_14", "macd_line", "macd_signal", "macd_histogram",
    "adx_14", "plus_di_14", "minus_di_14",
    "sma_50", "sma_200",
    "signal_score", "technical_status", "computed_at",
]

_UPSERT_SQL = """
    INSERT INTO technicals_daily
        (symbol, date, cmp, volume,
         rsi_14, macd_line, macd_signal, macd_histogram,
         adx_14, plus_di_14, minus_di_14,
         sma_50, sma_200,
         signal_score, technical_status, computed_at)
    VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
        cmp              = EXCLUDED.cmp,
        volume           = EXCLUDED.volume,
        rsi_14           = EXCLUDED.rsi_14,
        macd_line        = EXCLUDED.macd_line,
        macd_signal      = EXCLUDED.macd_signal,
        macd_histogram   = EXCLUDED.macd_histogram,
        adx_14           = EXCLUDED.adx_14,
        plus_di_14       = EXCLUDED.plus_di_14,
        minus_di_14      = EXCLUDED.minus_di_14,
        sma_50           = EXCLUDED.sma_50,
        sma_200          = EXCLUDED.sma_200,
        signal_score     = EXCLUDED.signal_score,
        technical_status = EXCLUDED.technical_status,
        computed_at      = EXCLUDED.computed_at
"""


def _batch_upsert(rows: list):
    """Upsert a batch of indicator rows into technicals_daily."""
    now = datetime.now(timezone.utc)
    tuples = []
    for r in rows:
        tuples.append((
            r["symbol"], r["date"], r["cmp"], r["volume"],
            r["rsi_14"], r["macd_line"], r["macd_signal"], r["macd_histogram"],
            r["adx_14"], r["plus_di_14"], r["minus_di_14"],
            r["sma_50"], r["sma_200"],
            r["signal_score"], r["technical_status"], now,
        ))

    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, tuples, page_size=500)
        conn.commit()
    finally:
        conn.close()


def _log_run(engine, run_id, started_at, status, total, success, failed, error=None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO technicals_refresh_log
                (run_id, started_at, finished_at, stocks_total,
                 stocks_success, stocks_failed, status, error_message)
            VALUES
                (:run_id, :started_at, NOW(), :total,
                 :success, :failed, :status, :error)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at    = NOW(),
                stocks_total   = EXCLUDED.stocks_total,
                stocks_success = EXCLUDED.stocks_success,
                stocks_failed  = EXCLUDED.stocks_failed,
                status         = EXCLUDED.status,
                error_message  = EXCLUDED.error_message
        """), {
            "run_id":    str(run_id),
            "started_at": started_at,
            "total":     total,
            "success":   success,
            "failed":    failed,
            "status":    status,
            "error":     error,
        })


def run_technical_refresh():
    """
    Main entry point — compute and persist technical indicators for all active stocks.
    Called by daily_refresh.py as the final step, or run standalone.
    """
    run_id    = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    logger.info(f"=== Technical refresh started  run_id={run_id} ===")

    engine = get_engine()

    # ── 1. Determine target date ──────────────────────────────────────────────
    target_date = _get_latest_prices_date(engine)
    if target_date is None:
        logger.error("prices_daily is empty — cannot compute technicals.")
        return
    logger.info(f"  Target date: {target_date}")

    # ── 2. Fetch all OHLCV in one query ──────────────────────────────────────
    ohlcv_by_symbol = _fetch_all_ohlcv(engine)

    # ── 3. Load active symbols ─────────────────────────────────────────────────
    active_symbols = _load_active_symbols(engine)
    total = len(active_symbols)
    logger.info(f"  Processing {total} active stocks...")

    # ── 4. Compute indicators per symbol ─────────────────────────────────────
    results       = []
    count_full    = 0   # got full indicators
    count_partial = 0   # insufficient history
    verdict_counts = {}

    for symbol in active_symbols:
        bars = ohlcv_by_symbol.get(symbol, [])
        row  = _compute_for_symbol(symbol, bars, target_date)
        results.append(row)

        if row["technical_status"] == "⚪ Insufficient Data":
            count_partial += 1
        else:
            count_full += 1
            v = row["technical_status"]
            verdict_counts[v] = verdict_counts.get(v, 0) + 1

        # Batch upsert every UPSERT_BATCH_SIZE stocks
        if len(results) >= UPSERT_BATCH_SIZE:
            _batch_upsert(results)
            logger.info(f"  Upserted batch — {count_full + count_partial}/{total} processed")
            results = []

    # Flush remaining
    if results:
        _batch_upsert(results)

    # ── 5. Summary ────────────────────────────────────────────────────────────
    adx_vals = []
    for sym in active_symbols:
        bars = ohlcv_by_symbol.get(sym, [])
        relevant = [b for b in bars if b["date"] <= target_date and b["high"] and b["low"] and b["close"]]
        if len(relevant) >= MIN_ROWS_REQUIRED:
            from indicators import compute_adx as _adx
            closes = [b["close"] for b in relevant]
            highs  = [b["high"]  for b in relevant]
            lows   = [b["low"]   for b in relevant]
            r = _adx(highs, lows, closes)
            if r:
                adx_vals.append(r["adx"])

    avg_adx = sum(adx_vals) / len(adx_vals) if adx_vals else 0.0

    logger.info(f"=== Technical refresh complete ===")
    logger.info(f"  Total: {total} | Full indicators: {count_full} | Insufficient data: {count_partial}")
    logger.info(f"  Average ADX (stocks with data): {avg_adx:.1f}")
    logger.info("  Verdict breakdown:")
    for verdict, cnt in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        logger.info(f"    {verdict}: {cnt}")

    # ── 6. Log the run ────────────────────────────────────────────────────────
    run_status = "success" if count_partial == 0 else ("partial" if count_full > 0 else "failed")
    _log_run(engine, run_id, started_at, run_status, total, count_full, count_partial)

    logger.info(f"  Run logged — status={run_status}")


if __name__ == "__main__":
    run_technical_refresh()

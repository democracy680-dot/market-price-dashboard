"""
compute_setup_candidates.py — Orchestrator for the Breakout & Reversal Scanner.

Loads price history and latest technicals for all active stocks, runs all 10
pattern detectors, applies mutual exclusion rules, and bulk-upserts results
into setup_candidates_daily.

Usage (standalone):
    python backend/compute_setup_candidates.py

Called from daily_refresh.py as the final step.
"""

import sys
import time
import logging
from collections import defaultdict
from datetime import date, timedelta, datetime

import pandas as pd
from psycopg2.extras import execute_values

from db import get_engine, get_psycopg2_conn
from setup_detectors import (
    detect_52w_retest,
    detect_ath_retest,
    detect_consolidation,
    detect_ma_approach,
    detect_volume_dryup,
    detect_oversold_bounce,
    detect_bullish_divergence,
    detect_double_bottom,
    detect_macd_crossover_setup,
    detect_200dma_reclaim,
    apply_exclusion_rules,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


DETECTORS = [
    ("BR_52W_RETEST",    "breakout", detect_52w_retest),
    ("BR_ATH_RETEST",    "breakout", detect_ath_retest),
    ("BR_CONSOLIDATION", "breakout", detect_consolidation),
    ("BR_MA_APPROACH",   "breakout", detect_ma_approach),
    ("BR_VOLUME_DRYUP",  "breakout", detect_volume_dryup),
    ("RV_OVERSOLD",      "reversal", detect_oversold_bounce),
    ("RV_DIVERGENCE",    "reversal", detect_bullish_divergence),
    ("RV_DOUBLE_BOTTOM", "reversal", detect_double_bottom),
    ("RV_MACD_CROSS",    "reversal", detect_macd_crossover_setup),
    ("RV_200DMA_RECLAIM","reversal", detect_200dma_reclaim),
]


# ── Data loading ──────────────────────────────────────────────────────────────

def load_active_stocks(engine) -> list[dict]:
    sql = """
        SELECT s.symbol,
               sd.market_cap_cr
        FROM stocks s
        LEFT JOIN LATERAL (
            SELECT market_cap_cr
            FROM snapshots_daily
            WHERE symbol = s.symbol
            ORDER BY date DESC
            LIMIT 1
        ) sd ON TRUE
        WHERE s.is_active = TRUE
    """
    with engine.connect() as conn:
        rows = conn.execute(__import__("sqlalchemy").text(sql)).fetchall()
    return [{"symbol": r[0], "market_cap_cr": r[1]} for r in rows]


def load_bulk_prices(engine, min_date: date) -> dict[str, pd.DataFrame]:
    """Load all OHLCV since min_date in one query. Group by symbol in Python."""
    sql = """
        SELECT symbol, date, open, high, low, close, volume
        FROM prices_daily
        WHERE date >= :min_date
        ORDER BY symbol, date
    """
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(
                    __import__("sqlalchemy").text(sql),
                    conn,
                    params={"min_date": min_date},
                )
            break
        except Exception as exc:
            if attempt == 3:
                raise
            logger.warning(f"load_bulk_prices attempt {attempt} failed ({exc}); retrying in 10s...")
            time.sleep(10)

    if df.empty:
        return {}

    result = {}
    for symbol, group in df.groupby("symbol"):
        g = group.sort_values("date").reset_index(drop=True)
        g = g[["open", "high", "low", "close", "volume"]].astype(float)
        result[symbol] = g
    return result


def load_latest_technicals(engine, as_of_date: date) -> dict[str, dict]:
    """Latest technicals row per symbol on or before as_of_date."""
    sql = """
        SELECT DISTINCT ON (symbol)
            symbol, date,
            rsi_14, macd_line, macd_signal, macd_histogram,
            adx_14, plus_di_14, minus_di_14,
            sma_50, sma_200, sma_200_slope, volume_ratio,
            signal_score_v2
        FROM technicals_daily
        WHERE date <= :as_of_date
        ORDER BY symbol, date DESC
    """
    with engine.connect() as conn:
        rows = conn.execute(
            __import__("sqlalchemy").text(sql),
            {"as_of_date": as_of_date},
        ).fetchall()

    result = {}
    for r in rows:
        d = dict(r._mapping)
        # Attach market_cap_cr placeholder (filled later during merge)
        d.setdefault("market_cap_cr", None)
        result[d["symbol"]] = d
    return result


# ── DB write ──────────────────────────────────────────────────────────────────

UPSERT_SQL = """
    INSERT INTO setup_candidates_daily
        (symbol, date, pattern_code, pattern_category,
         setup_strength, is_candidate, cmp,
         trigger_level, pct_from_trigger, days_in_base,
         volume_ratio, notes, computed_at)
    VALUES %s
    ON CONFLICT (symbol, date, pattern_code) DO UPDATE SET
        pattern_category  = EXCLUDED.pattern_category,
        setup_strength    = EXCLUDED.setup_strength,
        is_candidate      = EXCLUDED.is_candidate,
        cmp               = EXCLUDED.cmp,
        trigger_level     = EXCLUDED.trigger_level,
        pct_from_trigger  = EXCLUDED.pct_from_trigger,
        days_in_base      = EXCLUDED.days_in_base,
        volume_ratio      = EXCLUDED.volume_ratio,
        notes             = EXCLUDED.notes,
        computed_at       = EXCLUDED.computed_at
"""


def upsert_setup_candidates(rows: list[dict]):
    if not rows:
        return
    values = [
        (
            r["symbol"], r["date"], r["pattern_code"], r["pattern_category"],
            r["setup_strength"], True, r.get("cmp"),
            r.get("trigger_level"), r.get("pct_from_trigger"), r.get("days_in_base"),
            r.get("volume_ratio"), r.get("notes", ""), datetime.utcnow(),
        )
        for r in rows
    ]
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values, page_size=500)
        conn.commit()
    finally:
        conn.close()


# ── Main orchestrator ─────────────────────────────────────────────────────────

def compute_setups_for_all_stocks(as_of_date: date) -> dict[str, int]:
    """
    Run all detectors on all active stocks and write results to DB.
    Returns per-pattern candidate counts.
    """
    engine = get_engine()

    logger.info("Loading active stocks...")
    all_stocks = load_active_stocks(engine)
    mcap_map = {s["symbol"]: s["market_cap_cr"] for s in all_stocks}
    logger.info(f"  {len(all_stocks)} active stocks")

    logger.info("Loading bulk price history...")
    min_date = as_of_date - timedelta(days=400)
    bulk_prices = load_bulk_prices(engine, min_date)
    logger.info(f"  Price data for {len(bulk_prices)} symbols")

    logger.info("Loading latest technicals...")
    latest_tech = load_latest_technicals(engine, as_of_date)
    logger.info(f"  Technicals for {len(latest_tech)} symbols")

    pattern_counts: dict[str, int] = defaultdict(int)
    rows_to_insert: list[dict] = []
    skipped = 0

    for stock in all_stocks:
        symbol = stock["symbol"]

        prices_df = bulk_prices.get(symbol)
        if prices_df is None or len(prices_df) < 100:
            skipped += 1
            continue

        tech_row = latest_tech.get(symbol)
        if tech_row is None:
            skipped += 1
            continue

        # Inject market cap (needed by some detectors, e.g. RV_OVERSOLD quality check)
        tech_row["market_cap_cr"] = mcap_map.get(symbol)
        cmp = prices_df["close"].iloc[-1]

        flagged: list[tuple] = []
        for pattern_code, category, detector_fn in DETECTORS:
            try:
                is_candidate, strength, metadata = detector_fn(prices_df, tech_row)
                if is_candidate:
                    flagged.append((pattern_code, category, strength, metadata))
            except Exception as exc:
                logger.debug(f"Detector {pattern_code} error for {symbol}: {exc}")

        flagged = apply_exclusion_rules(flagged)

        for pattern_code, category, strength, metadata in flagged:
            rows_to_insert.append({
                "symbol":          symbol,
                "date":            as_of_date,
                "pattern_code":    pattern_code,
                "pattern_category":category,
                "setup_strength":  round(strength, 1),
                "cmp":             round(float(cmp), 2),
                "trigger_level":   metadata.get("trigger_level"),
                "pct_from_trigger":metadata.get("pct_from_trigger"),
                "days_in_base":    metadata.get("days_in_base"),
                "volume_ratio":    metadata.get("volume_ratio"),
                "notes":           metadata.get("notes", ""),
            })
            pattern_counts[pattern_code] += 1

    logger.info(f"Skipped {skipped} stocks (insufficient data)")
    logger.info(f"Inserting {len(rows_to_insert)} candidate rows...")
    upsert_setup_candidates(rows_to_insert)

    return dict(pattern_counts)


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    target_date = date.today()
    if len(sys.argv) > 1:
        target_date = date.fromisoformat(sys.argv[1])

    logger.info(f"Computing setup candidates for {target_date}")
    counts = compute_setups_for_all_stocks(target_date)
    logger.info("Pattern counts:")
    for code, n in sorted(counts.items()):
        logger.info(f"  {code:<20} {n:>4}")
    total = sum(counts.values())
    logger.info(f"Total candidates: {total}")

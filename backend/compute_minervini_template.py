"""
compute_minervini_template.py — Minervini Trend Template screener.

Evaluates all 8 Minervini criteria for every active stock and writes
results to minervini_template_daily. Called from daily_refresh.py.

Usage (standalone):
    python backend/compute_minervini_template.py
"""

import sys
import logging
import time
from datetime import date, timedelta, datetime

import pandas as pd
from psycopg2.extras import execute_values

from db import get_engine, get_psycopg2_conn
from indicators import compute_sma

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Pure math helpers ─────────────────────────────────────────────────────────

def compute_sma_slope_pct(closes: list[float], period: int, lookback: int) -> float | None:
    """
    Percent change in SMA(period) over `lookback` bars.
    Efficiently computes only the two required SMA values (current and lookback-ago).
    Returns None if not enough data.
    """
    needed = period + lookback
    if len(closes) < needed:
        return None
    sma_now = sum(closes[-period:]) / period
    sma_ago = sum(closes[-(period + lookback):-lookback]) / period
    if sma_ago == 0:
        return None
    return ((sma_now - sma_ago) / sma_ago) * 100.0


def compute_rs_ranks_for_universe(returns_dict: dict[str, float]) -> dict[str, float]:
    """
    Given {symbol: 12-month return %}, returns {symbol: rs_rank} 1-99 percentile.
    Highest return → 99, lowest → 1. Symbols with None return are excluded.
    """
    valid = {s: r for s, r in returns_dict.items() if r is not None}
    if not valid:
        return {}
    sorted_symbols = sorted(valid.keys(), key=lambda s: valid[s])
    n = len(sorted_symbols)
    return {sym: round(((i + 1) / n) * 99, 1) for i, sym in enumerate(sorted_symbols)}


def evaluate_minervini_template(
    cmp: float,
    sma_50: float,
    sma_150: float,
    sma_200: float,
    sma_200_slope_22d: float | None,
    sma_200_slope_110d: float | None,
    high_52w: float,
    low_52w: float,
    rs_rank_12m: float,
    sma_50_slope_22d: float | None = None,
    recent_volume_ratio: float | None = None,
) -> dict:
    """Evaluates all 8 Minervini criteria. Returns flags, pass/fail, and score."""
    required = [cmp, sma_50, sma_150, sma_200, high_52w, low_52w, rs_rank_12m]
    if any(v is None for v in required):
        return {
            "criterion_1_pass": False, "criterion_2_pass": False,
            "criterion_3_pass": False, "criterion_4_pass": False,
            "criterion_5_pass": False, "criterion_6_pass": False,
            "criterion_7_pass": False, "criterion_8_pass": False,
            "template_pass": False, "template_score": 0.0,
            "criteria_count": 0,
            "pct_from_52w_high": None, "pct_above_52w_low": None,
        }

    c1 = cmp > sma_150 and cmp > sma_200
    c2 = sma_150 > sma_200
    c3 = sma_200_slope_22d is not None and sma_200_slope_22d > 0
    c4 = sma_50 > sma_150 and sma_50 > sma_200
    c5 = cmp > sma_50

    pct_above_low = ((cmp - low_52w) / low_52w) * 100 if low_52w > 0 else None
    c6 = pct_above_low is not None and pct_above_low >= 30

    pct_from_high = ((cmp - high_52w) / high_52w) * 100 if high_52w > 0 else None
    c7 = pct_from_high is not None and pct_from_high >= -25

    c8 = rs_rank_12m >= 70

    template_pass = all([c1, c2, c3, c4, c5, c6, c7, c8])
    criteria_count = sum([c1, c2, c3, c4, c5, c6, c7, c8])

    score = 0.0
    if template_pass:
        score = 5.0
        if sma_200_slope_110d is not None and sma_200_slope_110d > 0:
            score += 1.0
        if rs_rank_12m >= 80:
            score += 1.0
        if rs_rank_12m >= 90:
            score += 1.0
        if pct_from_high is not None and pct_from_high >= -10:
            score += 1.0
        if sma_50_slope_22d is not None and sma_50_slope_22d > 0:
            score += 1.0
        if recent_volume_ratio is not None and recent_volume_ratio > 1.0:
            score += 1.0
        score = min(score, 10.0)

    return {
        "criterion_1_pass": c1, "criterion_2_pass": c2,
        "criterion_3_pass": c3, "criterion_4_pass": c4,
        "criterion_5_pass": c5, "criterion_6_pass": c6,
        "criterion_7_pass": c7, "criterion_8_pass": c8,
        "template_pass": template_pass,
        "template_score": round(score, 1),
        "criteria_count": criteria_count,
        "pct_from_52w_high": round(pct_from_high, 2) if pct_from_high is not None else None,
        "pct_above_52w_low": round(pct_above_low, 2) if pct_above_low is not None else None,
    }


# ── Data loading ──────────────────────────────────────────────────────────────

def _load_bulk_prices(engine, min_date: date) -> dict[str, pd.DataFrame]:
    """Load all OHLCV since min_date in one query. Returns {symbol: DataFrame}."""
    from sqlalchemy import text
    sql = """
        SELECT symbol, date, open, high, low, close, volume
        FROM prices_daily
        WHERE date >= :min_date
        ORDER BY symbol, date
    """
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(text(sql), conn, params={"min_date": min_date})
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
        result[symbol] = g[["open", "high", "low", "close", "volume"]].astype(float)
    return result


# ── DB write ──────────────────────────────────────────────────────────────────

_UPSERT_SQL = """
    INSERT INTO minervini_template_daily (
        symbol, date,
        criterion_1_pass, criterion_2_pass, criterion_3_pass, criterion_4_pass,
        criterion_5_pass, criterion_6_pass, criterion_7_pass, criterion_8_pass,
        template_pass, template_score, criteria_count,
        cmp, sma_50, sma_150, sma_200,
        sma_200_slope_22d, sma_200_slope_110d,
        high_52w, low_52w,
        pct_from_52w_high, pct_above_52w_low,
        rs_rank_12m, return_12m, computed_at
    ) VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
        criterion_1_pass  = EXCLUDED.criterion_1_pass,
        criterion_2_pass  = EXCLUDED.criterion_2_pass,
        criterion_3_pass  = EXCLUDED.criterion_3_pass,
        criterion_4_pass  = EXCLUDED.criterion_4_pass,
        criterion_5_pass  = EXCLUDED.criterion_5_pass,
        criterion_6_pass  = EXCLUDED.criterion_6_pass,
        criterion_7_pass  = EXCLUDED.criterion_7_pass,
        criterion_8_pass  = EXCLUDED.criterion_8_pass,
        template_pass     = EXCLUDED.template_pass,
        template_score    = EXCLUDED.template_score,
        criteria_count    = EXCLUDED.criteria_count,
        cmp               = EXCLUDED.cmp,
        sma_50            = EXCLUDED.sma_50,
        sma_150           = EXCLUDED.sma_150,
        sma_200           = EXCLUDED.sma_200,
        sma_200_slope_22d = EXCLUDED.sma_200_slope_22d,
        sma_200_slope_110d= EXCLUDED.sma_200_slope_110d,
        high_52w          = EXCLUDED.high_52w,
        low_52w           = EXCLUDED.low_52w,
        pct_from_52w_high = EXCLUDED.pct_from_52w_high,
        pct_above_52w_low = EXCLUDED.pct_above_52w_low,
        rs_rank_12m       = EXCLUDED.rs_rank_12m,
        return_12m        = EXCLUDED.return_12m,
        computed_at       = EXCLUDED.computed_at
"""


def _upsert_minervini_template(rows: list[dict]):
    if not rows:
        return
    values = [
        (
            r["symbol"], r["date"],
            r["criterion_1_pass"], r["criterion_2_pass"],
            r["criterion_3_pass"], r["criterion_4_pass"],
            r["criterion_5_pass"], r["criterion_6_pass"],
            r["criterion_7_pass"], r["criterion_8_pass"],
            r["template_pass"], r["template_score"], r["criteria_count"],
            r.get("cmp"), r.get("sma_50"), r.get("sma_150"), r.get("sma_200"),
            r.get("sma_200_slope_22d"), r.get("sma_200_slope_110d"),
            r.get("high_52w"), r.get("low_52w"),
            r.get("pct_from_52w_high"), r.get("pct_above_52w_low"),
            r.get("rs_rank_12m"), r.get("return_12m"),
            datetime.utcnow(),
        )
        for r in rows
    ]
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, values, page_size=500)
        conn.commit()
    finally:
        conn.close()


# ── Main orchestrator ─────────────────────────────────────────────────────────

def compute_minervini_for_all_stocks(as_of_date: date) -> int:
    """
    Evaluate Minervini Trend Template for every active stock on as_of_date.
    Returns count of stocks where template_pass = True.
    """
    engine = get_engine()
    t0 = time.time()

    logger.info("Loading bulk price history for Minervini...")
    min_date = as_of_date - timedelta(days=420)  # 420 days covers SMA200 + 110d slope + buffer
    bulk_prices = _load_bulk_prices(engine, min_date)
    logger.info(f"  Price data for {len(bulk_prices)} symbols")

    # ── Step 1: compute 12-month returns for RS Rank ──────────────────────────
    returns_12m: dict[str, float] = {}
    for symbol, prices_df in bulk_prices.items():
        if len(prices_df) < 200:
            continue
        closes = prices_df["close"].tolist()
        new_close = closes[-1]
        if len(closes) >= 252:
            old_close = closes[-252]
        else:
            old_close = closes[0]
        if old_close and old_close > 0:
            returns_12m[symbol] = ((new_close - old_close) / old_close) * 100.0

    # ── Step 2: compute RS Ranks across the universe ──────────────────────────
    rs_ranks = compute_rs_ranks_for_universe(returns_12m)
    logger.info(f"  RS Ranks computed for {len(rs_ranks)} symbols")

    # ── Step 3: evaluate template for each stock ──────────────────────────────
    rows_to_insert: list[dict] = []
    skipped = 0

    for symbol, prices_df in bulk_prices.items():
        if len(prices_df) < 200:
            skipped += 1
            continue

        closes = prices_df["close"].tolist()
        highs  = prices_df["high"].tolist()
        lows   = prices_df["low"].tolist()
        vols   = prices_df["volume"].tolist()

        cmp = closes[-1]

        sma_50  = compute_sma(closes, 50)
        sma_150 = compute_sma(closes, 150)
        sma_200 = compute_sma(closes, 200)

        sma_200_slope_22d  = compute_sma_slope_pct(closes, 200, 22)
        sma_200_slope_110d = compute_sma_slope_pct(closes, 200, 110)
        sma_50_slope_22d   = compute_sma_slope_pct(closes, 50, 22)

        tail_252_highs = highs[-252:] if len(highs) >= 252 else highs
        tail_252_lows  = lows[-252:]  if len(lows)  >= 252 else lows
        high_52w = max(tail_252_highs)
        low_52w  = min(tail_252_lows)

        recent_vol  = sum(vols[-5:])  / 5  if len(vols) >= 5  else None
        avg_20d_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else None
        volume_ratio = (recent_vol / avg_20d_vol) if (recent_vol and avg_20d_vol and avg_20d_vol > 0) else None

        rs_rank = rs_ranks.get(symbol)

        result = evaluate_minervini_template(
            cmp=cmp, sma_50=sma_50, sma_150=sma_150, sma_200=sma_200,
            sma_200_slope_22d=sma_200_slope_22d,
            sma_200_slope_110d=sma_200_slope_110d,
            high_52w=high_52w, low_52w=low_52w,
            rs_rank_12m=rs_rank,
            sma_50_slope_22d=sma_50_slope_22d,
            recent_volume_ratio=volume_ratio,
        )

        rows_to_insert.append({
            "symbol": symbol, "date": as_of_date,
            **result,
            "cmp": cmp, "sma_50": sma_50, "sma_150": sma_150, "sma_200": sma_200,
            "sma_200_slope_22d": sma_200_slope_22d,
            "sma_200_slope_110d": sma_200_slope_110d,
            "high_52w": high_52w, "low_52w": low_52w,
            "rs_rank_12m": rs_rank,
            "return_12m": returns_12m.get(symbol),
        })

    _upsert_minervini_template(rows_to_insert)

    pass_count = sum(1 for r in rows_to_insert if r.get("template_pass"))
    elapsed = time.time() - t0
    logger.info(
        f"Minervini Template: {pass_count} pass / {len(rows_to_insert)} evaluated "
        f"/ {skipped} skipped — {elapsed:.1f}s"
    )
    return pass_count


def run_minervini_refresh(as_of_date: date | None = None) -> int:
    if as_of_date is None:
        as_of_date = date.today()
    return compute_minervini_for_all_stocks(as_of_date)


if __name__ == "__main__":
    result = run_minervini_refresh()
    logger.info(f"Done. {result} stocks passed all 8 criteria.")
    sys.exit(0)

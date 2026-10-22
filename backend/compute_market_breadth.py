"""
compute_market_breadth.py — Market Mood orchestrator.

What it does:
  1. Loads prices_daily (close, volume) for ALL active stocks.
  2. Computes per-date breadth (±3/5/10/20% movers, 50/200 DMA %, RSI
     overbought/oversold, 20-day advance ratio, composite mood score, turnover).
  3. Upserts into market_breadth_daily.
  4. Re-aggregates market_turnover_monthly from the daily table.

Usage:
    python backend/compute_market_breadth.py            # incremental (last ~45 days)
    python backend/compute_market_breadth.py --full     # rebuild entire history

Also called from daily_refresh.py:
    from compute_market_breadth import run_market_breadth_refresh
    run_market_breadth_refresh()
"""
import sys
import math
import logging
from datetime import date, timedelta

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn
from market_breadth import compute_breadth_frame, compute_monthly_turnover

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Incremental mode: publish breadth for the last N calendar days,
# but load enough price history before that for SMA-200 / RSI warm-up.
PUBLISH_WINDOW_DAYS = 45
WARMUP_DAYS = 330
SYMBOL_BATCH = 200
# Ignore dates where only a handful of stocks traded (data holes / half-seeded days)
MIN_STOCKS_PER_DATE = 100

DAILY_COLS = [
    "date", "total_stocks", "advances", "declines", "unchanged",
    "up_3", "down_3", "up_5", "down_5", "up_10", "down_10", "up_20", "down_20",
    "dma50_count", "above_50dma", "below_50dma", "pct_above_50dma", "pct_below_50dma",
    "dma200_count", "above_200dma", "below_200dma", "pct_above_200dma", "pct_below_200dma",
    "rsi_count", "overbought", "oversold", "rsi_above_50", "pct_rsi_above_50",
    "adv_ratio_20d", "mood_score", "mood_label", "momentum_pct", "turnover_cr",
]

_UPSERT_DAILY = f"""
    INSERT INTO market_breadth_daily ({", ".join(DAILY_COLS)}, computed_at)
    VALUES %s
    ON CONFLICT (date) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in DAILY_COLS if c != "date")},
        computed_at = NOW()
"""

_UPSERT_MONTHLY = """
    INSERT INTO market_turnover_monthly (month, turnover_cr, trading_days, is_partial, computed_at)
    VALUES %s
    ON CONFLICT (month) DO UPDATE SET
        turnover_cr  = EXCLUDED.turnover_cr,
        trading_days = EXCLUDED.trading_days,
        is_partial   = EXCLUDED.is_partial,
        computed_at  = NOW()
"""


def _clean(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "item"):          # numpy scalar → python
        v = v.item()
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
    return v


def _load_prices(engine, since: date | None) -> pd.DataFrame:
    """Load close/volume for all active symbols, in symbol batches."""
    # Exclude index benchmarks (^NSEI, ^CRSLDX) that live in `stocks` for RS / theme math.
    with engine.connect() as conn:
        symbols = [r[0] for r in conn.execute(
            text("SELECT symbol FROM stocks WHERE is_active = TRUE AND symbol NOT LIKE '^%' ORDER BY symbol")
        ).fetchall()]
    logger.info(f"Loading prices for {len(symbols)} symbols (since={since or 'beginning'})...")

    frames = []
    date_clause = "AND date >= :since" if since is not None else ""
    sql = text(f"""
        SELECT symbol, date, close, volume
        FROM prices_daily
        WHERE symbol = ANY(:syms) {date_clause}
        ORDER BY symbol, date
    """)
    for i in range(0, len(symbols), SYMBOL_BATCH):
        batch = symbols[i:i + SYMBOL_BATCH]
        params = {"syms": batch} if since is None else {"syms": batch, "since": since}
        with engine.connect() as conn:
            frames.append(pd.read_sql(sql, conn, params=params))
        logger.info(f"  batch {i // SYMBOL_BATCH + 1}/{(len(symbols) + SYMBOL_BATCH - 1) // SYMBOL_BATCH}")
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logger.info(f"  {len(df):,} price rows loaded")
    return df


def _upsert(sql: str, rows: list[tuple], n_cols: int) -> int:
    """Bulk upsert with a trailing NOW() for computed_at."""
    if not rows:
        return 0
    template = "(" + ", ".join(["%s"] * n_cols) + ", NOW())"
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, template=template, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def _rebuild_monthly(engine) -> int:
    with engine.connect() as conn:
        daily = pd.read_sql(text("SELECT date, turnover_cr FROM market_breadth_daily ORDER BY date"), conn)
    if daily.empty:
        return 0
    monthly = compute_monthly_turnover(daily)
    rows = [
        (r["month"].date(), _clean(r["turnover_cr"]), int(r["trading_days"]), bool(r["is_partial"]))
        for _, r in monthly.iterrows()
    ]
    return _upsert(_UPSERT_MONTHLY, rows, 4)


def run_market_breadth_refresh(full: bool = False) -> int:
    """Compute + upsert market breadth. Returns number of daily rows written."""
    engine = get_engine()
    today = date.today()

    if full:
        since, publish_from = None, None
    else:
        publish_from = today - timedelta(days=PUBLISH_WINDOW_DAYS)
        since = publish_from - timedelta(days=WARMUP_DAYS)

    prices = _load_prices(engine, since)
    if prices.empty:
        logger.warning("No prices loaded — nothing to compute.")
        return 0

    logger.info("Computing breadth frame...")
    breadth = compute_breadth_frame(prices, min_stocks=MIN_STOCKS_PER_DATE)
    if publish_from is not None:
        breadth = breadth[breadth["date"] >= pd.Timestamp(publish_from)]
    if breadth.empty:
        logger.warning("Breadth frame empty after filtering — nothing to write.")
        return 0

    rows = [
        tuple(r["date"].date() if c == "date" else _clean(r[c]) for c in DAILY_COLS)
        for _, r in breadth.iterrows()
    ]
    _upsert(_UPSERT_DAILY, rows, len(DAILY_COLS))

    # Drop stale rows in the recomputed window (e.g. holiday ghost sessions that
    # an earlier run published before the ghost filter existed).
    window_start = breadth["date"].min().date()
    keep = [d.date() for d in breadth["date"]]
    with engine.begin() as conn:
        res = conn.execute(
            text("DELETE FROM market_breadth_daily WHERE date >= :start AND date <> ALL(:keep)"),
            {"start": window_start, "keep": keep},
        )
    if res.rowcount:
        logger.info(f"  Removed {res.rowcount} stale rows (non-session dates)")
    logger.info(f"  Upserted {len(rows)} market_breadth_daily rows "
                f"({breadth['date'].min().date()} → {breadth['date'].max().date()})")

    n_months = _rebuild_monthly(engine)
    logger.info(f"  Rebuilt {n_months} market_turnover_monthly rows")

    last = breadth.iloc[-1]
    logger.info(f"  Latest: {last['date'].date()}  mood={last['mood_score']} ({last['mood_label']})  "
                f"momentum={last['momentum_pct']}%  >200DMA={last['pct_above_200dma']}%  "
                f"OB/OS={int(last['overbought'])}/{int(last['oversold'])}")
    return len(rows)


if __name__ == "__main__":
    full = "--full" in sys.argv
    n = run_market_breadth_refresh(full=full)
    logger.info(f"Done — {n} rows ({'full' if full else 'incremental'})")

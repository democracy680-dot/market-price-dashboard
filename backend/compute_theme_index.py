"""
compute_theme_index.py — Pre-computes per-theme daily index levels.

Pure functions (unit-tested):
  compute_equal_weight_index / compute_mcap_weight_index / compute_theme_index_frame

Orchestrator (Task 4): run_theme_index_refresh()
"""
import logging
import pandas as pd
from datetime import datetime, timezone
from psycopg2.extras import execute_values
from sqlalchemy import text
from db import get_engine, get_psycopg2_conn

logger = logging.getLogger(__name__)


def compute_equal_weight_index(pivot: pd.DataFrame) -> pd.Series:
    """Equal-weight index level, base 100 at the first row.
    pivot: index=date (sorted), columns=symbol, values=close (NaN where missing).
    Chain-links the daily cross-sectional mean of member returns, so members with
    missing data or a mid-window listing contribute only on days they have a return.
    """
    pivot = pivot.sort_index()
    # fill_method=None: do NOT forward-fill. A member's trailing/leading NaN must
    # stay NaN so it is excluded that day (pandas 2.2 default would ffill, turning a
    # gap into a synthetic 0% return). Also avoids the pct_change FutureWarning.
    daily_ret = pivot.pct_change(fill_method=None)   # NaN unless a member has both days
    mean_ret = daily_ret.mean(axis=1)         # skipna=True: mean over members present
    idx = (1.0 + mean_ret.fillna(0.0)).cumprod() * 100.0
    return idx


def compute_mcap_weight_index(pivot: pd.DataFrame, shares: dict) -> pd.Series:
    """Market-cap-weight index level, base 100. shares: constant share counts per symbol.
    Chain-links cap-weighted daily returns over the set of members present on BOTH
    the day and the prior day (avoids jumps when a member appears/disappears).
    """
    pivot = pivot.sort_index()
    sh = pd.Series(shares).reindex(pivot.columns).fillna(0.0)
    mv = pivot.mul(sh, axis=1)                 # market value per member per day
    mv_prev = mv.shift(1)
    valid = mv.notna() & mv_prev.notna()
    num = mv.where(valid).sum(axis=1)
    den = mv_prev.where(valid).sum(axis=1)
    ret = (num / den) - 1.0
    ret = ret.replace([float("inf"), float("-inf")], 0.0)
    idx = (1.0 + ret.fillna(0.0)).cumprod() * 100.0
    return idx


def compute_theme_index_frame(closes_long: pd.DataFrame, shares: dict) -> pd.DataFrame:
    """closes_long: columns [symbol, date, close]. Returns [date, index_ew, index_mcap, n_members]."""
    pivot = closes_long.pivot(index="date", columns="symbol", values="close").sort_index()
    ew = compute_equal_weight_index(pivot)
    mcap = compute_mcap_weight_index(pivot, shares)
    n_members = pivot.notna().sum(axis=1)
    return pd.DataFrame({
        "date": pivot.index,
        "index_ew": ew.values,
        "index_mcap": mcap.values,
        "n_members": n_members.values.astype(int),
    })


LOOKBACK_DAYS = 400
_UPSERT_SQL = """
    INSERT INTO theme_index_daily (theme_slug, date, index_ew, index_mcap, n_members)
    VALUES %s
    ON CONFLICT (theme_slug, date) DO UPDATE SET
        index_ew   = EXCLUDED.index_ew,
        index_mcap = EXCLUDED.index_mcap,
        n_members  = EXCLUDED.n_members
"""


def _load_inputs(engine):
    """Returns (membership_df[theme_slug,symbol], closes_df[symbol,date,close], shares{symbol:float})."""
    with engine.connect() as conn:
        membership = pd.read_sql(text("SELECT theme_slug, symbol FROM theme_membership"), conn)
        closes = pd.read_sql(text(f"""
            SELECT p.symbol, p.date, p.close
            FROM prices_daily p
            WHERE p.date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} days'
              AND p.symbol IN (SELECT DISTINCT symbol FROM theme_membership)
            ORDER BY p.symbol, p.date
        """), conn)
        snap = pd.read_sql(text("""
            SELECT symbol, market_cap_cr, cmp
            FROM snapshots_daily
            WHERE date = (SELECT MAX(date) FROM snapshots_daily)
        """), conn)
    shares = {}
    for _, r in snap.iterrows():
        mc, cmp_ = r["market_cap_cr"], r["cmp"]
        if pd.notna(mc) and pd.notna(cmp_) and cmp_:
            shares[r["symbol"]] = float(mc) / float(cmp_)
    return membership, closes, shares


def run_theme_index_refresh() -> int:
    engine = get_engine()
    membership, closes, shares = _load_inputs(engine)
    if closes.empty or membership.empty:
        logger.warning("  theme index: no membership/price data — skipping")
        return 0

    rows = []
    for slug, grp in membership.groupby("theme_slug"):
        syms = set(grp["symbol"])
        sub = closes[closes["symbol"].isin(syms)]
        if sub.empty:
            continue
        frame = compute_theme_index_frame(sub, {s: shares.get(s, 0.0) for s in syms})
        for _, fr in frame.iterrows():
            d = fr["date"].date() if hasattr(fr["date"], "date") else fr["date"]
            rows.append((slug, d,
                         None if pd.isna(fr["index_ew"]) else float(fr["index_ew"]),
                         None if pd.isna(fr["index_mcap"]) else float(fr["index_mcap"]),
                         int(fr["n_members"])))

    if not rows:
        logger.warning("  theme index: nothing computed")
        return 0

    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    logger.info("  theme_index_daily: %d rows upserted", len(rows))
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    run_theme_index_refresh()

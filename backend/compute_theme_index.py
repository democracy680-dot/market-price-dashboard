"""
compute_theme_index.py — Pre-computes a daily index level for every theme.

Each theme index starts at 100 on its first trading day (full available history)
and is chain-linked from member returns:

  * Equal weight (headline): each day's move = average % change of members.
  * Market cap (secondary):  weights = prior-day market value, any single stock
                             capped at MCAP_WEIGHT_CAP (25%).
  * turnover_cr:             total ₹ crore traded in the theme's stocks that day.

Data hygiene (all pure + unit-tested):
  * Only real NSE sessions are used (the ^NSEI trading calendar), so Yahoo's
    flat "ghost" rows on exchange holidays are ignored.
  * A member missing up to FFILL_LIMIT sessions is carried forward, so its move
    is counted when it trades again instead of being dropped.
  * A single member's one-day move beyond ±MAX_ABS_RET (40%) is treated as a data
    error (unadjusted split/bonus, demerger, bad tick) and excluded that day.

The refresh is a full, idempotent recompute: each run deletes and rewrites every
theme's rows in one transaction, so no stale rows from older runs survive.
"""
import logging
import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from db import get_psycopg2_conn

logger = logging.getLogger(__name__)

MAX_ABS_RET = 0.40       # single-stock daily move treated as bad data beyond this
FFILL_LIMIT = 3          # sessions a missing member is carried forward
MCAP_WEIGHT_CAP = 0.25   # max weight of one stock in the market-cap index
BENCHMARK_CALENDAR = "^NSEI"


# ── Pure functions ────────────────────────────────────────────────────────────

def member_returns(pivot: pd.DataFrame, max_abs_ret: float = MAX_ABS_RET,
                   ffill_limit: int = FFILL_LIMIT) -> pd.DataFrame:
    """Daily returns per member (index=date, columns=symbol).

    Gaps of up to `ffill_limit` sessions *inside* a member's history are bridged
    (0% on the missing days, the full move on the day it trades again). Leading
    NaN (not yet listed) and trailing NaN (no price yet today) stay NaN, so the
    member simply doesn't contribute that day. Moves beyond ±max_abs_ret -> NaN.
    """
    pivot = pivot.sort_index()
    filled = pivot.ffill(limit=ffill_limit, limit_area="inside") if ffill_limit else pivot
    ret = filled.pct_change(fill_method=None)
    if max_abs_ret is not None:
        ret = ret.where(ret.abs() <= max_abs_ret)
    return ret


def compute_equal_weight_index(pivot: pd.DataFrame, max_abs_ret: float = MAX_ABS_RET,
                               ffill_limit: int = FFILL_LIMIT) -> pd.Series:
    """Equal-weight index level, base 100 at the first row.
    pivot: index=date (sorted), columns=symbol, values=close (NaN where missing).
    """
    ret = member_returns(pivot, max_abs_ret, ffill_limit)
    mean_ret = ret.mean(axis=1)                      # skipna: members present that day
    return (1.0 + mean_ret.fillna(0.0)).cumprod() * 100.0


def cap_weights(w: np.ndarray, cap: float | None) -> np.ndarray:
    """Normalise weights to sum 1 and cap any single weight at `cap`,
    redistributing the excess pro-rata to uncapped members. If the cap is
    infeasible (cap * n < 1) the effective cap becomes 1/n (equal weight)."""
    w = np.asarray(w, dtype=float)
    total = w.sum()
    if total <= 0 or len(w) == 0:
        return np.full(len(w), 1.0 / len(w)) if len(w) else w
    w = w / total
    if cap is None:
        return w
    cap = max(cap, 1.0 / len(w))
    for _ in range(len(w)):
        over = w > cap + 1e-12
        if not over.any():
            break
        excess = (w[over] - cap).sum()
        w[over] = cap
        free = ~over & (w < cap - 1e-12)
        if not free.any() or w[free].sum() <= 0:
            break
        w[free] += excess * w[free] / w[free].sum()
    return w


def compute_mcap_weight_index(pivot: pd.DataFrame, shares: dict, cap: float | None = None,
                              max_abs_ret: float = MAX_ABS_RET,
                              ffill_limit: int = FFILL_LIMIT) -> pd.Series:
    """Market-cap-weight index level, base 100. shares: constant share counts per symbol.
    Weights = prior-day market value of members with a valid return that day,
    optionally capped per stock. Members without a share count get no weight;
    if no member has weight on a day the index is flat that day."""
    pivot = pivot.sort_index()
    ret = member_returns(pivot, max_abs_ret, ffill_limit)
    filled = pivot.ffill(limit=ffill_limit, limit_area="inside") if ffill_limit else pivot
    sh = pd.Series(shares, dtype=float).reindex(pivot.columns).fillna(0.0)
    mv_prev = filled.shift(1).mul(sh, axis=1)
    r, mv = ret.to_numpy(), mv_prev.to_numpy()
    out = np.zeros(len(pivot))
    for t in range(len(pivot)):
        ok = ~np.isnan(r[t]) & ~np.isnan(mv[t]) & (mv[t] > 0)
        if ok.any():
            out[t] = float(np.dot(cap_weights(mv[t][ok], cap), r[t][ok]))
    return pd.Series((1.0 + out).cumprod() * 100.0, index=pivot.index)


def compute_theme_index_frame(long: pd.DataFrame, shares: dict, calendar=None,
                              cap: float | None = None) -> pd.DataFrame:
    """long: columns [symbol, date, close] (+ optional volume).
    calendar: optional iterable of valid session dates; rows on other dates are dropped
    (dates after the calendar's last date are kept, in case the benchmark lags).
    Returns [date, index_ew, index_mcap, n_members, turnover_cr]."""
    df = long.copy()
    if calendar is not None and len(calendar) > 0:
        cal = pd.DatetimeIndex(pd.to_datetime(list(calendar)))
        d = pd.to_datetime(df["date"])
        df = df[d.isin(cal) | (d > cal.max())]
    df["date"] = pd.to_datetime(df["date"])
    pivot = df.pivot(index="date", columns="symbol", values="close").sort_index()
    ret = member_returns(pivot)
    n_members = ret.notna().sum(axis=1)
    n_members.iloc[0] = int(pivot.iloc[0].notna().sum())
    if "volume" in df.columns:
        tv = (df["close"].astype(float) * df["volume"].fillna(0).astype(float) / 1e7)
        turnover = tv.groupby(df["date"]).sum().reindex(pivot.index).fillna(0.0)
    else:
        turnover = pd.Series(np.nan, index=pivot.index)
    return pd.DataFrame({
        "date": pivot.index,
        "index_ew": compute_equal_weight_index(pivot).values,
        "index_mcap": compute_mcap_weight_index(pivot, shares, cap=cap).values,
        "n_members": n_members.values.astype(int),
        "turnover_cr": turnover.values,
    })


# ── IO ────────────────────────────────────────────────────────────────────────

def _load_inputs(conn):
    """Returns (membership[theme_slug,symbol], prices[symbol,date,close,volume],
    shares{symbol: float}, calendar[list of dates])."""
    cur = conn.cursor()
    cur.execute("SELECT theme_slug, symbol FROM theme_membership")
    membership = pd.DataFrame(cur.fetchall(), columns=["theme_slug", "symbol"])
    symbols = sorted(membership["symbol"].unique().tolist())

    rows = []
    for i in range(0, len(symbols), 100):
        cur.execute("""SELECT symbol, date, close, volume FROM prices_daily
                       WHERE symbol = ANY(%s) AND close IS NOT NULL""", (symbols[i:i + 100],))
        rows += cur.fetchall()
    prices = pd.DataFrame(rows, columns=["symbol", "date", "close", "volume"])
    prices["close"] = prices["close"].astype(float)
    prices["volume"] = pd.to_numeric(prices["volume"], errors="coerce")

    # Share counts from each member's latest snapshot that has a market cap.
    cur.execute("""SELECT DISTINCT ON (symbol) symbol, market_cap_cr, cmp
                   FROM snapshots_daily
                   WHERE symbol = ANY(%s) AND market_cap_cr IS NOT NULL AND cmp > 0
                   ORDER BY symbol, date DESC""", (symbols,))
    shares = {s: float(mc) / float(c) for s, mc, c in cur.fetchall()}

    cur.execute("SELECT date FROM prices_daily WHERE symbol = %s ORDER BY date", (BENCHMARK_CALENDAR,))
    calendar = [r[0] for r in cur.fetchall()]
    cur.close()
    return membership, prices, shares, calendar


def run_theme_index_refresh() -> int:
    conn = get_psycopg2_conn()
    try:
        membership, prices, shares, calendar = _load_inputs(conn)
        if prices.empty or membership.empty:
            logger.warning("  theme index: no membership/price data — skipping")
            return 0
        if not calendar:
            logger.warning("  theme index: no %s calendar — using all price dates", BENCHMARK_CALENDAR)

        rows = []
        for slug, grp in membership.groupby("theme_slug"):
            syms = set(grp["symbol"])
            sub = prices[prices["symbol"].isin(syms)]
            if sub.empty:
                continue
            frame = compute_theme_index_frame(sub, {s: shares[s] for s in syms if s in shares},
                                              calendar=calendar or None, cap=MCAP_WEIGHT_CAP)
            for fr in frame.itertuples(index=False):
                rows.append((slug, fr.date.date(), float(fr.index_ew), float(fr.index_mcap),
                             int(fr.n_members),
                             None if pd.isna(fr.turnover_cr) else round(float(fr.turnover_cr), 4)))
        if not rows:
            logger.warning("  theme index: nothing computed")
            return 0

        with conn.cursor() as cur:
            cur.execute("DELETE FROM theme_index_daily")
            execute_values(cur, """
                INSERT INTO theme_index_daily
                    (theme_slug, date, index_ew, index_mcap, n_members, turnover_cr, computed_at)
                VALUES %s""", rows, template="(%s,%s,%s,%s,%s,%s,NOW())", page_size=1000)
        conn.commit()
        logger.info("  theme_index_daily: %d rows written for %d themes",
                    len(rows), membership["theme_slug"].nunique())
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    run_theme_index_refresh()

"""
compute_theme_index.py — Pre-computes per-theme daily index levels.

Pure functions (unit-tested):
  compute_equal_weight_index / compute_mcap_weight_index / compute_theme_index_frame

Orchestrator (Task 4): run_theme_index_refresh()
"""
import logging
import pandas as pd

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

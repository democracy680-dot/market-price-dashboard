"""
market_breadth.py — Pure, vectorised market-breadth math for the Market Mood tab.

No DB access here; compute_market_breadth.py is the orchestrator.

Input: a long DataFrame of prices_daily rows with columns
    symbol, date, close, volume
Output: one row per date with counts / percentages / composite mood score.

All stocks in the frame are treated as the universe (caller decides what to pass).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

RSI_PERIOD = 14
SMA_SHORT = 50
SMA_LONG = 200
ADV_RATIO_WINDOW = 20
OVERBOUGHT = 70.0
OVERSOLD = 30.0

MOOD_BEAR_MAX = 35.0   # score <  35 → Bear
MOOD_DULL_MAX = 65.0   # score <  65 → Dull, else Bull

MOVE_BUCKETS = (3, 5, 10, 20)   # ±3%, ±5%, ±10%, ±20%
HL_WINDOW = 252                 # sessions in a 52-week high/low lookback

# A date where more than this share of stocks closed exactly flat is not a real
# session (yfinance repeats the previous bar on exchange holidays).
GHOST_FLAT_SHARE = 0.5


# ── Indicator helpers ─────────────────────────────────────────────────────────

def wilder_rsi_series(closes: np.ndarray, period: int = RSI_PERIOD) -> np.ndarray:
    """
    Full RSI series (Wilder smoothing, SMA seed) — same convention as
    indicators.compute_rsi, but returns a value for every bar instead of only
    the last one. Bars before `period` are NaN. NaN closes propagate as NaN.
    """
    n = len(closes)
    out = np.full(n, np.nan)
    if n < period + 1:
        return out

    changes = np.diff(closes)
    gains = np.where(changes > 0, changes, 0.0)
    losses = np.where(changes < 0, -changes, 0.0)

    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))

    def _rsi(g, l):
        if g == 0.0 and l == 0.0:
            return 50.0
        if l == 0.0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + g / l)

    out[period] = _rsi(avg_gain, avg_loss)
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i + 1] = _rsi(avg_gain, avg_loss)
    return out


def drop_ghost_sessions(prices: pd.DataFrame, flat_share: float = GHOST_FLAT_SHARE) -> pd.DataFrame:
    """
    Remove exchange-holiday rows: dates where more than `flat_share` of the
    stocks that have a prior close closed exactly unchanged.
    Expects a sorted frame with numeric close. Returns a filtered copy.
    """
    if prices.empty:
        return prices
    chg = prices.groupby("symbol", sort=False)["close"].diff()
    flat = pd.DataFrame({"date": prices["date"], "flat": (chg == 0).astype(int), "has_prev": chg.notna().astype(int)})
    per_date = flat.groupby("date").sum()
    ghost = per_date.index[(per_date["has_prev"] > 0) & (per_date["flat"] / per_date["has_prev"].replace(0, np.nan) > flat_share)]
    if len(ghost) == 0:
        return prices
    return prices[~prices["date"].isin(ghost)].reset_index(drop=True)


def add_per_stock_indicators(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Add ret_1d, sma_50, sma_200, rsi_14, turnover_cr, new_high_52w,
    new_low_52w columns per symbol.
    Expects columns: symbol, date, close, volume; optional high, low (fall
    back to close when missing). Returns a sorted copy with holiday ghost
    sessions removed.
    """
    df = prices.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce").astype(float)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    for col in ("high", "low"):
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float).fillna(df["close"])
    df = df.dropna(subset=["close"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    df = drop_ghost_sessions(df)

    g = df.groupby("symbol", sort=False)["close"]
    df["ret_1d"] = g.pct_change()
    df["sma_50"] = g.transform(lambda s: s.rolling(SMA_SHORT, min_periods=SMA_SHORT).mean())
    df["sma_200"] = g.transform(lambda s: s.rolling(SMA_LONG, min_periods=SMA_LONG).mean())
    df["rsi_14"] = g.transform(lambda s: pd.Series(wilder_rsi_series(s.to_numpy()), index=s.index))
    df["turnover_cr"] = (df["close"] * df["volume"].fillna(0)) / 1e7

    # 52-week high / low: today's high (low) equals the max (min) of the last
    # HL_WINDOW sessions, today included. Needs a full window of history.
    gh = df.groupby("symbol", sort=False)["high"]
    gl = df.groupby("symbol", sort=False)["low"]
    roll_hi = gh.transform(lambda s: s.rolling(HL_WINDOW, min_periods=HL_WINDOW).max())
    roll_lo = gl.transform(lambda s: s.rolling(HL_WINDOW, min_periods=HL_WINDOW).min())
    df["hl_eligible"] = roll_hi.notna()
    df["new_high_52w"] = df["hl_eligible"] & (df["high"] >= roll_hi)
    df["new_low_52w"] = df["hl_eligible"] & (df["low"] <= roll_lo)
    return df


# ── Daily aggregation ─────────────────────────────────────────────────────────

def _pct(num: pd.Series, den: pd.Series) -> pd.Series:
    return (num / den.replace(0, np.nan) * 100).round(2)


def aggregate_daily(ind: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-stock indicator rows into one breadth row per date."""
    d = ind
    r = d["ret_1d"]

    flags = pd.DataFrame({
        "date": d["date"],
        "total_stocks": 1,
        "advances": (r > 0).astype(int),
        "declines": (r < 0).astype(int),
        "unchanged": (r == 0).astype(int),
        "dma50_count": d["sma_50"].notna().astype(int),
        "above_50dma": (d["close"] > d["sma_50"]).astype(int),
        "below_50dma": (d["close"] < d["sma_50"]).astype(int),
        "dma200_count": d["sma_200"].notna().astype(int),
        "above_200dma": (d["close"] > d["sma_200"]).astype(int),
        "below_200dma": (d["close"] < d["sma_200"]).astype(int),
        "rsi_count": d["rsi_14"].notna().astype(int),
        "overbought": (d["rsi_14"] > OVERBOUGHT).astype(int),
        "oversold": (d["rsi_14"] < OVERSOLD).astype(int),
        "rsi_above_50": (d["rsi_14"] > 50).astype(int),
        "hl_count": d["hl_eligible"].astype(int),
        "new_highs_52w": d["new_high_52w"].astype(int),
        "new_lows_52w": d["new_low_52w"].astype(int),
        "turnover_cr": d["turnover_cr"].fillna(0.0),
    })
    for b in MOVE_BUCKETS:
        flags[f"up_{b}"] = (r >= b / 100).astype(int)
        flags[f"down_{b}"] = (r <= -b / 100).astype(int)

    out = flags.groupby("date", sort=True).sum().reset_index()

    out["pct_above_50dma"] = _pct(out["above_50dma"], out["dma50_count"])
    out["pct_below_50dma"] = _pct(out["below_50dma"], out["dma50_count"])
    out["pct_above_200dma"] = _pct(out["above_200dma"], out["dma200_count"])
    out["pct_below_200dma"] = _pct(out["below_200dma"], out["dma200_count"])
    out["pct_rsi_above_50"] = _pct(out["rsi_above_50"], out["rsi_count"])

    adv_roll = out["advances"].rolling(ADV_RATIO_WINDOW, min_periods=ADV_RATIO_WINDOW).sum()
    dec_roll = out["declines"].rolling(ADV_RATIO_WINDOW, min_periods=ADV_RATIO_WINDOW).sum()
    out["adv_ratio_20d"] = _pct(adv_roll, adv_roll + dec_roll)

    out["turnover_cr"] = out["turnover_cr"].round(2)
    return out


# ── Composite mood ────────────────────────────────────────────────────────────

def mood_label(score) -> str | None:
    if score is None or pd.isna(score):
        return None
    if score < MOOD_BEAR_MAX:
        return "Bear"
    if score < MOOD_DULL_MAX:
        return "Dull"
    return "Bull"


def add_mood(daily: pd.DataFrame) -> pd.DataFrame:
    """
    mood_score = mean of the four breadth inputs that are available on that date.
    Requires at least 2 inputs to publish a score (early history has no 200 DMA).
    momentum_pct = pct_above_50dma.
    """
    out = daily.copy()
    inputs = out[["pct_above_200dma", "pct_above_50dma", "adv_ratio_20d", "pct_rsi_above_50"]]
    avail = inputs.notna().sum(axis=1)
    out["mood_score"] = inputs.mean(axis=1, skipna=True).where(avail >= 2).round(2)
    out["mood_label"] = out["mood_score"].map(mood_label)
    out["momentum_pct"] = out["pct_above_50dma"]
    return out


def compute_breadth_frame(prices: pd.DataFrame, min_stocks: int = 1) -> pd.DataFrame:
    """End-to-end: prices (symbol, date, close, volume) → breadth rows per date."""
    if prices.empty:
        return pd.DataFrame()
    ind = add_per_stock_indicators(prices)
    daily = aggregate_daily(ind)
    daily = daily[daily["total_stocks"] >= min_stocks].reset_index(drop=True)
    return add_mood(daily)


# ── Monthly turnover ──────────────────────────────────────────────────────────

def compute_monthly_turnover(daily: pd.DataFrame, as_of=None) -> pd.DataFrame:
    """
    Sum daily turnover_cr into calendar months.
    `is_partial` marks the month containing `as_of` (default: last date in frame).
    """
    if daily.empty:
        return pd.DataFrame(columns=["month", "turnover_cr", "trading_days", "is_partial"])
    d = daily[["date", "turnover_cr"]].copy()
    d["date"] = pd.to_datetime(d["date"])
    d["month"] = d["date"].dt.to_period("M").dt.to_timestamp()
    m = d.groupby("month").agg(turnover_cr=("turnover_cr", "sum"),
                               trading_days=("date", "count")).reset_index()
    last = pd.to_datetime(as_of) if as_of is not None else d["date"].max()
    m["is_partial"] = m["month"] == last.to_period("M").to_timestamp()
    m["turnover_cr"] = m["turnover_cr"].round(2)
    return m

"""
Unit tests for market_breadth.py pure functions.
Run: pytest backend/tests/test_market_breadth.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import pytest

from indicators import compute_rsi
from market_breadth import (
    drop_ghost_sessions,
    wilder_rsi_series,
    add_per_stock_indicators,
    aggregate_daily,
    add_mood,
    mood_label,
    compute_breadth_frame,
    compute_monthly_turnover,
)


def _prices(symbol, closes, start="2025-01-01", volume=1_000_000):
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"symbol": symbol, "date": dates, "close": closes, "volume": volume})


# ── RSI series matches the single-value reference implementation ─────────────

def test_rsi_series_last_value_matches_compute_rsi():
    rng = np.random.default_rng(7)
    closes = 100 + np.cumsum(rng.normal(0, 1, 120))
    series = wilder_rsi_series(closes)
    assert np.isnan(series[:14]).all()
    assert series[14] == pytest.approx(compute_rsi(list(closes[:15])))
    assert series[-1] == pytest.approx(compute_rsi(list(closes)))
    # every intermediate point equals the reference computed on the prefix
    for i in (20, 57, 99):
        assert series[i] == pytest.approx(compute_rsi(list(closes[: i + 1])))


def test_rsi_series_too_short_is_all_nan():
    assert np.isnan(wilder_rsi_series(np.arange(10, dtype=float))).all()


# ── Daily move buckets ────────────────────────────────────────────────────────

def test_move_buckets_and_advance_decline():
    # Day 2 returns: A +3.5%, B -5%, C 0%, D +25%
    df = pd.concat([
        _prices("A", [100, 103.5]),
        _prices("B", [100, 95]),
        _prices("C", [100, 100]),
        _prices("D", [100, 125]),
    ])
    out = aggregate_daily(add_per_stock_indicators(df))
    d2 = out.iloc[1]
    assert d2["total_stocks"] == 4
    assert d2["advances"] == 2 and d2["declines"] == 1 and d2["unchanged"] == 1
    assert d2["up_3"] == 2 and d2["up_5"] == 1 and d2["up_10"] == 1 and d2["up_20"] == 1
    assert d2["down_3"] == 1 and d2["down_5"] == 1 and d2["down_10"] == 0
    # Day 1 has no prior close → no moves counted
    d1 = out.iloc[0]
    assert d1["advances"] == 0 and d1["declines"] == 0 and d1["up_3"] == 0


def test_holiday_ghost_session_is_dropped():
    # Day 2 repeats day 1 closes for every stock (yfinance holiday artefact)
    df = pd.concat([
        _prices("A", [100, 100, 104]),
        _prices("B", [50, 50, 49]),
        _prices("C", [10, 10, 10]),
    ])
    ind = add_per_stock_indicators(df)
    assert ind["date"].nunique() == 2
    assert pd.Timestamp("2025-01-02") not in set(ind["date"])
    out = aggregate_daily(ind)
    # Day 3 return is measured against day 1 (the last real session)
    assert out.iloc[-1]["up_3"] == 1 and out.iloc[-1]["declines"] == 1 and out.iloc[-1]["unchanged"] == 1


def test_real_flat_session_is_kept():
    # Only 1 of 3 stocks flat → real session
    df = pd.concat([_prices("A", [100, 101]), _prices("B", [50, 49]), _prices("C", [10, 10])])
    df["date"] = pd.to_datetime(df["date"])
    kept = drop_ghost_sessions(df.sort_values(["symbol", "date"]).reset_index(drop=True))
    assert kept["date"].nunique() == 2


# ── DMA breadth ───────────────────────────────────────────────────────────────

def test_dma_breadth_percentages():
    n = 210
    up = np.linspace(100, 200, n)      # rising → above both DMAs
    down = np.linspace(200, 100, n)    # falling → below both DMAs
    df = pd.concat([_prices("UP", up), _prices("DN", down), _prices("SHORT", [100, 101, 102])])
    out = aggregate_daily(add_per_stock_indicators(df))
    last = out.iloc[-1]
    assert last["dma50_count"] == 2 and last["dma200_count"] == 2
    assert last["pct_above_50dma"] == 50.0 and last["pct_below_50dma"] == 50.0
    assert last["pct_above_200dma"] == 50.0 and last["pct_below_200dma"] == 50.0
    # before 200 bars no 200-DMA breadth is published
    assert pd.isna(out.iloc[100]["pct_above_200dma"])
    assert out.iloc[100]["pct_above_50dma"] == 50.0


# ── RSI breadth ───────────────────────────────────────────────────────────────

def test_overbought_oversold_counts():
    n = 40
    df = pd.concat([
        _prices("OB", np.linspace(100, 180, n)),   # straight up → RSI 100
        _prices("OS", np.linspace(180, 100, n)),   # straight down → RSI 0
        _prices("FLAT", [100.0] * n),              # RSI 50 → neither
    ])
    last = aggregate_daily(add_per_stock_indicators(df)).iloc[-1]
    assert last["rsi_count"] == 3
    assert last["overbought"] == 1 and last["oversold"] == 1
    assert last["rsi_above_50"] == 1
    assert last["pct_rsi_above_50"] == pytest.approx(33.33, abs=0.01)


# ── Turnover ──────────────────────────────────────────────────────────────────

def test_turnover_in_crores_and_monthly_rollup():
    df = pd.concat([
        _prices("A", [100, 125, 100], start="2025-01-30", volume=[1_000_000, 800_000, 1_000_000]),   # 10 cr/day
        _prices("B", [50, 40, 50], start="2025-01-30", volume=[2_000_000, 2_500_000, 2_000_000]),    # 10 cr/day
    ])
    daily = aggregate_daily(add_per_stock_indicators(df))
    assert (daily["turnover_cr"] == 20.0).all()
    monthly = compute_monthly_turnover(daily)
    # 30 Jan, 31 Jan → Jan; 3 Feb → Feb (partial, since as_of = last date)
    assert list(monthly["month"].dt.strftime("%Y-%m")) == ["2025-01", "2025-02"]
    assert monthly.iloc[0]["turnover_cr"] == 40.0 and monthly.iloc[0]["trading_days"] == 2
    assert monthly.iloc[1]["turnover_cr"] == 20.0
    assert bool(monthly.iloc[0]["is_partial"]) is False
    assert bool(monthly.iloc[1]["is_partial"]) is True


# ── Mood score ────────────────────────────────────────────────────────────────

def test_mood_label_thresholds():
    assert mood_label(10) == "Bear"
    assert mood_label(34.99) == "Bear"
    assert mood_label(35) == "Dull"
    assert mood_label(64.99) == "Dull"
    assert mood_label(65) == "Bull"
    assert mood_label(None) is None
    assert mood_label(float("nan")) is None


def test_mood_score_is_mean_of_available_inputs():
    daily = pd.DataFrame({
        "pct_above_200dma": [np.nan, 80.0],
        "pct_above_50dma":  [20.0, 60.0],
        "adv_ratio_20d":    [np.nan, 70.0],
        "pct_rsi_above_50": [30.0, 90.0],
    })
    out = add_mood(daily)
    assert out.iloc[0]["mood_score"] == 25.0 and out.iloc[0]["mood_label"] == "Bear"
    assert out.iloc[1]["mood_score"] == 75.0 and out.iloc[1]["mood_label"] == "Bull"
    assert out.iloc[1]["momentum_pct"] == 60.0


def test_mood_score_needs_two_inputs():
    daily = pd.DataFrame({
        "pct_above_200dma": [np.nan], "pct_above_50dma": [np.nan],
        "adv_ratio_20d": [np.nan], "pct_rsi_above_50": [40.0],
    })
    out = add_mood(daily)
    assert pd.isna(out.iloc[0]["mood_score"]) and out.iloc[0]["mood_label"] is None


def test_adv_ratio_20d_rolling():
    n = 30
    df = pd.concat([_prices("A", np.linspace(100, 130, n)), _prices("B", np.linspace(100, 70, n))])
    out = aggregate_daily(add_per_stock_indicators(df))
    assert pd.isna(out.iloc[18]["adv_ratio_20d"])
    assert out.iloc[-1]["adv_ratio_20d"] == 50.0


def test_compute_breadth_frame_min_stocks_and_empty():
    assert compute_breadth_frame(pd.DataFrame(columns=["symbol", "date", "close", "volume"])).empty
    df = pd.concat([_prices("A", [1, 2, 3]), _prices("B", [1, 2], start="2025-01-02")])
    out = compute_breadth_frame(df, min_stocks=2)
    # Only 02-Jan and 03-Jan have both stocks
    assert len(out) == 2


# ── 52-week new highs / lows ──────────────────────────────────────────────────

def _ohlc(symbol, closes, highs=None, lows=None, start="2024-01-01"):
    df = _prices(symbol, closes, start=start)
    if highs is not None:
        df["high"] = highs
    if lows is not None:
        df["low"] = lows
    return df


def test_new_highs_and_lows_need_full_window():
    n = 260
    up = np.linspace(100, 200, n)      # every bar from 252 onward is a new high
    down = np.linspace(200, 100, n)    # every bar from 252 onward is a new low
    flat = np.full(n, 100.0)
    df = pd.concat([_ohlc("UP", up), _ohlc("DN", down), _ohlc("FL", np.r_[flat[:-1], 99.0])])
    out = aggregate_daily(add_per_stock_indicators(df))
    # Before a full 252-session window nobody is eligible
    assert out.iloc[250]["hl_count"] == 0 and out.iloc[250]["new_highs_52w"] == 0
    last = out.iloc[-1]
    assert last["hl_count"] == 3
    assert last["new_highs_52w"] == 1          # UP
    assert last["new_lows_52w"] == 2           # DN, and FL closing at a fresh low of 99


def test_new_high_uses_intraday_high_not_close():
    n = 253
    closes = np.full(n, 100.0)
    highs = np.full(n, 101.0)
    highs[-1] = 105.0                  # intraday breakout, close unchanged
    lows = np.full(n, 99.0)
    lows[5] = 95.0                     # an older low inside the window, so A is not at a low today
    df = pd.concat([_ohlc("A", closes, highs, lows), _ohlc("B", np.linspace(100, 90, n))])
    last = aggregate_daily(add_per_stock_indicators(df)).iloc[-1]
    assert last["new_highs_52w"] == 1          # A via intraday high
    assert last["new_lows_52w"] == 1           # B falling (high/low fall back to close)

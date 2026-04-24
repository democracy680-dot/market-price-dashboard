"""
test_setup_detectors.py — 16 unit tests for setup pattern detectors.

All tests are standalone (no network, no DB).
Run with: pytest backend/tests/test_setup_detectors.py -v
"""

import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from setup_detectors import (
    detect_52w_retest,
    detect_consolidation,
    detect_ma_approach,
    detect_volume_dryup,
    detect_oversold_bounce,
    detect_bullish_divergence,
    detect_double_bottom,
    detect_macd_crossover_setup,
    detect_200dma_reclaim,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_df(closes, highs=None, lows=None, opens=None, volumes=None) -> pd.DataFrame:
    n = len(closes)
    return pd.DataFrame({
        "open":   opens   if opens   is not None else [c - 0.3 for c in closes],
        "high":   highs   if highs   is not None else [c + 1.0 for c in closes],
        "low":    lows    if lows    is not None else [c - 1.0 for c in closes],
        "close":  closes,
        "volume": volumes if volumes is not None else [1_000_000] * n,
    })


def trending_up_df(n, start=80.0, end=100.0, volume=1_000_000):
    closes = [start + (end - start) * i / (n - 1) for i in range(n)]
    return pd.DataFrame({
        "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes,
        "volume": [volume] * n,
    })


def trending_down_df(n, start=120.0, end=80.0, volume=1_000_000):
    closes = [start + (end - start) * i / (n - 1) for i in range(n)]
    return pd.DataFrame({
        "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes,
        "volume": [volume] * n,
    })


def make_tech(cmp=100.0, rsi=50.0, sma_50=95.0, sma_200=90.0,
              macd_line=-0.3, macd_signal=0.0, macd_histogram=-0.3,
              adx=15.0, sma_200_slope=0.1, volume_ratio=0.8,
              market_cap_cr=10000) -> dict:
    return {
        "cmp": cmp, "rsi_14": rsi, "sma_50": sma_50, "sma_200": sma_200,
        "macd_line": macd_line, "macd_signal": macd_signal,
        "macd_histogram": macd_histogram, "adx_14": adx,
        "sma_200_slope": sma_200_slope, "volume_ratio": volume_ratio,
        "market_cap_cr": market_cap_cr,
    }


# ── Test 1: 52W retest happy path ─────────────────────────────────────────────

def test_52w_retest_happy_path():
    """52W high 40+ days ago, real pullback to ~80, now recovered to 105.6 (4% below)."""
    n = 252
    closes = [95.0] * n
    highs  = [96.0] * n

    # 52W high at bar 210 (41 bars from end)
    highs[210]  = 110.0
    closes[210] = 110.0

    # Real pullback: bars 211-224 drop to ~79 (28% below 110)
    for i in range(14):
        closes[211 + i] = 110.0 - (i + 1) * 2.2   # 107.8 → 79.2
        highs[211 + i]  = closes[211 + i] + 1.0

    # Recovery: bars 225-251 at 105.6 (~4% below 110)
    for i in range(227, n):
        closes[i] = 105.6
        highs[i]  = 106.5

    lows = [c * 0.99 for c in closes]
    df = make_df(closes, highs=highs, lows=lows, volumes=[800_000] * n)
    tech = make_tech(cmp=105.6, sma_50=102.0)

    is_cand, strength, meta = detect_52w_retest(df, tech)
    assert is_cand, "Should detect 52W retest"
    assert strength >= 6, f"Expected strength >= 6, got {strength}"
    assert meta["trigger_level"] == pytest.approx(110.0, abs=0.1)


# ── Test 2: 52W high too recent ───────────────────────────────────────────────

def test_52w_retest_too_recent():
    """52W high made only 10 days ago — reject."""
    n = 252
    closes = [95.0] * n
    highs  = [96.0] * n

    highs[241]  = 110.0
    closes[241] = 110.0
    for i in range(242, n):
        closes[i] = 105.0
        highs[i]  = 106.0

    df = make_df(closes, highs=highs, lows=[c * 0.99 for c in closes],
                 volumes=[1_000_000] * n)
    is_cand, _, _ = detect_52w_retest(df, make_tech())
    assert not is_cand


# ── Test 3: Already broken out ────────────────────────────────────────────────

def test_52w_retest_already_broken_out():
    """CMP above 52W high — already broke out."""
    n = 252
    closes = [95.0] * n
    highs  = [96.0] * n

    highs[200]  = 110.0
    closes[200] = 110.0
    for i in range(201, n):
        closes[i] = 115.0
        highs[i]  = 116.0

    df = make_df(closes, highs=highs, lows=[c * 0.99 for c in closes],
                 volumes=[1_000_000] * n)
    is_cand, _, _ = detect_52w_retest(df, make_tech(cmp=115.0))
    assert not is_cand


# ── Test 4: Too far from 52W high ────────────────────────────────────────────

def test_52w_retest_too_far_from_high():
    """CMP 15% below 52W high — too far to be a retest."""
    n = 252
    closes = [95.0] * n
    highs  = [96.0] * n

    highs[200]  = 120.0
    closes[200] = 120.0
    for i in range(201, n):
        closes[i] = 102.0   # 15% below 120
        highs[i]  = 103.0

    df = make_df(closes, highs=highs, lows=[c * 0.99 for c in closes],
                 volumes=[1_000_000] * n)
    is_cand, _, _ = detect_52w_retest(df, make_tech(cmp=102.0))
    assert not is_cand


# ── Test 5: Consolidation happy path ─────────────────────────────────────────

def test_consolidation_happy_path():
    """25 prior days wide range, last 20 days tight range with declining volatility."""
    # 25 prior bars: wide ATR (need >= 24 TRs for _atr_20 to return a value)
    prior_closes = [100.0] * 25
    prior_highs  = [108.0] * 25
    prior_lows   = [92.0]  * 25

    # 20 recent bars: tight range, CMP in upper half
    recent_closes = [102.5] * 20
    recent_highs  = [103.5] * 20
    recent_lows   = [97.0]  * 20

    all_c = prior_closes + recent_closes
    all_h = prior_highs  + recent_highs
    all_l = prior_lows   + recent_lows

    df = pd.DataFrame({
        "open": all_c, "high": all_h, "low": all_l, "close": all_c,
        "volume": [1_200_000] * 25 + [700_000] * 20,
    })
    tech = make_tech(cmp=102.5, sma_50=99.0)

    is_cand, strength, meta = detect_consolidation(df, tech)
    assert is_cand, "Tight consolidation with declining ATR should fire"
    assert strength >= 6


# ── Test 6: Consolidation too volatile ───────────────────────────────────────

def test_consolidation_too_volatile():
    """20-day range of 18% — too wide to qualify."""
    df = pd.DataFrame({
        "open": [100.0] * 60, "high": [111.0] * 60,
        "low": [93.0] * 60, "close": [104.0] * 60,
        "volume": [1_000_000] * 60,
    })
    is_cand, _, _ = detect_consolidation(df, make_tech())
    assert not is_cand


# ── Test 7: MA approach below 200 DMA ────────────────────────────────────────

def test_ma_approach_below_200dma():
    """CMP 3% below 200 DMA, in an uptrend."""
    df = trending_up_df(260, start=90.0, end=97.0)
    tech = make_tech(cmp=97.0, sma_200=100.0, sma_50=95.0)

    is_cand, strength, meta = detect_ma_approach(df, tech)
    assert is_cand
    assert strength >= 7


# ── Test 8: MA approach in downtrend — reject ────────────────────────────────

def test_ma_approach_in_downtrend():
    """3% below 200 DMA but in a downtrend — reject."""
    df = trending_down_df(260, start=110.0, end=97.0)
    tech = make_tech(cmp=97.0, sma_200=100.0, sma_50=105.0)

    is_cand, _, _ = detect_ma_approach(df, tech)
    assert not is_cand


# ── Test 9: Volume dry-up happy path ─────────────────────────────────────────

def test_volume_dryup_happy_path():
    """Volume: 60d avg=2.275M, 20d avg=825K, 5d=300K. Price up ~4%. Clear dry-up."""
    # 80 bars total; last 60 bars have tiered volume: 40d@3M, 15d@1M, 5d@300K
    closes = [100.0 + i * (5.0 / 79) for i in range(80)]
    volumes = [3_000_000] * 60 + [1_000_000] * 15 + [300_000] * 5

    df = make_df(closes, volumes=volumes)
    tech = make_tech(cmp=closes[-1], rsi=50.0, adx=15.0)

    is_cand, strength, meta = detect_volume_dryup(df, tech)
    assert is_cand, "Volume dry-up with stable price should fire"
    assert strength >= 5


# ── Test 10: Volume dry-up but price crashing ────────────────────────────────

def test_volume_dryup_price_crashing():
    """Volume declining but price down 20% — not accumulation."""
    closes = [100.0 - 20.0 * i / 119 for i in range(120)]
    vols   = [2_000_000 - 1_000_000 * i / 119 for i in range(120)]

    df = make_df(closes, volumes=vols)
    is_cand, _, _ = detect_volume_dryup(df, make_tech())
    assert not is_cand


# ── Test 11: Oversold bounce happy path ──────────────────────────────────────

def test_oversold_bounce_happy_path():
    """58 bars of decline (RSI→0), then 2 green bars of +0.8 — RSI curls to ~20."""
    # 58 bars declining -0.5/bar: close goes 100 → 71.5
    closes = [100.0 - i * 0.5 for i in range(58)]
    # 2 recovery bars: each +0.8 (RSI turns up but stays < 30)
    closes.append(closes[-1] + 0.8)   # close[58] = 72.3
    closes.append(closes[-1] + 0.8)   # close[59] = 73.1

    highs   = [c + 0.3 for c in closes]
    lows    = [c - 0.6 for c in closes]
    opens   = [c - 0.5 for c in closes]    # open below close → green candle
    volumes = [1_000_000] * 60

    df = pd.DataFrame({
        "open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes
    })
    tech = make_tech(cmp=closes[-1], rsi=25.0, market_cap_cr=10000)

    is_cand, strength, meta = detect_oversold_bounce(df, tech)
    assert is_cand, "Deeply oversold with RSI curling up should fire"
    assert strength >= 5


# ── Test 12: Oversold but still falling ──────────────────────────────────────

def test_oversold_still_falling():
    """RSI < 30 but open > close on last bar (red candle) — reject."""
    closes = [100.0 - 0.5 * i for i in range(60)]
    df = pd.DataFrame({
        "open":   [c + 0.5 for c in closes],   # red candle
        "high":   [c + 0.6 for c in closes],
        "low":    [c - 0.3 for c in closes],
        "close":  closes,
        "volume": [1_000_000] * 60,
    })
    is_cand, _, _ = detect_oversold_bounce(df, make_tech(cmp=closes[-1], rsi=27.0))
    assert not is_cand


# ── Test 13: Bullish divergence happy path ───────────────────────────────────

def test_divergence_happy_path():
    """Price makes lower low; RSI at second low is higher (bounce history remembered)."""
    closes = []
    closes += [100.0] * 5                                          # flat
    for i in range(1, 21): closes.append(100.0 - i * 1.0)         # decline 99→80 (bars 5-24)
    for i in range(8):     closes.append(81.0 + i * 0.75)         # bounce 81→86.25 (bars 25-32)
    for i in range(20):    closes.append(86.25 - i * 0.421)       # decline 86.25→78.25 (bars 33-52)
    # Recovery starts ABOVE the second bottom (78.25) to create a strict local min
    closes.append(78.5)                                            # bar 53
    closes.append(78.8)                                            # bar 54
    closes.append(79.1)                                            # bar 55 (n=56 ≥ 55)

    # Verify overall downtrend: closes[-1]=79.1, closes[-40]=closes[16]≈84
    lows = [c - 1.0 for c in closes]
    df = make_df(closes, lows=lows, highs=[c + 1.0 for c in closes])
    tech = make_tech(cmp=closes[-1], rsi=38.0, macd_histogram=-0.1)

    is_cand, strength, meta = detect_bullish_divergence(df, tech)
    assert is_cand, "Bullish divergence: price lower low, RSI higher low should fire"
    assert strength >= 6


# ── Test 14: Double bottom happy path ────────────────────────────────────────

def test_double_bottom_happy_path():
    """Two distinct lows ~1.3% apart, 24 days apart; CMP between lows and neckline."""
    closes = [90.0] * 15
    # Drop to first bottom (79.5) — ensure prior bar is higher
    for i in range(15): closes.append(90.0 - i * 0.70)   # 90→80.5 (bars 15-29)
    closes.append(79.5)                                    # bar 30: first bottom
    # Bounce starts clearly above 79.5
    for i in range(10): closes.append(81.0 + i * 0.67)   # 81→87.7 (bars 31-40)
    # Second decline — end at 79.0 (close to 79.5, within 1%)
    for i in range(13): closes.append(87.7 - i * 0.67)   # 87.7→79.5 (bars 41-53)
    closes.append(79.0)                                    # bar 54: second bottom
    # Recovery starts clearly above 79.0
    for i in range(10): closes.append(80.5 + i * 0.5)    # 80.5→85 (bars 55-64)

    # Pad to 65 if needed
    while len(closes) < 65:
        closes.append(closes[-1])

    highs   = [c + 1.0 for c in closes]
    lows    = [c - 1.5 for c in closes]
    volumes = [1_200_000] * 31 + [800_000] * (len(closes) - 31)  # lower vol at 2nd bottom

    df = pd.DataFrame({
        "open": closes, "high": highs, "low": lows, "close": closes, "volume": volumes
    })
    tech = make_tech(cmp=closes[-1])

    is_cand, strength, meta = detect_double_bottom(df, tech)
    assert is_cand, "Double bottom with CMP between lows and neckline should fire"
    assert strength >= 5


# ── Test 15: MACD crossover setup (about to cross) ───────────────────────────

def test_macd_crossover_about_to_cross():
    """Accelerating decline creates MACD < signal; 2 tiny recovery bars close the gap."""
    # Accelerating decline: each bar's loss grows, driving MACD well below signal
    closes = [100.0 - i * 0.5 - (i / 10) ** 2 for i in range(70)]
    # Tiny recovery: 2 bars of +0.05 — MACD line starts rising, still below signal
    closes.append(closes[-1] + 0.05)
    closes.append(closes[-1] + 0.05)

    df = make_df(closes)
    # Technicals override: RSI=45 so we don't trip the rsi<=30 guard
    tech = make_tech(cmp=closes[-1], rsi=45.0, adx=18.0,
                     macd_line=-10.5, macd_signal=-10.1, macd_histogram=-0.4)

    is_cand, strength, meta = detect_macd_crossover_setup(df, tech)
    assert is_cand, "MACD approaching signal from below should fire"
    assert strength >= 4


# ── Test 16: 200 DMA reclaim happy path ──────────────────────────────────────

def test_200dma_reclaim_happy_path():
    """71 bars below 200 DMA (>=60), CMP ~2% below SMA200, recovered 21% from low."""
    # n=270: 160 flat at 110, then 110-bar pattern (55 decline 98→80, 55 recovery 80→96)
    # SMA200 computable from bar 199 onward; bars 199-269 (71 bars) all below SMA200.
    closes = [110.0] * 160
    for i in range(55): closes.append(98.0 - i * (18.0 / 54))   # 98→80
    for i in range(55): closes.append(80.0 + i * (16.0 / 54))   # 80→96

    cmp = closes[-1]
    actual_sma200 = sum(closes[-200:]) / 200

    df = make_df(closes, highs=[c + 1.0 for c in closes], lows=[c - 1.0 for c in closes])
    tech = make_tech(cmp=cmp, sma_200=actual_sma200, sma_200_slope=0.05,
                     rsi=52.0, macd_line=0.1)

    is_cand, strength, meta = detect_200dma_reclaim(df, tech)
    assert is_cand, (
        f"71 days below 200 DMA, within 5%, recovered 21% — should fire. "
        f"cmp={cmp:.2f} sma200={actual_sma200:.2f} "
        f"pct={(cmp-actual_sma200)/actual_sma200*100:.1f}%"
    )
    assert strength >= 5

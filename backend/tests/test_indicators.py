"""
test_indicators.py — Unit tests for the indicators.py math library.

Run from the repo root:
    pytest backend/tests/test_indicators.py -v

No network access required — all tests use synthetic price series.
"""

import sys
import os
import math

# Allow importing from backend/ without installing as a package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from indicators import (
    compute_rsi,
    compute_ema_series,
    compute_macd,
    compute_adx,
    compute_sma,
    compute_technical_status_v1,
    compute_sma_slope,
    compute_volume_ratio,
    compute_technical_status_v2,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _steady_up(n: int, start: float = 100.0, step: float = 1.0) -> list:
    """Strictly increasing price series."""
    return [start + i * step for i in range(n)]


def _steady_down(n: int, start: float = 200.0, step: float = 1.0) -> list:
    """Strictly decreasing price series."""
    return [start - i * step for i in range(n)]


def _flat(n: int, value: float = 100.0) -> list:
    """Constant price series."""
    return [value] * n


def _zigzag(n: int, amplitude: float = 2.0, base: float = 100.0) -> list:
    """Alternating up/down prices — models a sideways/choppy market."""
    return [base + amplitude * (1 if i % 2 == 0 else -1) for i in range(n)]


# ── RSI ───────────────────────────────────────────────────────────────────────

def test_rsi_known_values():
    """
    A strong uptrend should produce RSI near 100.
    A flat series should produce RSI == 50.
    """
    # 30-bar steady uptrend: every close is +1 → all gains, no losses → RSI ≈ 100
    up_series = _steady_up(30)
    rsi_up = compute_rsi(up_series)
    assert rsi_up is not None, "Expected a numeric RSI for uptrend series"
    assert rsi_up > 85, f"Uptrend RSI should be near 100, got {rsi_up:.2f}"

    # Flat series: no gains, no losses → both avg_gain and avg_loss reach 0 → RSI = 50
    flat_series = _flat(30)
    rsi_flat = compute_rsi(flat_series)
    assert rsi_flat == 50.0, f"Flat RSI should be exactly 50.0, got {rsi_flat}"


def test_rsi_no_losses():
    """
    An all-increasing series has avgLoss = 0.
    Must return exactly 100.0, not crash or return NaN.
    """
    series = _steady_up(50)
    rsi = compute_rsi(series)
    assert rsi == 100.0, f"All-gains RSI should be 100.0, got {rsi}"


def test_rsi_insufficient_data():
    """Fewer than period + 1 closes must return None."""
    assert compute_rsi([100.0] * 14) is None   # exactly period, not period+1
    assert compute_rsi([100.0] * 5)  is None


def test_rsi_all_declining():
    """
    All-declining series: avgGain stays 0, avgLoss > 0.
    RSI formula gives 0: RS = 0 → RSI = 100 - 100/(1+0) = 0.
    """
    series = _steady_down(30)
    rsi = compute_rsi(series)
    assert rsi is not None
    assert rsi < 15, f"All-declines RSI should be near 0, got {rsi:.2f}"


# ── MACD ─────────────────────────────────────────────────────────────────────

def test_macd_shape():
    """
    Feed 100 data points; assert line, signal, histogram are all finite floats.
    """
    closes = _steady_up(100, start=100.0, step=0.5)
    result = compute_macd(closes)
    assert result is not None, "Expected MACD dict for 100-bar series"
    assert set(result.keys()) == {"line", "signal", "histogram"}
    for key, val in result.items():
        assert isinstance(val, float), f"MACD[{key}] should be float, got {type(val)}"
        assert math.isfinite(val), f"MACD[{key}] should be finite, got {val}"


def test_macd_insufficient_data():
    """Fewer than 60 closes should return None."""
    assert compute_macd(_steady_up(59)) is None
    assert compute_macd(_steady_up(30)) is None


def test_macd_histogram_is_line_minus_signal():
    """Histogram must always equal line - signal (within floating-point tolerance)."""
    closes = _steady_up(120, start=50.0, step=0.3)
    result = compute_macd(closes)
    assert result is not None
    diff = abs(result["histogram"] - (result["line"] - result["signal"]))
    assert diff < 1e-9, f"Histogram != line - signal, diff={diff}"


# ── ADX ───────────────────────────────────────────────────────────────────────

def test_adx_range():
    """
    A strong trending series should produce ADX > 25.
    A choppy sideways series should produce ADX < 20.
    """
    n = 80

    # Strong trend: steadily rising prices
    highs  = _steady_up(n, start=105.0, step=1.0)
    lows   = _steady_up(n, start=98.0,  step=1.0)
    closes = _steady_up(n, start=100.0, step=1.0)
    result = compute_adx(highs, lows, closes)
    assert result is not None, "Expected ADX dict for strong trend"
    assert result["adx"] > 25, f"Strong trend ADX should be >25, got {result['adx']:.2f}"

    # Choppy market: zigzag prices
    highs2  = [v + 1.0 for v in _zigzag(n)]
    lows2   = [v - 1.0 for v in _zigzag(n)]
    closes2 = _zigzag(n)
    result2 = compute_adx(highs2, lows2, closes2)
    assert result2 is not None, "Expected ADX dict for sideways series"
    assert result2["adx"] < 25, f"Sideways ADX should be <25, got {result2['adx']:.2f}"


def test_adx_insufficient_data():
    """Fewer than 2*period+1 bars should return None."""
    n = 27   # needs >= 2*14+1 = 29
    highs  = _steady_up(n, start=105.0)
    lows   = _steady_up(n, start=98.0)
    closes = _steady_up(n, start=100.0)
    assert compute_adx(highs, lows, closes) is None


def test_adx_output_keys():
    """ADX result dict must contain adx, plus_di, minus_di."""
    n = 60
    highs  = _steady_up(n, start=105.0)
    lows   = _steady_up(n, start=98.0)
    closes = _steady_up(n, start=100.0)
    result = compute_adx(highs, lows, closes)
    assert result is not None
    assert "adx"      in result
    assert "plus_di"  in result
    assert "minus_di" in result


# ── SMA ───────────────────────────────────────────────────────────────────────

def test_sma_correctness():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert compute_sma(values, 3) == 4.0    # mean of [3,4,5]
    assert compute_sma(values, 5) == 3.0    # mean of all
    assert compute_sma(values, 6) is None   # insufficient


# ── Technical Status ──────────────────────────────────────────────────────────

def test_status_strong_buy():
    """
    Ideal bullish inputs:
      - CMP well above 50 DMA above 200 DMA  (+3)
      - MACD histogram > 0, line > 0          (+2)
      - ADX > 25 (amplifier)                  score * 1.3
      Total before amplification: 5 → after: int(5*1.3) = 6 → Strong Buy
    """
    score, label = compute_technical_status_v1(
        cmp=150.0, rsi=60.0,
        sma_50=140.0, sma_200=120.0,
        macd_line=2.5, macd_signal=2.0, macd_histogram=0.5,
        adx=30.0,
    )
    assert "Strong Buy" in label, f"Expected Strong Buy, got: {label}"
    assert score >= 5


def test_status_sell_avoid():
    """
    Bearish inputs:
      - CMP below both MAs (-2)
      - MACD histogram < 0, line < 0 (-2)
      Total: -4 → Sell / Avoid
    """
    score, label = compute_technical_status_v1(
        cmp=80.0, rsi=40.0,
        sma_50=95.0, sma_200=110.0,
        macd_line=-1.5, macd_signal=-1.0, macd_histogram=-0.5,
        adx=None,
    )
    assert "Sell" in label, f"Expected Sell/Avoid, got: {label}"
    assert score <= -3


def test_status_insufficient_data():
    """Any None argument (except adx) must return Insufficient Data."""
    score, label = compute_technical_status_v1(
        cmp=None, rsi=50.0,
        sma_50=100.0, sma_200=95.0,
        macd_line=0.5, macd_signal=0.3, macd_histogram=0.2,
        adx=20.0,
    )
    assert "Insufficient Data" in label
    assert score == 0

    score2, label2 = compute_technical_status_v1(
        cmp=100.0, rsi=None,
        sma_50=95.0, sma_200=90.0,
        macd_line=None, macd_signal=None, macd_histogram=None,
        adx=None,
    )
    assert "Insufficient Data" in label2


def test_status_overbought():
    """RSI >= 80 with a weak score < 2 → Overbought warning."""
    score, label = compute_technical_status_v1(
        cmp=105.0, rsi=85.0,
        sma_50=100.0, sma_200=110.0,   # below 200 DMA → score -2
        macd_line=0.1, macd_signal=0.05, macd_histogram=0.05,  # +1
        adx=None,
    )
    # score = -2 + 1 = -1 < 2, rsi >= 80 → Overbought
    assert "Overbought" in label


def test_status_adx_amplifies_bullish():
    """ADX > 25 should increase the score, not flip direction."""
    _, label_no_adx = compute_technical_status_v1(
        cmp=150.0, rsi=65.0,
        sma_50=140.0, sma_200=120.0,
        macd_line=1.0, macd_signal=0.8, macd_histogram=0.2,
        adx=None,
    )
    _, label_with_adx = compute_technical_status_v1(
        cmp=150.0, rsi=65.0,
        sma_50=140.0, sma_200=120.0,
        macd_line=1.0, macd_signal=0.8, macd_histogram=0.2,
        adx=35.0,
    )
    # Both should be bullish; with ADX the score is amplified (could be stronger label)
    bullish_labels = {
        "📈 Mild Bullish", "✅ Buy / Accumulate",
        "🚀 Strong Buy (Trend + Momentum)",
    }
    assert label_no_adx  in bullish_labels, f"Unexpected: {label_no_adx}"
    assert label_with_adx in bullish_labels, f"Unexpected: {label_with_adx}"


# ── SMA Slope ─────────────────────────────────────────────────────────────────

def test_sma_slope_rising():
    """Ascending SMA series should produce a positive slope."""
    # 22 values steadily rising: slope of last vs 21 bars ago is clearly > 0
    sma_series = _steady_up(22, start=100.0, step=0.5)
    slope = compute_sma_slope(sma_series, lookback=20)
    assert slope is not None
    assert slope > 0, f"Expected positive slope for rising SMA, got {slope}"


def test_sma_slope_falling():
    """Descending SMA series should produce a negative slope."""
    sma_series = _steady_down(22, start=200.0, step=0.5)
    slope = compute_sma_slope(sma_series, lookback=20)
    assert slope is not None
    assert slope < 0, f"Expected negative slope for falling SMA, got {slope}"


def test_sma_slope_insufficient():
    """Series shorter than lookback+1 must return None."""
    assert compute_sma_slope([100.0] * 20, lookback=20) is None   # exactly lookback, not +1
    assert compute_sma_slope([100.0] * 5,  lookback=20) is None
    assert compute_sma_slope(None,          lookback=20) is None


# ── Volume Ratio ──────────────────────────────────────────────────────────────

def test_volume_ratio_surge():
    """Flat prior volumes + 2x today should produce ratio ≈ 2.0."""
    # 20 prior days all at 1_000_000, today at 2_000_000
    volumes = [1_000_000] * 20 + [2_000_000]
    ratio = compute_volume_ratio(volumes, lookback=20)
    assert ratio is not None
    assert abs(ratio - 2.0) < 0.01, f"Expected ratio ≈ 2.0, got {ratio}"


def test_volume_ratio_zero_avg():
    """All-zero prior volumes must return None (avoid division by zero)."""
    volumes = [0] * 20 + [500_000]
    assert compute_volume_ratio(volumes, lookback=20) is None


# ── v2 Scoring ────────────────────────────────────────────────────────────────

def _bullish_kwargs(**overrides):
    """Base kwargs for a clear uptrend setup."""
    base = dict(
        cmp=150.0, rsi=60.0,
        sma_50=140.0, sma_200=120.0, sma_200_slope=2.0,
        macd_line=1.5, macd_signal=1.0, macd_histogram=0.5,
        adx=None, volume_ratio=None,
    )
    base.update(overrides)
    return base


def test_v2_vs_v1_agreement():
    """For a clear uptrend, both v1 and v2 should return bullish labels."""
    bullish_labels = {
        "📈 Mild Bullish", "✅ Buy / Accumulate",
        "🚀 Strong Buy (Trend + Momentum)",
    }
    _, v1_label = compute_technical_status_v1(
        cmp=150.0, rsi=60.0,
        sma_50=140.0, sma_200=120.0,
        macd_line=1.5, macd_signal=1.0, macd_histogram=0.5,
        adx=None,
    )
    _, v2_label = compute_technical_status_v2(**_bullish_kwargs())
    assert v1_label in bullish_labels, f"v1 not bullish: {v1_label}"
    assert v2_label in bullish_labels, f"v2 not bullish: {v2_label}"


def test_v2_slope_penalizes_weak_trend():
    """
    Stock above SMA200 but below SMA50, with a falling SMA200:
    v2 score should be lower (or equal at worst) than v1 score.
    """
    v1_score, _ = compute_technical_status_v1(
        cmp=125.0, rsi=50.0,
        sma_50=130.0, sma_200=120.0,
        macd_line=0.1, macd_signal=0.05, macd_histogram=0.05,
        adx=None,
    )
    v2_score, _ = compute_technical_status_v2(
        cmp=125.0, rsi=50.0,
        sma_50=130.0, sma_200=120.0, sma_200_slope=-2.0,  # falling MA
        macd_line=0.1, macd_signal=0.05, macd_histogram=0.05,
        adx=None, volume_ratio=None,
    )
    assert v2_score <= v1_score, (
        f"v2 score ({v2_score}) should be <= v1 score ({v1_score}) "
        "when SMA200 slope is falling"
    )


def test_v2_volume_rewards_confirmation():
    """Same inputs with vs without volume surge: v2 with surge > v2 without."""
    base = _bullish_kwargs(volume_ratio=None)
    surge = _bullish_kwargs(volume_ratio=2.0)

    score_no_surge, _ = compute_technical_status_v2(**base)
    score_surge, _    = compute_technical_status_v2(**surge)

    assert score_surge > score_no_surge, (
        f"Expected volume surge to boost score: {score_no_surge} → {score_surge}"
    )


def test_v2_volume_does_not_convert_neutral():
    """
    A neutral score (score == 0) with a volume surge must NOT become a buy signal.
    The volume bonus only applies when score > 0 or score < 0.
    """
    # Construct a truly neutral setup: CMP between SMA50 and SMA200, MACD near zero
    # CMP > SMA200 with falling slope → score contribution = 0.0
    # MACD histogram == 0 → no momentum contribution
    score, label = compute_technical_status_v2(
        cmp=125.0, rsi=50.0,
        sma_50=130.0, sma_200=120.0, sma_200_slope=-2.0,
        macd_line=0.0, macd_signal=0.0, macd_histogram=0.0,
        adx=None, volume_ratio=2.0,
    )
    buy_labels = {"✅ Buy / Accumulate", "🚀 Strong Buy (Trend + Momentum)", "📈 Mild Bullish"}
    assert label not in buy_labels, (
        f"Volume surge on neutral score should not produce buy signal, got: {label}"
    )

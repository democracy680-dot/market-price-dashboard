"""
indicators.py — Pure-Python technical indicator math library.

No database calls, no network access. Independently testable with pytest.

Functions:
    compute_rsi                  — 14-period Wilder's RSI
    compute_ema_series           — EMA series (seeded with SMA) used by MACD
    compute_macd                 — MACD line, signal, histogram
    compute_adx                  — ADX, +DI, -DI (Wilder's method)
    compute_sma                  — Simple moving average of last N values
    compute_technical_status_v1  — Original signal score + label (preserved for comparison)
    compute_sma_slope            — SMA200 percent change over N trading days
    compute_volume_ratio         — Today's volume / N-day average volume
    compute_technical_status_v2  — Improved score with slope awareness + volume confirmation
"""

import math


# ── RSI ───────────────────────────────────────────────────────────────────────

def compute_rsi(closes: list, period: int = 14):
    """
    Compute RSI using Wilder's smoothing.

    Edge cases handled:
      - avgLoss == 0, avgGain > 0  → 100.0  (avoids ZeroDivisionError)
      - avgGain == 0, avgLoss == 0 → 50.0   (flat price series)
      - len(closes) < period + 1   → None   (not enough data)

    Returns float or None.
    """
    if len(closes) < period + 1:
        return None

    # Day-over-day changes (one fewer element than closes)
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # Seed: simple average of first `period` gains and losses
    first_gains  = [max(0.0, c) for c in changes[:period]]
    first_losses = [max(0.0, -c) for c in changes[:period]]
    avg_gain = sum(first_gains)  / period
    avg_loss = sum(first_losses) / period

    # Subsequent values: Wilder's exponential smoothing
    for c in changes[period:]:
        gain = max(0.0, c)
        loss = max(0.0, -c)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    # Edge cases
    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ── EMA ───────────────────────────────────────────────────────────────────────

def compute_ema_series(values: list, period: int) -> list:
    """
    Compute a full EMA series seeded with the SMA of the first `period` values.

    Multiplier k = 2 / (period + 1).
    Returns a list starting from index (period - 1) of the input.
    Returns an empty list if len(values) < period.
    """
    if len(values) < period:
        return []

    k = 2.0 / (period + 1)

    # Seed
    ema = sum(values[:period]) / period
    result = [ema]

    for v in values[period:]:
        ema = v * k + ema * (1.0 - k)
        result.append(ema)

    return result


# ── MACD ──────────────────────────────────────────────────────────────────────

def compute_macd(closes: list):
    """
    Compute MACD(12, 26, 9).

    Returns {"line": float, "signal": float, "histogram": float}
    or None if insufficient data (requires at least ~60 data points to be stable).
    """
    if len(closes) < 60:
        return None

    ema12 = compute_ema_series(closes, 12)
    ema26 = compute_ema_series(closes, 26)

    # ema12 length = len(closes) - 11
    # ema26 length = len(closes) - 25
    # Align by truncating the front of ema12 so both series start at day 26
    offset = len(ema12) - len(ema26)   # = 26 - 12 = 14
    macd_line_series = [ema12[i + offset] - ema26[i] for i in range(len(ema26))]

    if len(macd_line_series) < 9:
        return None

    signal_series = compute_ema_series(macd_line_series, 9)
    if not signal_series:
        return None

    line      = macd_line_series[-1]
    signal    = signal_series[-1]
    histogram = line - signal

    if not all(math.isfinite(v) for v in [line, signal, histogram]):
        return None

    return {"line": line, "signal": signal, "histogram": histogram}


# ── ADX ───────────────────────────────────────────────────────────────────────

def compute_adx(highs: list, lows: list, closes: list, period: int = 14):
    """
    Compute ADX, +DI, -DI using Wilder's method.

    Returns {"adx": float, "plus_di": float, "minus_di": float}
    or None if insufficient data or division by zero.

    Requires at least (2 * period + 1) data points to produce a meaningful ADX.
    """
    n = len(closes)
    if n != len(highs) or n != len(lows):
        return None
    if n < period * 2 + 1:
        return None

    # ── Step 1: True Range, +DM, -DM for each bar (index 1 onward) ───────────
    tr_list       = []
    plus_dm_list  = []
    minus_dm_list = []

    for i in range(1, n):
        high       = highs[i];   prev_high  = highs[i - 1]
        low        = lows[i];    prev_low   = lows[i - 1]
        close      = closes[i];  prev_close = closes[i - 1]

        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))

        up_move   = high - prev_high
        down_move = prev_low - low

        plus_dm  = up_move   if (up_move   > down_move and up_move   > 0) else 0.0
        minus_dm = down_move if (down_move > up_move   and down_move > 0) else 0.0

        tr_list.append(tr)
        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)

    if len(tr_list) < period:
        return None

    # ── Step 2: Seed smoothed values with sum of first `period` values ────────
    smooth_tr       = sum(tr_list[:period])
    smooth_plus_dm  = sum(plus_dm_list[:period])
    smooth_minus_dm = sum(minus_dm_list[:period])

    def _di_pair(s_tr, s_pdm, s_mdm):
        """Return (+DI, -DI) or (None, None) on zero TR."""
        if s_tr == 0.0:
            return None, None
        return 100.0 * s_pdm / s_tr, 100.0 * s_mdm / s_tr

    # ── Step 3: Build DX series ───────────────────────────────────────────────
    dx_list = []

    # First DX from seeded values
    pdi, mdi = _di_pair(smooth_tr, smooth_plus_dm, smooth_minus_dm)
    if pdi is None:
        return None
    di_sum = pdi + mdi
    dx_list.append(100.0 * abs(pdi - mdi) / di_sum if di_sum > 0 else 0.0)

    for i in range(period, len(tr_list)):
        smooth_tr       = smooth_tr       - smooth_tr       / period + tr_list[i]
        smooth_plus_dm  = smooth_plus_dm  - smooth_plus_dm  / period + plus_dm_list[i]
        smooth_minus_dm = smooth_minus_dm - smooth_minus_dm / period + minus_dm_list[i]

        pdi, mdi = _di_pair(smooth_tr, smooth_plus_dm, smooth_minus_dm)
        if pdi is None:
            dx_list.append(0.0)
            continue

        di_sum = pdi + mdi
        dx_list.append(100.0 * abs(pdi - mdi) / di_sum if di_sum > 0 else 0.0)

    if len(dx_list) < period:
        return None

    # ── Step 4: ADX = Wilder-smoothed DX over `period` ───────────────────────
    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = (adx * (period - 1) + dx) / period

    # Final +DI / -DI from the last smoothed values
    if smooth_tr == 0.0:
        return None

    final_plus_di  = 100.0 * smooth_plus_dm  / smooth_tr
    final_minus_di = 100.0 * smooth_minus_dm / smooth_tr

    if not all(math.isfinite(v) for v in [adx, final_plus_di, final_minus_di]):
        return None

    return {"adx": adx, "plus_di": final_plus_di, "minus_di": final_minus_di}


# ── SMA ───────────────────────────────────────────────────────────────────────

def compute_sma(values: list, period: int):
    """
    Simple moving average of the last `period` values.
    Returns None if len(values) < period.
    """
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


# ── Technical Status v1 (preserved for rollout comparison) ───────────────────

def compute_technical_status_v1(
    cmp, rsi, sma_50, sma_200,
    macd_line, macd_signal, macd_histogram,
    adx,
) -> tuple:
    """
    Original scoring logic — preserved for comparison during v2 rollout.
    Do not modify this function.

    adx may be None (treated as "no trend strength info" — score is not amplified).
    All other inputs being None → returns (0, "⚪ Insufficient Data").

    Returns (score: int, label: str).
    """
    required = [cmp, rsi, sma_50, sma_200, macd_line, macd_signal, macd_histogram]
    if any(v is None for v in required):
        return (0, "⚪ Insufficient Data")

    score = 0

    # ── Trend: price vs MAs ───────────────────────────────────────────────────
    if cmp > sma_50 > sma_200:
        score += 3      # strong uptrend — above both, 50 above 200
    elif cmp > sma_200:
        score += 1      # long-term support holding
    elif cmp < sma_200:
        score -= 2      # long-term trend broken

    # ── Momentum: MACD ────────────────────────────────────────────────────────
    if macd_histogram > 0 and macd_line > 0:
        score += 2      # bullish crossover in positive territory
    elif macd_histogram > 0:
        score += 1      # crossover still below zero line
    elif macd_histogram < 0 and macd_line < 0:
        score -= 2      # bearish + negative territory
    elif macd_histogram < 0:
        score -= 1      # bearish crossover above zero

    # ── Trend strength: ADX amplifies existing signal ─────────────────────────
    # A strong trend (ADX > 25) amplifies the direction already computed above.
    # It does NOT flip direction — it just increases conviction.
    if adx is not None and adx > 25:
        score = int(score * 1.3)

    # ── RSI safety checks (applied after scoring, not as an early return) ─────
    # Overbought in a weak trend = risk.   Oversold in a weak downtrend = bounce candidate.
    # Neither overrides a fundamentally strong or weak trend — context matters.
    if rsi >= 80 and score < 2:
        return (score, "⚠️ Overbought – Risk of Pullback")
    if rsi <= 20 and score > -2:
        return (score, "🔥 Oversold – Possible Bounce")

    # ── Final verdict mapping ─────────────────────────────────────────────────
    if score >= 5:
        return (score, "🚀 Strong Buy (Trend + Momentum)")
    if score >= 3:
        return (score, "✅ Buy / Accumulate")
    if score >= 1:
        return (score, "📈 Mild Bullish")
    if score <= -3:
        return (score, "🔻 Sell / Avoid")
    if score <= -1:
        return (score, "📉 Mild Bearish")
    return (score, "⚖️ Neutral / Hold")


# ── SMA Slope ─────────────────────────────────────────────────────────────────

def compute_sma_slope(sma_series: list, lookback: int = 20):
    """
    Percent change in SMA from `lookback` bars ago to latest.
    Returns None if series too short or earlier value is zero.
    """
    if sma_series is None or len(sma_series) <= lookback:
        return None
    earlier = sma_series[-(lookback + 1)]
    latest = sma_series[-1]
    if earlier is None or latest is None or earlier == 0:
        return None
    return (latest - earlier) / earlier * 100.0


# ── Volume Ratio ──────────────────────────────────────────────────────────────

def compute_volume_ratio(volumes: list, lookback: int = 20):
    """
    Today's volume divided by the mean of the prior `lookback` sessions
    (not including today, to avoid self-bias).
    Returns None if insufficient data or average is zero.
    """
    if volumes is None or len(volumes) < lookback + 1:
        return None
    prior = volumes[-(lookback + 1):-1]
    prior_clean = [v for v in prior if v is not None and v > 0]
    if len(prior_clean) < lookback // 2:  # need at least half the window to be valid
        return None
    avg = sum(prior_clean) / len(prior_clean)
    if avg == 0:
        return None
    today = volumes[-1]
    if today is None or today <= 0:
        return None
    return today / avg


# ── Technical Status v2 ───────────────────────────────────────────────────────

def compute_technical_status_v2(
    cmp, rsi, sma_50, sma_200, sma_200_slope,
    macd_line, macd_signal, macd_histogram,
    adx, volume_ratio
) -> tuple:
    """
    v2 scoring: adds SMA200 slope awareness and volume confirmation.
    Returns (score, label). Score is numeric (allows for +0.5 granularity).
    """
    # Guard: if any core indicator is missing, return insufficient data
    if any(x is None for x in [cmp, rsi, sma_50, sma_200, macd_line, macd_signal, macd_histogram]):
        return (0, "⚪ Insufficient Data")

    score = 0.0

    # --- 1. TREND (with slope awareness) ---
    # v1 was: CMP>SMA50>SMA200 → +3; CMP>SMA200 → +1; CMP<SMA200 → -2
    # v2 refines the middle cases using slope
    if cmp > sma_50 > sma_200:
        # Golden alignment — but amplify only if slope is rising
        if sma_200_slope is not None and sma_200_slope > 1.0:
            score += 3.0  # full strong uptrend
        else:
            score += 2.0  # alignment but trend flattening
    elif cmp > sma_200:
        # Above long-term MA — quality depends on slope
        if sma_200_slope is not None:
            if sma_200_slope > 1.0:
                score += 1.5      # healthy drift up
            elif sma_200_slope < -1.0:
                score += 0.0      # drifting above a falling MA — low quality
            else:
                score += 0.5      # flat long-term trend
        else:
            score += 1.0  # slope unknown, fall back to v1 behavior
    elif cmp < sma_200:
        # Below long-term MA — slope determines severity
        if sma_200_slope is not None and sma_200_slope < -1.0:
            score -= 3.0      # below a falling MA = broken trend
        else:
            score -= 2.0      # below MA but trend may still be OK

    # --- 2. MOMENTUM (MACD) — unchanged from v1 ---
    if macd_histogram > 0 and macd_line > 0:
        score += 2.0
    elif macd_histogram > 0:
        score += 1.0
    elif macd_histogram < 0 and macd_line < 0:
        score -= 2.0
    elif macd_histogram < 0:
        score -= 1.0

    # --- 3. ADX amplifier — unchanged from v1 ---
    if adx is not None and adx > 25:
        score = score * 1.3

    # --- 4. VOLUME CONFIRMATION (new in v2) ---
    # Only rewards when volume confirms an already-meaningful move.
    # A +1.5x volume day on a neutral score should NOT be a buy signal.
    if volume_ratio is not None:
        if volume_ratio >= 1.5 and score > 0:
            score += 1.0          # confirms strength
        elif volume_ratio >= 1.5 and score < 0:
            score -= 1.0          # confirms weakness (heavy distribution)
        # weak volume (<0.5) doesn't change the score — just not a bonus

    # --- 5. RSI OVERRIDE (unchanged from v1) ---
    if rsi >= 80 and score < 2:
        return (score, "⚠️ Overbought – Risk of Pullback")
    if rsi <= 20 and score > -2:
        return (score, "🔥 Oversold – Possible Bounce")

    # --- 6. LABEL MAPPING ---
    # Note: thresholds kept at v1 values deliberately. With ADX multiplier AND
    # volume bonus, scores can reach ~6.5 in extreme cases, but the distribution
    # of real signals should remain roughly similar.
    if score >= 5:
        return (score, "🚀 Strong Buy (Trend + Momentum)")
    if score >= 3:
        return (score, "✅ Buy / Accumulate")
    if score >= 1:
        return (score, "📈 Mild Bullish")
    if score <= -3:
        return (score, "🔻 Sell / Avoid")
    if score <= -1:
        return (score, "📉 Mild Bearish")
    return (score, "⚖️ Neutral / Hold")

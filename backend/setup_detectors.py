"""
setup_detectors.py — Pattern detection functions for the Breakout & Reversal Scanner.

All functions share the signature:
    detect_<pattern>(prices_df, technicals_row) -> (bool, float, dict)

prices_df : DataFrame with columns [open, high, low, close, volume], chronological order,
            last 280 trading days. Index is arbitrary (date or int).
technicals_row : dict with latest values from technicals_daily for that stock.
                 Keys: rsi_14, macd_line, macd_signal, macd_histogram, adx_14,
                       sma_50, sma_200, sma_200_slope, volume_ratio, cmp, market_cap_cr

Returns:
    is_candidate (bool)
    setup_strength (float, 0-10)
    metadata (dict): trigger_level, pct_from_trigger, days_in_base, volume_ratio, notes
"""

import math
import pandas as pd

from indicators import compute_rsi, compute_macd, compute_adx, compute_ema_series, compute_sma


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe(val, default=None):
    """Return val if finite, else default."""
    if val is None:
        return default
    try:
        if math.isnan(val) or math.isinf(val):
            return default
    except TypeError:
        pass
    return val


def _vol_ratio(volumes: list, short=5, long=20) -> float | None:
    """short-window avg / long-window avg volume."""
    if len(volumes) < long:
        return None
    short_avg = sum(volumes[-short:]) / short
    long_avg  = sum(volumes[-long:])  / long
    if long_avg == 0:
        return None
    return short_avg / long_avg


def _rsi_series(closes: list, period: int = 14, n: int = 3) -> list:
    """Return list of last n RSI values (oldest first)."""
    result = []
    for offset in range(n - 1, -1, -1):
        sub = closes[:len(closes) - offset] if offset else closes
        result.append(compute_rsi(sub, period))
    return result


def _atr_20(highs, lows, closes):
    """Average True Range of the last (up to) 20 bars.
    Requires at least 15 TRs (16 input bars) to return a value."""
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < 15:
        return None
    return sum(trs[-20:]) / len(trs[-20:])


def _meta(**kwargs) -> dict:
    return {k: _safe(v) for k, v in kwargs.items()}


# ── 1. BR_52W_RETEST ──────────────────────────────────────────────────────────

def detect_52w_retest(prices_df: pd.DataFrame, technicals_row: dict):
    closes  = prices_df["close"].tolist()
    highs   = prices_df["high"].tolist()
    volumes = prices_df["volume"].tolist()
    n       = len(closes)

    if n < 60:
        return False, 0.0, {}

    window = min(252, n)
    sub_highs = highs[-window:]
    high_52w  = max(sub_highs)
    high_idx  = len(sub_highs) - 1 - sub_highs[::-1].index(high_52w)
    days_since = (window - 1) - high_idx

    # Must be at least 20 trading days ago
    if days_since < 20:
        return False, 0.0, {}

    cmp = closes[-1]

    # CMP must be within 0–7% below 52W high (not broken out, not too far)
    pct_from = (cmp - high_52w) / high_52w * 100
    if pct_from >= 0 or pct_from < -7:
        return False, 0.0, {}

    # Must have pulled back at least 5% from 52W high during the base
    base_lows  = [prices_df["low"].iloc[-(days_since):].min()]
    pullback   = (high_52w - min(base_lows)) / high_52w * 100
    if pullback < 5:
        return False, 0.0, {}

    # ── Strength scoring ──────────────────────────────────────────────────────
    score = 5.0

    if -3 <= pct_from < 0:
        score += 1
    if 30 <= days_since <= 90:
        score += 1
    if 10 <= pullback <= 20:
        score += 1

    # Declining 20-day volatility (coiling)
    if n >= 40:
        atr_recent = _atr_20(highs[-20:], prices_df["low"].tolist()[-20:], closes[-20:])
        atr_prior  = _atr_20(highs[-40:-20], prices_df["low"].tolist()[-40:-20], closes[-40:-20])
        if atr_recent and atr_prior and atr_recent < atr_prior * 0.9:
            score += 1

    vr = _vol_ratio(volumes)
    if vr and vr < 0.8:
        score += 1

    sma50 = _safe(technicals_row.get("sma_50"))
    if sma50 and cmp < sma50:
        score -= 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"52W High: {high_52w:.2f} ({days_since}d ago), "
        f"pulled back {pullback:.1f}%, now {pct_from:.1f}% away"
    )
    return True, score, _meta(
        trigger_level=high_52w,
        pct_from_trigger=round(pct_from, 2),
        days_in_base=days_since,
        volume_ratio=vr,
        notes=notes,
    )


# ── 2. BR_ATH_RETEST ─────────────────────────────────────────────────────────

def detect_ath_retest(prices_df: pd.DataFrame, technicals_row: dict):
    closes  = prices_df["close"].tolist()
    highs   = prices_df["high"].tolist()
    volumes = prices_df["volume"].tolist()
    n       = len(closes)

    if n < 60:
        return False, 0.0, {}

    ath        = max(highs)
    ath_idx    = highs.index(ath)
    days_since = (n - 1) - ath_idx

    # ATH must be made at least 20 trading days ago
    if days_since < 20:
        return False, 0.0, {}

    # If ATH is within last 252 days — prefer 52W_RETEST; skip here
    if days_since <= 252:
        return False, 0.0, {}

    cmp      = closes[-1]
    pct_from = (cmp - ath) / ath * 100
    if pct_from >= 0 or pct_from < -7:
        return False, 0.0, {}

    base_low  = min(prices_df["low"].tolist()[-days_since:])
    pullback  = (ath - base_low) / ath * 100
    if pullback < 5:
        return False, 0.0, {}

    score = 5.0

    if -3 <= pct_from < 0:
        score += 1
    if 30 <= days_since <= 90:
        score += 1
    if 10 <= pullback <= 20:
        score += 1

    # Older ATH = stronger structural resistance
    years_old = days_since / 252
    if years_old >= 2:
        score += 1

    if n >= 40:
        atr_recent = _atr_20(highs[-20:], prices_df["low"].tolist()[-20:], closes[-20:])
        atr_prior  = _atr_20(highs[-40:-20], prices_df["low"].tolist()[-40:-20], closes[-40:-20])
        if atr_recent and atr_prior and atr_recent < atr_prior * 0.9:
            score += 1

    vr = _vol_ratio(volumes)
    if vr and vr < 0.8:
        score += 1

    sma50 = _safe(technicals_row.get("sma_50"))
    if sma50 and cmp < sma50:
        score -= 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"ATH: {ath:.2f} ({days_since}d ago, {years_old:.1f}y), "
        f"pulled back {pullback:.1f}%, now {pct_from:.1f}% away"
    )
    return True, score, _meta(
        trigger_level=ath,
        pct_from_trigger=round(pct_from, 2),
        days_in_base=days_since,
        volume_ratio=vr,
        notes=notes,
    )


# ── 3. BR_CONSOLIDATION ───────────────────────────────────────────────────────

def detect_consolidation(prices_df: pd.DataFrame, technicals_row: dict):
    closes  = prices_df["close"].tolist()
    highs   = prices_df["high"].tolist()
    lows    = prices_df["low"].tolist()
    volumes = prices_df["volume"].tolist()
    n       = len(closes)

    if n < 40:
        return False, 0.0, {}

    # Last 20 days
    h20 = max(highs[-20:])
    l20 = min(lows[-20:])
    avg_c20 = sum(closes[-20:]) / 20
    if avg_c20 == 0:
        return False, 0.0, {}

    price_range = (h20 - l20) / avg_c20
    if price_range >= 0.08:
        return False, 0.0, {}

    # Volatility must be declining
    atr_recent = _atr_20(highs[-20:], lows[-20:], closes[-20:])
    atr_prior  = _atr_20(highs[-40:-20], lows[-40:-20], closes[-40:-20])
    if not atr_recent or not atr_prior or atr_recent >= atr_prior * 0.8:
        return False, 0.0, {}

    # CMP in upper half of 20-day range (bullish bias)
    cmp = closes[-1]
    mid = (h20 + l20) / 2
    if cmp < mid:
        return False, 0.0, {}

    score = 5.0

    # Try to measure consolidation length (up to 60 days)
    cons_len = 20
    if n >= 60:
        h60 = max(highs[-60:])
        l60 = min(lows[-60:])
        avg60 = sum(closes[-60:]) / 60
        if avg60 > 0 and (h60 - l60) / avg60 < 0.08:
            cons_len = 60

    if cons_len >= 30:
        score += 1
    if price_range < 0.05:
        score += 1

    vr = _vol_ratio(volumes)
    if vr and vr < 0.8:
        score += 1

    range_pos = (cmp - l20) / (h20 - l20) if h20 != l20 else 0.5
    if range_pos >= 0.75:
        score += 1

    sma50 = _safe(technicals_row.get("sma_50"))
    if sma50 and cmp > sma50:
        score += 1

    cmp_60d_ago = closes[-60] if n >= 60 else closes[0]
    if cmp > cmp_60d_ago:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"Range {price_range*100:.1f}% over {cons_len}d, "
        f"CMP in top {range_pos*100:.0f}% of range"
    )
    return True, score, _meta(
        trigger_level=h20,
        pct_from_trigger=round((cmp - h20) / h20 * 100, 2),
        days_in_base=cons_len,
        volume_ratio=vr,
        notes=notes,
    )


# ── 4. BR_MA_APPROACH ─────────────────────────────────────────────────────────

def detect_ma_approach(prices_df: pd.DataFrame, technicals_row: dict):
    closes = prices_df["close"].tolist()
    n      = len(closes)

    if n < 60:
        return False, 0.0, {}

    cmp    = closes[-1]
    sma50  = _safe(technicals_row.get("sma_50"))
    sma200 = _safe(technicals_row.get("sma_200"))
    rsi    = _safe(technicals_row.get("rsi_14"))

    # Must be in overall uptrend (price > 60 days ago)
    if cmp <= closes[-60]:
        return False, 0.0, {}

    # Prefer 200 DMA; fall back to 50 DMA
    use_200 = False
    target_ma = None

    if sma200 and 0 < (sma200 - cmp) / sma200 <= 0.05:
        target_ma = sma200
        use_200   = True
    elif sma50 and 0 < (sma50 - cmp) / sma50 <= 0.05:
        target_ma = sma50

    if target_ma is None:
        return False, 0.0, {}

    pct_from = (cmp - target_ma) / target_ma * 100  # negative (below MA)

    score = 5.0
    if use_200:
        score += 2
    if abs(pct_from) <= 2:
        score += 1

    rsi_series = _rsi_series(closes, n=3)
    if all(v is not None for v in rsi_series) and rsi_series[-1] > rsi_series[0]:
        score += 1

    cmp_120d = closes[-120] if n >= 120 else closes[0]
    if cmp > cmp_120d:
        score += 1

    # Days since CMP was last above the MA
    days_below = 0
    for c in reversed(closes[:-1]):
        if c >= target_ma:
            break
        days_below += 1

    score = max(0.0, min(10.0, score))

    ma_label = "200 DMA" if use_200 else "50 DMA"
    notes = f"Approaching {ma_label} from below ({pct_from:.1f}% away)"
    return True, score, _meta(
        trigger_level=target_ma,
        pct_from_trigger=round(pct_from, 2),
        days_in_base=days_below,
        notes=notes,
    )


# ── 5. BR_VOLUME_DRYUP ────────────────────────────────────────────────────────

def detect_volume_dryup(prices_df: pd.DataFrame, technicals_row: dict):
    closes  = prices_df["close"].tolist()
    volumes = prices_df["volume"].tolist()
    n       = len(closes)

    if n < 65:
        return False, 0.0, {}

    vol_5d  = sum(volumes[-5:])  / 5
    vol_20d = sum(volumes[-20:]) / 20
    vol_60d = sum(volumes[-60:]) / 60

    if vol_60d == 0 or vol_20d == 0:
        return False, 0.0, {}

    # 20-day avg must be declining vs 60-day avg
    if vol_20d >= vol_60d * 0.85:
        return False, 0.0, {}

    # 5-day avg must still be declining vs 20-day avg
    if vol_5d >= vol_20d * 0.9:
        return False, 0.0, {}

    # Price must be stable or slightly up (not crashing)
    price_change_60d = (closes[-1] - closes[-60]) / closes[-60] * 100
    if not (-5 <= price_change_60d <= 10):
        return False, 0.0, {}

    vol_decline_pct = (1 - vol_5d / vol_60d) * 100
    vr = vol_5d / vol_60d

    score = 4.0

    if vol_decline_pct > 30:
        score += 1
    if 2 <= price_change_60d <= 10:
        score += 1

    sma50  = _safe(technicals_row.get("sma_50"))
    sma200 = _safe(technicals_row.get("sma_200"))
    cmp    = closes[-1]
    if (sma50 and abs(cmp - sma50) / sma50 < 0.03) or \
       (sma200 and abs(cmp - sma200) / sma200 < 0.03):
        score += 1

    rsi = _safe(technicals_row.get("rsi_14"))
    if rsi and 40 <= rsi <= 60:
        score += 1

    adx = _safe(technicals_row.get("adx_14"))
    if adx and adx < 20:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"60d vol down {vol_decline_pct:.1f}%, "
        f"price change {price_change_60d:+.1f}%"
    )
    return True, score, _meta(
        volume_ratio=round(vr, 3),
        days_in_base=60,
        notes=notes,
    )


# ── 6. RV_OVERSOLD ────────────────────────────────────────────────────────────

def detect_oversold_bounce(prices_df: pd.DataFrame, technicals_row: dict):
    closes  = prices_df["close"].tolist()
    opens   = prices_df["open"].tolist()
    highs   = prices_df["high"].tolist()
    volumes = prices_df["volume"].tolist()
    n       = len(closes)

    if n < 30:
        return False, 0.0, {}

    rsi_vals = _rsi_series(closes, n=3)
    if any(v is None for v in rsi_vals):
        return False, 0.0, {}

    rsi_now, rsi_prev, rsi_prev2 = rsi_vals[2], rsi_vals[1], rsi_vals[0]

    # RSI must be < 30
    if rsi_now >= 30:
        return False, 0.0, {}

    # RSI must be curling up (consecutive improvement)
    if not (rsi_now > rsi_prev):
        return False, 0.0, {}

    # Today must be a bullish (green) candle
    if closes[-1] <= opens[-1]:
        return False, 0.0, {}

    # Must have fallen at least 15% from 60-day peak
    peak_60d = max(highs[-60:]) if n >= 60 else max(highs)
    drawdown = (peak_60d - closes[-1]) / peak_60d * 100
    if drawdown < 15:
        return False, 0.0, {}

    days_since_peak = next(
        (i for i, h in enumerate(reversed(highs), 1) if h == peak_60d), 0
    )

    score = 5.0

    if rsi_now < 25:
        score += 1
    if drawdown >= 25:
        score += 1

    vol_20d_avg = sum(volumes[-20:]) / 20 if n >= 20 else None
    if vol_20d_avg and volumes[-1] > vol_20d_avg:
        score += 1

    sma200 = _safe(technicals_row.get("sma_200"))
    if sma200 and abs(closes[-1] - sma200) / sma200 < 0.05:
        score += 1

    mcap = _safe(technicals_row.get("market_cap_cr"), 0)
    if mcap >= 5000:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"RSI {rsi_now:.1f} (curling up), "
        f"down {drawdown:.1f}% from {days_since_peak}d peak"
    )
    return True, score, _meta(
        trigger_level=peak_60d,
        pct_from_trigger=round((closes[-1] - peak_60d) / peak_60d * 100, 2),
        days_in_base=days_since_peak,
        notes=notes,
    )


# ── 7. RV_DIVERGENCE ─────────────────────────────────────────────────────────

def detect_bullish_divergence(prices_df: pd.DataFrame, technicals_row: dict):
    closes = prices_df["close"].tolist()
    lows   = prices_df["low"].tolist()
    n      = len(closes)

    if n < 55:
        return False, 0.0, {}

    window = closes[-40:]
    lows40 = lows[-40:]

    # Must be in overall downtrend
    if closes[-1] >= closes[-40]:
        return False, 0.0, {}

    # Find two distinct local lows at least 10 days apart
    def find_local_lows(low_series):
        """Return list of (idx, price) for local lows."""
        locs = []
        for i in range(1, len(low_series) - 1):
            if low_series[i] < low_series[i - 1] and low_series[i] < low_series[i + 1]:
                locs.append((i, low_series[i]))
        return locs

    local_lows = find_local_lows(lows40)
    if len(local_lows) < 2:
        return False, 0.0, {}

    # Pair: low_1 older, low_2 newer, at least 10 days apart
    pair = None
    for i in range(len(local_lows) - 1):
        for j in range(i + 1, len(local_lows)):
            idx1, p1 = local_lows[i]
            idx2, p2 = local_lows[j]
            if idx2 - idx1 >= 10 and p2 < p1:
                pair = (idx1, p1, idx2, p2)

    if not pair:
        return False, 0.0, {}

    idx1, p1, idx2, p2 = pair

    # Compute RSI at each local low using full price history up to that bar.
    # Using +1 (not +15) so we measure RSI exactly at the low bar, not 15 bars later
    # (which would land in the post-bounce decline and invert the divergence signal).
    base_offset = n - 40
    rsi1 = compute_rsi(closes[:base_offset + idx1 + 1])
    rsi2 = compute_rsi(closes[:base_offset + idx2 + 1])

    if rsi1 is None or rsi2 is None:
        return False, 0.0, {}

    # Bullish divergence: price lower low, RSI higher low
    if rsi2 <= rsi1:
        return False, 0.0, {}

    rsi_divergence = rsi2 - rsi1
    price_drop_pct = (p2 - p1) / p1 * 100

    score = 6.0

    if rsi_divergence > 10:
        score += 1
    if abs(price_drop_pct) >= 5:
        score += 1

    rsi_now = _safe(technicals_row.get("rsi_14"))
    if rsi_now and rsi_now > 35:
        score += 1

    vol_20d_avg = sum(prices_df["volume"].tolist()[-20:]) / 20
    if prices_df["volume"].tolist()[-1] > vol_20d_avg:
        score += 1

    macd_hist = _safe(technicals_row.get("macd_histogram"))
    macd_hist_prev = None
    if n >= 2:
        closes_prev = closes[:-1]
        m = compute_macd(closes_prev)
        if m:
            macd_hist_prev = m["histogram"]
    if macd_hist is not None and macd_hist_prev is not None and macd_hist > macd_hist_prev:
        score += 1

    score = max(0.0, min(10.0, score))

    days_in_base = idx2 - idx1

    notes = (
        f"Bullish divergence: price {p1:.2f}→{p2:.2f}, "
        f"RSI {rsi1:.1f}→{rsi2:.1f}"
    )
    return True, score, _meta(
        trigger_level=p1,
        days_in_base=days_in_base,
        notes=notes,
    )


# ── 8. RV_DOUBLE_BOTTOM ───────────────────────────────────────────────────────

def detect_double_bottom(prices_df: pd.DataFrame, technicals_row: dict):
    closes = prices_df["close"].tolist()
    lows   = prices_df["low"].tolist()
    highs  = prices_df["high"].tolist()
    n      = len(closes)

    if n < 65:
        return False, 0.0, {}

    lows60 = lows[-60:]
    vols60 = prices_df["volume"].tolist()[-60:]

    def find_local_lows_idx(low_series):
        result = []
        for i in range(1, len(low_series) - 1):
            if low_series[i] < low_series[i - 1] and low_series[i] < low_series[i + 1]:
                result.append(i)
        return result

    idxs = find_local_lows_idx(lows60)
    if len(idxs) < 2:
        return False, 0.0, {}

    # Find best pair: within 3-5% of each other, 15+ days apart
    best = None
    for i in range(len(idxs) - 1):
        for j in range(i + 1, len(idxs)):
            idx1, idx2 = idxs[i], idxs[j]
            p1, p2 = lows60[idx1], lows60[idx2]
            gap_days = idx2 - idx1
            if gap_days < 15:
                continue
            pct_diff = abs(p1 - p2) / max(p1, p2) * 100
            if pct_diff > 5:
                continue
            best = (idx1, p1, idx2, p2)

    if not best:
        return False, 0.0, {}

    idx1, p1, idx2, p2 = best
    gap_days = idx2 - idx1

    # Neckline = highest high between the two lows
    neckline = max(highs[-60:][idx1:idx2 + 1])
    cmp = closes[-1]

    # CMP should be between the lows and neckline (inside the pattern)
    bottom = min(p1, p2)
    if not (bottom <= cmp <= neckline):
        return False, 0.0, {}

    pct_from = (cmp - neckline) / neckline * 100

    # RSI at each bottom for divergence check
    base_offset = n - 60
    rsi1 = compute_rsi(closes[:base_offset + idx1 + 15])
    rsi2 = compute_rsi(closes[:base_offset + idx2 + 15])
    has_rsi_div = (rsi1 is not None and rsi2 is not None and rsi2 > rsi1)

    # Volume at 2nd bottom vs 1st
    vol1 = vols60[idx1]
    vol2 = vols60[idx2]
    vol_exhaustion = vol2 < vol1

    score = 5.0

    pct_diff = abs(p1 - p2) / max(p1, p2) * 100
    if pct_diff <= 2:
        score += 1
    if 20 <= gap_days <= 40:
        score += 1
    if vol_exhaustion:
        score += 1

    # Prior peak (before the first low)
    prior_high = max(highs[-60:][:idx1 + 1]) if idx1 > 0 else highs[-60:][0]
    prior_drawdown = (prior_high - bottom) / prior_high * 100
    if prior_drawdown >= 20:
        score += 1
    if has_rsi_div:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"Double bottom at {p1:.2f}/{p2:.2f} ({gap_days}d apart), "
        f"neckline {neckline:.2f}"
    )
    return True, score, _meta(
        trigger_level=neckline,
        pct_from_trigger=round(pct_from, 2),
        days_in_base=idx1,
        notes=notes,
    )


# ── 9. RV_MACD_CROSS ─────────────────────────────────────────────────────────

def detect_macd_crossover_setup(prices_df: pd.DataFrame, technicals_row: dict):
    closes = prices_df["close"].tolist()
    n      = len(closes)

    if n < 70:
        return False, 0.0, {}

    macd_now = compute_macd(closes)
    if not macd_now:
        return False, 0.0, {}

    macd_3d  = compute_macd(closes[:-3]) if n > 73 else None

    line    = macd_now["line"]
    signal  = macd_now["signal"]
    hist    = macd_now["histogram"]

    # MACD line must be below signal (not yet crossed)
    if line >= signal:
        return False, 0.0, {}

    # Gap must be shrinking vs 3 days ago
    gap_now  = signal - line
    if macd_3d:
        gap_3d = macd_3d["signal"] - macd_3d["line"]
        if gap_now >= gap_3d:
            return False, 0.0, {}

    # Histogram must be less negative (improving)
    if macd_3d and hist <= macd_3d["histogram"]:
        return False, 0.0, {}

    # RSI > 30 (not deeply oversold — that's RV_OVERSOLD territory)
    rsi = _safe(technicals_row.get("rsi_14"))
    if rsi is not None and rsi <= 30:
        return False, 0.0, {}

    score = 4.0

    if gap_now <= 0.5:
        score += 2  # very close to crossing

    # Count consecutive days of rising histogram
    hist_series = []
    for lookback in range(1, 6):
        m = compute_macd(closes[:-lookback] if lookback < n else closes)
        hist_series.append(m["histogram"] if m else None)
    hist_series.reverse()
    rising_days = 0
    for i in range(1, len(hist_series)):
        if hist_series[i] is not None and hist_series[i - 1] is not None:
            if hist_series[i] > hist_series[i - 1]:
                rising_days += 1
            else:
                break
    if rising_days >= 3:
        score += 1

    if line < 0 and signal < 0:
        score += 1  # both in negative territory — early reversal

    adx = _safe(technicals_row.get("adx_14"))
    if adx and adx < 20:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = f"MACD: {line:.3f}, Signal: {signal:.3f}, gap {gap_now:.3f} (closing)"
    return True, score, _meta(
        trigger_level=signal,
        notes=notes,
    )


# ── 10. RV_200DMA_RECLAIM ────────────────────────────────────────────────────

def detect_200dma_reclaim(prices_df: pd.DataFrame, technicals_row: dict):
    closes = prices_df["close"].tolist()
    n      = len(closes)

    if n < 220:
        return False, 0.0, {}

    sma200 = _safe(technicals_row.get("sma_200"))
    if not sma200:
        return False, 0.0, {}

    cmp = closes[-1]

    # Must be below 200 DMA
    if cmp >= sma200:
        return False, 0.0, {}

    # Must be within 5% of 200 DMA
    pct_from = (cmp - sma200) / sma200 * 100
    if pct_from < -5:
        return False, 0.0, {}

    # Count consecutive days below 200 DMA
    sma200_series = []
    for i in range(len(closes) - 1, max(len(closes) - 250, -1), -1):
        sma_i = compute_sma(closes[:i + 1], 200)
        if sma_i is None:
            break
        if closes[i] < sma_i:
            sma200_series.append(i)
        else:
            break

    days_below = len(sma200_series)
    if days_below < 60:
        return False, 0.0, {}

    # Must have recovered at least 10% from the low of the last 60 days
    low_60d = min(prices_df["low"].tolist()[-60:])
    recovery = (cmp - low_60d) / low_60d * 100
    if recovery < 10:
        return False, 0.0, {}

    score = 5.0

    if abs(pct_from) <= 2:
        score += 1
    if days_below > 120:
        score += 1

    # 200 DMA flattening or rising
    sma200_slope = _safe(technicals_row.get("sma_200_slope"))
    if sma200_slope is not None and sma200_slope >= 0:
        score += 1

    rsi = _safe(technicals_row.get("rsi_14"))
    if rsi and rsi > 50:
        score += 1

    macd_line = _safe(technicals_row.get("macd_line"))
    if macd_line and macd_line > 0:
        score += 1

    score = max(0.0, min(10.0, score))

    notes = (
        f"Below 200 DMA for {days_below}d, "
        f"now {pct_from:.1f}% away, recovered {recovery:.1f}% from low"
    )
    return True, score, _meta(
        trigger_level=sma200,
        pct_from_trigger=round(pct_from, 2),
        days_in_base=days_below,
        notes=notes,
    )


# ── Mutual exclusion ──────────────────────────────────────────────────────────

def apply_exclusion_rules(flagged_patterns: list) -> list:
    """Remove conflicting patterns per rules in spec."""
    codes = {p[0] for p in flagged_patterns}

    if "BR_ATH_RETEST" in codes and "BR_52W_RETEST" in codes:
        flagged_patterns = [p for p in flagged_patterns if p[0] != "BR_52W_RETEST"]

    if "RV_OVERSOLD" in codes and "RV_DIVERGENCE" in codes:
        flagged_patterns = [p for p in flagged_patterns if p[0] != "RV_DIVERGENCE"]

    return flagged_patterns

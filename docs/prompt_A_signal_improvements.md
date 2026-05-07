# Claude Code Prompt A — Two Targeted Improvements to Technical Status Scoring

Copy everything below the line into Claude Code. Run it in the existing project folder.

---

## CONTEXT

I am extending the Technical Analysis feature in my Streamlit + Supabase + GitHub Actions dashboard. The existing implementation is documented in `technical_analysis_claude_code_prompt.md` in the repo root — read it before writing any code.

Current state:
- `technicals_daily` table stores RSI, MACD, ADX, SMA50, SMA200, volume, signal_score, technical_status per stock per day
- `compute_technical_status()` in `backend/indicators.py` implements the current scoring engine
- `backend/compute_technicals.py` orchestrates daily computation and writes to Supabase
- The Technical Analysis tab in `frontend/app.py` displays the results

I have reviewed suggestions from multiple AI models to improve the signal logic. Most suggestions are premature (they require backtesting data I don't yet have). I am implementing **only two well-justified improvements** now:

1. **SMA200 slope awareness** — distinguish stocks above a *rising* long-term trend from stocks drifting above a *falling* one
2. **Volume confirmation bonus** — reward signals that come with genuine participation

Everything else from the AI critiques is deferred until I have 60-90 days of empirical signal performance data (that's Prompt B).

## NON-NEGOTIABLE RULES

1. **Do not change existing column types or primary keys.** Add columns, don't rename or drop.
2. **Preserve old signal logic as a reference.** Do not delete `compute_technical_status()` — rename it to `compute_technical_status_v1()` so we can compare old vs new signals during rollout.
3. **Test-first.** Add unit tests for the new logic BEFORE modifying production code paths.
4. **One step at a time.** After each step, stop and wait for my confirmation.
5. **Do not break the UI.** The display column name stays `technical_status`. Only the underlying math changes.

## IMPROVEMENTS — LOCKED DEFINITIONS

### Improvement 1: SMA200 Slope

Slope measured as: percent change in SMA200 from 20 trading days ago to today.

```
slope_pct = (sma_200_today - sma_200_twenty_days_ago) / sma_200_twenty_days_ago * 100
```

Classification:
- **Rising:** slope_pct > +1.0% (meaningful upward drift)
- **Flat:** slope_pct between -1.0% and +1.0%
- **Falling:** slope_pct < -1.0%

Why 1.0% over 20 days: this corresponds to ~13% annualized. Slower than that is noise.

### Improvement 2: Volume Confirmation

`volume_ratio = today_volume / average_volume_last_20_days`

- **Surge:** volume_ratio >= 1.5
- **Normal:** volume_ratio between 0.5 and 1.5
- **Weak:** volume_ratio < 0.5

Why 1.5x over 2.0x: Indian mid/small caps see frequent 2x spikes on almost nothing; 1.5x is a better threshold for Indian markets.

## STEP 1 — DATABASE SCHEMA CHANGES

Create `backend/schema_technicals_v2.sql`.

Add these columns to `technicals_daily`:

| Column          | Type    | Notes                                         |
|-----------------|---------|-----------------------------------------------|
| sma_200_slope   | numeric | Percent change over 20 days, can be negative  |
| volume_ratio    | numeric | today_volume / 20-day avg volume              |
| signal_score_v2 | numeric | New score using improved logic (keep int-style but allow decimals for future flexibility) |
| technical_status_v1 | text | Preserved old verdict — for comparison during rollout |

SQL:
```sql
ALTER TABLE technicals_daily
    ADD COLUMN IF NOT EXISTS sma_200_slope numeric,
    ADD COLUMN IF NOT EXISTS volume_ratio numeric,
    ADD COLUMN IF NOT EXISTS signal_score_v2 numeric,
    ADD COLUMN IF NOT EXISTS technical_status_v1 text;

-- Backfill old label into the new archive column so we preserve history
UPDATE technicals_daily
SET technical_status_v1 = technical_status
WHERE technical_status_v1 IS NULL;
```

Stop after Step 1. I will run in Supabase SQL Editor and verify the ALTER TABLE succeeded before you proceed.

## STEP 2 — UPDATE THE MATH LIBRARY

Modify `backend/indicators.py`.

### 2.1 Preserve the old function
Rename the existing `compute_technical_status()` to `compute_technical_status_v1()`. Do not change its logic.

### 2.2 Add two new indicator functions

```python
def compute_sma_slope(sma_series: list[float], lookback: int = 20) -> float | None:
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


def compute_volume_ratio(volumes: list[float | int], lookback: int = 20) -> float | None:
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
```

### 2.3 Write the v2 scoring function

```python
def compute_technical_status_v2(
    cmp, rsi, sma_50, sma_200, sma_200_slope,
    macd_line, macd_signal, macd_histogram,
    adx, volume_ratio
) -> tuple[float, str]:
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
```

### 2.4 Add unit tests
Extend `backend/tests/test_indicators.py`:

1. `test_sma_slope_rising` — feed an ascending SMA series, assert slope is positive.
2. `test_sma_slope_falling` — feed a descending series, assert slope is negative.
3. `test_sma_slope_insufficient` — pass a series shorter than lookback+1, assert returns None.
4. `test_volume_ratio_surge` — feed flat prior volumes and a 2x today, assert ratio ≈ 2.0.
5. `test_volume_ratio_zero_avg` — feed all-zero priors, assert None.
6. `test_v2_vs_v1_agreement` — for a clear uptrend setup, both v1 and v2 should return similar bullish labels.
7. `test_v2_slope_penalizes_weak_trend` — for a stock above a falling SMA200, v2 score should be lower than v1 score.
8. `test_v2_volume_rewards_confirmation` — same inputs with/without volume surge, v2 with surge > v2 without.
9. `test_v2_volume_does_not_convert_neutral` — a neutral score + volume surge should NOT become a buy signal.

All tests must run via `pytest backend/tests/ -v` with no network access.

Stop after Step 2. I will run tests locally before you proceed.

## STEP 3 — UPDATE THE COMPUTE SCRIPT

Modify `backend/compute_technicals.py`.

### 3.1 Load additional data
When fetching OHLCV history per stock, we need enough SMA200 history to compute slope. Extend the lookback from 260 days to 280 days. Still a cheap change since it's one SQL query.

### 3.2 Compute the full SMA200 series (not just latest value)
Refactor so that inside `compute_sma()` or via a new helper, we can get the **last 21 values of SMA200** (today and 20 days back). Pass this to `compute_sma_slope()`.

### 3.3 Compute volume_ratio
Extract the last 21 volume values and pass to `compute_volume_ratio()`.

### 3.4 Dual-scoring during rollout
For each stock, compute BOTH v1 and v2 scores/labels:

```python
# old logic — preserved for comparison
v1_score, v1_label = compute_technical_status_v1(...)

# new logic
v2_score, v2_label = compute_technical_status_v2(...)

# Production display uses v2
upsert_row(
    ...,
    sma_200_slope=slope,
    volume_ratio=vol_ratio,
    signal_score=v2_score,       # v2 becomes the primary
    signal_score_v2=v2_score,
    technical_status=v2_label,
    technical_status_v1=v1_label  # v1 archived for comparison
)
```

### 3.5 Summary print
At the end of the run, print:
- How many stocks changed label between v1 and v2
- Breakdown by transition type (e.g., "v1 Mild Bullish → v2 Neutral: 34 stocks")
- Avg slope across the universe (sanity check: during an uptrending market should be slightly positive)

This print output is how you'll sanity-check the rollout.

Stop after Step 3. I will run `python backend/compute_technicals.py` manually, review the diff summary, spot-check 5-10 stocks where v1 and v2 disagree, and confirm before you proceed.

## STEP 4 — UI ADDITIONS

Modify `frontend/app.py`.

### 4.1 Add new columns to the Technical Analysis table
After the existing columns, add:

- **SMA200 Slope** — display as `+1.2%` / `-0.8%` with color (green if >+1%, red if <-1%, gray otherwise)
- **Volume Ratio** — display as `1.8x` / `0.6x` (bold if >=1.5x)
- **v1 Label** — optional comparison column, toggleable via a checkbox "Show v1 signal" in the sidebar (default OFF)

### 4.2 Add filters to the sidebar (Technical Analysis screen only)
- **SMA200 Slope filter** — radio: "Any" / "Rising only" / "Falling only" (default Any)
- **Volume Ratio minimum slider** — 0.0 to 3.0, default 0.0

### 4.3 Add a debug panel (expandable, collapsed by default)
`st.expander("🔍 v1 vs v2 Signal Comparison")` showing:
- A table of stocks where v1 and v2 labels differ
- Columns: Symbol, Name, v1 Label, v2 Label, v1 Score, v2 Score, Slope, Volume Ratio
- This is for YOU to audit the new logic over the first 30 days

Stop after Step 4. I will verify the UI looks right and confirm acceptance criteria.

## ACCEPTANCE CRITERIA

1. `pytest backend/tests/ -v` passes all tests including the 9 new ones.
2. `SELECT COUNT(*) FROM technicals_daily WHERE sma_200_slope IS NOT NULL` is close to the active stock count (excluding those with <220 days of history).
3. For RELIANCE, TCS, HDFCBANK: verify the SMA200 slope matches what TradingView shows (within ±0.2%).
4. At least some stocks show different v1 vs v2 labels. If ALL labels are identical, the new logic isn't actually doing anything — something is wrong.
5. Stocks above a rising SMA200 should skew toward "Buy" labels; stocks above a falling SMA200 should mostly be "Neutral" or "Mild Bullish" — not "Strong Buy".
6. None of the existing tabs (Nifty 50, Themes, Peer Comparison, etc.) changed.

## START

Begin with **Step 1 only**: write `backend/schema_technicals_v2.sql`. Do not write any other file. Stop and wait for me to run it in Supabase before proceeding.

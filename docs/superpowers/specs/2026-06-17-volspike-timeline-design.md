# Vol Spikes — Timeline Selector (Today / Weekly / Monthly)

**Date:** 2026-06-17
**Tab:** Vol Spikes (`frontend/app.py` → `render_volspike_view`)
**Status:** Design approved, pending spec review

## Problem

The Vol Spikes tab only shows volume spikes for the current trading day
(today's volume ÷ 30-day average daily volume). Users want to find stocks
whose volume surged over a longer window — the past week or the past month —
not just today.

## Goal

Add a **Timeline** dropdown (`Today` / `Weekly` / `Monthly`) alongside the
existing filter dropdowns. Selecting a period changes what the **Vol Spike**
column measures and re-sorts the table by that value, descending. Everything
else about the tab stays the same.

## Decisions (locked during brainstorming)

1. **Spike meaning = period total vs baseline.** Weekly/Monthly compare the
   *aggregate* volume of the recent window against a typical window.
2. **Baseline = trailing 3-month average window**, excluding the current window.
3. **Presentation = only the spike number changes.** Same columns, same
   filters, same sort controls. (Caption text updates to name the period
   accurately — otherwise it would mislead.)
4. **Architecture = dedicated cached query** for the Vol Spikes tab; the shared
   `_load_all_snapshots()` query and all other tabs are left untouched.

## Data integrity — twice-daily refresh

The app currently refreshes up to twice a day. Verified against the live DB
(2026-06-17): `prices_daily` and `snapshots_daily` are both
`PRIMARY KEY (symbol, date)`, so a second same-day refresh **upserts** (last
write wins) rather than appending — there is **exactly one row per
(symbol, date)** (0 duplicate groups across the last 40 days). The trading-day
windows below (one row = one trading day) are therefore accurate as-is.

**Defensive dedup (belt-and-suspenders):** even though the PK guarantees
uniqueness today, the period query begins with a CTE that collapses any
`(symbol, date)` to a single row before ranking:

```sql
dedup AS (
    SELECT symbol, date, MAX(volume) AS volume
    FROM prices_daily
    WHERE date <= CAST(:date AS date)
    GROUP BY symbol, date
)
```

All trading-day ranking and sums read from `dedup`, so the ratios stay correct
even if the refresh schedule ever changes and a duplicate slips past the PK.

> Note (out of scope): if a refresh runs *intraday*, the current day's row may
> carry partial-session volume. This already affects today's existing spike and
> is not introduced or changed by this feature.

## Definitions

Windows are measured in **trading days** (rows in the deduped set), not calendar
days, so market holidays don't distort the ratios. A per-symbol ranked CTE
(`ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC)`) over `dedup`
assigns each row a recency rank `rn` (1 = the snapshot date), and the windows
are row-range sums.

- **Today** (unchanged): `today_volume / avg_daily_volume_30d`.
- **Weekly:** `Σ(volume where rn ≤ 5) / ( Σ(volume where 6 ≤ rn ≤ 70) / 13 )`
  - Numerator: total volume of the last **5** trading days.
  - Denominator: average 5-day total across the prior **~13 weeks** (65 trading
    days → divided by 13).
- **Monthly:** `Σ(volume where rn ≤ 21) / ( Σ(volume where 22 ≤ rn ≤ 147) / 6 )`
  - Numerator: total volume of the last **21** trading days.
  - Denominator: average 21-day total across the prior **~6 months** (126
    trading days → divided by 6).

A symbol's weekly/monthly ratio is `NULL` when it lacks enough history to fill
the baseline window (treated like today's `NULL` spike — filtered out / sorted
last).

## Architecture

```
render_volspike_view(snap_date)
  ├─ _load_all_snapshots(snap_date)        # existing shared query (today's vol_spike)
  ├─ _load_period_vol_spikes(snap_date)    # NEW cached query → weekly + monthly ratios
  │     returns df[symbol, vol_spike_weekly, vol_spike_monthly]
  ├─ merge period columns onto the snapshot df by symbol
  ├─ _apply_timeline(df, period)           # NEW pure helper: swaps active vol_spike
  └─ _filter_sort_volspike(...)            # existing pure filter/sort (unchanged)
```

### `_load_period_vol_spikes(snap_date)` — new

- Decorated with `@st.cache_data(ttl=300, show_spinner=False)` to match
  `_load_all_snapshots`.
- One SQL query against `prices_daily` for all active symbols on `snap_date`,
  using the ranked-CTE definitions above. Returns a DataFrame with columns
  `symbol`, `vol_spike_weekly`, `vol_spike_monthly`, each rounded to 1 decimal
  (matching today's `ROUND(..., 1)`), forced to `float64`.
- Computes **both** periods in one query so toggling the dropdown never
  re-queries the database.

### `_apply_timeline(df, period)` — new pure helper

```python
def _apply_timeline(df, period):
    """Return df with `vol_spike` set to the column for the chosen period.
    period ∈ {"Today", "Weekly", "Monthly"}. "Today" is a no-op.
    Pure (no Streamlit) for testability.
    """
```

- `"Today"` → return `df` unchanged (keeps the existing `vol_spike`).
- `"Weekly"` → copy `vol_spike_weekly` into `vol_spike`.
- `"Monthly"` → copy `vol_spike_monthly` into `vol_spike`.
- If the chosen period column is missing (e.g. period query returned nothing),
  fall back to the existing `vol_spike` so the tab still renders.

Because the active value is always written back into the `vol_spike` column,
**every downstream consumer is unchanged**: `_filter_sort_volspike`, the Min
spike filter, the Vol Spike sort option, the formatter, and the cell colouring
all continue to operate on `vol_spike`.

## UI

- New `st.selectbox("Timeline", ["Today", "Weekly", "Monthly"], index=0,
  key="vs_timeline")` added to the filter row. The filter row currently holds 3
  columns (Min spike, Sector, 52W High); it becomes a 4-column row with Timeline
  first (or the row is split as needed to stay readable).
- Default `Today` → the tab opens exactly as it does today.
- The empty-data guard stays keyed off the existing `vol_spike` column from
  `_load_all_snapshots`; if today's data is missing the tab still shows its
  existing info message.
- **Caption** becomes period-aware, e.g.:
  - Today: "Stocks where today's volume significantly exceeds the 30-day average…"
  - Weekly: "Stocks whose last-week total volume most exceeds a typical week
    (trailing 3-month average)."
  - Monthly: "Stocks whose last-month total volume most exceeds a typical month
    (trailing 3-month average)."

## Testing

- Existing `frontend/tests/test_volspike_filter_sort.py` stays green
  (`_filter_sort_volspike` is unchanged).
- New tests for `_apply_timeline`:
  - `"Today"` leaves `vol_spike` untouched.
  - `"Weekly"` / `"Monthly"` copy the right source column into `vol_spike`.
  - Missing source column falls back gracefully without raising.
  - `NaN` values in the period columns survive (so they sort last / filter out).
- SQL is not unit-tested (DB-dependent), consistent with the existing codebase.

## Out of scope (YAGNI)

- No relabelling of the "Vol Spike" header per period.
- No adapting/reordering of the return columns to the selected period.
- No new threshold options — the existing Min spike thresholds (1.5×, 2×, 3×,
  5×) apply to the active period's ratio as-is.
- No changes to the daily refresh pipeline or any DB schema — the new query
  reads existing `prices_daily` data.

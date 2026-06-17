# Vol Spikes tab — 52W-High filter + persistent sort

**Date:** 2026-06-17
**Status:** Approved (design)
**Area:** `frontend/app.py` → `render_volspike_view()`

## Problem

On the **Vol Spikes** tab a user can currently narrow the list by **Min spike**
and **Sector** only. There is no way to filter by proximity to the 52-week high,
and the table is always force-sorted by Vol Spike (highest first). The user wants
to answer questions like:

> "Show me Industrial-sector stocks with a 5×+ volume spike that are trading
> within 10% of their 52-week high."

…and to control how the result set is ordered.

## Goal

Add to the Vol Spikes tab:

1. A **52W-High proximity filter** that combines with the existing Min spike and
   Sector filters.
2. A **persistent sort control** (column + direction) that replaces the
   hard-coded "Vol Spike, highest first" sort.

Non-goals: changing any other tab, changing the underlying data/snapshot pipeline,
or removing the existing click-to-sort-on-header behavior (it still works on top).

## Current state

`render_volspike_view(snap_date)` in `frontend/app.py`:

- Loads snapshots, keeps rows with `vol_spike > 0`.
- Filter block: `fc1, fc2, fc3 = st.columns([1, 1, 2])`
  - `fc1`: `Min spike` selectbox (`spike_options`, key `vs_min`, default `2×+`).
  - `fc2`: `Sector` multiselect (key `vs_sector`).
  - `fc3`: an HTML explanatory blurb.
- Applies the two filters, then `df.sort_values("vol_spike", ascending=False)`.
- Builds the numeric `disp` frame and renders via `Styler.format` (per the recent
  sorting fix). `pct_from_52wh` is in `_VS_PCT_COLS` and rendered as `52W High%`.

Key data fact: `pct_from_52wh` is stored as a **ratio** where negative means
**below** the high (e.g. `-0.0061` displays as `-0.61%`). Some rows may be `NaN`.

## Design

### 1. Layout

Replace the single 3-column filter row with **two rows**:

- **Row 1 — Filters:** `st.columns(3)` → `Min spike` · `Sector` · `52W High` (new).
- **Row 2 — Sort:** `st.columns([1, 1, 2])` → `Sort by` (new) · `Direction` (new) ·
  spacer.
- The explanatory blurb moves to a `st.caption(...)` rendered under the two rows
  (plain text, no longer needs its own column).

### 2. 52W-High proximity filter

A preset selectbox mirroring the Min spike pattern:

```python
near_high_options = {
    "Any": None,
    "Within 5%": 0.05,
    "Within 10%": 0.10,
    "Within 20%": 0.20,
    "Within 30%": 0.30,
}
```

- Widget: `st.selectbox("52W High", list(near_high_options), index=0, key="vs_52wh")`.
- Apply: when the selected threshold `thr` is not `None`:
  `df = df[df["pct_from_52wh"] >= -thr]`.
  Because the value is a ratio and negative = below the high, this keeps stocks no
  more than `thr` below their 52-week high. Rows where `pct_from_52wh` is `NaN` are
  excluded when a threshold is active (the `>=` comparison drops NaN).
- Default `Any` ⇒ no change to current behavior.
- Defensive: only apply / show the filter if `pct_from_52wh` exists in the frame
  (it is part of `_VS_COLS`, but guard to avoid a hard failure on schema drift).

### 3. Persistent sort control

Two selectboxes:

```python
sort_options = {
    "Vol Spike": "vol_spike",
    "52W High%": "pct_from_52wh",
    "1D%": "ret_1d",
    "1W%": "ret_1w",
    "30D%": "ret_30d",
    "1Y%": "ret_365d",
    "CMP": "cmp",
    "MCap (Cr)": "market_cap_cr",
    "P/E": "pe_ratio",
}
```

- `Sort by`: `st.selectbox("Sort by", list(sort_options), index=0, key="vs_sort_by")`.
- `Direction`: `st.selectbox("Direction", ["Highest first", "Lowest first"], index=0, key="vs_sort_dir")`.
  "Highest first" is clearer than Asc/Desc — for `52W High%` it naturally means
  closest-to-high first.
- Apply (replaces the hard-coded sort):
  ```python
  sort_col = sort_options[st.session_state.get("vs_sort_by", "Vol Spike")]
  ascending = (direction == "Lowest first")
  if sort_col in df.columns:
      df = df.sort_values(sort_col, ascending=ascending, na_position="last")
  df = df.reset_index(drop=True)
  ```
  NaNs always sort last regardless of direction. Guard `sort_col in df.columns`.
- The count caption updates to reflect the active sort, e.g.
  `"179 stocks · sorted by 52W High%, highest first"`.
- All three new widgets persist via `session_state` (`vs_52wh`, `vs_sort_by`,
  `vs_sort_dir`), so selections survive Streamlit reruns. The existing
  click-a-header sort still works as an ad-hoc override.

## Components & boundaries

All changes are local to `render_volspike_view`. The display-building and
`Styler.format` rendering downstream are unchanged. No new module-level state.

## Error handling / edge cases

- Empty result after filtering: existing `if total == 0: st.warning(...)` path
  already handles this.
- `pct_from_52wh` / sort column missing from the frame: guarded with
  `in df.columns` checks; the feature degrades gracefully (filter skipped / sort
  falls back to leaving order unchanged).
- All-NaN sort column: `sort_values` keeps rows, NaNs last.

## Testing / verification

- `python -m py_compile frontend/app.py`.
- Manual reproduction of the pure logic on a small sample DataFrame: confirm the
  `>= -thr` mask selects the right rows and the column+direction mapping orders as
  expected (including NaN-last). The function itself calls `st.*`, so it is not
  unit-testable in isolation — same constraint documented for the sorting fix.

## Rollout

Per project convention, commit and push to `origin main` after implementation so
the change auto-deploys.

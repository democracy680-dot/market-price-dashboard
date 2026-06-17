# Vol Spikes Timeline Selector — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Today / Weekly / Monthly timeline dropdown to the Vol Spikes tab so the Vol Spike column measures volume surge over the selected period (week/month total vs a trailing 3-month-average baseline), default-sorted highest-first.

**Architecture:** A new cached query (`_load_period_vol_spikes`) computes weekly and monthly spike ratios for all stocks on the selected date, reading existing `prices_daily` data. A pure helper (`_apply_timeline`) swaps the chosen period's column into the existing `vol_spike` column, so every downstream consumer (filter, sort, formatter, colouring) is unchanged. `render_volspike_view` gains a Timeline dropdown and a period-aware caption. The shared `_load_all_snapshots` query and all other tabs are untouched.

**Tech Stack:** Python, Streamlit, SQLAlchemy + PostgreSQL (Supabase), pandas, pytest.

## Global Constraints

- Frontend reads only — no DB writes; the new query is a `SELECT`.
- Pre-compute in SQL — the period query does the aggregation in PostgreSQL, not in Python.
- Cached DB reads use `@st.cache_data(ttl=300, show_spinner=False)` (match `_load_all_snapshots`).
- Numeric columns coerced to `float64` via `pd.to_numeric(..., errors="coerce")` so tables sort numerically.
- Spike ratios rounded to 1 decimal (`ROUND(..., 1)`), matching today's `vol_spike`.
- Windows measured in **trading days** (one row per `(symbol, date)`); the period query begins with a dedup CTE (`GROUP BY symbol, date`, `MAX(volume)`) so it stays correct even if the twice-daily refresh ever produces a duplicate.
- Definitions (verbatim from spec):
  - **Weekly** = `Σ(volume, rn ≤ 5) / ( Σ(volume, 6 ≤ rn ≤ 70) / 13 )`, NULL unless the full 65-day baseline is present.
  - **Monthly** = `Σ(volume, rn ≤ 21) / ( Σ(volume, 22 ≤ rn ≤ 147) / 6 )`, NULL unless the full 126-day baseline is present.
  - `rn` = `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC)` over the deduped set (rn = 1 is the snapshot date).
- Presentation: only the spike number changes per period; columns, filters, and sort controls stay identical. The caption text becomes period-aware.
- Tests run from `frontend/tests/` via `python -m pytest`. Pure helpers are loaded through the existing AST harness in `test_display_sorting.py` (`APP[...]`). SQL is not unit-tested (DB-dependent), consistent with the codebase.

---

### Task 1: Pure helper `_apply_timeline` + timeline column mapping

Adds the period→column mapping and the pure swap helper, with unit tests. No DB, no Streamlit execution — fully testable through the AST harness.

**Files:**
- Modify: `frontend/app.py` — add `VOLSPIKE_TIMELINE_COLUMNS` and `_apply_timeline` next to `VOLSPIKE_SORT_COLUMNS` / `_filter_sort_volspike` (currently `frontend/app.py:4515-4547`).
- Test: `frontend/tests/test_volspike_timeline.py` (new).

**Interfaces:**
- Produces:
  - `VOLSPIKE_TIMELINE_COLUMNS: dict[str, str]` = `{"Today": "vol_spike", "Weekly": "vol_spike_weekly", "Monthly": "vol_spike_monthly"}`
  - `_apply_timeline(df: pd.DataFrame, period: str) -> pd.DataFrame` — returns a df whose `vol_spike` column holds the chosen period's values. `"Today"` (or unknown period, or missing source column) returns `df` unchanged. Otherwise returns a copy with `vol_spike` overwritten by the period column (NaNs preserved).

- [ ] **Step 1: Write the failing test**

Create `frontend/tests/test_volspike_timeline.py`:

```python
"""
Tests for the Vol Spikes timeline helper (_apply_timeline).

Reuses the AST-loader harness from test_display_sorting (app.py runs Streamlit
at module scope and can't be imported directly).
"""
import numpy as np
import pandas as pd

from test_display_sorting import APP  # loads app.py's pure functions

_apply_timeline = APP["_apply_timeline"]
TIMELINE_COLS = APP["VOLSPIKE_TIMELINE_COLUMNS"]


def _df():
    return pd.DataFrame(
        {
            "symbol": ["A", "B", "C"],
            "vol_spike": [2.0, 5.0, 1.0],
            "vol_spike_weekly": [3.5, np.nan, 0.8],
            "vol_spike_monthly": [1.2, 4.4, 9.9],
        }
    )


def test_mapping_exists():
    assert TIMELINE_COLS["Today"] == "vol_spike"
    assert TIMELINE_COLS["Weekly"] == "vol_spike_weekly"
    assert TIMELINE_COLS["Monthly"] == "vol_spike_monthly"


def test_today_is_noop():
    out = _apply_timeline(_df(), "Today")
    assert out["vol_spike"].tolist() == [2.0, 5.0, 1.0]


def test_weekly_swaps_in_weekly_column():
    out = _apply_timeline(_df(), "Weekly")
    # NaN preserved for B so it sorts last / filters out downstream
    assert out["vol_spike"].tolist()[0] == 3.5
    assert np.isnan(out["vol_spike"].tolist()[1])
    assert out["vol_spike"].tolist()[2] == 0.8


def test_monthly_swaps_in_monthly_column():
    out = _apply_timeline(_df(), "Monthly")
    assert out["vol_spike"].tolist() == [1.2, 4.4, 9.9]


def test_missing_source_column_falls_back():
    df = pd.DataFrame({"symbol": ["A"], "vol_spike": [2.0]})  # no weekly/monthly cols
    out = _apply_timeline(df, "Weekly")
    assert out["vol_spike"].tolist() == [2.0]  # unchanged, no raise


def test_unknown_period_is_noop():
    out = _apply_timeline(_df(), "Quarterly")
    assert out["vol_spike"].tolist() == [2.0, 5.0, 1.0]


def test_does_not_mutate_input():
    df = _df()
    _apply_timeline(df, "Weekly")
    assert df["vol_spike"].tolist() == [2.0, 5.0, 1.0]  # original untouched
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_volspike_timeline.py -q` (from `frontend/tests/`)
Expected: FAIL — `KeyError: '_apply_timeline'` (and `VOLSPIKE_TIMELINE_COLUMNS`) because the names don't exist yet.

- [ ] **Step 3: Add the mapping and helper to app.py**

In `frontend/app.py`, immediately after the `VOLSPIKE_SORT_COLUMNS = { ... }` block (ends `frontend/app.py:4526`) and before `def _filter_sort_volspike`, insert:

```python
# Timeline options for the Vol Spikes tab: label → df column holding that period's spike.
VOLSPIKE_TIMELINE_COLUMNS = {
    "Today":   "vol_spike",
    "Weekly":  "vol_spike_weekly",
    "Monthly": "vol_spike_monthly",
}


def _apply_timeline(df, period):
    """Return df with `vol_spike` set to the chosen period's column.

    period ∈ {"Today", "Weekly", "Monthly"}. "Today" (or an unknown period, or a
    missing source column) is a no-op that returns df unchanged. Otherwise the
    chosen period's column is copied into `vol_spike` so every downstream
    consumer (filter, sort, formatter, colouring) keeps operating on `vol_spike`.
    NaNs are preserved. Pure (no Streamlit) for testability.
    """
    col = VOLSPIKE_TIMELINE_COLUMNS.get(period, "vol_spike")
    if col == "vol_spike" or col not in df.columns:
        return df
    out = df.copy()
    out["vol_spike"] = df[col]
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_volspike_timeline.py -q` (from `frontend/tests/`)
Expected: PASS (7 passed).

- [ ] **Step 5: Run the full frontend test suite (no regression)**

Run: `python -m pytest -q` (from `frontend/tests/`)
Expected: all tests pass (existing 8 in `test_volspike_filter_sort.py` + `test_display_sorting.py` + 7 new).

- [ ] **Step 6: Commit**

```powershell
git add frontend/app.py frontend/tests/test_volspike_timeline.py
git commit -m @'
feat(volspike): pure timeline helper (_apply_timeline) with tests

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
'@
```

---

### Task 2: Cached period-spike query `_load_period_vol_spikes`

Adds the SQL that computes weekly and monthly spike ratios for all stocks on a date, reading existing `prices_daily`. Verified against the live DB (SQL isn't unit-tested per codebase convention).

**Files:**
- Modify: `frontend/app.py` — add `_load_period_vol_spikes` immediately after `_load_all_snapshots` (ends `frontend/app.py:988`).

**Interfaces:**
- Consumes: module-level `engine` (SQLAlchemy) and `text` from sqlalchemy, `measure` context manager, and `pd` — all already imported/defined and used by `_load_all_snapshots`.
- Produces: `_load_period_vol_spikes(snap_date) -> pd.DataFrame` with columns `symbol` (str), `vol_spike_weekly` (float64), `vol_spike_monthly` (float64). Ratios rounded to 1 decimal; NULL where the baseline window isn't fully present. One row per active symbol that has any price history on/before `snap_date`.

- [ ] **Step 1: Add the cached query function**

In `frontend/app.py`, immediately after the end of `_load_all_snapshots` (after the `return`/dataframe-coercion block ending at `frontend/app.py:988`), insert:

```python
@st.cache_data(ttl=300, show_spinner=False)
def _load_period_vol_spikes(snap_date) -> pd.DataFrame:
    """Weekly & monthly volume-spike ratios for all symbols on snap_date.

    Period total vs trailing-3-month-average window, measured in trading days
    (one row per (symbol, date)). A dedup CTE collapses any accidental duplicate
    (symbol, date) before ranking, so the math stays correct even if the
    twice-daily refresh ever produces a duplicate. Only the Vol Spikes tab calls
    this; the shared _load_all_snapshots query is left untouched.
    """
    sql = text("""
        WITH dedup AS (
            SELECT symbol, date, MAX(volume) AS volume
            FROM prices_daily
            WHERE date <= CAST(:date AS date)
              AND date >  CAST(:date AS date) - INTERVAL '400 days'
            GROUP BY symbol, date
        ),
        ranked AS (
            SELECT symbol, volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
            FROM dedup
        ),
        agg AS (
            SELECT symbol,
                SUM(volume) FILTER (WHERE rn <= 5)               AS wk_recent,
                SUM(volume) FILTER (WHERE rn BETWEEN 6 AND 70)   AS wk_base_sum,
                COUNT(*)    FILTER (WHERE rn BETWEEN 6 AND 70)   AS wk_base_days,
                SUM(volume) FILTER (WHERE rn <= 21)              AS mo_recent,
                SUM(volume) FILTER (WHERE rn BETWEEN 22 AND 147) AS mo_base_sum,
                COUNT(*)    FILTER (WHERE rn BETWEEN 22 AND 147) AS mo_base_days
            FROM ranked
            GROUP BY symbol
        )
        SELECT symbol,
            CASE WHEN wk_base_days >= 65 AND wk_base_sum > 0
                 THEN ROUND((wk_recent / (wk_base_sum::numeric / 13))::numeric, 1)
                 ELSE NULL END AS vol_spike_weekly,
            CASE WHEN mo_base_days >= 126 AND mo_base_sum > 0
                 THEN ROUND((mo_recent / (mo_base_sum::numeric / 6))::numeric, 1)
                 ELSE NULL END AS vol_spike_monthly
        FROM agg
    """)
    with measure("_load_period_vol_spikes__sql"):
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"date": str(snap_date)})

    for c in ("vol_spike_weekly", "vol_spike_monthly"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
```

- [ ] **Step 2: Verify against the live DB**

Create `frontend/_verify_period_query.py` (throwaway):

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "backend"))
from db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()
# Latest snapshot date
with engine.connect() as c:
    snap = c.execute(text("SELECT MAX(date) FROM prices_daily")).scalar()
print("snap_date:", snap)

sql = text(open(pathlib.Path(__file__).resolve().parent / "_period_sql.txt").read())
with engine.connect() as c:
    df = pd.read_sql(sql, c, params={"date": str(snap)})
df["vol_spike_weekly"] = pd.to_numeric(df["vol_spike_weekly"], errors="coerce")
df["vol_spike_monthly"] = pd.to_numeric(df["vol_spike_monthly"], errors="coerce")
print("rows:", len(df), "cols:", list(df.columns))
print("weekly non-null:", df["vol_spike_weekly"].notna().sum(),
      "monthly non-null:", df["vol_spike_monthly"].notna().sum())
print("\nTop 5 weekly:")
print(df.sort_values("vol_spike_weekly", ascending=False).head(5).to_string(index=False))
print("\nTop 5 monthly:")
print(df.sort_values("vol_spike_monthly", ascending=False).head(5).to_string(index=False))
print("\nWeekly describe:\n", df["vol_spike_weekly"].describe())
```

Copy the exact SQL string (the contents of the `text(""" ... """)` block from Step 1, without the Python wrapper) into `frontend/_period_sql.txt`.

Run: `python frontend/_verify_period_query.py` (from repo root)
Expected: prints ~1600 rows; both columns present; a healthy count of non-null weekly/monthly values; top values are finite positive numbers (typically a few × up to maybe ~10–30×); no exceptions. Sanity-check that the median weekly ratio sits near ~1.0 (a typical week ≈ baseline).

- [ ] **Step 3: Remove the throwaway verification files**

```powershell
Remove-Item frontend/_verify_period_query.py, frontend/_period_sql.txt -Force
```

- [ ] **Step 4: Commit**

```powershell
git add frontend/app.py
git commit -m @'
feat(volspike): cached weekly/monthly volume-spike query

Computes period-total-vs-trailing-3mo-baseline ratios in SQL with a dedup
CTE; called only by the Vol Spikes tab. Shared snapshot query untouched.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
'@
```

---

### Task 3: Wire the Timeline dropdown into `render_volspike_view`

Adds the Timeline selectbox alongside the existing filters, merges the period columns, applies `_apply_timeline` before filtering/sorting, and makes the caption period-aware.

**Files:**
- Modify: `frontend/app.py` — `render_volspike_view` (`frontend/app.py:4567-4677`).

**Interfaces:**
- Consumes: `_load_period_vol_spikes` (Task 2), `_apply_timeline` / `VOLSPIKE_TIMELINE_COLUMNS` (Task 1), and the unchanged `_filter_sort_volspike`.

- [ ] **Step 1: Restructure the data-loading + guard prologue**

In `render_volspike_view`, replace the current prologue (`frontend/app.py:4568-4577`):

```python
    df = _load_all_snapshots(snap_date)

    if "vol_spike" not in df.columns or df["vol_spike"].isna().all():
        st.info(
            "Volume spike data isn't available yet — it requires `prices_daily` "
            "data for this date. Try a more recent date or wait for the next refresh."
        )
        return

    df = df[df["vol_spike"].notna() & (df["vol_spike"] > 0)].copy()
```

with:

```python
    df = _load_all_snapshots(snap_date)
    if df.empty:
        st.info(
            "Volume spike data isn't available yet — it requires `prices_daily` "
            "data for this date. Try a more recent date or wait for the next refresh."
        )
        return

    # Merge weekly/monthly spike ratios (cached; computed once regardless of the
    # selected period so toggling the Timeline dropdown never re-queries).
    period_df = _load_period_vol_spikes(snap_date)
    df = df.merge(period_df, on="symbol", how="left")
```

(The notna/`> 0` filter moves below, after the timeline is applied, so it filters on the *active* period's column.)

- [ ] **Step 2: Add the Timeline dropdown to the filter row**

Replace the filter-row block (`frontend/app.py:4586-4596`):

```python
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        min_label = st.selectbox("Min spike", list(spike_options.keys()), index=2, key="vs_min")
        min_val   = spike_options[min_label]
    with fc2:
        sectors    = sorted(df["sector"].dropna().unique().tolist())
        sel_sector = st.multiselect("Sector", sectors, default=[], key="vs_sector",
                                    placeholder="All sectors")
    with fc3:
        near_label = st.selectbox("52W High", list(near_high_options.keys()), index=0, key="vs_52wh")
        near_thr   = near_high_options[near_label]
```

with (Timeline first, now a 4-column row):

```python
    fc0, fc1, fc2, fc3 = st.columns(4)
    with fc0:
        period = st.selectbox("Timeline", list(VOLSPIKE_TIMELINE_COLUMNS.keys()),
                              index=0, key="vs_timeline")
    with fc1:
        min_label = st.selectbox("Min spike", list(spike_options.keys()), index=2, key="vs_min")
        min_val   = spike_options[min_label]
    with fc2:
        sectors    = sorted(df["sector"].dropna().unique().tolist())
        sel_sector = st.multiselect("Sector", sectors, default=[], key="vs_sector",
                                    placeholder="All sectors")
    with fc3:
        near_label = st.selectbox("52W High", list(near_high_options.keys()), index=0, key="vs_52wh")
        near_thr   = near_high_options[near_label]
```

- [ ] **Step 3: Apply the timeline, then filter on the active column**

Immediately after the sort-row block (`sc1, sc2, _sc3 = st.columns([1, 1, 2])` … ends `frontend/app.py:4604`) and before the `st.caption(...)` call (`frontend/app.py:4606`), insert:

```python
    # Swap in the selected period's spike value, then drop rows without a valid
    # spike for THAT period (mirrors the old today-only notna/>0 filter).
    df = _apply_timeline(df, period)
    if "vol_spike" not in df.columns or df["vol_spike"].isna().all():
        st.info(
            f"{period} volume-spike data isn't available for this date yet — it "
            "needs enough `prices_daily` history. Try a more recent date or "
            "another timeline."
        )
        return
    df = df[df["vol_spike"].notna() & (df["vol_spike"] > 0)].copy()
```

- [ ] **Step 4: Make the caption period-aware**

Replace the static caption (`frontend/app.py:4606-4609`):

```python
    st.caption(
        "Stocks where today's volume significantly exceeds the 30-day average — "
        "often signals unusual activity, breakouts, or news-driven moves."
    )
```

with:

```python
    _vs_captions = {
        "Today": "Stocks where today's volume significantly exceeds the 30-day "
                 "average — often signals unusual activity, breakouts, or news-driven moves.",
        "Weekly": "Stocks whose last-week total volume (5 trading days) most exceeds "
                  "a typical week, measured against the trailing 3-month average.",
        "Monthly": "Stocks whose last-month total volume (21 trading days) most exceeds "
                   "a typical month, measured against the trailing 3-month average.",
    }
    st.caption(_vs_captions.get(period, _vs_captions["Today"]))
```

- [ ] **Step 5: Run the full frontend test suite (no regression)**

Run: `python -m pytest -q` (from `frontend/tests/`)
Expected: all tests pass — `_filter_sort_volspike` and `_apply_timeline` are unchanged by this task; this confirms the AST harness still loads `app.py` (i.e. no syntax error introduced).

- [ ] **Step 6: Manual smoke test in Streamlit**

Run the app (e.g. `streamlit run frontend/app.py` from repo root, or the project's usual launch script) and open the **Vol Spikes** tab. Verify:
- A **Timeline** dropdown shows `Today / Weekly / Monthly`, defaulting to `Today`; the tab looks exactly as before on `Today`.
- Switching to **Weekly** re-populates the table, sorted by Vol Spike highest-first, with the caption updating to the weekly text; switching to **Monthly** does the same. Toggling back and forth is instant (no spinner / re-query).
- Min spike, Sector, 52W High, Sort by, and Direction all still work for each timeline.

- [ ] **Step 7: Commit**

```powershell
git add frontend/app.py
git commit -m @'
feat(volspike): add Today/Weekly/Monthly timeline dropdown

Timeline selector swaps the active spike column (today vs week/month total
vs trailing-3mo baseline); filters on the active period and updates the
caption. Other tabs and the shared snapshot query are unaffected.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
'@
```

- [ ] **Step 8: Push to origin main**

```powershell
git push origin main
```

---

## Notes for the implementer

- **Line numbers** in this plan reflect `app.py` at plan-writing time; if Task 1/Task 2 shift line numbers, locate the anchors by the quoted code, not the numbers.
- **`measure`** is the perf context manager already used by `_load_all_snapshots` (`with measure("_load_all_snapshots__sql"):`). Reuse it exactly as shown.
- Do **not** touch `_load_all_snapshots`, `_filter_sort_volspike`, `VOLSPIKE_SORT_COLUMNS`, `_VS_COLS`, the formatter, or any other tab — the whole point is to leave them unchanged.
- The merge is a left join on `symbol`; symbols missing from `period_df` get NaN weekly/monthly, which `_apply_timeline` + the notna filter handle gracefully.

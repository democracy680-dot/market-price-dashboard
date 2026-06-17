# Vol Spikes 52W-High Filter + Persistent Sort — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a 52-week-high proximity filter and a persistent column+direction sort control to the Vol Spikes tab.

**Architecture:** Extract the filter+sort math out of `render_volspike_view` into a small pure function `_filter_sort_volspike` (unit-testable), then rewire `render_volspike_view` to render the new widgets (two control rows) and delegate the data work to that helper.

**Tech Stack:** Python, pandas, Streamlit 1.x. Tests via pytest, reusing the AST-loader harness from `frontend/tests/test_display_sorting.py` (app.py can't be imported directly — it runs Streamlit at module scope).

## Global Constraints

- Frontend reads only; no DB writes. (CLAUDE.md)
- `pct_from_52wh` is a **ratio**, negative = below the high (`-0.0061` ⇒ `-0.61%`).
- Streamlit floor `streamlit>=1.37` — use only widgets available there (`st.selectbox`, `st.columns`, `st.caption`). Do **not** use `st.segmented_control`.
- After implementation, commit and push to `origin main` (auto-deploy convention).
- Tests live under `frontend/tests/`.

---

### Task 1: Pure filter+sort helper with tests

**Files:**
- Modify: `frontend/app.py` (add `VOLSPIKE_SORT_COLUMNS` dict + `_filter_sort_volspike` function just above `_VS_COLS` / `render_volspike_view`, near line 4514)
- Test: `frontend/tests/test_volspike_filter_sort.py` (create)

**Interfaces:**
- Produces:
  - `VOLSPIKE_SORT_COLUMNS: dict[str, str]` — pretty label → df column. Exact contents:
    `{"Vol Spike":"vol_spike", "52W High%":"pct_from_52wh", "1D%":"ret_1d", "1W%":"ret_1w", "30D%":"ret_30d", "1Y%":"ret_365d", "CMP":"cmp", "MCap (Cr)":"market_cap_cr", "P/E":"pe_ratio"}`
  - `_filter_sort_volspike(df, *, min_spike=0.0, sectors=None, near_high_thr=None, sort_col="vol_spike", ascending=False) -> pd.DataFrame`
    - Filters by `vol_spike >= min_spike` (when `min_spike > 0`), `sector in sectors` (when truthy), `pct_from_52wh >= -near_high_thr` (when `near_high_thr is not None` and column present).
    - Sorts by `sort_col` (when present in columns) with `ascending`, `na_position="last"`.
    - Returns a new frame with `reset_index(drop=True)`.

- [ ] **Step 1: Write the failing test**

Create `frontend/tests/test_volspike_filter_sort.py`:

```python
"""
Tests for the Vol Spikes tab filter+sort helper (_filter_sort_volspike).

Reuses the AST-loader harness from test_display_sorting (app.py runs Streamlit at
module scope and can't be imported directly).
"""
import numpy as np
import pandas as pd

from test_display_sorting import APP  # loads app.py's pure functions

_filter_sort = APP["_filter_sort_volspike"]
SORT_COLS = APP["VOLSPIKE_SORT_COLUMNS"]


def _df():
    return pd.DataFrame(
        {
            "symbol": ["A", "B", "C", "D"],
            "sector": ["Industrials", "Industrials", "Pharma", "Auto"],
            "vol_spike": [2.2, 13.2, 5.5, 1.4],
            "pct_from_52wh": [-0.006, -0.50, -0.08, np.nan],
            "ret_1d": [0.03, 0.19, 0.07, -0.02],
            "cmp": [9926.5, 109.35, 115.65, 143.29],
            "market_cap_cr": [122103.0, 1036.0, 1101.0, 1495.0],
            "pe_ratio": [46.7, np.nan, 17.1, 28.8],
            "ret_1w": [0.04, 0.27, 0.09, 0.01],
            "ret_30d": [0.08, 0.57, 0.14, 0.29],
            "ret_365d": [0.64, 0.55, 0.27, 0.87],
        }
    )


def test_min_spike_filter():
    out = _filter_sort(_df(), min_spike=5.0)
    assert set(out["symbol"]) == {"B", "C"}


def test_sector_filter():
    out = _filter_sort(_df(), sectors=["Industrials"])
    assert set(out["symbol"]) == {"A", "B"}


def test_near_high_filter_keeps_within_threshold():
    # Within 10% of high → pct_from_52wh >= -0.10 → A (-0.006) and C (-0.08); NaN (D) excluded
    out = _filter_sort(_df(), near_high_thr=0.10)
    assert set(out["symbol"]) == {"A", "C"}


def test_combined_filters_match_user_example():
    # 5x+ spike, Industrials, within 10% of 52W high
    out = _filter_sort(_df(), min_spike=5.0, sectors=["Industrials"], near_high_thr=0.10)
    # B is Industrials & 5x+ but 50% below high → excluded; result empty
    assert out.empty


def test_sort_by_52wh_highest_first():
    out = _filter_sort(_df(), sort_col="pct_from_52wh", ascending=False)
    # highest (closest to high) first, NaN last
    assert out["symbol"].tolist() == ["A", "C", "B", "D"]


def test_sort_lowest_first_nan_still_last():
    out = _filter_sort(_df(), sort_col="pe_ratio", ascending=True)
    assert out["symbol"].tolist()[-1] == "B"  # NaN P/E sorts last even ascending


def test_default_sort_is_vol_spike_desc():
    out = _filter_sort(_df())
    assert out["symbol"].tolist() == ["B", "C", "A", "D"]


def test_sort_columns_mapping_exists():
    assert SORT_COLS["Vol Spike"] == "vol_spike"
    assert SORT_COLS["52W High%"] == "pct_from_52wh"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd "c:/Users/Sumit meena/OneDrive/Desktop/Claude Code/Market Price Dashboard" && python -m pytest frontend/tests/test_volspike_filter_sort.py -q`
Expected: FAIL / collection error — `KeyError: '_filter_sort_volspike'` (helper not defined yet).

- [ ] **Step 3: Write minimal implementation**

In `frontend/app.py`, immediately **above** the `_VS_COLS = {` definition (~line 4514), add:

```python
# Sort options for the Vol Spikes tab: pretty label → underlying df column.
VOLSPIKE_SORT_COLUMNS = {
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


def _filter_sort_volspike(df, *, min_spike=0.0, sectors=None, near_high_thr=None,
                          sort_col="vol_spike", ascending=False):
    """Apply the Vol Spikes filters and sort. Pure (no Streamlit) for testability.

    - min_spike: keep vol_spike >= min_spike when > 0
    - sectors: keep sector in sectors when truthy
    - near_high_thr: keep pct_from_52wh >= -near_high_thr (ratio; negative = below high)
    - sort_col / ascending: sort with NaNs always last
    """
    out = df
    if min_spike and min_spike > 0:
        out = out[out["vol_spike"] >= min_spike]
    if sectors:
        out = out[out["sector"].isin(sectors)]
    if near_high_thr is not None and "pct_from_52wh" in out.columns:
        out = out[out["pct_from_52wh"] >= -near_high_thr]
    if sort_col in out.columns:
        out = out.sort_values(sort_col, ascending=ascending, na_position="last")
    return out.reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest frontend/tests/test_volspike_filter_sort.py -q`
Expected: PASS (8 passed).

- [ ] **Step 5: Commit**

```bash
git add frontend/app.py frontend/tests/test_volspike_filter_sort.py
git commit -m "feat(volspike): pure filter+sort helper with tests"
```

---

### Task 2: Wire new widgets into render_volspike_view

**Files:**
- Modify: `frontend/app.py` → `render_volspike_view` filter block (currently ~lines 4543–4566), the sort line (~4566), and the count caption (~4573–4578).

**Interfaces:**
- Consumes: `VOLSPIKE_SORT_COLUMNS`, `_filter_sort_volspike` from Task 1.

- [ ] **Step 1: Replace the filter block, sort, and caption**

Find this block in `render_volspike_view`:

```python
    # ── Filters ─────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([1, 1, 2])
    with fc1:
        spike_options = {"Any (all)": 0.0, "1.5×+": 1.5, "2×+": 2.0, "3×+": 3.0, "5×+": 5.0}
        min_label = st.selectbox("Min spike", list(spike_options.keys()), index=2, key="vs_min")
        min_val   = spike_options[min_label]
    with fc2:
        sectors    = sorted(df["sector"].dropna().unique().tolist())
        sel_sector = st.multiselect("Sector", sectors, default=[], key="vs_sector",
                                    placeholder="All sectors")
    with fc3:
        st.markdown(
            f"<div style='font-size:11px;color:{_T['text_label']};padding-top:28px;'>"
            "Stocks where today's volume significantly exceeds the 30-day average — "
            "often signals unusual activity, breakouts, or news-driven moves.</div>",
            unsafe_allow_html=True,
        )

    if min_val > 0:
        df = df[df["vol_spike"] >= min_val]
    if sel_sector:
        df = df[df["sector"].isin(sel_sector)]

    df = df.sort_values("vol_spike", ascending=False).reset_index(drop=True)
```

Replace it with:

```python
    # ── Filters (row 1) + sort (row 2) ───────────────────────────────────────
    spike_options = {"Any (all)": 0.0, "1.5×+": 1.5, "2×+": 2.0, "3×+": 3.0, "5×+": 5.0}
    near_high_options = {
        "Any": None, "Within 5%": 0.05, "Within 10%": 0.10,
        "Within 20%": 0.20, "Within 30%": 0.30,
    }

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

    sc1, sc2, _sc3 = st.columns([1, 1, 2])
    with sc1:
        sort_label = st.selectbox("Sort by", list(VOLSPIKE_SORT_COLUMNS.keys()),
                                  index=0, key="vs_sort_by")
    with sc2:
        dir_label = st.selectbox("Direction", ["Highest first", "Lowest first"],
                                 index=0, key="vs_sort_dir")

    st.caption(
        "Stocks where today's volume significantly exceeds the 30-day average — "
        "often signals unusual activity, breakouts, or news-driven moves."
    )

    sort_col  = VOLSPIKE_SORT_COLUMNS[sort_label]
    ascending = (dir_label == "Lowest first")
    df = _filter_sort_volspike(
        df, min_spike=min_val, sectors=sel_sector, near_high_thr=near_thr,
        sort_col=sort_col, ascending=ascending,
    )
```

- [ ] **Step 2: Update the count caption**

Find:

```python
    st.markdown(
        f"<div style='font-size:11.5px;color:{_T['text_soft']};margin:4px 0 8px;'>"
        f"{total} stocks · sorted highest Vol Spike first"
        f"</div>",
        unsafe_allow_html=True,
    )
```

Replace the inner text so it reflects the active sort:

```python
    _dir_word = "lowest first" if ascending else "highest first"
    st.markdown(
        f"<div style='font-size:11.5px;color:{_T['text_soft']};margin:4px 0 8px;'>"
        f"{total} stocks · sorted by {sort_label}, {_dir_word}"
        f"</div>",
        unsafe_allow_html=True,
    )
```

- [ ] **Step 3: Verify the module compiles**

Run: `cd "c:/Users/Sumit meena/OneDrive/Desktop/Claude Code/Market Price Dashboard" && python -m py_compile frontend/app.py && echo COMPILE OK`
Expected: `COMPILE OK`

- [ ] **Step 4: Re-run the helper tests (still green)**

Run: `python -m pytest frontend/tests/test_volspike_filter_sort.py frontend/tests/test_display_sorting.py -q`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add frontend/app.py
git commit -m "feat(volspike): add 52W-high filter and persistent sort controls"
```

---

### Task 3: Deploy

**Files:** none (push only).

- [ ] **Step 1: Push to origin main**

Run: `cd "c:/Users/Sumit meena/OneDrive/Desktop/Claude Code/Market Price Dashboard" && git push origin main`
Expected: push succeeds, new commits on `main`.

---

## Self-Review

**Spec coverage:**
- Layout two rows + caption → Task 2 Step 1. ✓
- 52W-high preset filter (Any/5/10/20/30, `>= -thr`, NaN excluded) → Task 1 (logic + tests `test_near_high_filter_keeps_within_threshold`) + Task 2 (widget). ✓
- Sort control (9 columns, Highest/Lowest first, default Vol Spike highest, NaN last, session_state persistence) → Task 1 (`VOLSPIKE_SORT_COLUMNS`, sort logic, tests) + Task 2 (widgets with `key=`). ✓
- Count caption reflects active sort → Task 2 Step 2. ✓
- Guards for missing columns → `_filter_sort_volspike` (`in out.columns` checks). ✓
- Verification: py_compile + pytest → Task 2 Steps 3–4. ✓
- Commit/push → Tasks 1–3. ✓

**Placeholder scan:** No TBD/TODO; all steps contain concrete code/commands. ✓

**Type consistency:** `VOLSPIKE_SORT_COLUMNS` and `_filter_sort_volspike` signature identical across Task 1 definition, Task 1 tests, and Task 2 call site. `near_high_thr`/`min_spike`/`sectors`/`sort_col`/`ascending` names match throughout. ✓

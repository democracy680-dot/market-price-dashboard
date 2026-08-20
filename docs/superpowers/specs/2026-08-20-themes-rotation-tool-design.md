# Themes Rotation Tool — Design Spec

**Date:** 2026-08-20
**Status:** Approved (pending spec review)
**Author:** Claude (brainstormed with Sumit)

## 1. Summary

Replace the current **Themes** tab with a "Theme Rotation Tool" modelled on the
Sector Rotation Tool screenshots the user shared. Instead of a single-number
average return per theme, each theme is shown as a **card** containing a
rebased-to-100 index line chart of the theme's constituents plotted against a
**Nifty 500** benchmark, plus headline return, today's move, and breadth
(advancers/decliners). A toolbar drives period, scale, moving-average overlays,
weighting method, grid/table view, filtering, sorting and search. Clicking a
card drills into the theme's constituent stock table.

The whole interactive grid renders as **one self-contained client-side HTML/JS
component** fed by a single pre-computed JSON payload, so switching
period/scale/MA/filter is instant with no Streamlit rerun.

## 2. Goals & non-goals

**Goals**
- Visual, at-a-glance performance of every theme vs a market benchmark.
- Snappy client-side interactions matching the reference tool.
- Reuse existing pre-computed data; add only what's needed.
- Keep the constituent detail that today's Themes tab provides (as a drill-down).

**Non-goals (v2 / out of scope)**
- 5Y / multi-year history (needs a price-history backfill — see §8).
- True free-float-cap weighting (no float data stored — see §4.3).
- Cross-device starred sync (v1 uses browser localStorage).
- Unit-testing the component's JavaScript.

## 3. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Placement | **Replace** the Themes tab | User choice; drill-down preserves the old detail table |
| Scope | **Fuller replica** | Grid + table + scale + MA + weighting toggle + starred |
| Benchmark | **Nifty 500** (`^CRSLDX`), one-time seed | Matches screenshots; only Nifty 50 exists today |
| Weighting | **Equal-Weight** and **Market-Cap** | No free-float data; market-cap is the honest stand-in for "Free-Float Cap" |
| History | Cap at **1Y / YTD**; 5Y greyed out | `prices_daily` holds ~400 days |
| Rendering | **Single client-side HTML/JS component** | Only option that reproduces instant period/scale switching |

## 4. Backend / data model

### 4.1 Nifty 500 benchmark seed

New `backend/seed_nifty500_index.py`, mirroring
[seed_nifty_index.py](../../../backend/seed_nifty_index.py):
1. Insert `^CRSLDX` (Yahoo symbol for Nifty 500 TR/Index; verify at build time —
   fall back to `^CRSLDX`/`0P0000XVFN`-style ticker if needed) into `stocks`
   with `is_active = TRUE` so the daily refresh keeps it current.
2. Backfill ~2 years of OHLCV into `prices_daily`.

Because `daily_refresh.run()` fetches every active stock, once `^CRSLDX` is in
`stocks` it is refreshed automatically — no change to the fetch loop.

### 4.2 New table `theme_index_daily`

`backend/schema_theme_index.sql` (idempotent `CREATE TABLE IF NOT EXISTS`):

```sql
CREATE TABLE IF NOT EXISTS theme_index_daily (
    theme_slug  TEXT NOT NULL REFERENCES themes(theme_slug),
    date        DATE NOT NULL,
    index_ew    NUMERIC,   -- equal-weight index level (base 100 at window start)
    index_mcap  NUMERIC,   -- market-cap-weight index level (base 100)
    n_members   INT,       -- constituents contributing that day
    PRIMARY KEY (theme_slug, date)
);
CREATE INDEX IF NOT EXISTS idx_theme_index_daily_slug ON theme_index_daily(theme_slug);
```

### 4.3 Index methodology — `backend/compute_theme_index.py`

Loads the last ~400 days of `close` from `prices_daily` for all theme members in
one bulk query (same pattern as `compute_relative_strength.py`), plus latest
`market_cap_cr` and `cmp` from `snapshots_daily` for weights. For each theme:

- **Equal-weight (chain-linked daily returns):**
  `EW(t) = EW(t-1) × (1 + mean_i r_i(t))`, `EW(base) = 100`, where `r_i(t)` is
  member *i*'s daily return and the mean is over members with a valid return that
  day. Chain-linking makes the index robust to missing data and recent listings.

- **Market-cap-weight (constant-share approximation):**
  shares_i ≈ `market_cap_cr_i / cmp_i` (latest), held constant. Daily index
  return = `Σ_i w_i(t-1) · r_i(t)` where `w_i(t-1) = shares_i·p_i(t-1) / Σ_j shares_j·p_j(t-1)`.
  Chain-link from `MCAP(base) = 100`. This is a proper cap-weighted **price**
  index with constant share counts — the honest substitute for free-float cap.

Writes one row per theme per day into `theme_index_daily` via `execute_values`
upsert. Runs as a **non-fatal step** in `daily_refresh.py` after snapshots
(so `snapshots_daily` weights are fresh), following the try/except pattern of the
technicals/RS/setup steps.

### 4.4 daily_refresh integration

Add, after the relative-strength step in `daily_refresh.run()`:

```python
logger.info("Computing theme indices...")
try:
    from compute_theme_index import run_theme_index_refresh
    run_theme_index_refresh()
except Exception as ti_err:
    logger.error(f"  Theme index refresh failed (non-fatal): {ti_err}", exc_info=True)
```

## 5. Frontend architecture

New module **`frontend/themes_rotation_tab.py`** (mirrors `global_markets_tab.py`)
so `app.py` (already ~4,835 lines) doesn't grow further. `app.py`'s
`render_themes_view()` is replaced by a thin call into this module. The old
`load_themes` / `load_theme_averages` / `load_theme_stocks` helpers and the
`_prepare_theme_display` formatting are retired or moved (drill-down reuses the
same columns).

### 5.1 Data loader → JSON payload

A cached loader (`@st.cache_data(ttl=300)`) builds **one** payload:

```jsonc
{
  "as_of": "2026-08-20",
  "dark": true,                         // theme flag → component styling
  "dates": ["2025-06-01", ...],         // shared trading-day axis (~400)
  "benchmark": { "name": "NIFTY 500", "level": [100.0, ...] },  // aligned to dates
  "themes": [
    {
      "slug": "ev_battery", "name": "EV & Battery", "n_stocks": 12,
      "ew":   [100.0, ...],             // aligned to dates, null before data
      "mcap": [100.0, ...],
      "stats": {                        // from snapshots_daily returns
        "1D": {"ret": -0.5, "adv": 6, "dec": 6},
        "1W": {...}, "1M": {...}, "3M": {...}, "6M": {...}, "1Y": {...}
      },
      "constituents": [
        {"symbol":"...","name":"...","cmp":..., "ret_1d":..., "ret_1w":...,
         "ret_30d":..., "ret_60d":..., "ret_180d":..., "ret_365d":...,
         "mcap":..., "pe":..., "screener_url":"...", "tradingview_url":"..."}
      ]
    }
  ]
}
```

Notes:
- **Shared `dates` axis** + per-theme aligned arrays (null where a theme's index
  has no value yet) avoids repeating dates and keeps the payload small
  (~50 themes × 400 × 2 ≈ a few hundred KB JSON).
- `stats` come from one grouped query over `theme_membership ⋈ snapshots_daily`
  (extends today's `load_theme_averages`, adding positive/negative counts).
  Period→column map: 1D→`ret_1d`, 1W→`ret_1w`, 1M→`ret_30d`, 3M→`ret_60d`,
  6M→`ret_180d`, 1Y→`ret_365d`. **YTD** has no snapshot column: its headline
  return is derived client-side from the index series; its breadth reuses the 1Y
  bucket (documented approximation).
- `constituents` come from one combined query (today's `load_theme_stocks` logic,
  un-parameterised to fetch all themes at once) and power both the drill-down and
  the stock-search filter.

### 5.2 The component (client-side)

One `components.html(...)` string: HTML + CSS + vanilla JS, styled from the
`dark` flag and the app's `_T` palette passed in. No external libraries (charts
are hand-rolled inline SVG). Everything below is client-side — **no Streamlit
reruns** except the Refresh button (which clears the cache).

**Toolbar**
- Grid / Table view toggle
- Scale: Lin / Log (log applied to plotted values)
- MA: 20 / 50 / 200 overlays (SMA of the index level, toggle each)
- Index Type: Equal-Weight / Market-Cap (switches which series is charted)
- Period: 1D 1W 1M 3M 6M 1Y YTD (5Y rendered **disabled**)
- Search: matches theme name **or** a constituent symbol/name → filters to
  themes containing that stock
- Filter: All / Top 20 / Bottom 20 (by current-period return) / Above MA / Starred
- Sort: Return / Breadth / Name
- Refresh: clears `st.cache_data` and reruns (native button above the component)

**Card** (per theme): title + ★ toggle; period return (green/red) and today %;
`N stocks ▲x ▼y` breadth; an SVG line chart of the selected index series
**rebased to 100 over the visible period window** vs the benchmark (dotted),
with a crosshair hover tooltip showing date + theme value + benchmark value;
bottom legend `— <theme>  ·· NIFTY 500`.

**Client-side computations:** windowing to the selected period, rebasing the
window's first point to 100, YTD return, SMA overlays, log transform, and all
filter/sort/search. The headline card return equals the index's move over the
window (consistent with the chart line).

**Table view:** same themes as sortable rows — name, return (selected period),
today %, breadth, distance from selected MA — for scanning many at once.

**Drill-down:** clicking a card opens an in-component panel listing that theme's
constituents in a table with the same columns as today's Themes table (CMP,
1D–1Y returns, Market Cap, P/E) plus Screener / Chart links, sourced from the
`constituents` array. Preserves everything the current tab offers.

**Starred:** ★ per theme persisted in browser `localStorage` (keyed by slug);
the Starred filter reads it. Per-browser only (v2 = DB sync).

### 5.3 Fallback when `components.html` is unavailable

On Python 3.14+ (`_COMPONENTS_HTML_SAFE == False`) `components.html` deadlocks
and is skipped app-wide. The tab must degrade gracefully: render a native
`st.dataframe` of themes (name, per-period return, breadth) with a period
`st.radio`, and a `st.selectbox` + single `st.plotly_chart` to view one theme's
index vs benchmark. Usable, just not the live grid. A one-line caption explains
the reduced mode.

## 6. Period & history constraints

`prices_daily` holds ~400 days, so 1D–1Y and YTD are fully supported; **5Y is
disabled** until history is backfilled (§8). The benchmark seed pulls ~2 years,
but the shared date axis is bounded by stock history (~400 days).

## 7. Testing

- **`backend/tests/test_theme_index.py`** (pytest): synthetic-price fixtures
  asserting (a) EW chain-link math and base=100, (b) MCAP weighting favours the
  larger-cap member correctly, (c) missing member data on some days is handled
  without breaking the chain, (d) a member listed mid-window contributes only
  from its first valid day.
- **Payload builder test** (`frontend/tests/`): given a small fixture, the loader
  returns the documented keys/shapes, aligned array lengths equal `len(dates)`,
  and the period→column map is applied correctly.
- The component JS is kept minimal and inspectable; not unit-tested (v2).

## 8. Future work (v2)

- Backfill multi-year `prices_daily` to enable 3Y/5Y periods.
- Real free-float weights if a float data source is added.
- Persist stars in Supabase (`theme_stars` table) for cross-device sync.
- Optional: theme-vs-theme overlay (compare 2–3 theme indices on one chart).

## 9. File-by-file changes

| File | Change |
|---|---|
| `backend/schema_theme_index.sql` | **New** — `theme_index_daily` table |
| `backend/seed_nifty500_index.py` | **New** — seed `^CRSLDX` into stocks + backfill prices |
| `backend/compute_theme_index.py` | **New** — compute EW + MCAP daily indices → `theme_index_daily` |
| `backend/daily_refresh.py` | **Edit** — add non-fatal `run_theme_index_refresh()` step |
| `backend/tests/test_theme_index.py` | **New** — index-methodology unit tests |
| `frontend/themes_rotation_tab.py` | **New** — payload builder + component + fallback |
| `frontend/app.py` | **Edit** — `render_themes_view()` calls the new module; retire old theme loaders/formatters |
| `frontend/tests/test_theme_rotation_payload.py` | **New** — payload-shape test |

## 10. Risks

- **Payload size** if theme count is large — mitigated by shared date axis and
  numeric-only arrays; add pagination only if it proves heavy.
- **`^CRSLDX` availability on yfinance** — verify the ticker at build time; the
  benchmark line is the one hard external dependency.
- **Component maintainability** — all interactivity lives in one file; keep the
  JS structured (toolbar state → render()), documented, and free of frameworks.

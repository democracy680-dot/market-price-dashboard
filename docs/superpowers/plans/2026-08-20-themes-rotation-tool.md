# Themes Rotation Tool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Themes tab with a card-grid "Theme Rotation Tool" — each theme shown as a rebased-to-100 index (equal-weight + market-cap) charted against a Nifty 500 benchmark, with a client-side toolbar for period/scale/MA/weighting/filter/sort/search and a constituent drill-down.

**Architecture:** A pre-computed backend table `theme_index_daily` (populated in the daily refresh) holds each theme's daily equal-weight and market-cap index levels. A new frontend module `themes_rotation_tab.py` builds one JSON payload and renders the entire interactive grid as a single self-contained `components.html` HTML/JS component, so period/scale/MA/filter switching happens client-side with no Streamlit rerun. A native Streamlit fallback covers Python 3.14+ where `components.html` is disabled.

**Tech Stack:** Python, pandas, SQLAlchemy + psycopg2 (`execute_values`), Supabase/PostgreSQL, yfinance, Streamlit, vanilla JS + inline SVG (no chart libraries), pytest.

## Global Constraints

- No secrets outside `.env`; DB access via `SUPABASE_DB_URL` through `backend/db.py` / the app's `engine`.
- Bulk writes use psycopg2 `execute_values` — never row-by-row.
- Frontend is read-only except existing watchlists; the rotation tool reads only. Stars persist in browser `localStorage` (no DB write).
- Pre-compute everything: the grid reads `theme_index_daily`; it never computes indices at request time.
- Component code must be self-contained: no external JS/CSS/CDN; charts are hand-rolled inline SVG.
- History is ~400 days: periods 1D/1W/1M/3M/6M/1Y/YTD are supported; **5Y renders disabled**.
- Weighting labels: **"Equal Weight"** and **"Market Cap"** (no free-float data exists).
- Benchmark: **Nifty 500** via Yahoo symbol `^CRSLDX` (verify at build; see Task 2).
- Guard every `components.html` call with `_COMPONENTS_HTML_SAFE = sys.version_info < (3, 14)`; provide the native fallback.
- Execute this plan in an isolated git worktree; the Themes tab is only swapped to the new module once the component is functional, so `main` never deploys a half-built tab. Merge to `main` and push only when all tasks pass (honors the user's push-to-main + auto-deploy preference).

**Period → snapshot return column map (single source of truth, used in Tasks 5–8):**
`1D→ret_1d`, `1W→ret_1w`, `1M→ret_30d`, `3M→ret_60d`, `6M→ret_180d`, `1Y→ret_365d`. `YTD` has no column — its return/breadth are derived client-side from the index series (breadth reuses the 1Y bucket).

---

### Task 1: `theme_index_daily` schema

**Files:**
- Create: `backend/schema_theme_index.sql`

**Interfaces:**
- Produces: table `theme_index_daily(theme_slug TEXT, date DATE, index_ew NUMERIC, index_mcap NUMERIC, n_members INT, PRIMARY KEY(theme_slug,date))` — consumed by Tasks 4, 5.

- [ ] **Step 1: Write the schema file**

Create `backend/schema_theme_index.sql`:

```sql
-- Indian Equity Dashboard — Theme Index Schema (Extension)
-- Run in Supabase SQL Editor AFTER schema_themes.sql. Safe to re-run.

CREATE TABLE IF NOT EXISTS theme_index_daily (
    theme_slug  TEXT NOT NULL REFERENCES themes(theme_slug),
    date        DATE NOT NULL,
    index_ew    NUMERIC,   -- equal-weight index level, base 100 at window start
    index_mcap  NUMERIC,   -- market-cap-weight index level, base 100 at window start
    n_members   INT,       -- constituents with valid data that day
    PRIMARY KEY (theme_slug, date)
);

CREATE INDEX IF NOT EXISTS idx_theme_index_daily_slug ON theme_index_daily(theme_slug);
CREATE INDEX IF NOT EXISTS idx_theme_index_daily_date ON theme_index_daily(date);
```

- [ ] **Step 2: Apply it to the database**

Run the file's contents in the Supabase SQL editor (or `psql "$SUPABASE_DB_URL" -f backend/schema_theme_index.sql`).
Expected: `CREATE TABLE` / `CREATE INDEX` succeed with no error; re-running is a no-op.

- [ ] **Step 3: Verify the table exists**

Run: `psql "$SUPABASE_DB_URL" -c "\d theme_index_daily"`
Expected: shows the 5 columns and the primary key.

- [ ] **Step 4: Commit**

```bash
git add backend/schema_theme_index.sql
git commit -m "feat(themes): add theme_index_daily schema"
```

---

### Task 2: Nifty 500 benchmark seed

**Files:**
- Create: `backend/seed_nifty500_index.py`

**Interfaces:**
- Produces: rows in `stocks` (`^CRSLDX`, `is_active=TRUE`) and `prices_daily` for `^CRSLDX` — consumed by Task 5 benchmark loader. Because `daily_refresh.run()` fetches all active stocks, `^CRSLDX` refreshes automatically thereafter.

- [ ] **Step 1: Verify the Yahoo ticker returns data**

Run:
```bash
python -c "import yfinance as yf; df=yf.download('^CRSLDX', period='5d'); print(df.tail()); print('rows', len(df))"
```
Expected: a non-empty OHLCV frame. If empty, try `^CRSLDX` alternatives (`^CNX500`) and use whichever returns data; set that as `NIFTY500_YAHOO` in the script below.

- [ ] **Step 2: Write the seed script**

Create `backend/seed_nifty500_index.py` (modeled on `backend/seed_nifty_index.py`):

```python
"""
seed_nifty500_index.py — One-time script to add ^CRSLDX (Nifty 500) to the DB.

  1. Inserts ^CRSLDX into the stocks table (if not present), is_active=TRUE
     so daily_refresh keeps it current.
  2. Fetches ~2 years of ^CRSLDX OHLCV from yfinance and upserts into prices_daily.

Usage:
    python backend/seed_nifty500_index.py
"""

import sys
import logging
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from psycopg2.extras import execute_values
from sqlalchemy import text

from db import get_engine, get_psycopg2_conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

NIFTY500_SYMBOL = "^CRSLDX"
NIFTY500_YAHOO  = "^CRSLDX"   # confirmed in Task 2 Step 1
NIFTY500_NAME   = "Nifty 500"


def ensure_stock_row(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stocks (symbol, name, yahoo_symbol, sector, is_active)
            VALUES (:sym, :name, :yah, 'Index', TRUE)
            ON CONFLICT (symbol) DO UPDATE SET is_active = TRUE, yahoo_symbol = EXCLUDED.yahoo_symbol
        """), {"sym": NIFTY500_SYMBOL, "name": NIFTY500_NAME, "yah": NIFTY500_YAHOO})
    logger.info("  ^CRSLDX present in stocks (is_active=TRUE)")


def fetch_and_upsert_prices(engine):
    start = date.today() - timedelta(days=730)
    df = yf.download(NIFTY500_YAHOO, start=start.isoformat(), auto_adjust=False, progress=False)
    if df is None or df.empty:
        logger.error("  yfinance returned no data for %s — aborting.", NIFTY500_YAHOO)
        sys.exit(1)
    df = df.reset_index()
    # Flatten possible MultiIndex columns from yfinance
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    rows = []
    for _, r in df.iterrows():
        d = pd.to_datetime(r["Date"]).date()
        rows.append((NIFTY500_SYMBOL, d,
                     float(r["Open"]), float(r["High"]), float(r["Low"]),
                     float(r["Close"]), int(r["Volume"]) if pd.notna(r["Volume"]) else 0))
    sql = """
        INSERT INTO prices_daily (symbol, date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    logger.info("  Upserted %d ^CRSLDX price rows", len(rows))


def run():
    engine = get_engine()
    ensure_stock_row(engine)
    fetch_and_upsert_prices(engine)
    logger.info("=== Nifty 500 seed complete ===")


if __name__ == "__main__":
    run()
```

- [ ] **Step 3: Run the seed**

Run: `python backend/seed_nifty500_index.py`
Expected: logs "^CRSLDX present in stocks" and "Upserted N ^CRSLDX price rows" with N > 200.

- [ ] **Step 4: Verify prices landed**

Run: `psql "$SUPABASE_DB_URL" -c "SELECT COUNT(*), MIN(date), MAX(date) FROM prices_daily WHERE symbol='^CRSLDX';"`
Expected: count > 200, MAX(date) within the last few trading days.

- [ ] **Step 5: Commit**

```bash
git add backend/seed_nifty500_index.py
git commit -m "feat(themes): seed Nifty 500 (^CRSLDX) benchmark"
```

---

### Task 3: Index math pure functions (TDD)

**Files:**
- Create: `backend/compute_theme_index.py`
- Test: `backend/tests/test_theme_index.py`

**Interfaces:**
- Produces (consumed by Task 4):
  - `compute_equal_weight_index(pivot: pd.DataFrame) -> pd.Series` — pivot indexed by date, columns=symbols, values=close; returns EW index level (base 100), same date index.
  - `compute_mcap_weight_index(pivot: pd.DataFrame, shares: dict[str, float]) -> pd.Series` — cap-weighted level (base 100).
  - `compute_theme_index_frame(closes_long: pd.DataFrame, shares: dict[str, float]) -> pd.DataFrame` — input columns `[symbol, date, close]`; returns `[date, index_ew, index_mcap, n_members]`.

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/test_theme_index.py`:

```python
"""
Unit tests for compute_theme_index.py pure functions.
Run: pytest backend/tests/test_theme_index.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from compute_theme_index import (
    compute_equal_weight_index,
    compute_mcap_weight_index,
    compute_theme_index_frame,
)

DATES = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])


def _pivot(data):
    return pd.DataFrame(data, index=DATES)


def test_equal_weight_base_100_and_average_returns():
    # A: +10%, +10% ; B: +10%, -10%  -> EW: 100, 110, 110
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, 49.5]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[0] - 100.0) < 1e-9
    assert abs(ew.iloc[1] - 110.0) < 1e-9
    assert abs(ew.iloc[2] - 110.0) < 1e-6


def test_mcap_weight_favours_larger_cap_member():
    # shares A=1, B=10. B (heavier) falls on d2 -> index down to ~102.667
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, 49.5]})
    mcap = compute_mcap_weight_index(p, {"A": 1.0, "B": 10.0})
    assert abs(mcap.iloc[0] - 100.0) < 1e-9
    assert abs(mcap.iloc[1] - 110.0) < 1e-6
    assert abs(mcap.iloc[2] - 102.6667) < 1e-3


def test_missing_member_data_handled():
    # B missing on d2 -> EW d2 uses only A (+10%) -> 121
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, np.nan]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[2] - 121.0) < 1e-6


def test_member_listed_midwindow_contributes_from_first_valid_return():
    # C lists on d1 (NaN at d0); its first return is on d2. EW with A & C:
    # d1 = A only (+10%) -> 110 ; d2 = mean(A +10%, C +10%) -> 121
    p = _pivot({"A": [100, 110, 121.0], "C": [np.nan, 200, 220.0]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[1] - 110.0) < 1e-6
    assert abs(ew.iloc[2] - 121.0) < 1e-6


def test_frame_shape_and_n_members():
    long = pd.DataFrame({
        "symbol": ["A", "A", "A", "B", "B", "B"],
        "date":   list(DATES) * 2,
        "close":  [100, 110, 121.0, 50, 55, 49.5],
    })
    out = compute_theme_index_frame(long, {"A": 1.0, "B": 10.0})
    assert list(out.columns) == ["date", "index_ew", "index_mcap", "n_members"]
    assert len(out) == 3
    assert out["n_members"].tolist() == [2, 2, 2]
    assert abs(out["index_ew"].iloc[1] - 110.0) < 1e-6
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest backend/tests/test_theme_index.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'compute_theme_index'`.

- [ ] **Step 3: Write the implementation**

Create `backend/compute_theme_index.py` (pure functions only for now):

```python
"""
compute_theme_index.py — Pre-computes per-theme daily index levels.

Pure functions (unit-tested):
  compute_equal_weight_index / compute_mcap_weight_index / compute_theme_index_frame

Orchestrator (Task 4): run_theme_index_refresh()
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def compute_equal_weight_index(pivot: pd.DataFrame) -> pd.Series:
    """Equal-weight index level, base 100 at the first row.
    pivot: index=date (sorted), columns=symbol, values=close (NaN where missing).
    Chain-links the daily cross-sectional mean of member returns, so members with
    missing data or a mid-window listing contribute only on days they have a return.
    """
    pivot = pivot.sort_index()
    daily_ret = pivot.pct_change()            # NaN unless a member has both days
    mean_ret = daily_ret.mean(axis=1)         # skipna=True: mean over members present
    idx = (1.0 + mean_ret.fillna(0.0)).cumprod() * 100.0
    return idx


def compute_mcap_weight_index(pivot: pd.DataFrame, shares: dict) -> pd.Series:
    """Market-cap-weight index level, base 100. shares: constant share counts per symbol.
    Chain-links cap-weighted daily returns over the set of members present on BOTH
    the day and the prior day (avoids jumps when a member appears/disappears).
    """
    pivot = pivot.sort_index()
    sh = pd.Series(shares).reindex(pivot.columns).fillna(0.0)
    mv = pivot.mul(sh, axis=1)                 # market value per member per day
    mv_prev = mv.shift(1)
    valid = mv.notna() & mv_prev.notna()
    num = mv.where(valid).sum(axis=1)
    den = mv_prev.where(valid).sum(axis=1)
    ret = (num / den) - 1.0
    ret = ret.replace([float("inf"), float("-inf")], 0.0)
    idx = (1.0 + ret.fillna(0.0)).cumprod() * 100.0
    return idx


def compute_theme_index_frame(closes_long: pd.DataFrame, shares: dict) -> pd.DataFrame:
    """closes_long: columns [symbol, date, close]. Returns [date, index_ew, index_mcap, n_members]."""
    pivot = closes_long.pivot(index="date", columns="symbol", values="close").sort_index()
    ew = compute_equal_weight_index(pivot)
    mcap = compute_mcap_weight_index(pivot, shares)
    n_members = pivot.notna().sum(axis=1)
    return pd.DataFrame({
        "date": pivot.index,
        "index_ew": ew.values,
        "index_mcap": mcap.values,
        "n_members": n_members.values.astype(int),
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backend/tests/test_theme_index.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/compute_theme_index.py backend/tests/test_theme_index.py
git commit -m "feat(themes): equal-weight & market-cap index math with tests"
```

---

### Task 4: Index refresh orchestrator + daily_refresh wiring

**Files:**
- Modify: `backend/compute_theme_index.py` (append orchestrator)
- Modify: `backend/daily_refresh.py` (add non-fatal step after RS)

**Interfaces:**
- Consumes: `compute_theme_index_frame` (Task 3); tables `theme_membership`, `prices_daily`, `snapshots_daily`, `theme_index_daily` (Task 1).
- Produces: `run_theme_index_refresh() -> int` (rows upserted); populates `theme_index_daily`.

- [ ] **Step 1: Append the orchestrator to `compute_theme_index.py`**

Add these imports at the top of `backend/compute_theme_index.py`:

```python
from datetime import datetime, timezone
from psycopg2.extras import execute_values
from sqlalchemy import text
from db import get_engine, get_psycopg2_conn
```

Append:

```python
LOOKBACK_DAYS = 400
_UPSERT_SQL = """
    INSERT INTO theme_index_daily (theme_slug, date, index_ew, index_mcap, n_members)
    VALUES %s
    ON CONFLICT (theme_slug, date) DO UPDATE SET
        index_ew   = EXCLUDED.index_ew,
        index_mcap = EXCLUDED.index_mcap,
        n_members  = EXCLUDED.n_members
"""


def _load_inputs(engine):
    """Returns (membership_df[theme_slug,symbol], closes_df[symbol,date,close], shares{symbol:float})."""
    with engine.connect() as conn:
        membership = pd.read_sql(text("SELECT theme_slug, symbol FROM theme_membership"), conn)
        closes = pd.read_sql(text(f"""
            SELECT p.symbol, p.date, p.close
            FROM prices_daily p
            WHERE p.date >= CURRENT_DATE - INTERVAL '{LOOKBACK_DAYS} days'
              AND p.symbol IN (SELECT DISTINCT symbol FROM theme_membership)
            ORDER BY p.symbol, p.date
        """), conn)
        snap = pd.read_sql(text("""
            SELECT symbol, market_cap_cr, cmp
            FROM snapshots_daily
            WHERE date = (SELECT MAX(date) FROM snapshots_daily)
        """), conn)
    shares = {}
    for _, r in snap.iterrows():
        mc, cmp_ = r["market_cap_cr"], r["cmp"]
        if pd.notna(mc) and pd.notna(cmp_) and cmp_:
            shares[r["symbol"]] = float(mc) / float(cmp_)
    return membership, closes, shares


def run_theme_index_refresh() -> int:
    engine = get_engine()
    membership, closes, shares = _load_inputs(engine)
    if closes.empty or membership.empty:
        logger.warning("  theme index: no membership/price data — skipping")
        return 0

    rows = []
    for slug, grp in membership.groupby("theme_slug"):
        syms = set(grp["symbol"])
        sub = closes[closes["symbol"].isin(syms)]
        if sub.empty:
            continue
        frame = compute_theme_index_frame(sub, {s: shares.get(s, 0.0) for s in syms})
        for _, fr in frame.iterrows():
            d = fr["date"].date() if hasattr(fr["date"], "date") else fr["date"]
            rows.append((slug, d,
                         None if pd.isna(fr["index_ew"]) else float(fr["index_ew"]),
                         None if pd.isna(fr["index_mcap"]) else float(fr["index_mcap"]),
                         int(fr["n_members"])))

    if not rows:
        logger.warning("  theme index: nothing computed")
        return 0

    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    logger.info("  theme_index_daily: %d rows upserted", len(rows))
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    run_theme_index_refresh()
```

- [ ] **Step 2: Confirm pure-function tests still pass**

Run: `pytest backend/tests/test_theme_index.py -v`
Expected: all PASS (imports of psycopg2/sqlalchemy at module top must not break collection).

- [ ] **Step 3: Run the orchestrator against the DB**

Run: `python backend/compute_theme_index.py`
Expected: logs "theme_index_daily: N rows upserted" with N in the thousands.

- [ ] **Step 4: Verify populated data**

Run: `psql "$SUPABASE_DB_URL" -c "SELECT theme_slug, COUNT(*), MAX(index_ew) FROM theme_index_daily GROUP BY theme_slug LIMIT 5;"`
Expected: several themes, each with ~250–400 rows and non-null index values.

- [ ] **Step 5: Wire into `daily_refresh.py`**

In `backend/daily_refresh.py`, after the Relative Strength block (the `run_rs_refresh()` try/except ending around line 405), insert:

```python
    # ── 5c-2. Theme indices (equal-weight + market-cap) ───────────────────────
    logger.info("Computing theme indices...")
    try:
        from compute_theme_index import run_theme_index_refresh
        n_ti = run_theme_index_refresh()
        logger.info(f"  Theme indices: {n_ti} rows")
    except Exception as ti_err:
        logger.error(f"  Theme index refresh failed (non-fatal): {ti_err}", exc_info=True)
```

- [ ] **Step 6: Verify daily_refresh imports cleanly**

Run: `python -c "import sys; sys.path.insert(0,'backend'); import daily_refresh; print('ok')"`
Expected: prints `ok` (no import/syntax error).

- [ ] **Step 7: Commit**

```bash
git add backend/compute_theme_index.py backend/daily_refresh.py
git commit -m "feat(themes): theme index refresh orchestrator + daily_refresh step"
```

---

### Task 5: Frontend payload builder (TDD on pure assembly)

**Files:**
- Create: `frontend/themes_rotation_tab.py`
- Test: `frontend/tests/test_theme_rotation_payload.py`

**Interfaces:**
- Produces (consumed by Tasks 6–8):
  - `PERIOD_COL: dict[str,str]` — the period→column map from Global Constraints.
  - `assemble_payload(index_df, constituents_df, benchmark_df, dates, dark) -> dict` — pure; `index_df` cols `[theme_slug,date,index_ew,index_mcap]`, `constituents_df` cols `[theme_slug,symbol,name,cmp,ret_1d,ret_1w,ret_30d,ret_60d,ret_180d,ret_365d,market_cap_cr,pe_ratio,screener_url,tradingview_url,theme_name,n_stocks]`, `benchmark_df` cols `[date,close]`, `dates` a sorted list of `datetime.date`. Returns the payload dict documented in the spec.
  - `build_rotation_payload() -> dict` — cached DB loader that assembles the live payload.

- [ ] **Step 1: Write the failing test**

Create `frontend/tests/test_theme_rotation_payload.py`:

```python
"""
Tests for the theme rotation payload assembler.
Run: pytest frontend/tests/test_theme_rotation_payload.py -v
"""
import os, sys
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from themes_rotation_tab import assemble_payload, PERIOD_COL

DATES = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]


def _index_df():
    return pd.DataFrame({
        "theme_slug": ["t1"] * 3,
        "date": DATES,
        "index_ew": [100.0, 105.0, 110.0],
        "index_mcap": [100.0, 102.0, 101.0],
    })


def _constituents_df():
    return pd.DataFrame({
        "theme_slug": ["t1", "t1"],
        "symbol": ["A", "B"],
        "name": ["Alpha", "Bravo"],
        "cmp": [100.0, 50.0],
        "ret_1d": [0.01, -0.02],
        "ret_1w": [0.05, -0.01],
        "ret_30d": [0.10, 0.20],
        "ret_60d": [0.15, -0.05],
        "ret_180d": [0.30, 0.10],
        "ret_365d": [0.50, -0.10],
        "market_cap_cr": [1000.0, 500.0],
        "pe_ratio": [20.0, 15.0],
        "screener_url": ["http://s/A", None],
        "tradingview_url": ["http://tv/A", None],
        "theme_name": ["Theme One", "Theme One"],
        "n_stocks": [2, 2],
    })


def _benchmark_df():
    return pd.DataFrame({"date": DATES, "close": [20000.0, 20100.0, 20200.0]})


def test_payload_top_level_keys():
    p = assemble_payload(_index_df(), _constituents_df(), _benchmark_df(), DATES, dark=True)
    assert set(p.keys()) == {"as_of", "dark", "dates", "benchmark", "themes"}
    assert p["dark"] is True
    assert p["as_of"] == "2026-01-03"
    assert p["dates"] == ["2026-01-01", "2026-01-02", "2026-01-03"]


def test_benchmark_aligned_to_dates():
    p = assemble_payload(_index_df(), _constituents_df(), _benchmark_df(), DATES, dark=False)
    assert p["benchmark"]["name"] == "NIFTY 500"
    assert p["benchmark"]["level"] == [20000.0, 20100.0, 20200.0]


def test_theme_series_and_breadth():
    p = assemble_payload(_index_df(), _constituents_df(), _benchmark_df(), DATES, dark=False)
    t = p["themes"][0]
    assert t["slug"] == "t1"
    assert t["name"] == "Theme One"
    assert t["n_stocks"] == 2
    assert t["ew"] == [100.0, 105.0, 110.0]
    assert t["mcap"] == [100.0, 102.0, 101.0]
    # 1M -> ret_30d: A +0.10 (up), B +0.20 (up) => adv 2, dec 0
    assert t["stats"]["1M"] == {"adv": 2, "dec": 0}
    # 1Y -> ret_365d: A +0.50 (up), B -0.10 (down) => adv 1, dec 1
    assert t["stats"]["1Y"] == {"adv": 1, "dec": 1}
    assert len(t["constituents"]) == 2


def test_missing_theme_day_becomes_null():
    idx = _index_df().iloc[1:].copy()  # drop 2026-01-01 for t1
    p = assemble_payload(idx, _constituents_df(), _benchmark_df(), DATES, dark=False)
    t = p["themes"][0]
    assert t["ew"][0] is None
    assert t["ew"][1] == 105.0


def test_period_col_map_is_the_contract():
    assert PERIOD_COL == {"1D": "ret_1d", "1W": "ret_1w", "1M": "ret_30d",
                          "3M": "ret_60d", "6M": "ret_180d", "1Y": "ret_365d"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest frontend/tests/test_theme_rotation_payload.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'themes_rotation_tab'`.

- [ ] **Step 3: Write the module's pure assembler + loaders**

Create `frontend/themes_rotation_tab.py`:

```python
"""
themes_rotation_tab.py — Theme Rotation Tool (replaces the Themes tab).

Pure, unit-tested:  assemble_payload, PERIOD_COL
DB loaders:         build_rotation_payload  (cached)
Rendering:          render_rotation_tool, build_component_html, _render_fallback  (Tasks 6–9)
"""
import sys
import json
from datetime import date

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import text

_COMPONENTS_HTML_SAFE = sys.version_info < (3, 14)
BENCHMARK_NAME = "NIFTY 500"

PERIOD_COL = {
    "1D": "ret_1d", "1W": "ret_1w", "1M": "ret_30d",
    "3M": "ret_60d", "6M": "ret_180d", "1Y": "ret_365d",
}


def _iso(d) -> str:
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def assemble_payload(index_df, constituents_df, benchmark_df, dates, dark) -> dict:
    date_keys = [_iso(d) for d in dates]
    pos = {k: i for i, k in enumerate(date_keys)}

    # Benchmark aligned to the shared axis
    bench_level = [None] * len(date_keys)
    for _, r in benchmark_df.iterrows():
        k = _iso(r["date"])
        if k in pos and pd.notna(r["close"]):
            bench_level[pos[k]] = float(r["close"])

    themes = []
    for slug, grp in constituents_df.groupby("theme_slug"):
        first = grp.iloc[0]
        # Series aligned to shared axis
        ew = [None] * len(date_keys)
        mc = [None] * len(date_keys)
        idx_rows = index_df[index_df["theme_slug"] == slug]
        for _, ir in idx_rows.iterrows():
            k = _iso(ir["date"])
            if k in pos:
                ew[pos[k]] = None if pd.isna(ir["index_ew"]) else float(ir["index_ew"])
                mc[pos[k]] = None if pd.isna(ir["index_mcap"]) else float(ir["index_mcap"])
        # Breadth per period from constituent returns
        stats = {}
        for period, col in PERIOD_COL.items():
            vals = pd.to_numeric(grp[col], errors="coerce")
            stats[period] = {"adv": int((vals > 0).sum()), "dec": int((vals < 0).sum())}
        # Constituents for drill-down + stock search
        cons = []
        for _, c in grp.iterrows():
            cons.append({
                "symbol": c["symbol"], "name": c["name"],
                "cmp": _num(c["cmp"]),
                "ret_1d": _num(c["ret_1d"]), "ret_1w": _num(c["ret_1w"]),
                "ret_30d": _num(c["ret_30d"]), "ret_60d": _num(c["ret_60d"]),
                "ret_180d": _num(c["ret_180d"]), "ret_365d": _num(c["ret_365d"]),
                "mcap": _num(c["market_cap_cr"]), "pe": _num(c["pe_ratio"]),
                "screener_url": c["screener_url"] if pd.notna(c["screener_url"]) else None,
                "tradingview_url": c["tradingview_url"] if pd.notna(c["tradingview_url"]) else None,
            })
        themes.append({
            "slug": slug, "name": first["theme_name"],
            "n_stocks": int(first["n_stocks"]),
            "ew": ew, "mcap": mc, "stats": stats, "constituents": cons,
        })

    return {
        "as_of": date_keys[-1] if date_keys else None,
        "dark": bool(dark),
        "dates": date_keys,
        "benchmark": {"name": BENCHMARK_NAME, "level": bench_level},
        "themes": themes,
    }


def _num(v):
    return None if pd.isna(v) else float(v)


@st.cache_data(ttl=300, show_spinner=False)
def build_rotation_payload(dark: bool) -> dict:
    from app import engine  # reuse the app's NullPool engine
    with engine.connect() as conn:
        dates_df = pd.read_sql(text("""
            SELECT DISTINCT date FROM theme_index_daily ORDER BY date
        """), conn)
        index_df = pd.read_sql(text("""
            SELECT theme_slug, date, index_ew, index_mcap FROM theme_index_daily
            ORDER BY theme_slug, date
        """), conn)
        bench_df = pd.read_sql(text("""
            SELECT date, close FROM prices_daily WHERE symbol='^CRSLDX' ORDER BY date
        """), conn)
        cons_df = pd.read_sql(text("""
            SELECT tm.theme_slug, t.theme_name,
                   (SELECT COUNT(*) FROM theme_membership x WHERE x.theme_slug=tm.theme_slug) AS n_stocks,
                   s.symbol, s.name, s.screener_url, s.tradingview_url,
                   snap.cmp, snap.ret_1d, snap.ret_1w, snap.ret_30d, snap.ret_60d,
                   snap.ret_180d, snap.ret_365d, snap.market_cap_cr,
                   COALESCE(lf.pe_ttm, snap.pe_ratio) AS pe_ratio
            FROM theme_membership tm
            JOIN themes t ON t.theme_slug = tm.theme_slug
            JOIN stocks s ON s.symbol = tm.symbol
            LEFT JOIN snapshots_daily snap
                ON snap.symbol = s.symbol AND snap.date = (SELECT MAX(date) FROM snapshots_daily)
            LEFT JOIN latest_financials lf ON lf.symbol = s.symbol
            ORDER BY tm.theme_slug, snap.market_cap_cr DESC NULLS LAST
        """), conn)
    dates = [d.date() if hasattr(d, "date") else d for d in dates_df["date"].tolist()]
    return assemble_payload(index_df, cons_df, bench_df, dates, dark)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest frontend/tests/test_theme_rotation_payload.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/themes_rotation_tab.py frontend/tests/test_theme_rotation_payload.py
git commit -m "feat(themes): rotation payload builder with tests"
```

---

### Task 6: Component core — grid, SVG chart, benchmark, primary toolbar

**Files:**
- Modify: `frontend/themes_rotation_tab.py` (add `build_component_html`, `render_rotation_tool`)
- Modify: `frontend/app.py` (`_frag_themes` → call `render_rotation_tool`)

**Interfaces:**
- Consumes: `build_rotation_payload` (Task 5), `_COMPONENTS_HTML_SAFE`.
- Produces: `build_component_html(payload: dict, tokens: dict) -> str`; `render_rotation_tool(dark: bool, tokens: dict)`.

> The component JS uses a single global `S` (state) and one `render()` that redraws the grid from `S`. Task 6 delivers: card grid, per-card SVG line (selected weighting) + dotted benchmark, crosshair tooltip, and the toolbar controls Period / Scale (Lin/Log) / MA (20/50/200) / Index Type (Equal Weight / Market Cap) / Grid view. Filter/sort/search/star/table/drill-down are stubbed as no-ops here and implemented in Tasks 7–8.

- [ ] **Step 1: Add `build_component_html` and `render_rotation_tool`**

Append to `frontend/themes_rotation_tab.py`:

```python
def render_rotation_tool(dark: bool, tokens: dict):
    st.markdown("#### Theme Rotation Tool")
    _sp, _rf = st.columns([6, 1])
    with _rf:
        if st.button("↻ Refresh", key="rot_refresh", use_container_width=True):
            build_rotation_payload.clear()
            st.rerun()

    payload = build_rotation_payload(dark)
    if not payload["themes"]:
        st.info("No theme index data yet — run `python backend/compute_theme_index.py`.")
        return

    if _COMPONENTS_HTML_SAFE:
        n = len(payload["themes"])
        height = 190 + ((n + 2) // 3) * 250   # toolbar + card rows
        components.html(build_component_html(payload, tokens), height=min(height, 4000), scrolling=True)
    else:
        _render_fallback(payload)


def build_component_html(payload: dict, tokens: dict) -> str:
    data_json = json.dumps(payload)
    dark = payload["dark"]
    bg      = "#0f1729" if dark else "#ffffff"
    page_bg = "#080c14" if dark else "#f0f4f8"
    border  = "#1e2d45" if dark else "#e2e8f0"
    text_c  = "#f1f5f9" if dark else "#0f172a"
    sub_c   = "#64748b" if dark else "#94a3b8"
    line_c  = "#22c55e"
    bench_c = "#94a3b8" if dark else "#64748b"
    grid_c  = "#111827" if dark else "#e2e8f0"
    up_c    = "#22c55e"
    down_c  = "#ef4444"
    sel_bg  = "#1d3461" if dark else "#dbeafe"

    return f"""
<div id="rot-root" style="font-family:Inter,-apple-system,sans-serif;color:{text_c};background:{page_bg};padding:8px;">
  <div id="rot-toolbar" style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px;font-size:12px;">
    <div class="rot-seg" data-group="view"></div>
    <div class="rot-seg" data-group="scale"></div>
    <div class="rot-seg" data-group="ma"></div>
    <div class="rot-seg" data-group="weight"></div>
    <div class="rot-seg" data-group="period"></div>
    <input id="rot-search" placeholder="Search theme or stock…"
           style="margin-left:auto;padding:6px 10px;border:1px solid {border};border-radius:6px;
                  background:{bg};color:{text_c};min-width:200px;font-size:12px;">
    <div class="rot-seg" data-group="filter"></div>
    <div class="rot-seg" data-group="sort"></div>
  </div>
  <div id="rot-count" style="font-size:11px;color:{sub_c};margin-bottom:8px;"></div>
  <div id="rot-grid" style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;"></div>
  <div id="rot-drill"></div>
</div>
<script>
const DATA = {data_json};
const C = {{ bg:"{bg}", border:"{border}", text:"{text_c}", sub:"{sub_c}",
            line:"{line_c}", bench:"{bench_c}", grid:"{grid_c}", up:"{up_c}",
            down:"{down_c}", selBg:"{sel_bg}" }};

const S = {{ view:"grid", scale:"lin", ma:{{20:false,50:false,200:false}},
            weight:"ew", period:"3M", filter:"all", sort:"return",
            search:"", stars:loadStars() }};

const PERIOD_TDAYS = {{"1D":1,"1W":5,"1M":21,"3M":63,"6M":126,"1Y":252}};

function loadStars() {{
  try {{ return JSON.parse(localStorage.getItem("rot_stars")||"[]"); }} catch(e) {{ return []; }}
}}
function saveStars() {{ localStorage.setItem("rot_stars", JSON.stringify(S.stars)); }}

// ---- windowing + rebasing -------------------------------------------------
function windowIdx() {{
  const n = DATA.dates.length;
  if (S.period === "YTD") {{
    const yr = DATA.dates[n-1].slice(0,4);
    let i = DATA.dates.findIndex(d => d.slice(0,4) === yr);
    return i < 0 ? 0 : i;
  }}
  const td = PERIOD_TDAYS[S.period] || 63;
  return Math.max(0, n - 1 - td);
}}
function rebase(arr, start) {{
  const win = arr.slice(start);
  const base = win.find(v => v != null);
  if (base == null) return win.map(_ => null);
  return win.map(v => v == null ? null : (v / base) * 100);
}}
function sma(arr, period) {{
  const out = arr.map(_ => null);
  for (let i=0;i<arr.length;i++) {{
    if (i+1 < period) continue;
    let s=0, ok=true;
    for (let j=i-period+1;j<=i;j++) {{ if (arr[j]==null){{ok=false;break;}} s+=arr[j]; }}
    if (ok) out[i]=s/period;
  }}
  return out;
}}
function seriesFor(t) {{ return S.weight === "ew" ? t.ew : t.mcap; }}
function periodReturn(t) {{
  const s = windowIdx();
  const w = seriesFor(t).slice(s).filter(v=>v!=null);
  if (w.length < 2) return 0;
  return (w[w.length-1]/w[0]-1)*100;
}}
function todayReturn(t) {{
  const a = seriesFor(t).filter(v=>v!=null);
  if (a.length < 2) return 0;
  return (a[a.length-1]/a[a.length-2]-1)*100;
}}
function breadth(t) {{
  const p = S.period === "YTD" ? "1Y" : S.period;
  return t.stats[p] || {{adv:0,dec:0}};
}}

// ---- SVG chart ------------------------------------------------------------
function pathFor(vals, W, H, lo, hi) {{
  const n = vals.length; let d=""; let started=false;
  for (let i=0;i<n;i++) {{
    if (vals[i]==null) {{ started=false; continue; }}
    const x = (i/(n-1))*W;
    const y = H - ((vals[i]-lo)/(hi-lo))*H;
    d += (started?" L":" M") + x.toFixed(1) + " " + y.toFixed(1);
    started=true;
  }}
  return d;
}}
function drawCard(t) {{
  const start = windowIdx();
  let idx = rebase(seriesFor(t), start);
  let bench = rebase(DATA.benchmark.level, start);
  const mas = [20,50,200].filter(p=>S.ma[p]).map(p=>rebase(sma(seriesFor(t),p), start));
  let all = idx.concat(bench).concat(...mas).filter(v=>v!=null);
  if (S.scale === "log") {{ const f=v=>v==null?null:Math.log10(v);
      idx=idx.map(f); bench=bench.map(f); all=all.map(v=>Math.log10(v)); }}
  const lo=Math.min(...all), hi=Math.max(...all)||1; const W=260,H=120;
  const ret = periodReturn(t), tod = todayReturn(t), b = breadth(t);
  const retColor = ret>=0 ? C.up : C.down;
  const starred = S.stars.includes(t.slug);
  const maPaths = mas.map((m,k)=>{{
      const mm = S.scale==="log"? m.map(v=>v==null?null:Math.log10(v)) : m;
      const col = ["#eab308","#3b82f6","#a855f7"][k];
      return `<path d="${{pathFor(mm,W,H,lo,hi)}}" fill="none" stroke="${{col}}" stroke-width="1" opacity="0.8"/>`;
  }}).join("");
  return `
  <div class="rot-card" data-slug="${{t.slug}}"
       style="background:${{C.bg}};border:1px solid ${{C.border}};border-radius:10px;padding:12px;cursor:pointer;">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
      <div>
        <div style="font-weight:700;font-size:14px;">${{t.name}}</div>
        <div style="font-size:11px;color:${{C.sub}};margin-top:2px;">
          ${{t.n_stocks}} stocks <span style="color:${{C.up}}">▲${{b.adv}}</span>
          <span style="color:${{C.down}}">▼${{b.dec}}</span>
        </div>
      </div>
      <div style="text-align:right;">
        <div style="font-weight:700;color:${{retColor}};font-size:15px;">${{ret>=0?"+":""}}${{ret.toFixed(2)}}%</div>
        <div style="font-size:10px;color:${{C.sub}};">Today ${{tod>=0?"+":""}}${{tod.toFixed(2)}}%</div>
        <span class="rot-star" data-slug="${{t.slug}}"
              style="cursor:pointer;color:${{starred?'#eab308':C.sub}};">★</span>
      </div>
    </div>
    <svg viewBox="0 0 ${{W}} ${{H}}" width="100%" height="110" style="margin-top:8px;overflow:visible;"
         class="rot-svg" data-slug="${{t.slug}}">
      <path d="${{pathFor(bench,W,H,lo,hi)}}" fill="none" stroke="${{C.bench}}" stroke-width="1"
            stroke-dasharray="3 3"/>
      ${{maPaths}}
      <path d="${{pathFor(idx,W,H,lo,hi)}}" fill="none" stroke="${{C.line}}" stroke-width="1.5"/>
    </svg>
    <div style="font-size:10px;color:${{C.sub}};margin-top:4px;">
      — ${{t.name}} &nbsp;·· ${{DATA.benchmark.name}}
    </div>
  </div>`;
}}

// ---- filter / sort / search (Task 7 expands these) ------------------------
function visibleThemes() {{
  let list = DATA.themes.slice();
  // search + filters implemented in Task 7
  list.sort((a,b)=> periodReturn(b)-periodReturn(a));
  return list;
}}

// ---- toolbar --------------------------------------------------------------
const SEGS = {{
  view:   [["grid","Grid"],["table","Table"]],
  scale:  [["lin","Lin"],["log","Log"]],
  ma:     [["20","20"],["50","50"],["200","200"]],
  weight: [["ew","Equal Weight"],["mcap","Market Cap"]],
  period: [["1D","1D"],["1W","1W"],["1M","1M"],["3M","3M"],["6M","6M"],["1Y","1Y"],["YTD","YTD"],["5Y","5Y"]],
  filter: [["all","All"],["top","Top 20"],["bottom","Bottom 20"],["abovema","Above MA"],["starred","★"]],
  sort:   [["return","Return"],["breadth","Breadth"],["name","Name"]],
}};
function segActive(group, key) {{
  if (group==="ma") return S.ma[key];
  return S[group]===key;
}}
function buildToolbar() {{
  document.querySelectorAll(".rot-seg").forEach(seg=>{{
    const g = seg.dataset.group;
    seg.innerHTML = SEGS[g].map(([k,label])=>{{
      const on = segActive(g,k);
      const disabled = (g==="period" && k==="5Y");
      return `<button class="rot-btn" data-group="${{g}}" data-key="${{k}}" ${{disabled?"disabled":""}}
        style="padding:5px 9px;border:1px solid ${{on?C.line:C.border}};background:${{on?C.selBg:'transparent'}};
        color:${{disabled?C.sub:C.text}};border-radius:6px;margin-right:3px;cursor:${{disabled?'not-allowed':'pointer'}};
        font-size:11px;font-weight:${{on?700:500}};opacity:${{disabled?0.4:1}};">${{label}}</button>`;
    }}).join("");
  }});
}}
function onToolbarClick(e) {{
  const btn = e.target.closest(".rot-btn"); if (!btn || btn.disabled) return;
  const g = btn.dataset.group, k = btn.dataset.key;
  if (g==="ma") S.ma[k] = !S.ma[k];
  else S[g] = k;
  render();
}}

// ---- crosshair tooltip ----------------------------------------------------
function attachTooltips() {{
  document.querySelectorAll(".rot-svg").forEach(svg=>{{
    svg.addEventListener("mousemove", ev=>showTip(ev, svg));
    svg.addEventListener("mouseleave", ()=>hideTip());
  }});
}}
let tipEl=null;
function showTip(ev, svg) {{
  const t = DATA.themes.find(x=>x.slug===svg.dataset.slug); if(!t) return;
  const start = windowIdx();
  const dates = DATA.dates.slice(start);
  const rect = svg.getBoundingClientRect();
  const frac = Math.min(1,Math.max(0,(ev.clientX-rect.left)/rect.width));
  const i = Math.round(frac*(dates.length-1));
  const idx = rebase(seriesFor(t),start), bench = rebase(DATA.benchmark.level,start);
  if (!tipEl) {{ tipEl=document.createElement("div");
    tipEl.style.cssText=`position:fixed;pointer-events:none;background:${{C.bg}};border:1px solid ${{C.border}};
      border-radius:6px;padding:6px 8px;font-size:11px;color:${{C.text}};z-index:9999;`;
    document.body.appendChild(tipEl); }}
  tipEl.innerHTML = `${{dates[i]}}<br>${{t.name}} ${{idx[i]?idx[i].toFixed(2):"—"}}<br>`+
                    `${{DATA.benchmark.name}} ${{bench[i]?bench[i].toFixed(2):"—"}}`;
  tipEl.style.left=(ev.clientX+12)+"px"; tipEl.style.top=(ev.clientY+12)+"px"; tipEl.style.display="block";
}}
function hideTip() {{ if(tipEl) tipEl.style.display="none"; }}

// ---- render ---------------------------------------------------------------
function render() {{
  buildToolbar();
  const list = visibleThemes();
  document.getElementById("rot-count").textContent = `Showing ${{list.length}} of ${{DATA.themes.length}}`;
  const grid = document.getElementById("rot-grid");
  grid.innerHTML = list.map(drawCard).join("");
  attachTooltips();
}}
document.getElementById("rot-toolbar").addEventListener("click", onToolbarClick);
document.getElementById("rot-search").addEventListener("input", e=>{{ S.search=e.target.value; render(); }});
render();
</script>
"""
```

- [ ] **Step 2: Add a temporary fallback stub (real version in Task 9)**

Append to `frontend/themes_rotation_tab.py`:

```python
def _render_fallback(payload: dict):
    st.caption("Interactive grid unavailable on this Python build — showing a static table.")
    rows = [{"Theme": t["name"], "Stocks": t["n_stocks"],
             "▲ (1Y)": t["stats"]["1Y"]["adv"], "▼ (1Y)": t["stats"]["1Y"]["dec"]}
            for t in payload["themes"]]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
```

- [ ] **Step 3: Wire the Themes tab to the new tool**

In `frontend/app.py`, replace the body of `_frag_themes` (near line 2887):

```python
def _frag_themes():
    from themes_rotation_tab import render_rotation_tool
    render_rotation_tool(dark=_dark, tokens=_T)
```

- [ ] **Step 4: Run the app and verify the grid renders**

Run: `streamlit run frontend/app.py` (from repo root), open the **Themes** tab.
Expected: a toolbar (Grid/Table, Lin/Log, MA 20/50/200, Equal Weight/Market Cap, period pills with 5Y greyed) and a 3-column grid of theme cards, each with a green index line, a dotted Nifty 500 line, return %, today %, breadth, and a ★. Clicking period/scale/MA/weight updates all cards instantly. Hover shows the date + both values tooltip. (Search box present; filters/sort/star/table/drill-down are inert until Tasks 7–8.)

- [ ] **Step 5: Smoke-test the HTML builder**

Add to `frontend/tests/test_theme_rotation_payload.py`:

```python
def test_build_component_html_contains_key_tokens():
    from themes_rotation_tab import build_component_html
    p = assemble_payload(_index_df(), _constituents_df(), _benchmark_df(), DATES, dark=True)
    html = build_component_html(p, tokens={})
    assert "rot-grid" in html and "NIFTY 500" in html and "const DATA" in html
    assert "Equal Weight" in html and "Market Cap" in html
```

Run: `pytest frontend/tests/test_theme_rotation_payload.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/themes_rotation_tab.py frontend/app.py frontend/tests/test_theme_rotation_payload.py
git commit -m "feat(themes): rotation grid component — cards, SVG chart, benchmark, toolbar"
```

---

### Task 7: Filters, sort, search, starred, table view

**Files:**
- Modify: `frontend/themes_rotation_tab.py` (replace `visibleThemes`, add table render + star/click handlers)

**Interfaces:**
- Consumes: `S`, `periodReturn`, `breadth`, `sma`, `seriesFor`, `windowIdx` (Task 6).
- Produces: functional Filter / Sort / Search / Starred / Table view (no new Python signatures).

- [ ] **Step 1: Replace `visibleThemes()` with the full filter/sort/search logic**

In `build_component_html`, replace the Task-6 `visibleThemes()` stub with:

```javascript
function aboveMA(t) {{
  const sel = [20,50,200].filter(p=>S.ma[p]);
  const period = sel.length? sel[0] : 50;
  const s = seriesFor(t); const m = sma(s, period);
  const last = s.length-1;
  return s[last]!=null && m[last]!=null && s[last] > m[last];
}}
function matchesSearch(t) {{
  const q = S.search.trim().toLowerCase(); if (!q) return true;
  if (t.name.toLowerCase().includes(q)) return true;
  return t.constituents.some(c =>
     (c.symbol||"").toLowerCase().includes(q) || (c.name||"").toLowerCase().includes(q));
}}
function visibleThemes() {{
  let list = DATA.themes.filter(matchesSearch);
  if (S.filter === "starred")  list = list.filter(t=>S.stars.includes(t.slug));
  if (S.filter === "abovema")  list = list.filter(aboveMA);
  if (S.sort === "name")       list.sort((a,b)=>a.name.localeCompare(b.name));
  else if (S.sort === "breadth") list.sort((a,b)=>breadth(b).adv-breadth(a).adv);
  else                         list.sort((a,b)=>periodReturn(b)-periodReturn(a));
  if (S.filter === "top")    list = list.slice(0,20);
  if (S.filter === "bottom") {{ list = DATA.themes.filter(matchesSearch)
                                   .sort((a,b)=>periodReturn(a)-periodReturn(b)).slice(0,20); }}
  return list;
}}
```

- [ ] **Step 2: Add a table renderer and switch on `S.view`**

In `build_component_html`, replace the body of `render()` with:

```javascript
function render() {{
  buildToolbar();
  const list = visibleThemes();
  document.getElementById("rot-count").textContent = `Showing ${{list.length}} of ${{DATA.themes.length}}`;
  const grid = document.getElementById("rot-grid");
  if (S.view === "table") {{
    grid.style.display="block";
    grid.innerHTML = renderTable(list);
  }} else {{
    grid.style.display="grid";
    grid.innerHTML = list.map(drawCard).join("");
    attachTooltips();
  }}
}}
function renderTable(list) {{
  const rows = list.map(t=>{{
    const r=periodReturn(t), tod=todayReturn(t), b=breadth(t);
    const rc=r>=0?C.up:C.down;
    return `<tr data-slug="${{t.slug}}" class="rot-row" style="cursor:pointer;border-bottom:1px solid ${{C.border}};">
      <td style="padding:6px 10px;">${{t.name}}</td>
      <td style="padding:6px 10px;text-align:right;color:${{rc}};font-weight:700;">${{r>=0?"+":""}}${{r.toFixed(2)}}%</td>
      <td style="padding:6px 10px;text-align:right;color:${{C.sub}};">${{tod>=0?"+":""}}${{tod.toFixed(2)}}%</td>
      <td style="padding:6px 10px;text-align:right;"><span style="color:${{C.up}}">▲${{b.adv}}</span>
          <span style="color:${{C.down}}">▼${{b.dec}}</span></td>
      <td style="padding:6px 10px;text-align:right;color:${{C.sub}};">${{t.n_stocks}}</td></tr>`;
  }}).join("");
  return `<table style="width:100%;border-collapse:collapse;font-size:12px;color:${{C.text}};">
    <thead><tr style="color:${{C.sub}};text-align:left;">
      <th style="padding:6px 10px;">Theme</th><th style="padding:6px 10px;text-align:right;">Return</th>
      <th style="padding:6px 10px;text-align:right;">Today</th><th style="padding:6px 10px;text-align:right;">Breadth</th>
      <th style="padding:6px 10px;text-align:right;">Stocks</th></tr></thead><tbody>${{rows}}</tbody></table>`;
}}
```

- [ ] **Step 3: Wire the star toggle (stop card-click propagation)**

Add before the final `render();` call in the script:

```javascript
document.getElementById("rot-grid").addEventListener("click", e=>{{
  const star = e.target.closest(".rot-star");
  if (star) {{
    e.stopPropagation();
    const slug = star.dataset.slug;
    const i = S.stars.indexOf(slug);
    if (i>=0) S.stars.splice(i,1); else S.stars.push(slug);
    saveStars(); render(); return;
  }}
}});
```

- [ ] **Step 4: Run the app and verify interactions**

Run: `streamlit run frontend/app.py`, open **Themes**.
Expected: typing a stock symbol/name filters to themes containing it; typing a theme name filters by name; Top 20 / Bottom 20 / Above MA / ★ filters work; Sort by Return/Breadth/Name reorders; clicking ★ toggles gold and persists across a page reload (localStorage); the Table toggle shows a sortable row layout and back to Grid restores cards.

- [ ] **Step 5: Commit**

```bash
git add frontend/themes_rotation_tab.py
git commit -m "feat(themes): filters, sort, stock search, starred, table view"
```

---

### Task 8: Constituent drill-down panel

**Files:**
- Modify: `frontend/themes_rotation_tab.py` (add drill-down open/close + card-click handler)

**Interfaces:**
- Consumes: `t.constituents` (Task 5 payload), `C`.
- Produces: an in-component panel; no new Python signatures.

- [ ] **Step 1: Add the drill-down renderer and handlers**

Add these functions in the script (before the final `render();`):

```javascript
function fmtPct(v) {{ return v==null? "—" : ((v*100).toFixed(2)+"%"); }}
function fmtNum(v) {{ return v==null? "—" : v.toLocaleString('en-IN',{{maximumFractionDigits:2}}); }}
function openDrill(slug) {{
  const t = DATA.themes.find(x=>x.slug===slug); if(!t) return;
  const rows = t.constituents.map(c=>{{
    const cell=(v)=>`<td style="padding:5px 8px;text-align:right;color:${{v>=0?C.up:C.down}};">${{fmtPct(v)}}</td>`;
    const scr = c.screener_url? `<a href="${{c.screener_url}}" target="_blank" style="color:#3b82f6;">Screener ↗</a>`:"—";
    const cht = c.tradingview_url? `<a href="${{c.tradingview_url}}" target="_blank" style="color:#3b82f6;">📈</a>`:"—";
    return `<tr style="border-bottom:1px solid ${{C.border}};">
      <td style="padding:5px 8px;font-weight:600;">${{c.symbol}}</td>
      <td style="padding:5px 8px;color:${{C.sub}};">${{c.name}}</td>
      <td style="padding:5px 8px;text-align:right;">${{fmtNum(c.cmp)}}</td>
      ${{cell(c.ret_1d)}}${{cell(c.ret_1w)}}${{cell(c.ret_30d)}}${{cell(c.ret_60d)}}${{cell(c.ret_180d)}}${{cell(c.ret_365d)}}
      <td style="padding:5px 8px;text-align:right;">${{fmtNum(c.mcap)}}</td>
      <td style="padding:5px 8px;text-align:right;">${{fmtNum(c.pe)}}</td>
      <td style="padding:5px 8px;text-align:right;">${{scr}}</td>
      <td style="padding:5px 8px;text-align:right;">${{cht}}</td></tr>`;
  }}).join("");
  document.getElementById("rot-drill").innerHTML = `
    <div style="position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:9998;" id="rot-drill-bg"></div>
    <div style="position:fixed;top:5%;left:5%;right:5%;bottom:5%;overflow:auto;z-index:9999;
                background:${{C.bg}};border:1px solid ${{C.border}};border-radius:12px;padding:16px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <div style="font-weight:700;font-size:16px;">${{t.name}} — ${{t.n_stocks}} stocks</div>
        <button id="rot-drill-close" style="padding:5px 12px;border:1px solid ${{C.border}};background:transparent;
                color:${{C.text}};border-radius:6px;cursor:pointer;">✕ Close</button>
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:12px;color:${{C.text}};">
        <thead><tr style="color:${{C.sub}};text-align:right;">
          <th style="padding:5px 8px;text-align:left;">Symbol</th><th style="padding:5px 8px;text-align:left;">Name</th>
          <th style="padding:5px 8px;">CMP</th><th>1D</th><th>1W</th><th>1M</th><th>3M</th><th>6M</th><th>1Y</th>
          <th style="padding:5px 8px;">MCap</th><th>P/E</th><th>Screener</th><th>Chart</th></tr></thead>
        <tbody>${{rows}}</tbody></table>
    </div>`;
  document.getElementById("rot-drill-close").onclick = closeDrill;
  document.getElementById("rot-drill-bg").onclick = closeDrill;
}}
function closeDrill() {{ document.getElementById("rot-drill").innerHTML = ""; }}
```

- [ ] **Step 2: Route card and row clicks to the drill-down**

Extend the `rot-grid` click handler (added in Task 7) so non-star clicks open the drill-down:

```javascript
document.getElementById("rot-grid").addEventListener("click", e=>{{
  const star = e.target.closest(".rot-star");
  if (star) {{
    e.stopPropagation();
    const slug = star.dataset.slug;
    const i = S.stars.indexOf(slug);
    if (i>=0) S.stars.splice(i,1); else S.stars.push(slug);
    saveStars(); render(); return;
  }}
  const card = e.target.closest(".rot-card, .rot-row");
  if (card && !e.target.closest("a")) openDrill(card.dataset.slug);
}});
```

(Replace the Task-7 handler with this expanded version.)

- [ ] **Step 3: Run the app and verify drill-down**

Run: `streamlit run frontend/app.py`, open **Themes**.
Expected: clicking a card (or a table row) opens a modal listing that theme's constituents with CMP, 1D–1Y returns (green/red), Market Cap, P/E, and working Screener/Chart links; ✕ or backdrop closes it; clicking ★ does not open the modal.

- [ ] **Step 4: Commit**

```bash
git add frontend/themes_rotation_tab.py
git commit -m "feat(themes): constituent drill-down panel"
```

---

### Task 9: Retire old Themes code, finalize fallback, full verification

**Files:**
- Modify: `frontend/app.py` (remove the dead `render_themes_view` + theme-only loaders/formatters now unused)

**Interfaces:**
- Consumes: everything above.
- Produces: a clean Themes tab backed solely by `themes_rotation_tab.py`.

- [ ] **Step 1: Remove the superseded Themes code from `app.py`**

Delete the now-unused `render_themes_view()` (≈ lines 2019–2248) and the theme-only helpers that nothing else references: `load_themes`, `load_theme_averages`, `load_theme_stocks` (≈ 1350–1406), `THEME_PCT_COLS`, `THEME_DISPLAY_COLS`, `_fmt_mcap_cr`, `THEME_DISPLAY_FORMATTERS`, `_prepare_theme_display`, `_THEME_SORT_OPTIONS` (≈ 1970–2016). Keep `_frag_themes` (now calling `render_rotation_tool`).

- [ ] **Step 2: Confirm nothing else references the removed names**

Run: `grep -nE "render_themes_view|load_theme_stocks|load_theme_averages|_prepare_theme_display|_THEME_SORT_OPTIONS|THEME_DISPLAY_COLS" frontend/app.py`
Expected: no matches (all removed; `_frag_themes` remains).

- [ ] **Step 3: Verify the app boots and the Themes tab works**

Run: `streamlit run frontend/app.py`.
Expected: no import/NameError; Themes tab shows the rotation tool; all other tabs unaffected.

- [ ] **Step 4: Verify the Python-3.14 fallback path**

Temporarily force the fallback by editing `_COMPONENTS_HTML_SAFE = False` at the top of `themes_rotation_tab.py`, reload the Themes tab.
Expected: a caption + static table of themes with 1Y breadth. Revert the edit afterward.

- [ ] **Step 5: Run the full test suite**

Run: `pytest backend/tests/test_theme_index.py frontend/tests/test_theme_rotation_payload.py -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/app.py frontend/themes_rotation_tab.py
git commit -m "refactor(themes): retire legacy Themes view in favor of rotation tool"
```

- [ ] **Step 7: Finish the branch**

Use `superpowers:finishing-a-development-branch` to merge to `main` and push (honors the push-to-main + auto-deploy preference). Before merging, confirm `backend/schema_theme_index.sql` and `backend/seed_nifty500_index.py` have been applied to the production Supabase, and that the next `daily_refresh.py` run will keep `theme_index_daily` and `^CRSLDX` current.

---

## Self-Review

**Spec coverage:**
- Nifty 500 seed → Task 2. `theme_index_daily` → Task 1. EW/MCAP methodology → Task 3. daily_refresh step → Task 4. Payload builder → Task 5. Component (toolbar: period/scale/MA/weight/grid, cards, SVG chart, benchmark, crosshair) → Task 6. Filters/sort/search/star/table → Task 7. Drill-down → Task 8. `_COMPONENTS_HTML_SAFE` fallback → Tasks 6 (stub) + 9 (verify). Replace tab + retire old loaders → Tasks 6 + 9. History cap / 5Y disabled → Task 6 toolbar (`disabled` on 5Y). Market-cap weighting label → Task 6 SEGS. Tests → Tasks 3, 5, 6. All spec sections covered.

**Placeholder scan:** No TBD/TODO/"handle edge cases". Task 6 explicitly scopes filter/sort/star/table/drill-down as stubs, and Tasks 7–8 supply the real code — the stub is named and its replacement is concrete, not a placeholder.

**Type consistency:** `assemble_payload(index_df, constituents_df, benchmark_df, dates, dark)` signature matches its test and `build_rotation_payload` call. `PERIOD_COL` identical in module, test, and Global Constraints. JS state `S` fields (`view/scale/ma/weight/period/filter/sort/search/stars`) are defined in Task 6 and only read/reassigned (never renamed) in Tasks 7–8. `render()`, `visibleThemes()`, `windowIdx()`, `rebase()`, `sma()`, `seriesFor()`, `periodReturn()`, `breadth()` names are consistent across tasks. `theme_index_daily` columns identical in Tasks 1, 4, 5.

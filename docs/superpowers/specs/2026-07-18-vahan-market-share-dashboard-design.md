# Vahan Market-Share Dashboard — Design Spec

**Date:** 2026-07-18
**Status:** Approved (design) — pending spec review
**Author:** Sumit + Claude

---

## 1. Purpose

Build an **auto tool** that pulls vehicle-registration data from the Government of India
**Vahan Dashboard** (`vahan.parivahan.gov.in/vahan4dashboard`) and presents it as a
structured, interactive HTML dashboard so the user can quickly see **which manufacturers
("players") are selling more vehicles and gaining/losing market share**, broken down by
vehicle category.

The first run produces a dashboard for **the most recent completed month (June 2026)** with
month-over-month and year-over-year comparisons.

---

## 2. Decisions (locked in)

| Decision | Choice |
|---|---|
| **Data source** | Playwright headless-browser scraper driving the real Vahan dashboard (Option A below) |
| **Comparison basis** | MoM (vs previous month) **and** YoY (vs same month last year) |
| **Categories** | All Vahan vehicle categories (2W, 3W, PV/4W, CV variants, Tractors, others) |
| **Geography** | All-India total (nationwide aggregate; no state drilldown in v1) |
| **Output** | Single self-contained interactive HTML dashboard (no server) |
| **Automation** | One manual command per run (`python vahan_refresh.py`); scheduling can be added later |
| **History** | Accumulate each month's data in a local SQLite store to build a growing trend |
| **Location** | New standalone `vahan-dashboard/` folder inside the current repo |

**Target month math:** Today = 2026-07-18 → last completed month = **June 2026**,
MoM comparison = **May 2026**, YoY comparison = **June 2025**.

---

## 3. Scraper strategy

Three options were considered:

- **A. Full Playwright UI automation** *(chosen)* — drive the real dropdowns (Year, Month,
  Vehicle Category) and the Refresh button, then read the rendered maker table. Faithful to
  human use, survives backend changes as long as visible labels hold, debuggable. Slower
  (seconds per category) but robust.
- **B. Replay JSF AJAX POST** — replay the dashboard's XHR with `requests` + parsed
  ViewState. Fast but very brittle (stateful ViewState, fragile partial-response format).
- **C. Hybrid** — Playwright session + direct data POSTs. Middle ground, still brittle.

**Chosen: A.** For a monthly-cadence tool, robustness and visible failures beat raw speed.

### Recon-first principle
Vahan's exact control IDs / interaction flow must be **observed live**, not assumed. The
first implementation task is a **recon pass** that:
1. Loads the dashboard from *this machine* and confirms automated access is not blocked.
2. Identifies the selectors for Year, Month, Vehicle Category, Maker table, Refresh button.
3. Saves a real rendered HTML response as a **test fixture** for the parser.

If Vahan blocks headless access, we try a headed browser; if it is truly closed to
automated access from this machine, we **stop and report plainly** rather than fabricate
data. No invented numbers, ever.

---

## 4. Architecture

Self-contained folder in the repo:

```
vahan-dashboard/
  vahan_refresh.py            # Orchestrator: the single command to run
  scraper/
    __init__.py
    vahan_scraper.py          # Playwright automation (Option A)
    categories.py             # Canonical list + label mapping of Vahan categories
  analysis/
    __init__.py
    compute.py                # market share, MoM/YoY deltas, rank changes
  report/
    __init__.py
    build_html.py             # renders the self-contained interactive dashboard
    template.py               # HTML/CSS/JS string builder (inline, no external assets)
  store/
    __init__.py
    db.py                     # SQLite helpers (open, upsert, query)
    vahan.db                  # generated; git-ignored
  data/
    dashboard.html            # generated output the user opens
    fixtures/                 # saved real Vahan HTML for parser tests
  tests/
    test_compute.py
    test_parser.py
    test_build_html.py
  requirements.txt
  README.md
  .gitignore
```

### Component responsibilities

- **`scraper/vahan_scraper.py`** — Given `(year, month)`, iterate all vehicle categories,
  drive the Vahan UI, parse the maker table, and return normalized rows:
  `(month, category, maker, units)`. Owns retries, timeouts, and the recon selectors.
  Depends on Playwright only. No knowledge of storage or HTML.
- **`store/db.py`** — SQLite schema + idempotent upsert + query. One table:
  `sales(month TEXT, category TEXT, maker TEXT, units INTEGER, scraped_at TEXT,
  PRIMARY KEY(month, category, maker))`. Pure data layer.
- **`analysis/compute.py`** — Pure functions over rows from the store. Produces, per
  category: ranked makers with `units`, `share_pct`, `mom_units_delta`,
  `mom_share_pp_delta`, `yoy_units_delta`, `yoy_share_pp_delta`, `rank`, `rank_change`.
  Plus a "share movers" list. **No I/O** → fully unit-testable with synthetic data.
- **`report/build_html.py` + `template.py`** — Turn computed data into one self-contained
  HTML file (inline CSS/JS, no CDN). No network, no DB access — takes computed structures in,
  writes `data/dashboard.html` out.
- **`vahan_refresh.py`** — Orchestrates: resolve target/comparison months → for each needed
  month, use stored data or scrape it → upsert → compute → build HTML → print a summary.

### Data flow

```
python vahan_refresh.py
  └─ resolve months: target=2026-06, mom=2026-05, yoy=2025-06
  └─ for each month not already complete in store:
        scraper.scrape(year, month) -> rows -> store.upsert(rows)
  └─ compute.build_view(store, target, mom, yoy) -> per-category ranked tables + movers
  └─ build_html(view) -> data/dashboard.html
  └─ print run summary (rows scraped, categories, output path)
```

---

## 5. The HTML dashboard

- **Header:** target month, total registrations (all categories), generated timestamp.
- **Category segmented control:** 2W · 3W · PV · CV · Tractor · … (tabs, client-side switch).
- **Per-category ranked maker table:** Rank · Maker · Units · Market Share % · MoM Δ
  (units + share pp) · YoY Δ (units + share pp) · rank-change arrow. Inline horizontal
  share bars; columns sortable client-side. Gains green, losses red.
- **"Share Movers" panel:** biggest share gainers and losers for the month across makers.
- **Self-contained:** all CSS/JS inline; opens by double-click; works offline once generated.

---

## 6. Error handling

- **Network/slow/blocked:** bounded retries with backoff and explicit timeouts. A failed or
  empty scrape **must not overwrite** an existing good `dashboard.html` or wipe stored data.
- **Missing comparison month** (not in store and not scrapable): render "—" for that delta,
  never a fabricated value.
- **Category with no data:** skip gracefully; note it in the run summary.
- **Vahan UI changed / selector not found:** fail loudly with a clear message pointing back
  to the recon step, rather than silently producing wrong numbers.

---

## 7. Testing

- **`test_compute.py`** — synthetic rows → assert share %, MoM/YoY deltas, ranks, and rank
  changes. Deterministic, no network.
- **`test_parser.py`** — parse the saved real Vahan HTML fixture → assert expected maker
  rows. Guards against parser regressions without hitting the site.
- **`test_build_html.py`** — feed sample computed data → assert the HTML contains the
  expected tables/sections and is valid self-contained markup.

Live scraping is **not** part of the automated test suite (external, rate-limited site);
it is validated during the recon pass and each real run.

---

## 8. Out of scope (v1 — YAGNI)

- State / RTO drilldown (all-India totals only).
- Automated scheduling (manual command only; can add Task Scheduler/cron/Action later).
- Fuel-type, vehicle-class, or norms breakdowns.
- Hosting/publishing the HTML anywhere (it is a local file).

---

## 9. Dependencies & environment

- Python 3.11.4 (confirmed present).
- `playwright` (+ `playwright install chromium`), `pytest`. SQLite via stdlib `sqlite3`.
- Windows-first (user's OS), but code stays OS-agnostic.

---

## 10. Honesty notes

- The "fetch last month's data now and show it" deliverable depends on the recon pass
  succeeding from this machine. If Vahan blocks automated access, this will be reported
  openly and we will reconsider the data source (e.g., FADA published figures) — the tool
  will never display invented numbers.
- `store/vahan.db` and `data/dashboard.html` are generated artifacts; the store is
  git-ignored, the generated HTML may be committed as the shareable output.

# Quarter-aware Quarterly Results: Q1FY27 discovery + Q4FY26 archive

## Context

The Quarterly Results tab currently holds one season of data (Q4FY26, seeded from a user-provided CSV of result dates). Q1FY27 results season has begun. The user wants:

1. The tab split by quarter — a **Q1FY27** view (live season) and a **Q4FY26** view (frozen archive) — with all columns, metrics, and the Post-Result Strength Score **unchanged**.
2. Instead of a manual CSV this quarter, **automatically discover** which companies announced results by scraping BSE's corporate announcements API, matching against all active stocks in our DB, and inserting them into `earnings_calendar`.

Today the DB has no quarter concept — `earnings_calendar` is keyed on `(symbol, result_date)` only, and "Season to Date" shows every row `<= today`. User-confirmed decisions:

- Add a `quarter` column (backfill existing rows → `'Q4FY26'`; new rows → `'Q1FY27'`)
- Quarter sub-tabs **inside** the single top-level tab (not new top-level tabs)
- Daily automatic discovery via GitHub Actions; first run backfills from **1-Jul-2026**
- Q4FY26 sub-tab = full-season table only (no "Today's Results")
- Match against **all `stocks` rows with `is_active = TRUE`**; non-universe companies ignored
- Fix the "Quaterly" → "Quarterly" typo in tab label + page header

**Verified live (2026-07-16):** the BSE bulk announcements endpoint works — `AnnSubCategoryGetData/w?pageno={n}&strCat=Result&strPrevDate={YYYYMMDD}&strScrip=&strSearch=P&strToDate={YYYYMMDD}&strType=C&subcategory=Financial+Results` returns ALL companies' results filings (50 rows/page, total in `Table1[0]["ROWCNT"]`, rows carry `SCRIP_CD`, `NEWS_DT`, `NEWSSUB`, `SUBCATNAME`, `SLONGNAME`). So discovery is **one bulk paginated query + client-side filter**, not 500+ per-scrip calls.

Process note (per user memory): follow the Superpowers workflow — TDD for the pure helpers, save the plan doc under `docs/superpowers/plans/`, and commit + push to `origin main` when done (auto-deploy).

---

## Step 1 — DB migration

**Create `backend/migrate_add_quarter.sql`** (idempotent, matches existing `migrate_add_*.sql` convention; do NOT touch `schema_earnings.sql` — it was never updated for prior migrations either):

```sql
ALTER TABLE earnings_calendar
ADD COLUMN IF NOT EXISTS quarter TEXT;

UPDATE earnings_calendar
SET quarter = 'Q4FY26'
WHERE quarter IS NULL;

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_quarter ON earnings_calendar(quarter);
```

Apply via Supabase MCP (`apply_migration`) or SQL editor.
**Verify:** `SELECT quarter, COUNT(*) FROM earnings_calendar GROUP BY quarter` → single row `Q4FY26`.

## Step 2 — Discovery script (TDD: tests first)

**Create `backend/tests/test_discover_results.py`** (pytest, pure functions, no network/DB):

- `test_derive_quarter_boundaries`: 2026-07-01→`Q1FY27`, 2026-07-16→`Q1FY27`, 2026-09-30→`Q1FY27`, 2026-10-01→`Q2FY27`, 2027-01-15→`Q3FY27`, 2026-06-30→`Q4FY26`, 2026-04-05→`Q4FY26`, 2027-04-10→`Q4FY27`
- `test_parse_news_date`: `"2026-07-16T16:22:05.53"` → `date(2026,7,16)`; `None`/garbage → `None`
- `test_match_announcements_dedups_to_earliest_date`: two filings, same symbol, different days → earliest kept
- `test_match_announcements_skips_unknown_scrip`
- `test_match_announcements_subcat_and_keyword_filter`: accepted via SUBCATNAME; accepted via NEWSSUB keyword; rejected when neither

**Create `backend/discover_bse_results.py`**. Reuse machinery via `from fetch_bse_presentations import fetch_scrip_master, HEADERS, RESULTS_SUBCATS, RESULTS_KEYWORDS` ([fetch_bse_presentations.py](backend/fetch_bse_presentations.py) lines 40–68, 71–90). Same boilerplate: `dotenv`, `logging`, `psycopg2`, `os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")`.

```python
SEASON_START = date(2026, 7, 1)
DAILY_LOOKBACK_DAYS = 3          # survives skipped runs/holidays; dedup makes overlap free
BSE_ANN_BULK_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?pageno={page}&strCat=Result&strPrevDate={from_dt}&strScrip=&strSearch=P"
    "&strToDate={to_dt}&strType=C&subcategory=Financial+Results"
)

def derive_quarter(announce_date: date) -> str
    # reporting quarter = announcement quarter minus 1 (Indian FY Apr–Mar)
    # Jul–Sep 2026 announcement → quarter ended Jun 2026 → "Q1FY27"

def parse_news_date(news_dt) -> date | None      # ISO with fractional seconds

def fetch_result_announcements(from_dt: date, to_dt: date) -> list[dict]
    # paginated bulk fetch: pages = ceil(ROWCNT/50), 0.5s sleep/page,
    # defensive max_pages=200, log + return [] on malformed response

def match_announcements(announcements, scrip_to_symbol) -> dict[str, tuple[date, str]]
    # PURE (unit-tested): RESULTS_SUBCATS/RESULTS_KEYWORDS safety-net filter,
    # str(SCRIP_CD) -> symbol map, skip unknown scrips,
    # dedup -> {symbol: (earliest result_date, SLONGNAME company name)}

def load_active_symbols(db_url) -> set[str]      # WHERE is_active = TRUE
def load_existing_for_quarter(db_url, quarter) -> set[str]
def insert_results(db_url, matches, quarter) -> int
    # INSERT (symbol, result_date, quarter) ON CONFLICT (symbol, result_date) DO NOTHING
    # log each inserted "SYMBOL — Company Name (date)"

def main()  # argparse: --backfill (window = SEASON_START..today; default = last 3 days),
            #           --quarter (override derive_quarter)
```

`main()` flow: resolve window → `fetch_scrip_master()` → invert to `{scrip_cd: symbol}` restricted to `load_active_symbols()` → `fetch_result_announcements()` → `match_announcements()` → drop symbols already in `load_existing_for_quarter()` → `insert_results()` → summary log (fetched / matched / already-known / inserted company names).

Notes:
- `result_date = DATE(NEWS_DT)`. After-hours/weekend filings are already handled downstream by the `next_td` fallbacks in the season query.
- Known accepted limitation: a late FY26 annual filer in July gets tagged Q1FY27 (period-end parsing from NEWSSUB is a future refinement — noted, not built).
- Seed scripts (`seed_earnings_calendar.py`, `seed_earnings_from_results_csv.py`) don't set quarter — out of scope; re-running them would create NULL-quarter rows invisible to the new sub-tabs.
- `compute_earnings.py` (dead code) and `email_digest.py` (quarter-agnostic, date-keyed): untouched.

**Verify:** `cd backend && python -m pytest tests/test_discover_results.py -q` green; full `pytest tests` still green.

## Step 3 — Backfill run + enrichment

1. `cd backend && python discover_bse_results.py --backfill` — logs matched company names.
2. DB check: `SELECT quarter, COUNT(*) FROM earnings_calendar GROUP BY quarter` → Q4FY26 count unchanged, Q1FY27 > 0. Spot-check known July announcers (e.g. TCS).
3. Re-run → 0 inserts (idempotency).
4. `python fetch_bse_presentations.py` → new Q1FY27 rows get `presentation_url` / `result_pdf_url` (its `_load_pending` already picks up any NULL-url row).
5. Report the matched Q1FY27 company list to the user.

## Step 4 — Frontend restructure ([frontend/app.py](frontend/app.py))

All score SQL, LATERAL joins, columns, ordering stay byte-identical except the added quarter filter.

1. **`_render_earnings_table`** (line 2871): signature → `(df, mode, key_suffix="")`; lines 2923–2925 → `fname = f"earnings_{mode}{key_suffix}.csv"`, `key=f"dl_earn_{mode}{key_suffix}"`. **Mandatory** — the season table now renders twice (Q1 + Q4); the current fixed key would raise `StreamlitDuplicateElementKey`.
2. **Split `_frag_quarterly_results`** (lines 2928–3135) into two plain functions + a thin fragment shell:
   - `_render_earnings_today(snap_date, quarter)` = current lines 2932–3019 verbatim; WHERE (line 2994) → `WHERE ec.result_date = :today AND ec.quarter = :quarter`; params `{"today": snap_date, "quarter": quarter}`; table call gets `key_suffix=f"_{quarter}"`.
   - `_render_earnings_season(snap_date, quarter)` = current lines 3021–3135 verbatim; WHERE (line 3112) → `WHERE ec.result_date <= :today AND ec.quarter = :quarter`; same param + key_suffix additions.
   - New shell (keep `@st.fragment` on the outer function only; nested `st.tabs` is already used elsewhere in the app):
     ```python
     _CURRENT_QUARTER = "Q1FY27"
     _PREV_QUARTER = "Q4FY26"

     @st.fragment
     def _frag_quarterly_results(snap_date):
         tab_cur, tab_prev = st.tabs([_CURRENT_QUARTER, _PREV_QUARTER])
         with tab_cur:
             sub_today, sub_season = st.tabs(["Today's Results", "Season to Date"])
             with sub_today:
                 _render_earnings_today(snap_date, _CURRENT_QUARTER)
             with sub_season:
                 _render_earnings_season(snap_date, _CURRENT_QUARTER)
         with tab_prev:
             _render_earnings_season(snap_date, _PREV_QUARTER)
     ```
3. **Typo fixes:** line 4854 tab label → `"📅 Quarterly Results"`; line 5042 → `_page_header("Quarterly Results", ...)`.
4. **`_TAB_DESCRIPTIONS["quarterly_results"]`** (lines 4914–4920): mention quarter sub-tabs (Q1FY27 live with Today's/Season; Q4FY26 full-season archive).

**Verify:** `streamlit run frontend/app.py` — Q1FY27 shows Today's + Season filtered to Q1 rows; Q4FY26 shows the full prior season with score/column values identical to the pre-change Season view; both CSV buttons work; no duplicate-key error.

## Step 5 — GitHub Actions ([.github/workflows/daily_refresh.yml](.github/workflows/daily_refresh.yml))

Insert between "Run daily refresh" (ends line 37) and "Fetch BSE investor presentations" (line 39) — discovery must run **first** so the presentations step enriches the new rows in the same run:

```yaml
      - name: Discover BSE result announcements
        env:
          SUPABASE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}
        working-directory: backend
        run: python discover_bse_results.py
        continue-on-error: true
```

(No args = daily 3-day-lookback mode; quarter auto-derived → Q1FY27 through September, Q2FY27 after.)

## Step 6 — Ship

- Save plan doc to `docs/superpowers/plans/2026-07-16-q1fy27-results-discovery.md` (repo convention).
- Commit in logical units (migration + script + tests; frontend; workflow) and **push to `origin main`** (auto-deploy, per user memory).
- Optionally trigger `workflow_dispatch` to confirm the new step runs green and in order.

## Verification checklist

1. Migration applied; all pre-existing rows = `Q4FY26`.
2. `pytest backend/tests` green.
3. Backfill inserts Q1FY27 rows + logs names; second run inserts 0.
4. `fetch_bse_presentations.py` enriches a newly discovered row (spot-check `result_pdf_url`).
5. Streamlit: renamed tab, Q1FY27 (Today's + Season) and Q4FY26 (season-only) render; Q4FY26 values match pre-change Season view exactly.
6. Workflow dispatch green; discovery step ordered before presentations step.
7. Pushed to origin main; user given the list of Q1FY27 announcers.

## Critical files

- `backend/migrate_add_quarter.sql` (new)
- `backend/discover_bse_results.py` (new)
- `backend/tests/test_discover_results.py` (new)
- `frontend/app.py` (edit: lines 2871–3135, 4854, 4914–4920, 5042)
- `.github/workflows/daily_refresh.yml` (insert step before line 39)
- `backend/fetch_bse_presentations.py` (import-only reuse — no edits)

# BSE Quarterly Presentation Scraper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scrape BSE filings daily to find each earnings-season company's Investor Presentation PDF and surface a clickable "PPT" link in the Season to Date table.

**Architecture:** A new backend script `fetch_bse_presentations.py` hits the BSE announcements API per company, filters for "Investor Presentation" subcategory filings within a 10-day window of the result date, and writes the PDF URL to a new `presentation_url` column on `earnings_calendar`. The frontend reads this column and renders a link column. The script runs after the main daily refresh in both GitHub Actions and the local bat file.

**Tech Stack:** Python 3.11, requests (BSE REST API), psycopg2-binary (direct DB writes for speed), SQLAlchemy (frontend read), Streamlit st.column_config.LinkColumn

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `backend/migrate_add_presentation_url.sql` | Create | ALTER TABLE migration — adds `presentation_url TEXT` to `earnings_calendar` |
| `backend/fetch_bse_presentations.py` | Create | BSE scraper — maps ISIN→scrip code, fetches presentations, writes URLs to DB |
| `requirements.txt` | Modify | Add `requests>=2.31.0` explicitly |
| `.github/workflows/daily_refresh.yml` | Modify | Add step to run `fetch_bse_presentations.py` after main refresh |
| `scripts/run_refresh.bat` | Modify | Add call to `fetch_bse_presentations.py` after main refresh |
| `frontend/app.py` | Modify | Add `ec.presentation_url` to season SQL SELECT; add PPT link column in `_render_earnings_table` |

---

## Task 1: DB Migration — Add `presentation_url` Column

**Files:**
- Create: `backend/migrate_add_presentation_url.sql`

- [ ] **Step 1: Create the migration file**

```sql
-- Safe to re-run: IF NOT EXISTS guard
ALTER TABLE earnings_calendar
ADD COLUMN IF NOT EXISTS presentation_url TEXT;
```

Save to `backend/migrate_add_presentation_url.sql`.

- [ ] **Step 2: Run in Supabase SQL Editor**

Open Supabase dashboard → SQL Editor → paste and run the file contents.

Expected: `ALTER TABLE` success message, no error.

- [ ] **Step 3: Verify column exists**

Run in SQL Editor:
```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'earnings_calendar'
ORDER BY ordinal_position;
```

Expected: rows include `symbol`, `result_date`, `presentation_url` (data_type: `text`).

- [ ] **Step 4: Commit migration file**

```bash
git add backend/migrate_add_presentation_url.sql
git commit -m "feat: add presentation_url column to earnings_calendar"
```

---

## Task 2: Add `requests` to requirements.txt

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add requests**

Open `requirements.txt` and add this line after `feedparser`:
```
requests>=2.31.0
```

- [ ] **Step 2: Verify it installs**

```bash
pip install -r requirements.txt
```

Expected: `Requirement already satisfied: requests` (it was a transitive dep — now it's explicit).

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add requests as explicit dependency"
```

---

## Task 3: Build `fetch_bse_presentations.py`

**Files:**
- Create: `backend/fetch_bse_presentations.py`

- [ ] **Step 1: Create the script**

```python
"""Fetch Investor Presentation PDF links from BSE for earnings-season companies.

Runs after the daily refresh. For each company in earnings_calendar where
result_date <= today and presentation_url IS NULL, hits the BSE announcements
API and looks for an 'Investor Presentation' filing within result_date to
result_date + 10 days. Writes the PDF URL to earnings_calendar.presentation_url.
"""

import os
import time
import logging
from datetime import date, timedelta

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BSE_SCRIP_MASTER_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
BSE_ANN_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    "?strCat=-1&strType=C&strScrip={scrip}&strSearch=P"
    "&strFromdt={from_dt}&strTodt={to_dt}&strText=&industry=&subcat="
)
BSE_PDF_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}

PRESENTATION_KEYWORDS = ["investor presentation", "corporate presentation", "presentation"]


def fetch_scrip_master() -> dict[str, str]:
    """Return {isin: bse_scrip_code} for all active BSE equities."""
    log.info("Fetching BSE scrip master...")
    resp = requests.get(BSE_SCRIP_MASTER_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # API returns list directly or wrapped in a key — handle both
    items = data if isinstance(data, list) else data.get("Table", [])
    mapping = {}
    for item in items:
        isin = (item.get("ISIN_NUMBER") or "").strip()
        scrip = str(item.get("SCRIP_CD") or "").strip()
        if isin and scrip:
            mapping[isin] = scrip
    log.info(f"Scrip master loaded: {len(mapping)} entries")
    return mapping


def find_presentation_url(scrip_code: str, result_date: date) -> str | None:
    """Query BSE announcements API and return PDF URL if a presentation is found."""
    from_dt = result_date.strftime("%Y%m%d")
    to_dt = (result_date + timedelta(days=10)).strftime("%Y%m%d")
    url = BSE_ANN_URL.format(scrip=scrip_code, from_dt=from_dt, to_dt=to_dt)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"  BSE API error for scrip {scrip_code}: {e}")
        return None

    announcements = data if isinstance(data, list) else data.get("Table", [])
    # Find the most recent announcement whose headline contains a presentation keyword
    matches = []
    for ann in announcements:
        headline = (ann.get("NEWSSUB") or ann.get("HEADLINE") or "").lower()
        if any(kw in headline for kw in PRESENTATION_KEYWORDS):
            matches.append(ann)

    if not matches:
        return None

    # Take the most recent match (announcements are typically newest-first, but sort to be safe)
    best = sorted(matches, key=lambda a: a.get("NEWS_DT", ""), reverse=True)[0]
    attachment = (best.get("ATTACHMENTNAME") or "").strip()
    if not attachment:
        return None

    return BSE_PDF_BASE + attachment


def main():
    db_url = os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")
    conn = psycopg2.connect(db_url, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()

    today = date.today()

    # Load ISIN → scrip code mapping from BSE
    try:
        isin_to_scrip = fetch_scrip_master()
    except Exception as e:
        log.error(f"Could not fetch BSE scrip master: {e}. Aborting.")
        conn.close()
        return

    # Fetch all season companies missing a presentation URL
    cur.execute(
        """
        SELECT ec.symbol, ec.result_date, s.isin
        FROM earnings_calendar ec
        JOIN stocks s ON ec.symbol = s.symbol
        WHERE ec.result_date <= %s
          AND ec.presentation_url IS NULL
          AND s.isin IS NOT NULL
        ORDER BY ec.result_date DESC
        """,
        (today,),
    )
    rows = cur.fetchall()
    log.info(f"Companies to check: {len(rows)}")

    found = 0
    skipped = 0
    for symbol, result_date, isin in rows:
        scrip_code = isin_to_scrip.get(isin)
        if not scrip_code:
            log.debug(f"  {symbol}: no BSE scrip code for ISIN {isin}")
            skipped += 1
            continue

        pdf_url = find_presentation_url(scrip_code, result_date)
        if pdf_url:
            cur.execute(
                "UPDATE earnings_calendar SET presentation_url = %s WHERE symbol = %s AND result_date = %s",
                (pdf_url, symbol, result_date),
            )
            log.info(f"  {symbol} ({result_date}): found → {pdf_url}")
            found += 1
        else:
            log.info(f"  {symbol} ({result_date}): no presentation yet")

        time.sleep(0.5)  # be polite to BSE

    conn.commit()
    conn.close()
    log.info(f"Done. Found: {found} | Not found: {len(rows) - found - skipped} | No scrip code: {skipped}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Test the scrip master fetch in isolation**

```bash
cd backend
python -c "
from fetch_bse_presentations import fetch_scrip_master
m = fetch_scrip_master()
print('Total entries:', len(m))
# Print a couple to verify structure
for isin, scrip in list(m.items())[:3]:
    print(isin, '->', scrip)
"
```

Expected: `Total entries: 5000+`, three `INE... -> 500...` lines.

- [ ] **Step 3: Test presentation lookup for one known company**

Pick a company that announced results recently. Example: TCS (ISIN: `INE467B01029`, BSE scrip: `532540`).

```bash
python -c "
from datetime import date
from fetch_bse_presentations import find_presentation_url
url = find_presentation_url('532540', date(2026, 4, 10))
print('URL:', url)
"
```

Expected: either a `https://www.bseindia.com/xml-data/corpfiling/AttachLive/....pdf` URL, or `None` if TCS filed outside that window. Try adjusting the date if needed.

- [ ] **Step 4: Run full script as a dry observation (read-only check)**

Before letting it write to DB, run and let it log without committing. Temporarily add `conn.rollback()` before `conn.close()` at the end of `main()`, run, then revert:

```bash
python fetch_bse_presentations.py 2>&1 | head -40
```

Expected: scrip master loads, then per-company log lines showing found/not found. No Python exceptions.

- [ ] **Step 5: Remove the rollback, run for real**

Revert the temporary `rollback()` if added. Run:

```bash
python fetch_bse_presentations.py
```

Expected: final summary line like `Done. Found: 12 | Not found: 45 | No scrip code: 8`

- [ ] **Step 6: Verify DB was written**

```bash
python -c "
import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
url = os.environ['SUPABASE_DB_URL'].replace(':5432/', ':6543/')
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute(\"SELECT symbol, result_date, presentation_url FROM earnings_calendar WHERE presentation_url IS NOT NULL LIMIT 5\")
for r in cur.fetchall():
    print(r)
conn.close()
"
```

Expected: rows with non-NULL `presentation_url` values like `https://www.bseindia.com/xml-data/corpfiling/AttachLive/....pdf`

- [ ] **Step 7: Commit**

```bash
git add backend/fetch_bse_presentations.py
git commit -m "feat: add BSE presentation scraper script"
```

---

## Task 4: Wire into GitHub Actions

**Files:**
- Modify: `.github/workflows/daily_refresh.yml`

- [ ] **Step 1: Add BSE presentations step after the main refresh step**

Open `.github/workflows/daily_refresh.yml`. After the existing `Run daily refresh` step, add:

```yaml
      - name: Fetch BSE investor presentations
        env:
          SUPABASE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}
        working-directory: backend
        run: python fetch_bse_presentations.py
        continue-on-error: true
```

The full file should look like:

```yaml
name: Daily Refresh

on:
  schedule:
    # 11:00 UTC = 4:30 PM IST, Monday–Friday
    - cron: "0 11 * * 1-5"
  workflow_dispatch:

jobs:
  refresh:
    name: Fetch prices & compute snapshots
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run daily refresh
        env:
          SUPABASE_DB_URL:      ${{ secrets.SUPABASE_DB_URL }}
          GMAIL_APP_PASSWORD:   ${{ secrets.GMAIL_APP_PASSWORD }}
          DIGEST_EMAIL_FROM:    democracy680@gmail.com
          DIGEST_EMAIL_TO:      democracy680@gmail.com
        working-directory: backend
        run: python daily_refresh.py

      - name: Fetch BSE investor presentations
        env:
          SUPABASE_DB_URL: ${{ secrets.SUPABASE_DB_URL }}
        working-directory: backend
        run: python fetch_bse_presentations.py
        continue-on-error: true
```

Note: `continue-on-error: true` ensures BSE scraper failures never break the main refresh job.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/daily_refresh.yml
git commit -m "ci: run BSE presentation scraper after daily refresh"
```

---

## Task 5: Wire into Local Batch Script

**Files:**
- Modify: `scripts/run_refresh.bat`

- [ ] **Step 1: Add BSE presentations call**

Open `scripts/run_refresh.bat`. After the main refresh block, add:

```bat
@echo off
:: daily_refresh.bat — Runs the daily market data refresh
:: Scheduled via Windows Task Scheduler to fire at 3:30 PM IST (Mon-Fri)

cd /d "c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard"

echo [%DATE% %TIME%] Starting daily market refresh...
"C:\Users\Sumit meena\AppData\Local\Programs\Python\Python311\python.exe" backend\daily_refresh.py >> logs\refresh.log 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] Refresh completed successfully.
) else (
    echo [%DATE% %TIME%] Refresh FAILED with exit code %ERRORLEVEL%. Check logs\refresh.log
)

echo [%DATE% %TIME%] Fetching BSE investor presentations...
"C:\Users\Sumit meena\AppData\Local\Programs\Python\Python311\python.exe" backend\fetch_bse_presentations.py >> logs\refresh.log 2>&1

if %ERRORLEVEL% EQU 0 (
    echo [%DATE% %TIME%] BSE presentations fetch completed.
) else (
    echo [%DATE% %TIME%] BSE presentations fetch failed (non-critical). Check logs\refresh.log
)
```

- [ ] **Step 2: Commit**

```bash
git add scripts/run_refresh.bat
git commit -m "chore: add BSE presentation scraper to local refresh bat"
```

---

## Task 6: Frontend — PPT Column in Season to Date Table

**Files:**
- Modify: `frontend/app.py`

There are two changes: (1) add `ec.presentation_url` to the season SQL SELECT, and (2) add a PPT link column in `_render_earnings_table`.

- [ ] **Step 1: Add `presentation_url` to the season SQL SELECT**

In `_frag_quarterly_results`, find the season SQL query (around line 2938). The SELECT list currently ends with the score CASE expression. Add `ec.presentation_url` right before the final `, 0) AS score` closing:

Find this block (it's the last field before the FROM clause, after all the score CASE logic):

```python
                        , 0) AS score
                    FROM earnings_calendar ec
```

Change to:

```python
                        , 0) AS score,
                        ec.presentation_url
                    FROM earnings_calendar ec
```

- [ ] **Step 2: Add `presentation_url` to the DataFrame columns list**

Find:
```python
        df_season = pd.DataFrame(rows, columns=["symbol", "name", "result_date", "market_cap_cr",
                                                 "announcement_day_return",
                                                 "return_since_announcement", "today_return", "score"])
```

Change to:
```python
        df_season = pd.DataFrame(rows, columns=["symbol", "name", "result_date", "market_cap_cr",
                                                 "announcement_day_return",
                                                 "return_since_announcement", "today_return", "score",
                                                 "presentation_url"])
```

- [ ] **Step 3: Add PPT link column in `_render_earnings_table`**

In `_render_earnings_table`, find the section where Chart and Screener columns are added:

```python
    disp["Chart"] = df.apply(
        lambda r: f"https://www.tradingview.com/chart/?symbol=NSE%3A{r['symbol']}", axis=1
    )
    disp["Screener"] = df.apply(
        lambda r: f"https://www.screener.in/company/{r['symbol']}/consolidated/", axis=1
    )
```

Change to:

```python
    disp["Chart"] = df.apply(
        lambda r: f"https://www.tradingview.com/chart/?symbol=NSE%3A{r['symbol']}", axis=1
    )
    disp["Screener"] = df.apply(
        lambda r: f"https://www.screener.in/company/{r['symbol']}/consolidated/", axis=1
    )
    if mode == "season" and "presentation_url" in df.columns:
        disp["PPT"] = df["presentation_url"].fillna("")
```

- [ ] **Step 4: Add PPT to column_config**

Find the `col_cfg` dict in `_render_earnings_table`:

```python
    col_cfg = {
        "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
        "Screener": st.column_config.LinkColumn("Screener", display_text="🔍"),
        "Ann. Day Return": st.column_config.NumberColumn("Ann. Day Return", format="%.2f%%"),
        "Return Since Ann.": st.column_config.NumberColumn("Return Since Ann.", format="%.2f%%"),
        "Today's Return": st.column_config.NumberColumn("Today's Return", format="%.2f%%"),
    }
```

Change to:

```python
    col_cfg = {
        "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
        "Screener": st.column_config.LinkColumn("Screener", display_text="🔍"),
        "PPT": st.column_config.LinkColumn("PPT", display_text="📊"),
        "Ann. Day Return": st.column_config.NumberColumn("Ann. Day Return", format="%.2f%%"),
        "Return Since Ann.": st.column_config.NumberColumn("Return Since Ann.", format="%.2f%%"),
        "Today's Return": st.column_config.NumberColumn("Today's Return", format="%.2f%%"),
    }
```

- [ ] **Step 5: Verify the app loads without error**

```bash
cd "c:\Users\Sumit meena\OneDrive\Desktop\Claude Code\Market Price Dashboard"
streamlit run frontend/app.py
```

Navigate to **Quarterly Results → Season to Date**. Verify:
- Table loads without exception
- A "PPT" column appears
- Companies with a stored presentation URL show a `📊` clickable link
- Companies without one show an empty cell (not an error or broken link)
- Click a 📊 link — it should open a BSE PDF in the browser

- [ ] **Step 6: Commit and push**

```bash
git add frontend/app.py
git commit -m "feat: add PPT presentation link column to Season to Date table"
git push origin main
```

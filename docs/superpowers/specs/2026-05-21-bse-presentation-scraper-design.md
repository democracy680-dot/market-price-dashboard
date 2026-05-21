# BSE Quarterly Presentation Scraper — Design Spec
_Date: 2026-05-21_

## Goal

For every company that has announced quarterly results this season, automatically find and store the link to their **Investor Presentation PDF** filed on BSE. Surface this as a clickable "PPT" link column in the Season to Date table under the Quarterly Results tab.

---

## How BSE Filings Work (Context)

When a company announces results, it files multiple documents on BSE:
- Board meeting outcome
- Financial results PDF (P&L, Balance Sheet)
- Investor / Analyst Presentation (the PPT we want)
- Press release, concall audio link, etc.

The presentation is filed under subcategory **"Investor Presentation"** (sometimes also "Corporate Presentation"). It may be filed on the result date itself or up to a few days later.

---

## Architecture

### 1. DB Migration — `earnings_calendar.presentation_url`

Add a nullable `TEXT` column `presentation_url` to `earnings_calendar`. This stores the direct BSE PDF URL once found, or remains `NULL` until the presentation is filed.

Migration file: `backend/migrate_add_presentation_url.sql`

```sql
ALTER TABLE earnings_calendar
ADD COLUMN IF NOT EXISTS presentation_url TEXT;
```

### 2. BSE Scrip Code Mapping (in-memory, no new table)

BSE publishes a scrip master at:
`https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active`

This returns JSON with fields including `ISIN_NUMBER` and `SCRIP_CD`. Our `stocks` table already has `isin`. We build an in-memory dict `{isin: bse_scrip_code}` at script start, then look up each company's BSE scrip code via ISIN — no new DB table needed.

### 3. New Script — `backend/fetch_bse_presentations.py`

**Inputs:** Reads `earnings_calendar` for all companies where `result_date <= today` (entire current season). Only processes rows where `presentation_url IS NULL` — skips companies already found.

**Per company:**
1. Look up `bse_scrip_code` from ISIN mapping. Skip if not found.
2. Hit BSE announcements API for that scrip in window `result_date` to `result_date + 10 days`:
   `https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strScrip={code}&strSearch=P&strFromdt={from}&strTodt={to}&strCat=-1&strType=C`
3. Filter returned announcements where headline/category contains `"Presentation"` (case-insensitive).
4. Take the first match. Construct PDF URL:
   `https://www.bseindia.com/xml-data/corpfiling/AttachLive/{ATTACHMENTNAME}`
5. `UPDATE earnings_calendar SET presentation_url = ? WHERE symbol = ? AND result_date = ?`

**Rate limiting:** 0.5s sleep between scrip requests to avoid BSE blocking.

**Headers:** Set a browser-like `User-Agent` to avoid 403s.

**Logging:** Print per-company status (found / not found / no scrip code).

### 4. Daily Refresh Integration

`fetch_bse_presentations.py` runs as a **separate step after the main daily refresh**, called from `scripts/run_refresh.bat` and `.github/workflows/daily_refresh.yml`. It does not block the main pipeline — if it fails, the main refresh is unaffected.

### 5. Frontend — Season to Date Table

In `_render_earnings_table` (season mode) and the season SQL query:

- `presentation_url` is passed through as a column in `df_season`
- Add a `"PPT"` link column using `st.column_config.LinkColumn`, display text `"📊"`
- If `NULL` → Streamlit renders nothing (empty cell), consistent with Chart/Screener link columns

Column order: `Symbol | Company | Score | MCap | Result Date | Ann. Day Return | Return Since Ann. | Today's Return | PPT | Chart | Screener`

---

## Edge Cases

| Case | Handling |
|---|---|
| Presentation not filed yet | `presentation_url` stays NULL, cell is empty. Next day's refresh checks again. |
| Company not in BSE scrip master | Skip silently, log warning. |
| Multiple presentation filings | Take the most recent one (latest `NEWS_DT`). |
| BSE returns 403 / rate limit | Catch exception, log, skip that company — retry next day. |
| Result date is a holiday (no BSE filing that day) | Window of +10 days covers this. |

---

## Files Changed

| File | Change |
|---|---|
| `backend/migrate_add_presentation_url.sql` | New — ALTER TABLE migration |
| `backend/fetch_bse_presentations.py` | New — BSE scraper script |
| `backend/daily_refresh.py` | No change (script runs separately) |
| `scripts/run_refresh.bat` | Add call to fetch_bse_presentations.py |
| `.github/workflows/daily_refresh.yml` | Add step to run fetch_bse_presentations.py |
| `frontend/app.py` | Add presentation_url to season SQL + render table |

---

## Success Criteria

- PPT links appear in Season to Date table for companies that have filed a presentation on BSE
- Links open the correct BSE PDF directly
- Companies with no presentation filed show an empty cell (not an error)
- Script runs daily without breaking the main refresh pipeline
- No manual symbol mapping needed — ISIN join handles it automatically

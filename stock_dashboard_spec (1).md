# Indian Equity Dashboard — Technical Specification v1.0

**Owner:** Sumit
**Purpose:** Replace the Excel/Google Sheets/Power Query dashboard with a fast, shareable, web-based dashboard covering 1000+ Indian stocks across major indexes, with historical snapshots and custom CSV uploads.
**Build approach:** Vibe-coded with Claude Code, in two phases.

---

## 1. Goals and Non-Goals

### Goals
- Cover the full Nifty 500 universe plus thematic baskets (Banks, NBFCs, Pharma, Defence) plus F&O stocks.
- Support custom CSV uploads on top of the built-in universes.
- Store daily snapshots so you can look back at any past date and run historical comparisons.
- Be fast enough that adding 500 more stocks adds zero milliseconds to dashboard load time.
- Be shareable — your seniors can view it via a URL.
- Total cost: ₹0/month on free tiers.

### Non-Goals (v1)
- No intraday tick data. EOD only.
- No real-time alerts or notifications.
- No fundamental modeling (P&L, BS, CF) — those live in your Excel models, separate tool.
- No user accounts or per-user watchlists. Single shared instance with optional password gate.
- No mobile-first design. Desktop-first (research workflow).

---

## 2. Three-Layer Architecture

```
+----------------------------------------------------------+
| LAYER 3 - DASHBOARD (Streamlit, deployed to Streamlit    |
| Community Cloud, password-gated for sharing)             |
| - Reads only from Supabase                               |
| - Tabs per index, custom CSV upload, time-travel view    |
+----------------------------------------------------------+
                          ^
                          | reads only
                          |
+----------------------------------------------------------+
| LAYER 2 - STORAGE (Supabase Postgres, free tier)         |
| - 500 MB free, hosts all tables                          |
| - Single source of truth                                 |
+----------------------------------------------------------+
                          ^
                          | writes once daily
                          |
+----------------------------------------------------------+
| LAYER 1 - DATA FETCHER (Python script in GitHub repo,    |
| triggered by GitHub Actions cron at 4:30 PM IST daily)   |
| - Fetches OHLCV from yfinance for all stocks             |
| - Computes returns, DMAs, status flags                   |
| - Upserts into Supabase                                  |
+----------------------------------------------------------+
```

**Why this matters:** the dashboard never touches yfinance. All slowness happens at 4:30 PM in the background. When you or your seniors open the dashboard, it just runs SQL queries against a hosted database — milliseconds, not minutes.

---

## 3. Database Schema (Supabase Postgres)

### 3.1 Table: `stocks` (master list)
The single source of truth for "what stocks exist in our universe."

| Column          | Type        | Notes                                       |
|-----------------|-------------|---------------------------------------------|
| symbol          | text PK     | NSE symbol without prefix, e.g. `RELIANCE`  |
| name            | text        | Full company name                           |
| yahoo_symbol    | text        | e.g. `RELIANCE.NS` — used by yfinance       |
| sector          | text        | NSE sector classification                   |
| industry        | text        | Finer granularity than sector               |
| isin            | text        | Optional but useful                         |
| screener_url    | text        | Pre-computed link to screener.in            |
| tradingview_url | text        | Pre-computed link                           |
| is_active       | boolean     | False if delisted                           |
| added_at        | timestamptz | When we started tracking it                 |

### 3.2 Table: `index_membership`
Many-to-many — one stock can be in Nifty 50 AND Nifty 500 AND Nifty Bank simultaneously.

| Column     | Type    | Notes                                    |
|------------|---------|------------------------------------------|
| symbol     | text FK | References `stocks.symbol`               |
| index_name | text    | e.g. `NIFTY_50`, `NIFTY_500`, `NIFTY_BANK`, `PHARMA`, `DEFENCE`, `NBFCS`, `BANKS`, `FNO` |
| added_at   | date    | When stock joined this index             |

PRIMARY KEY: (symbol, index_name)

### 3.3 Table: `prices_daily`
Historical OHLCV. Used to compute returns and DMAs. Also useful for charting later.

| Column | Type    | Notes                              |
|--------|---------|------------------------------------|
| symbol | text FK |                                    |
| date   | date    |                                    |
| open   | numeric |                                    |
| high   | numeric |                                    |
| low    | numeric |                                    |
| close  | numeric | This is the one we use most        |
| volume | bigint  |                                    |

PRIMARY KEY: (symbol, date)
INDEX: (symbol), (date)

### 3.4 Table: `snapshots_daily`
The pre-computed daily snapshot — one row per stock per day. **This is what the dashboard reads from.** Heavy work happens at refresh time, not at view time.

| Column          | Type    | Notes                                    |
|-----------------|---------|------------------------------------------|
| symbol          | text FK |                                          |
| date            | date    | The date this snapshot represents        |
| cmp             | numeric | Close price on `date`                    |
| ret_1d          | numeric | Decimal: 0.0234 = +2.34%                 |
| ret_1w          | numeric |                                          |
| ret_30d         | numeric |                                          |
| ret_60d         | numeric |                                          |
| ret_180d        | numeric |                                          |
| ret_365d        | numeric |                                          |
| dma_50          | numeric |                                          |
| dma_200         | numeric |                                          |
| status_50dma    | text    | `Above 50DMA` or `Below 50DMA`           |
| status_200dma   | text    | `Above 200DMA` or `Below 200DMA`         |
| pe_ratio        | numeric | Nullable                                 |
| market_cap_cr   | numeric | In crores                                |

PRIMARY KEY: (symbol, date)
INDEX: (date), (symbol)

### 3.5 Table: `sector_performance_daily`
Pre-aggregated sector view. Saves the dashboard from running heavy GROUP BY queries on every page load.

| Column         | Type    | Notes                  |
|----------------|---------|------------------------|
| date           | date    |                        |
| sector         | text    |                        |
| num_companies  | int     |                        |
| advances       | int     | Stocks up that day     |
| declines       | int     | Stocks down that day   |
| day_change_pct | numeric | Median day change      |
| week_chg_pct   | numeric |                        |
| month_chg_pct  | numeric |                        |
| qtr_chg_pct    | numeric |                        |
| half_yr_chg_pct| numeric |                        |
| year_chg_pct   | numeric |                        |

PRIMARY KEY: (date, sector)

### 3.6 Table: `refresh_log`
So you can debug when something goes wrong.

| Column         | Type        | Notes                          |
|----------------|-------------|--------------------------------|
| run_id         | uuid PK     |                                |
| started_at     | timestamptz |                                |
| finished_at    | timestamptz |                                |
| stocks_total   | int         |                                |
| stocks_success | int         |                                |
| stocks_failed  | int         |                                |
| status         | text        | `success`, `partial`, `failed` |
| error_message  | text        | Nullable                       |

---

## 4. Daily Refresh Job (Layer 1)

### 4.1 What it does, in plain English
1. Connect to Supabase, read the full active stock list from `stocks`.
2. In batches of 50 stocks, fetch the last 250 trading days of OHLCV from yfinance.
3. Upsert all that data into `prices_daily`.
4. For each stock, compute today's `snapshots_daily` row using SQL/pandas:
   - `cmp` = today's close
   - `ret_1w` = (today's close / close 5 trading days ago) - 1
   - `ret_30d` = (today's close / close 22 trading days ago) - 1
   - same for 60d (44), 180d (132), 365d (252)
   - `dma_50` = mean of last 50 closes
   - `dma_200` = mean of last 200 closes
   - status flags from comparing cmp to DMAs
5. Insert one row per stock into `snapshots_daily` with today's date.
6. Run sector aggregations → write to `sector_performance_daily`.
7. Log to `refresh_log`.

### 4.2 Why batching matters
yfinance can fetch multiple tickers in one call (`yf.download(['RELIANCE.NS', 'TCS.NS', ...])`). Use batches of 50–100. For 1,200 stocks, that's ~15–25 calls total. Should complete in under 10 minutes.

### 4.3 Failure handling (this is where most beginners get burned)
- If a single stock fails (delisted, ticker change, network blip), log it and continue. **Never abort the whole run for one bad stock.**
- If the entire run fails, the previous day's snapshot is still in the database — dashboard keeps working.
- Always write the run result to `refresh_log` so you can see what happened.

### 4.4 Tech stack
- Python 3.11+
- Libraries: `yfinance`, `pandas`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`
- Configuration via environment variables (Supabase URL, password) — never hardcode secrets

### 4.5 Trigger
- GitHub Actions workflow file (`.github/workflows/daily_refresh.yml`)
- Cron: `0 11 * * 1-5` (11:00 UTC = 4:30 PM IST, Mon–Fri only)
- Secrets stored in GitHub repo settings
- Manual trigger button enabled for ad-hoc runs

---

## 5. Frontend (Layer 3) — Streamlit Dashboard

### 5.1 Sidebar (always visible)
- **Universe selector** (radio): Nifty 50, Nifty 500, Nifty Bank, F&O, Banks, NBFCs, Pharma, Defence, **Custom Upload**
- **As-of date picker** (default: latest available snapshot date)
- **Sector multi-select filter** (only stocks in selected sectors)
- **Market cap range slider**
- **Sort by** dropdown (CMP, 1W return, 30D return, etc.)

### 5.2 Main view — three sections stacked

**Section A — Header summary cards (4 cards in a row)**
- Median 1W return for selected universe
- Median 30D return
- Advance/Decline ratio for the day
- Number of stocks above 200 DMA (breadth indicator)

**Section B — The main table**
- Columns: Symbol, Name, CMP, 1W%, 30D%, 60D%, 180D%, 365D%, Mcap, P/E, Sector, 50DMA Status, 200DMA Status
- Color-coded returns (green for positive, red for negative)
- Click symbol → expander shows Screener link, TradingView link, recent news link
- CSV download button
- Pagination at 100 rows per page (don't render 1,200 rows at once)

**Section C — Sector breakdown bar chart**
- Median 30D return per sector for the selected universe
- Sortable, hoverable

### 5.3 Custom Upload tab
- Drag-and-drop CSV uploader
- CSV format: single column named `symbol` with NSE symbols
- Validates symbols against `stocks` master, flags any unknown ones
- Filters the main table view to just those symbols
- Optional: save the upload as a named watchlist (stretch goal v1.1)

### 5.4 Time-Travel view (this is where historical snapshots shine)
- Pick two dates (e.g., today vs 90 days ago)
- Side-by-side table showing how each stock's CMP and DMA status has changed
- Filter to "stocks that flipped from Below to Above 200DMA between these dates" — this is a real research signal

### 5.5 Auth
- Single password stored in `st.secrets`
- Splash screen asks for password before showing the dashboard
- Good enough for sharing with a small team. Real auth (per-user logins) is v2.

---

## 6. Index Constituents — How to Maintain Them

Index composition changes quarterly. You'll maintain CSV files in the repo:
```
data/indexes/nifty_50.csv
data/indexes/nifty_500.csv
data/indexes/nifty_bank.csv
data/indexes/pharma.csv
data/indexes/defence.csv
data/indexes/nbfcs.csv
data/indexes/banks.csv
data/indexes/fno.csv
```

Each CSV is just `symbol,name`. After every NSE rebalance, you update the CSV and commit. The daily refresh job reads these CSVs and rebuilds `index_membership` from them. **5 minutes of work per quarter, no scraping fragility.**

For your initial seed data, your existing Excel file already has all of these — we'll convert each tab to CSV in the bootstrap step.

---

## 7. Phased Build Plan

### Phase 1 — Backend foundation (build first, must work end-to-end before touching the UI)

**Step 1.1:** Set up Supabase account, create database, run schema SQL to create all tables.
**Step 1.2:** Convert your Excel sheets to seed CSVs in `data/indexes/`.
**Step 1.3:** Write `seed_stocks.py` — reads the index CSVs, populates `stocks` and `index_membership`.
**Step 1.4:** Write `daily_refresh.py` — fetches yfinance data, computes snapshots, upserts to DB.
**Step 1.5:** Run `daily_refresh.py` manually. Verify data shows up correctly in Supabase web UI.
**Step 1.6:** Set up GitHub Actions workflow to run `daily_refresh.py` on cron.
**Step 1.7:** Let it run for 3 days. Confirm `snapshots_daily` is accumulating one row per stock per day.

**Acceptance criteria for Phase 1:** You can open Supabase, run `SELECT * FROM snapshots_daily WHERE date = CURRENT_DATE LIMIT 10` and see real, fresh data for 1000+ stocks.

### Phase 2 — Dashboard UI

**Step 2.1:** Create Streamlit app skeleton with sidebar + empty main area.
**Step 2.2:** Connect to Supabase, render the main table for Nifty 50 (smallest universe — debug fast).
**Step 2.3:** Add universe selector. Make tabs work for all built-in indexes.
**Step 2.4:** Add filters (sector, mcap range, sort).
**Step 2.5:** Add summary cards section.
**Step 2.6:** Add sector bar chart.
**Step 2.7:** Add CSV upload tab.
**Step 2.8:** Add time-travel comparison view.
**Step 2.9:** Add password gate.
**Step 2.10:** Deploy to Streamlit Community Cloud, share URL with seniors.

---

## 8. Repo Structure

```
indian-equity-dashboard/
├── .github/
│   └── workflows/
│       └── daily_refresh.yml          # Cron job
├── data/
│   └── indexes/
│       ├── nifty_50.csv
│       ├── nifty_500.csv
│       ├── nifty_bank.csv
│       ├── pharma.csv
│       ├── defence.csv
│       ├── nbfcs.csv
│       ├── banks.csv
│       └── fno.csv
├── backend/
│   ├── seed_stocks.py                 # One-time setup
│   ├── daily_refresh.py               # The cron target
│   ├── db.py                          # SQLAlchemy connection helper
│   ├── fetcher.py                     # yfinance wrapper with batching
│   ├── compute.py                     # Returns and DMA calculations
│   └── schema.sql                     # Run once in Supabase
├── frontend/
│   └── app.py                         # The whole Streamlit app
├── requirements.txt
├── .env.example
└── README.md
```

---

## 9. Environment Variables (`.env`)

```
SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres
DASHBOARD_PASSWORD=your_chosen_password
```

In GitHub Actions, store these as **repository secrets**, never in code.
In Streamlit Community Cloud, store them via the secrets UI.

---

## 10. What This Architecture Buys You

1. **Adding 500 more stocks = zero impact on dashboard load time.** They get fetched once at 4:30 PM, then they're just rows in a table.
2. **Historical snapshots are free.** Every day's data is preserved. In 6 months, you'll have a 6-month time series across 1,000+ stocks — none of which your Excel could ever do.
3. **Shareable URL.** No more emailing Excel files. Seniors get a link.
4. **Resilient.** If yfinance has an outage one day, the dashboard still works — it just shows yesterday's snapshot.
5. **Extensible.** Want to add an alert system? Read from `snapshots_daily` and compare to a threshold. Want to add charts? You already have `prices_daily` with 250 days of history per stock.

---

## 11. Known Risks and How to Handle Them

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| yfinance breaks for some Indian tickers | High over time | Build the fetcher to log + skip failures, not abort. Monitor `refresh_log`. |
| Supabase free tier hits 500 MB cap | Low for 2-3 years | Add a monthly archive job that compresses old `prices_daily` data |
| GitHub Actions cron fails silently | Medium | Add a daily check in the dashboard — if latest snapshot is older than 1 day, show a banner |
| Index rebalances make data go stale | High (every quarter) | Calendar reminder to update CSVs after each NSE rebalance announcement |
| Vibe-coded code becomes spaghetti | High | Stick to the file structure above. One file = one job. Don't let any file exceed ~300 lines. |

---

## 12. What to Bring to Claude Code

Open Claude Code in an empty folder and start with this prompt:

> I'm building an Indian equity dashboard. I have a complete technical spec — please read it before writing any code: [paste this entire document]
>
> Start with **Phase 1, Step 1.1 only**: write `backend/schema.sql` that creates all the tables defined in section 3 of the spec, with the exact column names and types. Do not write any other code yet.

After that works, advance one step at a time. **Do not let Claude Code build everything in one shot** — that's how you get unmaintainable spaghetti. One file at a time, test each piece, then move on.

---

## END OF SPEC v1.0

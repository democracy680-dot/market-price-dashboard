# Agent Instructions — StockStack (Indian Equity Dashboard)

You're working inside the **WAT framework** (Workflows, Agents, Tools). This architecture separates concerns so that probabilistic AI handles reasoning while deterministic code handles execution.

## Project Overview

**StockStack** is a Streamlit-based Indian equity market dashboard backed by Supabase (PostgreSQL). It tracks ~500+ NSE stocks, computes daily technical and fundamental metrics, and presents them across 11 interactive tabs.

- **Frontend**: `frontend/app.py` — single-file Streamlit app (~4,835 lines), dark/light theme, live yfinance data for global markets
- **Backend**: Python scripts in `backend/` — data fetching, computation, seeding
- **Database**: Supabase (PostgreSQL) — all heavy computation stored pre-computed; the frontend mostly reads, rarely writes
- **Automation**: GitHub Actions (`daily_refresh.yml`) runs at 4:30 PM IST Mon–Fri; Windows batch scripts in `scripts/` for local runs

---

## The WAT Architecture

**Layer 1: Workflows** — Markdown SOPs in `docs/` (feature specs, prompts). These define the objective, required inputs, tools, and edge cases.

**Layer 2: Agents (you)** — Read the relevant spec/workflow, run backend scripts in sequence, handle errors, ask when uncertain. You connect intent to execution.

**Layer 3: Tools** — Python scripts in `backend/` that do the actual work: API calls, DB writes, indicator computation. Credentials in `.env`.

**Why this matters:** Offloading execution to deterministic scripts keeps you focused on orchestration. Don't try to compute RSI or fetch prices inline — run the right backend script.

---

## How to Operate

**1. Check existing backend scripts first**
Before writing new code, look in `backend/`. Most data tasks already have a script. Only create new scripts when genuinely nothing exists.

**2. When things fail**
- Read the full error and trace
- Fix the script and retest (if it uses paid API credits, ask before re-running)
- Update the relevant doc in `docs/` with the new approach

**3. Keep docs current**
Don't create or overwrite docs without asking. These are durable instructions.

---

## Directory Layout

```
frontend/
  app.py                    # Main Streamlit app — all 11 tabs
  global_markets_tab.py     # Global Markets tab (live yfinance)
  ticker_bar.py             # Top ticker bar component
  news_ticker.py            # News ticker component
  perf_logger.py            # DEBUG=true performance instrumentation
  assets/                   # Static assets (profile image)

backend/
  db.py                     # SQLAlchemy engine factory (reads SUPABASE_DB_URL)
  fetcher.py                # yfinance OHLCV + fundamentals fetch
  compute.py                # Snapshots + sector performance aggregation
  compute_technicals.py     # RSI, MACD, ADX computation → technicals_daily
  compute_relative_strength.py  # Excess returns + RS buckets → relative_strength_daily
  compute_setup_candidates.py   # Breakout/reversal scanner → setup_candidates_daily
  compute_minervini_template.py # 8-criterion template → minervini_template_daily
  compute_earnings.py       # Quarterly results score → earnings_calendar
  financials_fetcher.py     # Weekly fundamentals → financials_snapshots
  news_fetcher.py           # RSS ingestion → news_articles
  daily_refresh.py          # Main cron entry point (prices → snapshots → technicals → RS)
  indicators.py             # Technical indicator library (RSI, MACD, ADX, etc.)
  setup_detectors.py        # Pattern detection library (breakouts, reversals)
  seed_*.py                 # One-time seed scripts for DB population
  backfill_*.py             # Backfill scripts for historical data
  schema*.sql               # DB schema files (run once in Supabase SQL editor)
  tests/                    # pytest tests for indicators and detectors
  migrate_*.sql             # Safe ALTER TABLE migrations

data/
  indexes/                  # CSV files: nifty_50, nifty_500, nifty_bank, fno, pharma, defence, etc.
  themes/                   # Custom_Indices_Tickers.xlsx for Themes tab

docs/                       # Feature specs and build prompts
scripts/                    # Windows batch runners (run_refresh.bat, run_news_refresh.bat)
logs/                       # Local refresh logs

.env                        # SUPABASE_DB_URL and other secrets (never commit)
requirements.txt            # Python dependencies
.github/workflows/          # GitHub Actions CI (daily_refresh.yml)
```

---

## Database Schema (Supabase)

All tables live in Supabase. The frontend reads via SQLAlchemy + NullPool. The backend writes via psycopg2 `execute_values` for bulk upserts.

| Table / View | Purpose |
|---|---|
| `stocks` | Master list — symbol, name, yahoo_symbol, sector, screener_url, tradingview_url |
| `index_membership` | Many-to-many: stock ↔ index (NIFTY_50, NIFTY_500, NIFTY_BANK, FNO, PHARMA, etc.) |
| `prices_daily` | Raw OHLCV history |
| `snapshots_daily` | Pre-computed daily metrics: CMP, returns (1D/1W/30D/60D/180D/365D), DMAs, P/E, market cap |
| `sector_performance_daily` | Aggregated sector returns + advance/decline counts |
| `financials_snapshots` | Weekly fundamentals: P/E, P/B, EV/EBITDA, ROE, ROCE, margins, growth, leverage |
| `technicals_daily` | Daily technical indicators: RSI-14, MACD(12/26/9), ADX-14, SMA-50/200, signal_score |
| `relative_strength_daily` | Excess returns vs Nifty 50 across 1W/2W/1M/3M/6M/1Y with bucket labels |
| `setup_candidates_daily` | Breakout/reversal setup detections with pattern_code, strength, trigger_level |
| `minervini_template_daily` | 8-criterion Minervini trend template with per-criterion flags and template_score |
| `earnings_calendar` | Quarterly result announcement dates |
| `news_sources` | RSS feed registry (ET, Moneycontrol, BS, Livemint, FE, NDTV Profit, BL) |
| `news_articles` | Ingested articles with full-text search (ts_vector) |
| `news_article_symbols` | NSE stocks mentioned in each article |
| `watchlists` / `watchlist_members` | User-defined watchlists |
| `refresh_log` / `*_refresh_log` | Audit trail for each refresh run |
| `latest_*` views | DISTINCT ON (symbol) views for fast single-row-per-stock queries |

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| **Global Markets** | Session timeline, overnight futures, regional index cards, commodities, bonds, crypto, world heatmap — all live via yfinance |
| **Indexes** | Nifty 50 / Nifty 500 / Nifty Bank / F&O stock tables with returns, DMA status, analysis + breadth sub-tabs |
| **Sectors** | Sector-level performance with advance/decline + stock drilldown |
| **Sector Performance** | Cross-sector bar chart comparisons across timeframes |
| **Themes** | Custom index/theme baskets (from `data/themes/`) |
| **Quarterly Results** | Earnings calendar — Today's Results + Season to Date sub-tabs with Post-Result Strength Score |
| **Vol Spikes** | Stocks with unusual volume relative to average |
| **Technical Analysis** | RSI, MACD, ADX, relative strength per stock; Minervini screener sub-tab |
| **Scanner** | Breakout & reversal setup candidates with pattern codes and trigger levels |
| **My Watchlist** | User-defined watchlists stored in Supabase |
| **News** | RSS-ingested articles with All / By Company / By Source sub-tabs |

---

## Daily Refresh Pipeline

```
daily_refresh.py
  └─ fetcher.py       → prices_daily (OHLCV, last 250 days per stock, batches of 50)
  └─ compute.py       → snapshots_daily + sector_performance_daily
  └─ compute_technicals.py  → technicals_daily (RSI, MACD, ADX, signal_score)
  └─ compute_relative_strength.py → relative_strength_daily
  └─ refresh_log      (audit trail)
```

Additional refresh jobs (run separately):
- `financials_fetcher.py` — weekly fundamentals
- `news_fetcher.py` — RSS news ingestion (run by `scripts/run_news_refresh.bat`)
- `compute_setup_candidates.py` — breakout/reversal scanner
- `compute_minervini_template.py` — Minervini template
- `compute_earnings.py` — quarterly results scoring

---

## Key Conventions

- **No secrets outside `.env`** — `SUPABASE_DB_URL`, any API keys
- **Bulk writes use `execute_values`** — never row-by-row inserts for large datasets
- **Frontend reads only** — Streamlit never writes except for watchlists
- **Pre-compute everything** — the dashboard reads pre-computed snapshots; it never runs indicators at request time
- **`NullPool` for Streamlit** — prevents connection leaks in multi-user Streamlit sessions
- **Migrations are `ALTER TABLE` scripts** — prefix `migrate_` so they're never confused with schema setup files
- **Tests live in `backend/tests/`** — pytest; covers indicators, RS computation, Minervini template, setup detectors

---

## Bottom Line

You sit between what the user wants (specs in `docs/`) and what actually gets done (scripts in `backend/`). Read the relevant spec, run the right script, handle errors, and improve the system as you go. Stay pragmatic. Stay reliable.

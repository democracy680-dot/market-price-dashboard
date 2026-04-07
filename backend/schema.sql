-- Indian Equity Dashboard — Database Schema
-- Run this once in Supabase SQL Editor to create all tables.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.

-- ============================================================
-- 1. stocks — master list of all tracked securities
-- ============================================================
CREATE TABLE IF NOT EXISTS stocks (
    symbol          TEXT PRIMARY KEY,            -- NSE symbol, e.g. RELIANCE
    name            TEXT NOT NULL,               -- Full company name
    yahoo_symbol    TEXT,                        -- e.g. RELIANCE.NS (used by yfinance)
    sector          TEXT,                        -- NSE sector classification
    industry        TEXT,                        -- Finer than sector
    isin            TEXT,
    screener_url    TEXT,                        -- Pre-computed link to screener.in
    tradingview_url TEXT,                        -- Pre-computed link to TradingView
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 2. index_membership — many-to-many: stock ↔ index
-- ============================================================
CREATE TABLE IF NOT EXISTS index_membership (
    symbol      TEXT NOT NULL REFERENCES stocks(symbol),
    index_name  TEXT NOT NULL,   -- e.g. NIFTY_50, NIFTY_500, NIFTY_BANK, PHARMA, DEFENCE, NBFCS, BANKS, FNO
    added_at    DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (symbol, index_name)
);

-- ============================================================
-- 3. prices_daily — raw OHLCV history
-- ============================================================
CREATE TABLE IF NOT EXISTS prices_daily (
    symbol  TEXT    NOT NULL REFERENCES stocks(symbol),
    date    DATE    NOT NULL,
    open    NUMERIC,
    high    NUMERIC,
    low     NUMERIC,
    close   NUMERIC NOT NULL,
    volume  BIGINT,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol ON prices_daily(symbol);
CREATE INDEX IF NOT EXISTS idx_prices_daily_date   ON prices_daily(date);

-- ============================================================
-- 4. snapshots_daily — pre-computed daily metrics (what the dashboard reads)
-- ============================================================
CREATE TABLE IF NOT EXISTS snapshots_daily (
    symbol          TEXT    NOT NULL REFERENCES stocks(symbol),
    date            DATE    NOT NULL,
    cmp             NUMERIC NOT NULL,            -- Close price on this date
    ret_1d          NUMERIC,                     -- Decimal, e.g. 0.0234 = +2.34%
    ret_1w          NUMERIC,
    ret_30d         NUMERIC,
    ret_60d         NUMERIC,
    ret_180d        NUMERIC,
    ret_365d        NUMERIC,
    dma_50          NUMERIC,                     -- 50-day moving average
    dma_200         NUMERIC,                     -- 200-day moving average
    status_50dma    TEXT CHECK (status_50dma  IN ('Above 50DMA',  'Below 50DMA')),
    status_200dma   TEXT CHECK (status_200dma IN ('Above 200DMA', 'Below 200DMA')),
    pe_ratio        NUMERIC,
    market_cap_cr   NUMERIC,                     -- Market cap in crores (INR)
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date   ON snapshots_daily(date);
CREATE INDEX IF NOT EXISTS idx_snapshots_daily_symbol ON snapshots_daily(symbol);

-- ============================================================
-- 5. sector_performance_daily — pre-aggregated sector view
-- ============================================================
CREATE TABLE IF NOT EXISTS sector_performance_daily (
    date             DATE    NOT NULL,
    sector           TEXT    NOT NULL,
    num_companies    INT     NOT NULL,
    advances         INT     NOT NULL DEFAULT 0, -- Stocks up that day
    declines         INT     NOT NULL DEFAULT 0, -- Stocks down that day
    day_change_pct   NUMERIC,                    -- Median day change
    week_chg_pct     NUMERIC,
    month_chg_pct    NUMERIC,
    qtr_chg_pct      NUMERIC,
    half_yr_chg_pct  NUMERIC,
    year_chg_pct     NUMERIC,
    PRIMARY KEY (date, sector)
);

-- ============================================================
-- 6. refresh_log — audit trail for each daily refresh run
-- ============================================================
CREATE TABLE IF NOT EXISTS refresh_log (
    run_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    stocks_total    INT,
    stocks_success  INT,
    stocks_failed   INT,
    status          TEXT CHECK (status IN ('success', 'partial', 'failed')),
    error_message   TEXT
);

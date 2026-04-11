-- Indian Equity Dashboard — Financials Schema (Extension)
-- Run this in Supabase SQL Editor AFTER schema.sql.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE OR REPLACE VIEW.

-- ============================================================
-- 1. financials_snapshots — weekly fundamental snapshot per stock
-- ============================================================
CREATE TABLE IF NOT EXISTS financials_snapshots (
    symbol                TEXT    NOT NULL REFERENCES stocks(symbol),
    as_of_date            DATE    NOT NULL,

    -- Valuation
    pe_ttm                NUMERIC,                 -- Trailing P/E
    pb                    NUMERIC,                 -- Price-to-Book
    ev_ebitda             NUMERIC,                 -- EV/EBITDA
    dividend_yield        NUMERIC,                 -- Decimal: 0.0234 = 2.34%

    -- Profitability
    roe                   NUMERIC,                 -- Decimal
    roce                  NUMERIC,                 -- Decimal — computed: EBIT / (Total Assets - Current Liabilities)
    ebitda_margin         NUMERIC,                 -- Decimal
    pat_margin            NUMERIC,                 -- Decimal

    -- Growth
    revenue_growth_yoy    NUMERIC,                 -- Decimal YoY
    ebitda_growth_yoy     NUMERIC,                 -- Decimal — computed from quarterly financials
    pat_growth_yoy        NUMERIC,                 -- Decimal YoY

    -- Leverage
    debt_to_equity        NUMERIC,                 -- Decimal (normalized: yfinance returns percent, store as ratio)
    interest_coverage     NUMERIC,                 -- Computed: EBIT / Interest Expense

    -- Working Capital
    working_capital_days  NUMERIC,                 -- Computed: (Receivables + Inventory - Payables) / (Revenue/365)

    -- Meta
    data_quality_score    INT,                     -- 0-14: count of non-null fields above
    fetched_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_fin_snapshots_date   ON financials_snapshots(as_of_date);
CREATE INDEX IF NOT EXISTS idx_fin_snapshots_symbol ON financials_snapshots(symbol);

-- ============================================================
-- 2. financials_refresh_log — audit trail for weekly fetcher runs
-- ============================================================
CREATE TABLE IF NOT EXISTS financials_refresh_log (
    run_id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ,
    stocks_total          INT,
    stocks_success        INT,                     -- At least 1 field populated
    stocks_zero_coverage  INT,                     -- All 14 fields null
    avg_quality_score     NUMERIC,                 -- Average data_quality_score for the run
    status                TEXT CHECK (status IN ('success', 'partial', 'failed')),
    error_message         TEXT
);

-- ============================================================
-- 3. latest_financials — view: most recent snapshot per symbol
-- ============================================================
CREATE OR REPLACE VIEW latest_financials AS
SELECT DISTINCT ON (symbol) *
FROM financials_snapshots
ORDER BY symbol, as_of_date DESC;

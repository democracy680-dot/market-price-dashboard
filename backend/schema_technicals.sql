-- Indian Equity Dashboard — Technical Analysis Schema (Extension)
-- Run this in Supabase SQL Editor AFTER schema.sql.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE OR REPLACE VIEW.

-- ============================================================
-- 1. technicals_daily — daily computed technical indicators per stock
-- ============================================================
CREATE TABLE IF NOT EXISTS technicals_daily (
    symbol          TEXT        NOT NULL REFERENCES stocks(symbol),
    date            DATE        NOT NULL,

    -- Price / Volume (snapshot for the day)
    cmp             NUMERIC,                -- Close price on this date
    volume          BIGINT,                 -- Volume on this date

    -- RSI
    rsi_14          NUMERIC,               -- 14-period Wilder's RSI (0–100)

    -- MACD (12, 26, 9)
    macd_line       NUMERIC,               -- EMA(12) − EMA(26)
    macd_signal     NUMERIC,               -- 9-period EMA of MACD line
    macd_histogram  NUMERIC,               -- macd_line − macd_signal

    -- ADX (14-period)
    adx_14          NUMERIC,               -- Average Directional Index
    plus_di_14      NUMERIC,               -- +DI 14 (stored for debugging)
    minus_di_14     NUMERIC,               -- −DI 14 (stored for debugging)

    -- Moving Averages
    sma_50          NUMERIC,               -- 50-day simple moving average
    sma_200         NUMERIC,               -- 200-day simple moving average

    -- Combined signal
    signal_score    INT,                   -- Raw integer score from scoring engine
    technical_status TEXT,                 -- Human-readable verdict with emoji

    -- Metadata
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_technicals_daily_date   ON technicals_daily(date);
CREATE INDEX IF NOT EXISTS idx_technicals_daily_symbol ON technicals_daily(symbol);

-- ============================================================
-- 2. technicals_refresh_log — audit trail for each technical refresh run
-- ============================================================
CREATE TABLE IF NOT EXISTS technicals_refresh_log (
    run_id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    stocks_total    INT,
    stocks_success  INT,                   -- Stocks with full indicators computed
    stocks_failed   INT,                   -- Stocks with Insufficient Data (< 210 rows)
    status          TEXT CHECK (status IN ('success', 'partial', 'failed')),
    error_message   TEXT
);

-- ============================================================
-- 3. latest_technicals — view: most recent indicator row per symbol
-- ============================================================
CREATE OR REPLACE VIEW latest_technicals AS
SELECT DISTINCT ON (symbol) *
FROM technicals_daily
ORDER BY symbol, date DESC;

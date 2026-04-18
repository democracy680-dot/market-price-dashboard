-- Relative Strength (RS) schema
-- One row per (symbol, date). All 6 timeframes stored in one row for single-query lookups.
-- Run once in Supabase SQL editor. Never overwrite existing rows — always INSERT new rows on refresh.

CREATE TABLE IF NOT EXISTS relative_strength_daily (
    symbol          text NOT NULL REFERENCES stocks(symbol),
    date            date NOT NULL,

    -- Excess returns (stock_return - nifty50_return), expressed as percentages
    rs_excess_1w    numeric,
    rs_excess_2w    numeric,
    rs_excess_1m    numeric,
    rs_excess_3m    numeric,
    rs_excess_6m    numeric,
    rs_excess_1y    numeric,

    -- Bucket labels (computed from excess returns using threshold table)
    rs_bucket_1w    text,
    rs_bucket_2w    text,
    rs_bucket_1m    text,
    rs_bucket_3m    text,
    rs_bucket_6m    text,
    rs_bucket_1y    text,

    computed_at     timestamptz DEFAULT now(),

    PRIMARY KEY (symbol, date)
);

-- PERF: Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_rs_daily_date       ON relative_strength_daily(date);
CREATE INDEX IF NOT EXISTS idx_rs_daily_symbol     ON relative_strength_daily(symbol);
CREATE INDEX IF NOT EXISTS idx_rs_daily_bucket_1m  ON relative_strength_daily(rs_bucket_1m);
CREATE INDEX IF NOT EXISTS idx_rs_daily_bucket_3m  ON relative_strength_daily(rs_bucket_3m);
CREATE INDEX IF NOT EXISTS idx_rs_daily_bucket_1y  ON relative_strength_daily(rs_bucket_1y);

-- View: most recent RS row per stock — used by the Technical Analysis tab
CREATE OR REPLACE VIEW latest_relative_strength AS
SELECT DISTINCT ON (symbol) *
FROM relative_strength_daily
ORDER BY symbol, date DESC;

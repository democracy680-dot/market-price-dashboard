-- Indian Equity Dashboard — Earnings Calendar Schema
-- Run this in Supabase SQL Editor AFTER schema.sql.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS.

-- ============================================================
-- 1. earnings_calendar — quarterly result announcement dates
-- ============================================================
CREATE TABLE IF NOT EXISTS earnings_calendar (
    symbol      TEXT NOT NULL REFERENCES stocks(symbol),
    result_date DATE NOT NULL,
    PRIMARY KEY (symbol, result_date)
);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_date   ON earnings_calendar(result_date);
CREATE INDEX IF NOT EXISTS idx_earnings_calendar_symbol ON earnings_calendar(symbol);

-- Migration: Add performance indexes to speed up common dashboard queries
-- Run once. All indexes use IF NOT EXISTS so re-running is safe.

-- Snapshot queries (most frequent — drives every tab's data load)
CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date
    ON snapshots_daily(date DESC);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_symbol_date
    ON snapshots_daily(symbol, date DESC);

-- Technical filter queries (RSI, signal status filters)
CREATE INDEX IF NOT EXISTS idx_technicals_status_date
    ON technicals_daily(technical_status, date DESC);

CREATE INDEX IF NOT EXISTS idx_technicals_rsi_date
    ON technicals_daily(rsi_14, date DESC);

CREATE INDEX IF NOT EXISTS idx_technicals_signal_date
    ON technicals_daily(signal_score, date DESC);

-- Theme membership lookups (column is theme_slug, not theme_id)
-- Note: idx_theme_membership_theme_slug and idx_theme_membership_symbol
-- already exist in schema_themes.sql — skipping to avoid conflicts.

-- Relative strength sorting
-- Note: idx_rs_daily_date, idx_rs_daily_symbol, idx_rs_daily_bucket_* already
-- exist in schema_relative_strength.sql — skipping to avoid conflicts.

-- Prices lookups (used by compute pipeline)
CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol_date
    ON prices_daily(symbol, date DESC);

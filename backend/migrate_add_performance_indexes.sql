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

-- Theme membership lookups
CREATE INDEX IF NOT EXISTS idx_theme_membership_theme
    ON theme_membership(theme_id);

CREATE INDEX IF NOT EXISTS idx_theme_membership_symbol
    ON theme_membership(symbol);

-- Relative strength sorting
CREATE INDEX IF NOT EXISTS idx_rs_score_date
    ON relative_strength_daily(score_1m DESC, date DESC);

-- Prices lookups (used by compute pipeline)
CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol_date
    ON prices_daily(symbol, date DESC);

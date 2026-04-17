-- Performance migration: add missing indexes for dashboard query patterns.
-- Run once in Supabase SQL Editor. All statements are idempotent (IF NOT EXISTS).

-- PERF: Composite index on prices_daily(symbol, date) is the single biggest win.
-- The _load_all_snapshots query runs 3 subqueries against prices_daily, each filtering
-- by both symbol and date ranges (52W high, 30D avg vol, today vol). The existing
-- single-column indexes on (symbol) and (date) separately force a merge-join; this
-- composite index lets each subquery do an index range scan in one step.
CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol_date
    ON prices_daily (symbol, date DESC);

-- PERF: index_membership is filtered by index_name in load_snapshot() for every
-- universe tab load. Without this, every universe page scans the full table.
CREATE INDEX IF NOT EXISTS idx_index_membership_index_name
    ON index_membership (index_name);

-- PERF: theme_membership is joined by theme_slug for every theme click.
CREATE INDEX IF NOT EXISTS idx_theme_membership_theme_slug
    ON theme_membership (theme_slug);

-- PERF: snapshots_daily is queried by date on every page load.
-- Already exists in schema.sql, but include here as a safety net.
CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date
    ON snapshots_daily (date);

-- PERF: technicals_daily is accessed via the latest_technicals view which does
-- ORDER BY symbol, date DESC. A composite index speeds that up significantly.
CREATE INDEX IF NOT EXISTS idx_technicals_daily_symbol_date
    ON technicals_daily (symbol, date DESC);

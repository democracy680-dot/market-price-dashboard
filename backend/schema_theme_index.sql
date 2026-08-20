-- Indian Equity Dashboard — Theme Index Schema (Extension)
-- Run in Supabase SQL Editor AFTER schema_themes.sql. Safe to re-run.

CREATE TABLE IF NOT EXISTS theme_index_daily (
    theme_slug  TEXT NOT NULL REFERENCES themes(theme_slug),
    date        DATE NOT NULL,
    index_ew    NUMERIC,   -- equal-weight index level, base 100 at window start
    index_mcap  NUMERIC,   -- market-cap-weight index level, base 100 at window start
    n_members   INT,       -- constituents with valid data that day
    PRIMARY KEY (theme_slug, date)
);

CREATE INDEX IF NOT EXISTS idx_theme_index_daily_slug ON theme_index_daily(theme_slug);
CREATE INDEX IF NOT EXISTS idx_theme_index_daily_date ON theme_index_daily(date);

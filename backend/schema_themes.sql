-- Indian Equity Dashboard — Themes Schema (Extension)
-- Run this in Supabase SQL Editor AFTER schema.sql.
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS + CREATE OR REPLACE VIEW.

-- ============================================================
-- 1. themes — master list of custom thematic indices
-- ============================================================
CREATE TABLE IF NOT EXISTS themes (
    theme_slug   TEXT PRIMARY KEY,                  -- URL-safe slug, e.g. affordable_housing_finance
    theme_name   TEXT NOT NULL,                     -- Display name, e.g. Affordable Housing Finance
    theme_order  INT  NOT NULL,                     -- Sort order (1-based, from Excel)
    stock_count  INT  NOT NULL DEFAULT 0,           -- Count from Excel (used for validation)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 2. theme_membership — many-to-many: stock ↔ theme
-- ============================================================
CREATE TABLE IF NOT EXISTS theme_membership (
    theme_slug  TEXT NOT NULL REFERENCES themes(theme_slug),
    symbol      TEXT NOT NULL REFERENCES stocks(symbol),
    added_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (theme_slug, symbol)
);

CREATE INDEX IF NOT EXISTS idx_theme_membership_theme_slug ON theme_membership(theme_slug);
CREATE INDEX IF NOT EXISTS idx_theme_membership_symbol     ON theme_membership(symbol);

-- ============================================================
-- 3. themes_with_counts — live count from membership table
-- ============================================================
CREATE OR REPLACE VIEW themes_with_counts AS
SELECT
    t.*,
    COUNT(tm.symbol)::INT AS actual_stock_count
FROM themes t
LEFT JOIN theme_membership tm ON tm.theme_slug = t.theme_slug
GROUP BY t.theme_slug, t.theme_name, t.theme_order, t.stock_count, t.created_at;

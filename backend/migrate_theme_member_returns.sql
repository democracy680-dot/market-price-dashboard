-- Per-stock period returns for theme members, pre-computed by compute_theme_index.py
-- so the Themes tab never reads raw prices. Safe to re-run.
CREATE TABLE IF NOT EXISTS theme_member_returns (
    symbol      TEXT PRIMARY KEY,
    as_of       DATE,
    ret_1d      NUMERIC,   -- % vs previous session
    ret_1w      NUMERIC,   -- % vs last close on/before as_of - 7 days
    ret_1m      NUMERIC,   -- 30 days
    ret_3m      NUMERIC,   -- 91 days
    ret_6m      NUMERIC,   -- 182 days
    ret_1y      NUMERIC,   -- 365 days
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

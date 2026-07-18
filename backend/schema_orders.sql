-- ============================================================
-- StockStack — Order Tracker Schema
-- Order wins & major business updates filed with BSE for the
-- companies listed on our dashboard (the ~1600 active stocks).
-- Run in Supabase SQL Editor. Safe to re-run (IF NOT EXISTS).
-- ============================================================

-- 1. bse_orders: one row per BSE "Company Update" filing we track.
--    order_id = BSE NEWSID when present, else SHA-256[:40] of
--    scrip+news_dt+attachment → deterministic PK, safe for repeated upserts.
CREATE TABLE IF NOT EXISTS bse_orders (
    order_id          TEXT        PRIMARY KEY,
    scrip_code        TEXT        NOT NULL,
    symbol            TEXT        NOT NULL REFERENCES stocks(symbol),  -- always a dashboard stock
    company_name      TEXT        NOT NULL,           -- SLONGNAME from BSE
    announcement_type TEXT        NOT NULL,           -- 'order' | 'acquisition' | 'jv' | 'expansion'
    headline          TEXT        NOT NULL,           -- NEWSSUB
    order_value_cr    NUMERIC,                        -- parsed ₹ value in crore, nullable
    value_raw         TEXT,                           -- raw matched value text, for display/audit
    attachment_url    TEXT,                           -- BSE filing PDF link (BSE_PDF_BASE + ATTACHMENTNAME)
    announced_at      TIMESTAMPTZ NOT NULL,           -- NEWS_DT (IST)
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_announced_at ON bse_orders (announced_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_type         ON bse_orders (announcement_type);
CREATE INDEX IF NOT EXISTS idx_orders_symbol       ON bse_orders (symbol);

-- 2. bse_order_refresh_log: audit trail (mirrors news_refresh_log pattern)
CREATE TABLE IF NOT EXISTS bse_order_refresh_log (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    filings_fetched INT,
    orders_new      INT,
    status          TEXT CHECK (status IN ('success', 'partial', 'failed')),
    error_message   TEXT
);

-- 3. latest_orders: convenience view used by the Streamlit frontend.
--    Enforces the 180-day retention window at read time too (belt & suspenders
--    alongside the fetcher's purge step).
CREATE OR REPLACE VIEW latest_orders AS
SELECT
    o.order_id,
    o.scrip_code,
    o.symbol,
    o.company_name,
    o.announcement_type,
    o.headline,
    o.order_value_cr,
    o.value_raw,
    o.attachment_url,
    o.announced_at
FROM bse_orders o
WHERE o.announced_at > NOW() - INTERVAL '180 days'
ORDER BY o.announced_at DESC;

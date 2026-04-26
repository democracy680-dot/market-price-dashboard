-- ============================================================
-- StockStack — News Schema
-- Run in Supabase SQL Editor. Safe to re-run (IF NOT EXISTS).
-- ============================================================

-- 1. news_sources: master feed registry (editable without code deploys)
CREATE TABLE IF NOT EXISTS news_sources (
    source_id    TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    feed_url     TEXT NOT NULL,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO news_sources (source_id, display_name, feed_url) VALUES
  ('economic_times',    'Economic Times',    'https://economictimes.indiatimes.com/markets/rss.cms'),
  ('moneycontrol',      'Moneycontrol',      'https://www.moneycontrol.com/rss/buzzingstocks.xml'),
  ('business_standard', 'Business Standard', 'https://www.business-standard.com/rss/markets-106.rss'),
  ('livemint',          'Livemint',          'https://www.livemint.com/rss/markets'),
  ('financial_express', 'Financial Express', 'https://www.financialexpress.com/market/feed/'),
  ('ndtv_profit',       'NDTV Profit',       'https://feeds.feedburner.com/ndtvprofit-latest'),
  ('business_line',     'Business Line',     'https://www.thehindubusinessline.com/markets/feeder/default.rss')
ON CONFLICT (source_id) DO NOTHING;

-- 2. news_articles: one row per unique article
--    article_id = SHA-256[:40] of URL → deterministic PK, safe for repeated upserts
CREATE TABLE IF NOT EXISTS news_articles (
    article_id   TEXT        PRIMARY KEY,
    source_id    TEXT        NOT NULL REFERENCES news_sources(source_id),
    title        TEXT        NOT NULL,
    summary      TEXT,
    url          TEXT        NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ts_vector    TSVECTOR GENERATED ALWAYS AS (
                     to_tsvector('english',
                         coalesce(title, '') || ' ' || coalesce(summary, ''))
                 ) STORED
);

CREATE INDEX IF NOT EXISTS idx_news_published_at ON news_articles (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_source_id    ON news_articles (source_id);
CREATE INDEX IF NOT EXISTS idx_news_ts_vector    ON news_articles USING GIN (ts_vector);

-- 3. news_article_symbols: NSE stocks mentioned in each article
CREATE TABLE IF NOT EXISTS news_article_symbols (
    article_id TEXT NOT NULL REFERENCES news_articles(article_id) ON DELETE CASCADE,
    symbol     TEXT NOT NULL REFERENCES stocks(symbol),
    match_type TEXT NOT NULL DEFAULT 'name',  -- 'name' or 'symbol'
    PRIMARY KEY (article_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_nas_symbol     ON news_article_symbols (symbol);
CREATE INDEX IF NOT EXISTS idx_nas_article_id ON news_article_symbols (article_id);

-- 4. news_refresh_log: audit trail (mirrors refresh_log pattern)
CREATE TABLE IF NOT EXISTS news_refresh_log (
    run_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ,
    articles_fetched INT,
    articles_new     INT,
    articles_failed  INT,
    status           TEXT CHECK (status IN ('success', 'partial', 'failed')),
    error_message    TEXT
);

-- 5. latest_news: convenience view used by Streamlit frontend
CREATE OR REPLACE VIEW latest_news AS
SELECT
    a.article_id,
    a.source_id,
    ns.display_name AS source_name,
    a.title,
    a.summary,
    a.url,
    a.published_at,
    a.fetched_at
FROM news_articles a
JOIN news_sources ns ON ns.source_id = a.source_id
ORDER BY a.published_at DESC
LIMIT 200;

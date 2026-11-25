-- News sources refresh (Nov 2026): add CNBC-TV18 and ET stock news; switch off Financial
-- Express (its RSS feed returns no items) and Business Today (its 'markets' feed is mostly
-- lifestyle/entertainment with broken dates). Safe to re-run.
INSERT INTO news_sources (source_id, display_name, feed_url, is_active) VALUES
    ('cnbctv18_markets',  'CNBC-TV18',      'https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml',   TRUE),
    ('cnbctv18_business', 'CNBC-TV18',      'https://www.cnbctv18.com/commonfeeds/v1/cne/rss/business.xml', TRUE),
    ('business_today',    'Business Today', 'https://www.businesstoday.in/rss/markets',                     FALSE),
    ('et_stocks',         'Economic Times', 'https://economictimes.indiatimes.com/markets/stocks/news/rssfeeds/2146842.cms', TRUE)
ON CONFLICT (source_id) DO UPDATE SET display_name = EXCLUDED.display_name,
    feed_url = EXCLUDED.feed_url, is_active = EXCLUDED.is_active;
UPDATE news_sources SET is_active = FALSE WHERE source_id = 'financial_express';

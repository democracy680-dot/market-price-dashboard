-- NSE index valuation history (P/E, P/B, dividend yield) from niftyindices.com.
-- Filled by backend/fetch_index_valuation.py. Safe to re-run.
CREATE TABLE IF NOT EXISTS index_valuation_indices (
    api_name     TEXT PRIMARY KEY,   -- name niftyindices.com accepts, e.g. 'NIFTY SMLCAP 250'
    display_name TEXT NOT NULL,      -- e.g. 'Nifty Smallcap 250'
    grp          TEXT NOT NULL,      -- 'Broad' | 'Sectoral' | 'Thematic'
    sort_order   INT  NOT NULL
);

CREATE TABLE IF NOT EXISTS index_valuation_daily (
    api_name   TEXT NOT NULL REFERENCES index_valuation_indices(api_name),
    date       DATE NOT NULL,
    pe         NUMERIC,
    pb         NUMERIC,
    div_yield  NUMERIC,
    PRIMARY KEY (api_name, date)
);
CREATE INDEX IF NOT EXISTS idx_index_valuation_daily_date ON index_valuation_daily(date);

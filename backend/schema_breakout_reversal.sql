-- Breakout & Reversal Scanner Schema
-- Creates setup_candidates_daily table and latest_setup_candidates view
-- Run this in Supabase SQL editor before any other step.

CREATE TABLE IF NOT EXISTS setup_candidates_daily (
    symbol              text NOT NULL REFERENCES stocks(symbol),
    date                date NOT NULL,
    pattern_code        text NOT NULL,
    pattern_category    text NOT NULL,

    setup_strength      numeric NOT NULL,
    is_candidate        boolean NOT NULL DEFAULT true,

    cmp                 numeric,
    trigger_level       numeric,
    pct_from_trigger    numeric,
    days_in_base        integer,
    volume_ratio        numeric,

    notes               text,
    computed_at         timestamptz DEFAULT now(),

    PRIMARY KEY (symbol, date, pattern_code)
);

CREATE INDEX IF NOT EXISTS idx_setup_date_pattern   ON setup_candidates_daily(date, pattern_code);
CREATE INDEX IF NOT EXISTS idx_setup_symbol         ON setup_candidates_daily(symbol);
CREATE INDEX IF NOT EXISTS idx_setup_strength       ON setup_candidates_daily(setup_strength DESC);
CREATE INDEX IF NOT EXISTS idx_setup_category       ON setup_candidates_daily(pattern_category);

-- Pattern codes reference:
-- BR_52W_RETEST    | breakout | 52W High Retest Setup
-- BR_ATH_RETEST    | breakout | All-Time High Retest Setup
-- BR_CONSOLIDATION | breakout | Consolidation Breakout Setup
-- BR_MA_APPROACH   | breakout | Moving Average Breakout Setup
-- BR_VOLUME_DRYUP  | breakout | Volume Dry-Up Setup
-- RV_OVERSOLD      | reversal | Oversold Bounce Setup
-- RV_DIVERGENCE    | reversal | Downtrend Reversal (Bullish Divergence)
-- RV_DOUBLE_BOTTOM | reversal | Double Bottom Setup
-- RV_MACD_CROSS    | reversal | MACD Bullish Crossover Setup
-- RV_200DMA_RECLAIM| reversal | 200 DMA Reclaim Setup

CREATE OR REPLACE VIEW latest_setup_candidates AS
SELECT sc.*
FROM setup_candidates_daily sc
INNER JOIN (
    SELECT symbol, pattern_code, MAX(date) AS max_date
    FROM setup_candidates_daily
    GROUP BY symbol, pattern_code
) latest
    ON  sc.symbol       = latest.symbol
    AND sc.pattern_code = latest.pattern_code
    AND sc.date         = latest.max_date;

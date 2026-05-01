-- Minervini Trend Template Schema
-- Creates minervini_template_daily table and latest_minervini_template view.
-- Run this in Supabase SQL editor. Does NOT modify any existing tables.

CREATE TABLE IF NOT EXISTS minervini_template_daily (
    symbol              text NOT NULL REFERENCES stocks(symbol),
    date                date NOT NULL,

    -- The 8 criteria (boolean flags)
    criterion_1_pass    boolean,  -- Price > 150 DMA AND Price > 200 DMA
    criterion_2_pass    boolean,  -- 150 DMA > 200 DMA
    criterion_3_pass    boolean,  -- 200 DMA trending up 1+ month
    criterion_4_pass    boolean,  -- 50 DMA > 150 DMA AND 50 DMA > 200 DMA
    criterion_5_pass    boolean,  -- Price > 50 DMA
    criterion_6_pass    boolean,  -- Price >= 30% above 52w low
    criterion_7_pass    boolean,  -- Price within 25% of 52w high
    criterion_8_pass    boolean,  -- RS Rank >= 70

    -- Aggregate
    template_pass       boolean,  -- True if all 8 criteria pass
    template_score      numeric,  -- 0-10 quality score
    criteria_count      integer,  -- How many of 8 criteria passed (0-8)

    -- Supporting metrics
    cmp                 numeric,
    sma_50              numeric,
    sma_150             numeric,
    sma_200             numeric,
    sma_200_slope_22d   numeric,  -- 1-month slope %
    sma_200_slope_110d  numeric,  -- 5-month slope %
    high_52w            numeric,
    low_52w             numeric,
    pct_from_52w_high   numeric,  -- negative: distance below 52w high
    pct_above_52w_low   numeric,  -- positive: % above 52w low
    rs_rank_12m         numeric,  -- 1-99 IBD-style percentile rank
    return_12m          numeric,  -- raw 12-month return %

    computed_at         timestamptz DEFAULT now(),

    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_minervini_date     ON minervini_template_daily(date);
CREATE INDEX IF NOT EXISTS idx_minervini_pass     ON minervini_template_daily(template_pass);
CREATE INDEX IF NOT EXISTS idx_minervini_score    ON minervini_template_daily(template_score DESC);
CREATE INDEX IF NOT EXISTS idx_minervini_rs_rank  ON minervini_template_daily(rs_rank_12m DESC);

-- Latest row per symbol (for dashboard queries)
CREATE OR REPLACE VIEW latest_minervini_template AS
SELECT DISTINCT ON (symbol) *
FROM minervini_template_daily
ORDER BY symbol, date DESC;

-- ============================================================
-- Market Mood tab — pre-computed market-wide breadth series
-- Safe to re-run: uses CREATE TABLE IF NOT EXISTS.
-- Universe: ALL active stocks in `stocks` (not just Nifty 500).
-- Populated by backend/compute_market_breadth.py (daily_refresh step 5c-3).
-- ============================================================

CREATE TABLE IF NOT EXISTS market_breadth_daily (
    date                DATE PRIMARY KEY,
    total_stocks        INT     NOT NULL,       -- stocks with a close on this date

    -- Daily move buckets (based on 1-day % change)
    advances            INT,
    declines            INT,
    unchanged           INT,
    up_3                INT,                    -- ret_1d >= +3%
    down_3              INT,                    -- ret_1d <= -3%
    up_5                INT,
    down_5              INT,
    up_10               INT,
    down_10             INT,
    up_20               INT,
    down_20             INT,

    -- Moving-average breadth
    dma50_count         INT,                    -- stocks with a valid SMA-50
    above_50dma         INT,
    below_50dma         INT,
    pct_above_50dma     NUMERIC,                -- 0–100
    pct_below_50dma     NUMERIC,
    dma200_count        INT,
    above_200dma        INT,
    below_200dma        INT,
    pct_above_200dma    NUMERIC,
    pct_below_200dma    NUMERIC,

    -- RSI-14 breadth
    rsi_count           INT,                    -- stocks with a valid RSI
    overbought          INT,                    -- RSI > 70
    oversold            INT,                    -- RSI < 30
    rsi_above_50        INT,
    pct_rsi_above_50    NUMERIC,

    -- Rolling advance ratio: sum(adv) / sum(adv+dec) over last 20 sessions, 0–100
    adv_ratio_20d       NUMERIC,

    -- Composite mood: mean(pct_above_200dma, pct_above_50dma, adv_ratio_20d, pct_rsi_above_50)
    mood_score          NUMERIC,                -- 0–100
    mood_label          TEXT CHECK (mood_label IN ('Bear', 'Dull', 'Bull')),
    momentum_pct        NUMERIC,                -- = pct_above_50dma (Market Momentum gauge)

    -- Daily traded value across the universe, in INR crores (close × volume / 1e7)
    turnover_cr         NUMERIC,

    -- 52-week (252-session) new highs / lows, based on daily high / low
    hl_count            INT,                    -- stocks with a full 252-session window
    new_highs_52w       INT,
    new_lows_52w        INT,

    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_breadth_daily_date ON market_breadth_daily(date DESC);

-- Monthly traded value (sum of daily turnover_cr per calendar month)
CREATE TABLE IF NOT EXISTS market_turnover_monthly (
    month           DATE PRIMARY KEY,           -- first day of month
    turnover_cr     NUMERIC NOT NULL,
    trading_days    INT     NOT NULL,
    is_partial      BOOLEAN NOT NULL DEFAULT FALSE,  -- current month still in progress
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

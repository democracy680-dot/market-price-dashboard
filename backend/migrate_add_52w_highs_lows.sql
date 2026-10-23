-- Migration: 52-week new highs / new lows on market_breadth_daily (Market Mood tab)
-- Safe to re-run. Columns are nullable; repopulate with:
--     python backend/compute_market_breadth.py --full

ALTER TABLE market_breadth_daily
    ADD COLUMN IF NOT EXISTS hl_count       INT,
    ADD COLUMN IF NOT EXISTS new_highs_52w  INT,
    ADD COLUMN IF NOT EXISTS new_lows_52w   INT;

-- Theme index: add daily turnover (volume bars on the Themes chart) + audit timestamp.
-- Safe to re-run. Run in the Supabase SQL editor BEFORE deploying compute_theme_index.py
-- that writes these columns.
ALTER TABLE theme_index_daily ADD COLUMN IF NOT EXISTS turnover_cr NUMERIC;      -- ₹ crore traded in the theme's stocks that day
ALTER TABLE theme_index_daily ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ DEFAULT NOW();

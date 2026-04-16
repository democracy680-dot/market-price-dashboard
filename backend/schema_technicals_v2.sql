-- Indian Equity Dashboard — Technical Analysis Schema v2
-- Run this in Supabase SQL Editor AFTER schema_technicals.sql has been applied.
-- Safe to re-run: uses ADD COLUMN IF NOT EXISTS.
--
-- Adds four new columns to technicals_daily:
--   sma_200_slope       — SMA200 percent change over 20 trading days
--   volume_ratio        — today's volume / 20-day average volume
--   signal_score_v2     — new score from improved v2 scoring engine
--   technical_status_v1 — archived v1 label for rollout comparison

ALTER TABLE technicals_daily
    ADD COLUMN IF NOT EXISTS sma_200_slope        numeric,
    ADD COLUMN IF NOT EXISTS volume_ratio         numeric,
    ADD COLUMN IF NOT EXISTS signal_score_v2      numeric,
    ADD COLUMN IF NOT EXISTS technical_status_v1  text;

-- Backfill: preserve existing technical_status into the v1 archive column
-- so we have a historical baseline to compare against v2 signals.
UPDATE technicals_daily
SET technical_status_v1 = technical_status
WHERE technical_status_v1 IS NULL;

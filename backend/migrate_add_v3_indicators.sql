-- Migration: Add v3 indicator columns to technicals_daily
-- Run once against Supabase. All columns are nullable so existing rows stay valid.

ALTER TABLE technicals_daily
    ADD COLUMN IF NOT EXISTS bb_upper             NUMERIC,
    ADD COLUMN IF NOT EXISTS bb_lower             NUMERIC,
    ADD COLUMN IF NOT EXISTS bb_position          NUMERIC,
    ADD COLUMN IF NOT EXISTS atr_14               NUMERIC,
    ADD COLUMN IF NOT EXISTS atr_pct              NUMERIC,
    ADD COLUMN IF NOT EXISTS stoch_k              NUMERIC,
    ADD COLUMN IF NOT EXISTS stoch_d              NUMERIC,
    ADD COLUMN IF NOT EXISTS obv_trend            TEXT,
    ADD COLUMN IF NOT EXISTS supertrend_direction TEXT,
    ADD COLUMN IF NOT EXISTS supertrend_level     NUMERIC,
    ADD COLUMN IF NOT EXISTS signal_score_v3      NUMERIC,
    ADD COLUMN IF NOT EXISTS technical_status_v3  TEXT;

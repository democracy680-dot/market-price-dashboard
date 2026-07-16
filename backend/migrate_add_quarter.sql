-- Add quarter label to earnings_calendar and backfill the completed season.
-- All rows existing before this migration belong to the Q4FY26 results season.
-- Run once in the Supabase SQL editor (idempotent).

ALTER TABLE earnings_calendar
ADD COLUMN IF NOT EXISTS quarter TEXT;

UPDATE earnings_calendar
SET quarter = 'Q4FY26'
WHERE quarter IS NULL;

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_quarter ON earnings_calendar(quarter);

-- Safe to re-run: IF NOT EXISTS guard
ALTER TABLE earnings_calendar
ADD COLUMN IF NOT EXISTS presentation_url TEXT;

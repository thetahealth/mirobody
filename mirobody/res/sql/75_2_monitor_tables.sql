-- Monitor report tables for W3.1/W3.2 data health evaluation (TH-141)
-- platform_hourly_profile: hourly platform-level ingestion stats
-- indicator_daily_profile: daily indicator-level quality profiles

ALTER TABLE indicator_daily_profile
    ADD COLUMN IF NOT EXISTS p25 double precision,
    ADD COLUMN IF NOT EXISTS median_val double precision,
    ADD COLUMN IF NOT EXISTS p75 double precision;
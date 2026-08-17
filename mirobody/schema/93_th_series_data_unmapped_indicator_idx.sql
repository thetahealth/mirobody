-- Partial index to accelerate IndicatorSyncTask's discovery query:
--
--   SELECT DISTINCT indicator
--   FROM th_series_data
--   WHERE fhir_id IS NULL
--     AND deleted = 0
--     AND NOT EXISTS (SELECT 1 FROM th_series_dim WHERE original_indicator = indicator)
--
-- Without this index the scan is O(rows in th_series_data); with it the scan
-- is O(rows where fhir_id IS NULL AND deleted = 0) — typically orders of
-- magnitude smaller. The leading column is `indicator` so DISTINCT + anti-join
-- can use an index-only scan.
--
-- Prod rollout on a large table: prefer running as
--   CREATE INDEX CONCURRENTLY ...
-- manually (plain CREATE INDEX takes ACCESS EXCLUSIVE for the duration).

CREATE INDEX IF NOT EXISTS idx_th_series_data_indicator_fhir_null
    ON th_series_data (indicator)
    WHERE fhir_id IS NULL AND deleted = 0;

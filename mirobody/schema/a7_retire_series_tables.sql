-- Retire the tables the observation model replaces.
--
--   th_series_data              the old fact table: one row per reading, its
--                               value and unit in one text column, its code in
--                               a never-filled fhir_id, standardized by a second
--                               UPDATE. Replaced by th_observation and friends
--                               (a6_observation_model.sql).
--   th_series_dim               the per-name dimension the sync task
--                               materialized from it, with embeddings for a
--                               semantic search tier. Replaced by th_series.
--   fhir_indicators             a code registry nothing ever filled.
--   standard_indicators_device  a daily mirror of the in-code metric catalogue.
--
-- Renamed, not dropped: the rows are a person's history, and the migration
-- (`mirobody migrate-observations`) reads them off the retired name into the
-- new tables. Drop the `_retired_15` tables yourself once the migration has
-- run and been checked; nothing here will.
--
-- Guarded so the replay is safe: each rename happens only while the old name
-- exists and the retired name does not.

DO $$
DECLARE
    old_name text;
BEGIN
    FOREACH old_name IN ARRAY ARRAY['th_series_data', 'th_series_dim', 'fhir_indicators', 'standard_indicators_device']
    LOOP
        IF to_regclass(old_name) IS NOT NULL AND to_regclass(old_name || '_retired_15') IS NULL THEN
            EXECUTE format('ALTER TABLE %I RENAME TO %I', old_name, old_name || '_retired_15');
        END IF;
    END LOOP;
END $$;

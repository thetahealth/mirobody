-- 90_retire.sql: what a database built from older files still carries.
--
-- Removing a statement from a domain file only changes what a NEW database
-- gets; the replay has no ledger and never drops anything on its own. This
-- file is where a retirement is spelled out, so a dev database that ran the
-- old chain catches up. Last on purpose: it names objects the files before
-- it create. Nothing here destroys a person's data: tables with history are
-- renamed, tables and indexes that held none are dropped.

-- The reading tables the observation model replaces (30_observations.sql).
-- Renamed, not dropped: `mirobody migrate-observations` reads the rows off
-- the retired name. Drop the `_retired_15` tables yourself once it has run.
--   th_series_data              one row per reading, value and unit in one
--                               text column, code in a never-filled fhir_id
--   th_series_dim               the per-name dimension with embeddings
--   fhir_indicators             a code registry nothing ever filled
--   standard_indicators_device  a daily mirror of the in-code catalogue
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

-- Tables that held a second copy of something another table owns.
--   th_file_contents        hash -> original_text, the extraction cache;
--                           th_files carries both columns on the file's own row
--   deep_agent_workspace    the agent's virtual filesystem as a table, three
--                           of its four mounts copies of th_files and the
--                           profile; every mount is graph state or a
--                           projection now
--   th_share_permission_type  the advertised sharing vocabulary; the grant
--                           is one column with three values (10_accounts.sql)
DROP TABLE IF EXISTS th_file_contents;
DROP TABLE IF EXISTS deep_agent_workspace;
DROP TABLE IF EXISTS th_share_permission_type;

-- Indexes that charged every insert on the two busiest tables for a read
-- nothing performs: a trigram index over an always-empty comment, a tags
-- index for a feature this project does not have, the file listing that
-- moved to th_files, and a key into a dimension table no file here creates.
DROP INDEX IF EXISTS idx_th_messages_comment_trgm;
DROP INDEX IF EXISTS idx_th_sessions_tags;
DROP INDEX IF EXISTS idx_th_messages_file_list;
DROP INDEX IF EXISTS idx_th_series_data_full_dim_id;

-- An event-trigger function with an empty body that no trigger ever used.
DO $$
BEGIN
    IF to_regproc('prevent_table_drop') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtfoid = to_regproc('prevent_table_drop')) THEN
        DROP FUNCTION prevent_table_drop();
    END IF;
END $$;

-- 90_retire.sql: what a database built from older files still carries.
--
-- Removing a statement from a domain file only changes what a NEW database
-- gets; the replay has no ledger and never drops anything on its own. This
-- file is where a retirement is spelled out, so a dev database that ran the
-- old chain catches up. Last on purpose: it names objects the files before
-- it create. Nothing here destroys a person's data: a table with history is
-- dropped only once it holds none, the rest are dropped outright.

-- The reading tables the observation model replaced in 1.5.0, dropped in 1.5.4.
--   th_series_dim               the per-name dimension with embeddings
--   fhir_indicators             a code registry nothing ever filled
--   standard_indicators_device  a daily mirror of the in-code catalogue
-- `th_series_data` held the readings: renamed `*_retired_15`, which
-- `mirobody migrate-observations` reads and drops after a full pass. Here it
-- is dropped only once no live row is left, so an unmigrated history stays.
DROP TABLE IF EXISTS th_series_dim, th_series_dim_retired_15, fhir_indicators, fhir_indicators_retired_15,
                     standard_indicators_device, standard_indicators_device_retired_15 CASCADE;
DO $$
BEGIN
    IF to_regclass('th_series_data') IS NOT NULL AND to_regclass('th_series_data_retired_15') IS NULL THEN
        ALTER TABLE th_series_data RENAME TO th_series_data_retired_15;
    END IF;
    IF to_regclass('th_series_data_retired_15') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM th_series_data_retired_15 WHERE deleted = 0) THEN
        DROP TABLE th_series_data_retired_15;
    END IF;
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
--   th_series               a per-person catalogue refreshed on every write
--                           that nothing read: `catalog()` groups
--                           `v_observation`, 150 ms over 149k rows (1.5.4)
DROP TABLE IF EXISTS th_file_contents;
DROP TABLE IF EXISTS deep_agent_workspace;
DROP TABLE IF EXISTS th_share_permission_type;
DROP TABLE IF EXISTS th_series;

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

-- Agent checkpoint threads keyed on the session id alone, from before
-- `agent/checkpointer.py::thread_for`: anyone who posted another person's
-- session id resumed that person's conversation. Re-keyed as
-- `<owner>:<session>`, the owner being the sender of the session's first
-- message (th_messages is written by the asker, never the subject). A thread
-- with no message row keeps its old key and is unreachable from a turn. Runs
-- only while an old key is left, because the DISTINCT ON reads every message.
-- Two IFs, not one OR: PL/pgSQL parses a condition whole, so naming
-- `checkpoints` beside the to_regclass guard failed on a database that has no
-- such table yet and rolled back this entire file.
DO $$
DECLARE
    t text;
BEGIN
    IF to_regclass('checkpoints') IS NULL THEN
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM checkpoints WHERE strpos(thread_id, ':') = 0) THEN
        RETURN;
    END IF;
    FOREACH t IN ARRAY ARRAY['checkpoints', 'checkpoint_blobs', 'checkpoint_writes']
    LOOP
        IF to_regclass(t) IS NOT NULL THEN
            EXECUTE format(
                'UPDATE %I c SET thread_id = o.user_id || '':'' || c.thread_id
                   FROM (SELECT DISTINCT ON (session_id) session_id, user_id
                           FROM th_messages ORDER BY session_id, created_at) o
                  WHERE o.session_id = c.thread_id AND strpos(c.thread_id, '':'') = 0', t);
        END IF;
    END LOOP;
END $$;

-- Upload sources named after the web client's tabs (collect/files/services/
-- file_db_service.py SOURCE_DATA, SOURCE_ASK) rather than `web_drive`/`web_chat`.
UPDATE th_files SET created_source = CASE created_source WHEN 'web_drive' THEN 'data' ELSE 'ask' END
 WHERE created_source IN ('web_drive', 'web_chat');

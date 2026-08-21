-- Reclaim four indexes that cost writes and served no read, and one table that
-- stored a second copy of data th_files already holds.
--
-- Deleting a `CREATE INDEX` from a baseline only affects databases created
-- afterwards — the bootstrap has no ledger and never drops anything — so a
-- dev database that already ran the old files keeps paying for these until
-- told otherwise. That is what this file is for.
--
--   idx_th_messages_comment_trgm — GIN trigram index on `th_messages.comment`.
--     No query in the project reads that column, and its only writer was an
--     optional `comment=` argument on `update_message_content` that no caller
--     ever passed. So this index maintained trigrams for an always-NULL column
--     on every insert into the highest-write table in the schema.
--
--   idx_th_sessions_tags — GIN index on `th_sessions.tags`, same story for the
--     notes/journal feature that does not exist in this project.
--
--   idx_th_series_data_full_dim_id — a b-tree on `th_series_data.full_dim_id`,
--     the second key 42_ added beside `fhir_id`. It pointed into
--     `indicator_full_dim`, a dimension table no baseline here creates, owned by
--     a service this project no longer runs. Nothing in the repo ever read or
--     wrote the column, so the index kept an entry per row over a column that is
--     NULL in all of them — on every insert into th_series_data, the
--     highest-write table in the schema. 42_ no longer creates either.
--
--   idx_th_messages_file_list — (user_id, message_type, is_del, created_at DESC),
--     the last database-side remnant of the th_messages → th_files migration.
--     Files used to BE th_messages rows (message_type in 'file'/'pdf'/'image')
--     and this index served "list my uploaded files". That listing is now
--     `FileDbService.get_files_paginated` against th_files. The only surviving
--     th_messages query mentioning message_type (chat session history) also
--     filters session_id, which idx_th_message_sessionID serves far more
--     selectively — so this index charged every insert on the busiest table in
--     the schema for a read that no longer happens.
--
-- Columns are deliberately NOT dropped: DROP COLUMN destroys whatever an
-- existing deployment has in them, and an unused nullable column costs nothing
-- to keep. The baselines simply stop creating them going forward. Same reason
-- `th_health_report_summary` is not dropped here: its writer is gone (that text
-- is now `th_files.original_text`, encrypted, which the old table's was not),
-- and no baseline ever created the table — so a database bootstrapped from this
-- schema never had it, while anything that does got it from an older
-- provisioning path and may still hold rows.

-- th_file_contents (hash -> original_text) was the extraction dedup cache. It
-- normalised nothing: th_files stores content_hash AND original_text on the
-- same row anyway, so this was a second at-rest copy of the same health text —
-- with no user_id, no foreign key to the file, and no delete path, meaning a
-- user's extracted report survived the deletion of the file it came from. The
-- lookup now reads th_files directly (see `_read_original_text_cache`), which
-- makes "delete the file, lose the text" true.
DROP TABLE IF EXISTS th_file_contents;

DROP INDEX IF EXISTS idx_th_messages_comment_trgm;
DROP INDEX IF EXISTS idx_th_sessions_tags;
DROP INDEX IF EXISTS idx_th_messages_file_list;
DROP INDEX IF EXISTS idx_th_series_data_full_dim_id;

-- 20_files.sql: the documents a person uploads.
--
-- th_files is the file row; the bytes live in blob storage under `file_key`.
-- `original_text` is the extracted text, `content_hash` the SHA256 of the raw
-- bytes that lets a re-upload skip extraction. Both sit on this row on
-- purpose: deleting the file deletes the text with it.

CREATE TABLE IF NOT EXISTS th_files (
    id                bigserial PRIMARY KEY,
    user_id           varchar(50)   NOT NULL,
    query_user_id     varchar(50),
    file_name         varchar(255),
    file_type         varchar(255),                    -- the MIME type; Office types exceed 50
    file_key          varchar(255)  NOT NULL,
    file_content      text          NOT NULL DEFAULT '{}'::jsonb,   -- encrypted JSON metadata
    scene             varchar(20),                     -- food / report / medicine / journal / web / others
    created_source    varchar(20)   NOT NULL,
    created_source_id varchar(100),
    is_del            boolean       NOT NULL DEFAULT false,
    created_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    original_text     text,
    text_length       int DEFAULT 0,
    content_hash      VARCHAR(64)
);
ALTER TABLE th_files ADD COLUMN IF NOT EXISTS original_text text;
ALTER TABLE th_files ADD COLUMN IF NOT EXISTS text_length int DEFAULT 0;
ALTER TABLE th_files ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);
ALTER TABLE th_files ALTER COLUMN file_type TYPE varchar(255);

CREATE UNIQUE INDEX IF NOT EXISTS uq_th_files_file_key           ON th_files (file_key);
CREATE INDEX IF NOT EXISTS idx_th_files_user_id                  ON th_files (user_id);
CREATE INDEX IF NOT EXISTS idx_th_files_created_source           ON th_files (created_source);
CREATE INDEX IF NOT EXISTS idx_th_files_created_source_id        ON th_files (created_source_id);
CREATE INDEX IF NOT EXISTS idx_th_files_created_at               ON th_files (created_at);
CREATE INDEX IF NOT EXISTS idx_th_files_content_hash             ON th_files (content_hash);

COMMENT ON COLUMN th_files.content_hash IS 'SHA256 of raw file bytes; dedup key for original_text extraction';

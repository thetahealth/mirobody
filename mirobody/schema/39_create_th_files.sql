CREATE TABLE IF NOT EXISTS th_files
(
    id                bigserial PRIMARY KEY,
    user_id           varchar(50)   NOT NULL,
    query_user_id     varchar(50),
    file_name         varchar(255),
    file_type         varchar(255),  -- holds the MIME type; long ones (xlsx/docx/pptx) exceed 50
    file_key          varchar(255)  NOT NULL,
    file_content      text          NOT NULL DEFAULT '{}'::jsonb,  -- encrypted JSON metadata (encrypt_content); matches test/prod schema
    scene             varchar(20),  -- food/report/medicine/journal/web/others ...
    created_source    varchar(20)   NOT NULL,
    created_source_id varchar(100),
    is_del            boolean       NOT NULL DEFAULT false,
    created_at        timestamptz   NOT NULL DEFAULT now(),
    updated_at        timestamptz   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_th_files_file_key
    ON th_files (file_key);

CREATE INDEX IF NOT EXISTS idx_th_files_user_id
    ON th_files (user_id);

CREATE INDEX IF NOT EXISTS idx_th_files_created_source
    ON th_files (created_source);

CREATE INDEX IF NOT EXISTS idx_th_files_created_source_id
    ON th_files (created_source_id);

CREATE INDEX IF NOT EXISTS idx_th_files_created_at
    ON th_files (created_at);


ALTER TABLE th_files
ADD COLUMN IF NOT EXISTS original_text text;

ALTER TABLE th_files
ADD COLUMN IF NOT EXISTS text_length int DEFAULT 0;

-- SHA256 of the raw bytes. This is what makes re-uploading the same file skip
-- extraction: `_read_original_text_cache` looks up `original_text` by this
-- hash, so the dedup cache and the file row are one and the same.
--
-- A separate `th_file_contents` (hash -> original_text) table used to hold that
-- cache. It normalised nothing — th_files keeps its own copy of the text
-- regardless — so it was a second copy of the same health text, with no
-- user_id, no foreign key, and nothing that ever deleted from it: a user's
-- extracted report outlived the file they deleted. 99_… drops it.
ALTER TABLE th_files
ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_th_files_content_hash ON th_files(content_hash);

COMMENT ON COLUMN th_files.content_hash IS 'SHA256 of raw file bytes; dedup key for original_text extraction';




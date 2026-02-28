CREATE TABLE IF NOT EXISTS th_files
(
    id                bigserial PRIMARY KEY,
    user_id           varchar(50)   NOT NULL,
    query_user_id     varchar(50),
    file_name         varchar(255),
    file_type         varchar(50),
    file_key          varchar(255)  NOT NULL,
    file_content      jsonb         NOT NULL DEFAULT '{}'::jsonb,
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

-- Add content_hash column to th_files table (references th_file_contents)
ALTER TABLE th_files
ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64);

-- Create index
CREATE INDEX IF NOT EXISTS idx_th_files_content_hash ON th_files(content_hash);

COMMENT ON COLUMN th_files.content_hash IS 'SHA256 hash of file content, references th_file_contents table';



CREATE TABLE IF NOT EXISTS th_file_contents (
    id SERIAL PRIMARY KEY,
    content_hash VARCHAR(64) NOT NULL UNIQUE,  -- SHA256 hash (64 chars)
    original_text TEXT,                         -- Original text content
    text_length INT DEFAULT 0,                  -- Text length (character count)
    file_type VARCHAR(50),                      -- File type (pdf/image/etc)
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_th_file_contents_hash ON th_file_contents(content_hash);

-- Add comments
COMMENT ON TABLE th_file_contents IS 'File contents table with SHA256-based deduplication for original text';
COMMENT ON COLUMN th_file_contents.content_hash IS 'SHA256 hash of file content';
COMMENT ON COLUMN th_file_contents.original_text IS 'Original text content (PDF format: [Page 1]...)';
COMMENT ON COLUMN th_file_contents.text_length IS 'Text length (character count)';
COMMENT ON COLUMN th_file_contents.file_type IS 'File type';




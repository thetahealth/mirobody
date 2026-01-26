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

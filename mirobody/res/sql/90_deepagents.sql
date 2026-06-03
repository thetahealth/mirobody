-- DeepAgent virtual filesystem (scope-based, deepagents BackendProtocol).
--
-- One row per (user_id, session_id, scope, path). The agent's deepagents
-- CompositeBackend mounts several PgFilesystemBackend instances over this single
-- table; `scope` is what keeps the mounts isolated (workspace / memory / uploads
-- / library / charts), since the 5 mounts collapse onto only 2 session_id values.
--
-- Storage tiering (decided per write in PgFilesystemBackend._classify_and_store):
--   * inline  : utf-8 text <= 256 KB -> kept in `content`, object_storage_key NULL
--   * offload : large text or binary  -> raw bytes go to object storage
--               (AbstractStorage: S3 / Aliyun OSS / local), object_storage_key
--               points at them; `content` may carry extracted/greppable text.
CREATE TABLE IF NOT EXISTS deep_agent_workspace (
    user_id            VARCHAR(100) NOT NULL,
    session_id         VARCHAR(100) NOT NULL DEFAULT '',
    scope              VARCHAR(16)  NOT NULL DEFAULT 'workspace',
    path               VARCHAR(1024) NOT NULL,

    content            TEXT,
    encoding           VARCHAR(16)  NOT NULL DEFAULT 'utf-8',   -- 'utf-8' | 'base64'
    content_size       INTEGER      NOT NULL DEFAULT 0,         -- raw byte size
    mime_type          VARCHAR(128),

    object_storage_key VARCHAR(512),                            -- NULL when inline
    content_hash       VARCHAR(64),                             -- SHA256 of raw bytes
    file_key           VARCHAR(255),                            -- link to th_files.file_key
    source             VARCHAR(32)  NOT NULL DEFAULT 'agent_write',
        -- agent_write | agent_upload | user_upload | tool_generated

    metadata           JSONB        NOT NULL DEFAULT '{}'::jsonb,

    created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted            INTEGER      NOT NULL DEFAULT 0,

    PRIMARY KEY (user_id, session_id, scope, path)
);

-- Prefix scan for ls/glob/grep within a mount.
CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_scope_path
    ON deep_agent_workspace (user_id, session_id, scope, path);

CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_updated_at
    ON deep_agent_workspace (updated_at);

CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_object_key
    ON deep_agent_workspace (object_storage_key)
    WHERE object_storage_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_content_hash
    ON deep_agent_workspace (content_hash)
    WHERE content_hash IS NOT NULL;

COMMENT ON TABLE deep_agent_workspace IS
    'DeepAgent scope-based virtual filesystem (deepagents BackendProtocol); one audit row per file.';
COMMENT ON COLUMN deep_agent_workspace.scope IS 'Mount scope: workspace | memory | uploads | library | charts | shared';
COMMENT ON COLUMN deep_agent_workspace.path IS 'Absolute file path within the mount (starts with /)';
COMMENT ON COLUMN deep_agent_workspace.content IS 'Inline utf-8 payload, or extracted/greppable text for offloaded files';
COMMENT ON COLUMN deep_agent_workspace.encoding IS 'How to interpret the offloaded raw bytes: utf-8 (text) | base64 (binary)';
COMMENT ON COLUMN deep_agent_workspace.object_storage_key IS 'Object-storage key for offloaded bytes; NULL when stored inline';
COMMENT ON COLUMN deep_agent_workspace.content_hash IS 'SHA256 of the original raw bytes';
COMMENT ON COLUMN deep_agent_workspace.file_key IS 'Reference to th_files.file_key (for re-download of user uploads)';
COMMENT ON COLUMN deep_agent_workspace.source IS 'Provenance: agent_write | agent_upload | user_upload | tool_generated';

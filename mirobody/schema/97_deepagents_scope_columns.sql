-- Migrate the legacy deep_agent_workspace (PK session_id,user_id,key) to the
-- scope-based virtual-filesystem schema used by the deepagents PgFilesystemBackend.
-- Idempotent: safe to re-run.

-- 1. New columns.
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS scope              VARCHAR(16)  NOT NULL DEFAULT 'workspace';
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS encoding           VARCHAR(16)  NOT NULL DEFAULT 'utf-8';
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS content_size       INTEGER      NOT NULL DEFAULT 0;
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS mime_type          VARCHAR(128);
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS object_storage_key VARCHAR(512);
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS source             VARCHAR(32)  NOT NULL DEFAULT 'agent_write';
ALTER TABLE deep_agent_workspace ADD COLUMN IF NOT EXISTS deleted            INTEGER      NOT NULL DEFAULT 0;

-- session_id must allow the cross-session scopes (memory/library) to use ''.
ALTER TABLE deep_agent_workspace ALTER COLUMN session_id SET DEFAULT '';

-- 2. Rename the file-path column key -> path (only if not already done).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'deep_agent_workspace' AND column_name = 'key')
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'deep_agent_workspace' AND column_name = 'path') THEN
        ALTER TABLE deep_agent_workspace RENAME COLUMN key TO path;
    END IF;
END $$;

-- 3. Swap the primary key: (session_id, user_id, key) -> (user_id, session_id, scope, path).
DO $$
DECLARE
    pk_name text;
BEGIN
    SELECT conname INTO pk_name
    FROM pg_constraint
    WHERE conrelid = 'deep_agent_workspace'::regclass AND contype = 'p';

    IF pk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE deep_agent_workspace DROP CONSTRAINT %I', pk_name);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'deep_agent_workspace'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE deep_agent_workspace
            ADD CONSTRAINT deep_agent_workspace_pkey
            PRIMARY KEY (user_id, session_id, scope, path);
    END IF;
END $$;

-- 4. Indexes for the new access pattern.
CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_scope_path
    ON deep_agent_workspace (user_id, session_id, scope, path);
CREATE INDEX IF NOT EXISTS idx_deep_agent_workspace_object_key
    ON deep_agent_workspace (object_storage_key) WHERE object_storage_key IS NOT NULL;

COMMENT ON COLUMN deep_agent_workspace.scope IS 'Mount scope: workspace | memory | uploads | library';

-- DeepAgent Workspace
CREATE TABLE IF NOT EXISTS deep_agent_workspace (
    session_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    key VARCHAR(255) NOT NULL,
    
    content TEXT,
    
    file_key VARCHAR(255),
    content_hash VARCHAR(64),
    file_type VARCHAR(255),
    file_extension VARCHAR(20),
    parsed BOOLEAN DEFAULT false,
    
    metadata JSONB DEFAULT '{}'::jsonb,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (session_id, user_id, key)
);

CREATE INDEX IF NOT EXISTS idx_agent_workspace_session 
    ON deep_agent_workspace(session_id);

CREATE INDEX IF NOT EXISTS idx_agent_workspace_user 
    ON deep_agent_workspace(user_id);

CREATE INDEX IF NOT EXISTS idx_agent_workspace_content_hash 
    ON deep_agent_workspace(content_hash) 
    WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_agent_workspace_updated_at 
    ON deep_agent_workspace(updated_at);

CREATE INDEX IF NOT EXISTS idx_agent_workspace_session_user 
    ON deep_agent_workspace(session_id, user_id);
COMMENT ON TABLE deep_agent_workspace IS 
    'DeepAgent session workspace for parsed files and agent-created content';

COMMENT ON COLUMN deep_agent_workspace.session_id IS 'Session identifier';
COMMENT ON COLUMN deep_agent_workspace.user_id IS 'User identifier';
COMMENT ON COLUMN deep_agent_workspace.key IS 'File path in workspace';
COMMENT ON COLUMN deep_agent_workspace.content IS 'Parsed file content as plain text';
COMMENT ON COLUMN deep_agent_workspace.content_hash IS 'SHA256 hash for cache lookup';
COMMENT ON COLUMN deep_agent_workspace.file_key IS 'Reference to th_files.file_key';
COMMENT ON COLUMN deep_agent_workspace.file_type IS 'MIME type';
COMMENT ON COLUMN deep_agent_workspace.metadata IS 'Additional metadata (parse info, cache status)';
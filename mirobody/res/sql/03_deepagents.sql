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


-- Global file cache
CREATE TABLE IF NOT EXISTS deep_agent_file_cache (
    content_hash VARCHAR(64) PRIMARY KEY,
    
    content TEXT NOT NULL,
    
    file_type VARCHAR(255) NOT NULL,
    file_extension VARCHAR(20),
    original_size BIGINT,
    
    parse_method VARCHAR(50) NOT NULL,
    parse_model VARCHAR(100),
    parse_duration_ms INTEGER,
    parse_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    line_count INTEGER,
    char_count INTEGER,
    
    first_file_key VARCHAR(255),
    reference_count INTEGER DEFAULT 1,
    last_accessed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_file_cache_parse_timestamp 
    ON deep_agent_file_cache(parse_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_file_cache_last_accessed 
    ON deep_agent_file_cache(last_accessed_at DESC);

CREATE INDEX IF NOT EXISTS idx_file_cache_reference_count 
    ON deep_agent_file_cache(reference_count DESC);

CREATE INDEX IF NOT EXISTS idx_file_cache_file_type 
    ON deep_agent_file_cache(file_type);
COMMENT ON TABLE deep_agent_file_cache IS 
    'Global cache for parsed file content, shared across users and sessions';

COMMENT ON COLUMN deep_agent_file_cache.content_hash IS 'SHA256 hash of original file';
COMMENT ON COLUMN deep_agent_file_cache.content IS 'Parsed file content as plain text';
COMMENT ON COLUMN deep_agent_file_cache.parse_method IS 'Parse method used';
COMMENT ON COLUMN deep_agent_file_cache.parse_model IS 'Model used for parsing';
COMMENT ON COLUMN deep_agent_file_cache.parse_timestamp IS 'When file was parsed';
COMMENT ON COLUMN deep_agent_file_cache.reference_count IS 'Access count for LRU management';


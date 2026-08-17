
CREATE TABLE IF NOT EXISTS th_session_share (
    share_session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id VARCHAR(100) NOT NULL UNIQUE,
    user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_th_session_share_session_id 
    ON th_session_share(session_id);
    
CREATE INDEX IF NOT EXISTS idx_th_session_share_user_id 
    ON th_session_share(user_id);



CREATE TABLE IF NOT EXISTS user_mcp_config (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    mcp JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_mcp_config_user_id ON user_mcp_config(user_id);

CREATE INDEX IF NOT EXISTS idx_user_mcp_config_updated_at ON user_mcp_config(updated_at);

COMMENT ON TABLE user_mcp_config IS 'User-specific MCP server configurations';

COMMENT ON COLUMN user_mcp_config.id IS 'Primary key';
COMMENT ON COLUMN user_mcp_config.user_id IS 'User ID (unique)';
COMMENT ON COLUMN user_mcp_config.mcp IS 'MCP configuration in JSON format: {"name": {"url": "..."}}';
COMMENT ON COLUMN user_mcp_config.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN user_mcp_config.updated_at IS 'Last update timestamp';


CREATE TABLE IF NOT EXISTS user_agent_prompt (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    prompt JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_agent_prompt_user_id ON user_agent_prompt(user_id);
COMMENT ON TABLE user_agent_prompt IS 'User-specific agent prompt configurations';

COMMENT ON COLUMN user_agent_prompt.id IS 'Primary key';
COMMENT ON COLUMN user_agent_prompt.user_id IS 'User ID (unique)';
COMMENT ON COLUMN user_agent_prompt.prompt IS 'Agent prompt configuration in JSON format';
COMMENT ON COLUMN user_agent_prompt.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN user_agent_prompt.updated_at IS 'Last update timestamp';


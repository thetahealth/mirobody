
CREATE TABLE IF NOT EXISTS theta_ai.th_session_share (
    share_session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id VARCHAR(100) NOT NULL UNIQUE,
    user_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_th_session_share_session_id 
    ON theta_ai.th_session_share(session_id);
    
CREATE INDEX IF NOT EXISTS idx_th_session_share_user_id 
    ON theta_ai.th_session_share(user_id);



CREATE TABLE IF NOT EXISTS theta_ai.user_mcp_config (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    mcp JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_mcp_config_user_id ON theta_ai.user_mcp_config(user_id);

CREATE INDEX IF NOT EXISTS idx_user_mcp_config_updated_at ON theta_ai.user_mcp_config(updated_at);

COMMENT ON TABLE theta_ai.user_mcp_config IS 'User-specific MCP server configurations';

COMMENT ON COLUMN theta_ai.user_mcp_config.id IS 'Primary key';
COMMENT ON COLUMN theta_ai.user_mcp_config.user_id IS 'User ID (unique)';
COMMENT ON COLUMN theta_ai.user_mcp_config.mcp IS 'MCP configuration in JSON format: {"name": {"url": "..."}}';
COMMENT ON COLUMN theta_ai.user_mcp_config.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN theta_ai.user_mcp_config.updated_at IS 'Last update timestamp';


CREATE TABLE IF NOT EXISTS theta_ai.deep_agent_store (
    namespace VARCHAR(255) NOT NULL,
    key VARCHAR(255) NOT NULL,
    value JSONB NOT NULL,
    session_id VARCHAR(100),
    user_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (namespace, key)
);



CREATE INDEX IF NOT EXISTS idx_deep_agent_store_namespace_prefix 
    ON theta_ai.deep_agent_store(namespace text_pattern_ops);

CREATE INDEX IF NOT EXISTS idx_deep_agent_store_session 
    ON theta_ai.deep_agent_store(session_id) WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_deep_agent_store_user 
    ON theta_ai.deep_agent_store(user_id) WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_deep_agent_store_value 
    ON theta_ai.deep_agent_store USING gin (value);

CREATE INDEX IF NOT EXISTS idx_deep_agent_store_updated_at 
    ON theta_ai.deep_agent_store(updated_at);

COMMENT ON TABLE theta_ai.deep_agent_store IS 'Storage for DeepAgent files and state across conversation threads';

COMMENT ON COLUMN theta_ai.deep_agent_store.namespace IS 'Namespace identifier (format: "deep_agent/session_id/user_id")';
COMMENT ON COLUMN theta_ai.deep_agent_store.key IS 'Item key/identifier';
COMMENT ON COLUMN theta_ai.deep_agent_store.value IS 'Item value stored as JSONB';
COMMENT ON COLUMN theta_ai.deep_agent_store.session_id IS 'Session ID extracted from namespace for efficient querying';
COMMENT ON COLUMN theta_ai.deep_agent_store.user_id IS 'User ID extracted from namespace for efficient querying';
COMMENT ON COLUMN theta_ai.deep_agent_store.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN theta_ai.deep_agent_store.updated_at IS 'Last update timestamp';



CREATE TABLE IF NOT EXISTS theta_ai.user_agent_prompt (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL UNIQUE,
    prompt JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_agent_prompt_user_id ON theta_ai.user_agent_prompt(user_id);
COMMENT ON TABLE theta_ai.user_agent_prompt IS 'User-specific agent prompt configurations';

COMMENT ON COLUMN theta_ai.user_agent_prompt.id IS 'Primary key';
COMMENT ON COLUMN theta_ai.user_agent_prompt.user_id IS 'User ID (unique)';
COMMENT ON COLUMN theta_ai.user_agent_prompt.prompt IS 'Agent prompt configuration in JSON format';
COMMENT ON COLUMN theta_ai.user_agent_prompt.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN theta_ai.user_agent_prompt.updated_at IS 'Last update timestamp';


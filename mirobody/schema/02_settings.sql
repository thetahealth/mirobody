
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

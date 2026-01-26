ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS write_status INTEGER DEFAULT 1;
ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS ai_status    INTEGER DEFAULT 0;

-- Add comments to document the fields
COMMENT ON COLUMN th_sessions.write_status IS 'Write status: 0=edited (initial), 1=saved, 2=failed';
COMMENT ON COLUMN th_sessions.ai_status    IS 'AI processing status: 0=running, 1=succeeded, 2=failed';

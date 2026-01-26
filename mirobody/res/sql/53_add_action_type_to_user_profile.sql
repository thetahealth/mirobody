ALTER TABLE health_user_profile_by_system
ADD COLUMN IF NOT EXISTS action_type VARCHAR(32) DEFAULT NULL;

COMMENT ON COLUMN health_user_profile_by_system.action_type IS 'add, NULL/delete';

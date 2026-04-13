CREATE TABLE IF NOT EXISTS th_user_custom_skills (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    summary TEXT NOT NULL,
    when_to_use TEXT NOT NULL,
    when_not_to_use TEXT NOT NULL,
    tags TEXT NOT NULL,
    skill_md TEXT NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_th_user_custom_skills_user_id ON th_user_custom_skills(user_id);

COMMENT ON TABLE th_user_custom_skills IS 'User custom skills table';
COMMENT ON COLUMN th_user_custom_skills.user_id IS 'Owner user ID';
COMMENT ON COLUMN th_user_custom_skills.name IS 'Skill name';
COMMENT ON COLUMN th_user_custom_skills.summary IS 'Brief description of the skill';
COMMENT ON COLUMN th_user_custom_skills.when_to_use IS 'Scenarios where this skill should be used (JSON array)';
COMMENT ON COLUMN th_user_custom_skills.when_not_to_use IS 'Scenarios where this skill should NOT be used (JSON array)';
COMMENT ON COLUMN th_user_custom_skills.tags IS 'Tags for skill categorization (JSON array)';
COMMENT ON COLUMN th_user_custom_skills.skill_md IS 'Skill document in Markdown format';
COMMENT ON COLUMN th_user_custom_skills.is_deleted IS 'Whether the skill is deleted';
COMMENT ON COLUMN th_user_custom_skills.created_at IS 'Creation timestamp';
COMMENT ON COLUMN th_user_custom_skills.updated_at IS 'Last update timestamp';

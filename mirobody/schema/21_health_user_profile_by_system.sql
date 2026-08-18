ALTER TABLE health_user_profile_by_system
ADD COLUMN IF NOT EXISTS scenario_zh VARCHAR(512) DEFAULT NULL;

ALTER TABLE health_user_profile_by_system
ADD COLUMN IF NOT EXISTS scenario_en VARCHAR(512) DEFAULT NULL;

ALTER TABLE health_user_profile_by_system
ADD COLUMN IF NOT EXISTS scenario_image_url TEXT DEFAULT NULL;

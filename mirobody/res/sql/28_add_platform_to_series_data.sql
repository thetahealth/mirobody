
ALTER TABLE theta_ai.series_data
ADD COLUMN IF NOT EXISTS platform varchar(32) DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_series_data_user_platform
ON theta_ai.series_data(user_id, platform) WHERE platform IS NOT NULL;

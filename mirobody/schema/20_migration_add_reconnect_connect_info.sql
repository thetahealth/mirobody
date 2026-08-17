ALTER TABLE health_user_provider 
ADD COLUMN IF NOT EXISTS reconnect integer NOT NULL DEFAULT 0;

ALTER TABLE health_user_provider 
ADD COLUMN IF NOT EXISTS connect_info JSONB DEFAULT NULL;

COMMENT ON COLUMN health_user_provider.reconnect IS 'Reconnection flag: 0=normal, 1=needs reconnect. Pull tasks only process users with reconnect=0';

COMMENT ON COLUMN health_user_provider.connect_info IS 'Additional connection information stored as JSON (e.g., patient_id, device_info, etc.)';

UPDATE health_user_provider 
SET reconnect = 0 
WHERE reconnect IS NULL;


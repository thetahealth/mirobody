-- 40_devices.sql: connected devices and what they send.
--
--   health_user_provider       a person's connection to a provider
--   health_vital_user          the Vital user id behind a connection
--   health_data_{garmin,whoop,oura}   raw payloads as received, one table per
--                              provider that ships here
--   series_data                the raw point buffer aggregation reads from
--   th_data_source_priority    which source a day publishes from when two
--                              report the same metric (kernel/series.py elect)
--
-- Readings themselves are th_observation (30_observations.sql).

CREATE TABLE IF NOT EXISTS health_user_provider (
    id                  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at           timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at           timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del              boolean NOT NULL,
    user_id             character varying(128) COLLATE pg_catalog."default" NOT NULL DEFAULT 0,
    provider            character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    username            character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    password            character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    llm_access          integer DEFAULT 1,
    access_token        TEXT DEFAULT NULL,
    access_token_secret TEXT DEFAULT NULL,
    refresh_token       TEXT DEFAULT NULL,
    expires_at          TIMESTAMP,
    reconnect           integer NOT NULL DEFAULT 0,     -- 1 = needs reconnect; pull tasks skip it
    connect_info        JSONB DEFAULT NULL              -- provider-side ids (patient_id, device_info)
);
ALTER TABLE health_user_provider ADD COLUMN IF NOT EXISTS reconnect integer NOT NULL DEFAULT 0;
ALTER TABLE health_user_provider ADD COLUMN IF NOT EXISTS connect_info JSONB DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_health_user_provider_user_id ON health_user_provider(user_id);

COMMENT ON COLUMN health_user_provider.reconnect IS 'Reconnection flag: 0=normal, 1=needs reconnect. Pull tasks only process users with reconnect=0';
COMMENT ON COLUMN health_user_provider.connect_info IS 'Additional connection information stored as JSON (e.g., patient_id, device_info, etc.)';

CREATE TABLE IF NOT EXISTS health_vital_user (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del        boolean NOT NULL,
    app_user_id   character varying(100) NOT NULL DEFAULT 0,
    vital_user_id character varying NOT NULL DEFAULT ''::character varying
);
CREATE INDEX IF NOT EXISTS idx_health_vital_user_app_user_id   ON health_vital_user(app_user_id);
CREATE INDEX IF NOT EXISTS idx_health_vital_user_vital_user_id ON health_vital_user(vital_user_id);

-- Raw payloads, kept so a decoder can be re-run. Garmin's msg_id is unique;
-- WHOOP and Oura may resend one.
CREATE TABLE IF NOT EXISTS health_data_garmin (
    id               INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del           boolean NOT NULL DEFAULT false,
    msg_id           character varying(200) UNIQUE NOT NULL,
    raw_data         jsonb NOT NULL,
    theta_user_id    character varying(100),
    external_user_id character varying(100)
);
CREATE INDEX IF NOT EXISTS idx_health_data_garmin_theta_user_id ON health_data_garmin(theta_user_id);

CREATE TABLE IF NOT EXISTS health_data_whoop (
    id               INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del           boolean NOT NULL DEFAULT false,
    msg_id           character varying(200),
    raw_data         jsonb NOT NULL,
    theta_user_id    character varying(100),
    external_user_id character varying(100)
);
CREATE INDEX IF NOT EXISTS idx_health_data_whoop_theta_user_id ON health_data_whoop(theta_user_id);

CREATE TABLE IF NOT EXISTS health_data_oura (
    id               INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del           boolean NOT NULL DEFAULT false,
    msg_id           character varying(200),
    raw_data         jsonb NOT NULL,
    theta_user_id    character varying(100),
    external_user_id character varying(100)
);
CREATE INDEX IF NOT EXISTS idx_health_data_oura_theta_user_id ON health_data_oura(theta_user_id);

-- The point buffer: one row per (user, indicator, source, instant). The
-- aggregation pass reads it and writes day values as observations.
CREATE TABLE IF NOT EXISTS series_data (
    user_id     character varying NOT NULL,
    indicator   character varying NOT NULL,
    source      character varying,
    time        timestamp without time zone NOT NULL,
    value       text NOT NULL,
    create_time timestamp without time zone NOT NULL DEFAULT now(),
    update_time timestamp with time zone NOT NULL DEFAULT now(),
    timezone    character varying(50),
    task_id     character varying(200),
    source_id   character varying(128),
    platform    varchar(32) DEFAULT NULL,
    CONSTRAINT unique_series_data_user_indicator_source_time UNIQUE (user_id, indicator, source, time)
);
ALTER TABLE series_data ADD COLUMN IF NOT EXISTS platform varchar(32) DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_series_data_user_platform ON series_data(user_id, platform) WHERE platform IS NOT NULL;

-- Source priority: lower wins. Re-seeded on every boot; edit the rows here.
CREATE TABLE IF NOT EXISTS th_data_source_priority (
    id             integer GENERATED ALWAYS AS IDENTITY NOT NULL,
    source         character varying(255) COLLATE pg_catalog."default" NOT NULL,
    priority       integer NOT NULL,
    category       character varying(100) COLLATE pg_catalog."default",
    description    text COLLATE pg_catalog."default",
    is_active      boolean DEFAULT true,
    created_at     timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at     timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    support_status character varying(20) COLLATE pg_catalog."default" DEFAULT 'SUPPORTED'::character varying,
    is_device      boolean DEFAULT true,
    CONSTRAINT th_data_source_priority_pkey PRIMARY KEY (id),
    CONSTRAINT th_data_source_priority_source_key UNIQUE (source)
);
CREATE INDEX IF NOT EXISTS idx_th_data_source_priority_source ON th_data_source_priority(source);

COMMENT ON TABLE th_data_source_priority IS 'Data source priority configuration table';
COMMENT ON COLUMN th_data_source_priority.support_status IS 'Support status: SUPPORTED-fully supported, TBD-in development';
COMMENT ON COLUMN th_data_source_priority.is_device IS 'Whether the data source is a physical device (true) or software platform (false)';

INSERT INTO th_data_source_priority (source, priority, category, description, is_device) VALUES
('vital.dexcom_v3', 1, 'glucose', 'Dexcom', TRUE),
('vital.abbott_libreview', 2, 'glucose', 'Abbott FreeStyle Libre', TRUE),
('vital.freestyle_libre', 3, 'glucose', 'FreeStyle Libre', FALSE),

('vital.omron', 4, 'medical', 'Omron', TRUE),
('vital.ihealth', 5, 'medical', 'iHealth', TRUE),

('vital.withings', 6, 'body', 'Withings', TRUE),
('vital.renpho', 7, 'body', 'Renpho', TRUE),

('vital.garmin', 8, 'fitness', 'Garmin', TRUE),
('vital.polar', 9, 'fitness', 'Polar', TRUE),
('vital.whoop_v2', 10, 'fitness', 'WHOOP', FALSE),

('vital.fitbit', 11, 'fitness', 'Fitbit', TRUE),
('vital.oura', 12, 'fitness', 'Oura', TRUE),
('vital.ultrahuman', 13, 'fitness', 'Ultrahuman', TRUE),

('vital.eight_sleep', 14, 'sleep', 'Eight Sleep', TRUE),

('vital.beurer_api', 15, 'general', 'Beurer', TRUE),
('vital.hammerhead', 16, 'cycling', 'Hammerhead', TRUE),
('vital.wahoo', 17, 'cycling', 'Wahoo', TRUE),
('vital.zwift', 18, 'cycling', 'Zwift', TRUE),
('vital.peloton', 19, 'fitness', 'Peloton', TRUE),

('vital.cronometer', 20, 'nutrition', 'Cronometer', FALSE),
('vital.my_fitness_pal_v2', 21, 'nutrition', 'MyFitnessPal', FALSE),

('resmed', 22, 'fitness', 'resmed', TRUE),
('vital.map_my_fitness', 22, 'fitness', 'MapMyFitness', TRUE),
('frontierx', 23, 'fitness', 'frontierx', TRUE),

('apple_health', 99, 'general', 'Apple Health', FALSE)

ON CONFLICT (source) DO UPDATE SET
    priority = EXCLUDED.priority,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    is_device = EXCLUDED.is_device,
    is_active = COALESCE(EXCLUDED.is_active, TRUE),
    updated_at = CURRENT_TIMESTAMP;

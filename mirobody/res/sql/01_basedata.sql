
CREATE TABLE IF NOT EXISTS th_messages (
    id varchar(50) UNIQUE NOT NULL,
    user_id character varying(100) not null,
    user_name character varying(100),
    session_id character varying(100) not null,
    role character varying(20) not null,
    content text not null,
    reasoning text,
    agent character varying(50),
    provider character varying(50),
    input_prompt text,
    question_id character varying(50),
    rating integer,
    created_at timestamp with time zone default CURRENT_TIMESTAMP,
    comment text,
    message_type text,
    is_del boolean not null default false,
    updated_at timestamp with time zone default CURRENT_TIMESTAMP,
    group_id VARCHAR(64),
    scene VARCHAR(32) DEFAULT 'web',
    query_user_id VARCHAR(100), 
    reference_task_id VARCHAR(128) DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_th_message_sessionID ON th_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_th_message_questionID ON th_messages(question_id);
CREATE INDEX IF NOT EXISTS idx_th_messages_file_list ON th_messages(user_id, message_type, is_del, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_th_messages_comment_trgm ON th_messages USING GIN (comment gin_trgm_ops);


CREATE TABLE IF NOT EXISTS th_sessions (
    session_id varchar(100) UNIQUE NOT NULL,
    user_id character varying(100),
    user_name character varying(100),
    query_user_id VARCHAR(100),
    in_use BOOLEAN DEFAULT TRUE,
    summary text,
    created_at timestamp with time zone default CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_th_sessions_user_id ON th_sessions(user_id);

ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS category VARCHAR(50);


CREATE TABLE IF NOT EXISTS series_data (
    user_id character varying not null,
    indicator character varying not null,
    source character varying,
    time timestamp without time zone not null,
    value text not null,
    create_time timestamp with time zone not null default now(),
    update_time timestamp with time zone not null default now(),
    timezone character varying(50),
    task_id character varying(200),
    source_id character varying(128),
    platform varchar(32) DEFAULT NULL,
    CONSTRAINT unique_series_data_user_indicator_source_time UNIQUE (user_id, indicator, source, time)
);

-- Create composite index on user_id and platform for better query performance
CREATE INDEX IF NOT EXISTS idx_series_data_user_platform
ON series_data(user_id, platform) WHERE platform IS NOT NULL;


CREATE TABLE IF NOT EXISTS th_series_data
(
    id integer generated always as identity not null,
    user_id character varying(200) COLLATE pg_catalog."default",
    indicator character varying(200) COLLATE pg_catalog."default",
    value text COLLATE pg_catalog."default",
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    source_table character varying(200) COLLATE pg_catalog."default",
    source_table_id character varying(200) COLLATE pg_catalog."default",
    comment text COLLATE pg_catalog."default",
    indicator_id text COLLATE pg_catalog."default" NOT NULL DEFAULT ''::text,
    deleted integer NOT NULL DEFAULT 0,
    create_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    source character varying(128) COLLATE pg_catalog."default",
    task_id character varying(200) COLLATE pg_catalog."default",
    CONSTRAINT th_series_data_pkey PRIMARY KEY (id),
    CONSTRAINT unique_user_indicator_start_end_time UNIQUE (user_id, indicator, start_time, end_time)
);

CREATE INDEX IF NOT EXISTS idx_th_series_data_source_table_id ON th_series_data(source_table_id);


CREATE TABLE IF NOT EXISTS th_series_dim (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    original_indicator character varying(200) UNIQUE NOT NULL,
    standard_indicator character varying(200),
    category_group character varying(200),
    category character varying(200),
    original_indicator_embedding vector,
    standard_indicator_embedding vector,
    category_embedding vector,
    updated_at timestamp without time zone,
    unit character varying,
    deleted boolean not null default false,
    create_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    update_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    diagnosis_recommended_organ character varying(200),
    diagnosis_recommended_system character varying(200),
    diagnosis_recommended_disease text,
    department text,
    symptom text
);

COMMENT ON COLUMN th_series_dim.diagnosis_recommended_organ IS 'AI-recommended primary organ related to this health indicator';
COMMENT ON COLUMN th_series_dim.diagnosis_recommended_system IS 'AI-recommended body system related to this health indicator';
COMMENT ON COLUMN th_series_dim.diagnosis_recommended_disease IS 'AI-recommended possible diseases related to this health indicator (comma-separated)';
COMMENT ON COLUMN th_series_dim.department IS 'AI-recommended medical departments for this health indicator (comma-separated)';
COMMENT ON COLUMN th_series_dim.symptom IS 'AI-identified symptoms related to this health indicator (comma-separated)';

drop view if exists v_th_messages;
CREATE OR REPLACE VIEW v_th_messages AS
SELECT 
    id,
    user_id,
    user_name,
    session_id,
    role,
    decrypt_content(content) AS content,
    reasoning,
    agent,
    provider,
    input_prompt,
    question_id,
    rating,
    created_at
FROM th_messages
WHERE role='assistant' AND rating IS NOT NULL;

CREATE TABLE IF NOT EXISTS th_series_data_genetic
(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
    rsid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    chromosome character varying(10) COLLATE pg_catalog."default" NOT NULL,
    "position" integer NOT NULL,
    genotype character varying(10) COLLATE pg_catalog."default" NOT NULL,
    create_time timestamp without time zone NOT NULL DEFAULT now(),
    update_time timestamp without time zone NOT NULL DEFAULT now(),
    is_deleted boolean NOT NULL DEFAULT false,
    source_table character varying(200) COLLATE pg_catalog."default",
    source_table_id character varying(200) COLLATE pg_catalog."default");


COMMENT ON TABLE th_series_data_genetic IS 'User genetic data table';
COMMENT ON COLUMN th_series_data_genetic.id IS 'Primary key ID';
COMMENT ON COLUMN th_series_data_genetic.user_id IS 'User ID';
COMMENT ON COLUMN th_series_data_genetic.rsid IS 'Genetic locus ID';
COMMENT ON COLUMN th_series_data_genetic.chromosome IS 'Chromosome';
COMMENT ON COLUMN th_series_data_genetic."position" IS 'Position';
COMMENT ON COLUMN th_series_data_genetic.genotype IS 'Genotype';
COMMENT ON COLUMN th_series_data_genetic.create_time IS 'Creation time';
COMMENT ON COLUMN th_series_data_genetic.update_time IS 'Update time';
COMMENT ON COLUMN th_series_data_genetic.is_deleted IS 'Whether deleted';
CREATE INDEX IF NOT EXISTS idx_th_series_data_genetic_rsid
    ON th_series_data_genetic USING btree(user_id, rsid);
CREATE INDEX IF NOT EXISTS idx_th_series_data_genetic_user_id 
    ON th_series_data_genetic(user_id);

CREATE TABLE IF NOT EXISTS th_data_source_priority
(
    id integer generated always as identity not null,
    source character varying(255) COLLATE pg_catalog."default" NOT NULL,
    priority integer NOT NULL,
    category character varying(100) COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    support_status character varying(20) COLLATE pg_catalog."default" DEFAULT 'SUPPORTED'::character varying,
    is_device boolean DEFAULT true,
    CONSTRAINT th_data_source_priority_pkey PRIMARY KEY (id),
    CONSTRAINT th_data_source_priority_source_key UNIQUE (source)
);

COMMENT ON TABLE th_data_source_priority IS 'Data source priority configuration table';
COMMENT ON COLUMN th_data_source_priority.support_status IS 'Support status: SUPPORTED-fully supported, TBD-in development';
COMMENT ON COLUMN th_data_source_priority.is_device IS 'Whether the data source is a physical device (true) or software platform (false)';

CREATE INDEX IF NOT EXISTS idx_th_data_source_priority_source ON th_data_source_priority(source);

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


DROP VIEW IF EXISTS v_th_series_data;
CREATE OR REPLACE VIEW v_th_series_data
 AS
 SELECT
        CASE
            WHEN t2.standard_indicator IS NULL THEN t1.indicator
            ELSE t2.standard_indicator
        END AS standard_indicator,
        CASE
            WHEN t2.category_group IS NULL THEN 'other'::character varying
            ELSE t2.category_group
        END AS category_group,
        CASE
            WHEN t2.category IS NULL THEN t1.indicator
            ELSE t2.category
        END AS category,
    t2.original_indicator_embedding,
    t2.standard_indicator_embedding,
    t2.category_embedding,
    t2.unit,
    t2.diagnosis_recommended_organ,
    t2.diagnosis_recommended_system,
    t2.diagnosis_recommended_disease,
    t2.department,
    t2.symptom,
    t1.id,
    t1.user_id,
    t1.indicator,
    t1.value,
    t1.start_time,
    t1.end_time,
    t1.source_table,
    t1.source_table_id,
    t1.comment,
    t1.indicator_id,
    t1.deleted,
    t1.create_time,
    t1.update_time,
    t1.source
   FROM th_series_data t1
     LEFT JOIN th_series_dim t2 ON t1.indicator::text = t2.original_indicator::text;

set check_function_bodies = off;

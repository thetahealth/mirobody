
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
    message_type text,
    is_del boolean not null default false,
    updated_at timestamp with time zone default CURRENT_TIMESTAMP,
    group_id VARCHAR(64),
    scene VARCHAR(32) DEFAULT 'web',
    query_user_id VARCHAR(100),
    reference_task_id VARCHAR(128) DEFAULT NULL
);

-- `comment text` and its GIN trigram index used to sit here. No query in the
-- project ever read the column, and the one writer (`update_message_content`'s
-- optional `comment=` argument) was never passed by any caller — so the trigram
-- index was paying GIN maintenance on every insert into the busiest table in
-- the schema to make an always-empty column searchable. 99_… drops the index
-- from databases that already have it.
-- `idx_th_messages_file_list` (user_id, message_type, is_del, created_at DESC)
-- also used to sit here. It existed for one query — "list this user's uploaded
-- files" back when files WERE `th_messages` rows with message_type in
-- ('file','pdf','image'). Files moved to `th_files`, that listing is now
-- `FileDbService.get_files_paginated`, and no remaining `th_messages` query
-- filters on message_type without a far more selective `session_id` (served by
-- the session index below). 99_… drops it where it already exists.
CREATE INDEX IF NOT EXISTS idx_th_message_sessionID ON th_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_th_message_questionID ON th_messages(question_id);


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
ALTER TABLE th_sessions ADD COLUMN IF NOT EXISTS preview VARCHAR(200);  -- conversation preview snippet (matches test/prod)

-- `category` keeps its comment and index HERE, with the column it belongs to
-- (the README's "pick one owner" rule). They arrived with `27_add_tags_to_…`,
-- which is gone: the rest of that file — plus `34_…status_fields` and
-- `44_…add_status` — only added `tags`, `read_status`, `write_status`,
-- `ai_status` and `status`, columns for a notes/journal feature that does not
-- exist in this project and that no query here reads or writes.
-- `get_session_summaries` does filter on `category IS NULL`, so the column stays.
COMMENT ON COLUMN th_sessions.category IS 'File category: food, report, medicine, rtc, journal, other';
CREATE INDEX IF NOT EXISTS idx_th_sessions_category ON th_sessions(category);


CREATE TABLE IF NOT EXISTS fhir_indicators (
	id bigserial NOT NULL,
	indicator_standard varchar(255) NULL,
	code varchar(255) NULL,
	full_name text NULL,
	short_name text NULL,
	description text NULL,
	"system" text NULL,
	interpretation text NULL,
	unit text NULL,
	llm_description text NULL,
	"rank" int4 DEFAULT 0 NULL,
	llm_unit text NULL,
	embedding_gemini vector(1024) NULL,
	embedding_qwen3 vector(1024) NULL,            -- only column search needs for EMBEDDING_PROVIDER=qwen (_search_fhir_db); matches test/prod
	embedding_qwen3_8b vector(1024) NULL,         -- EMBEDDING_PROVIDER=openrouter (qwen/qwen3-embedding-8b) — the shipped default
	CONSTRAINT fhir_indicators_indicator_standard_code_unique UNIQUE (indicator_standard, code),
	CONSTRAINT fhir_indicators_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fhir_indicators_source_code ON fhir_indicators USING btree (indicator_standard, code);
CREATE INDEX IF NOT EXISTS idx_fhir_indicators_embedding_qwen3
    ON fhir_indicators USING hnsw (embedding_qwen3 vector_cosine_ops);

-- EMBEDDING_PROVIDER=openrouter (open-weights qwen/qwen3-embedding-8b, the
-- shipped default — one OPENROUTER_API_KEY covers the agent AND this column).
-- Named after the MODEL, not the provider: vectors are only comparable within
-- one (provider, model) pair, and a column named for a provider already lied
-- once (embedding_qwen3 actually holds DashScope text-embedding-v4 vectors).
-- ALTER (not just the CREATE above) because this schema is replayed at boot
-- against databases created before the column existed.
ALTER TABLE fhir_indicators ADD COLUMN IF NOT EXISTS embedding_qwen3_8b vector(1024);
CREATE INDEX IF NOT EXISTS idx_fhir_indicators_embedding_qwen3_8b
    ON fhir_indicators USING hnsw (embedding_qwen3_8b vector_cosine_ops);


CREATE TABLE IF NOT EXISTS series_data (
    user_id character varying not null,
    indicator character varying not null,
    source character varying,
    time timestamp without time zone not null,
    value text not null,
    create_time timestamp without time zone not null default now(),  -- matches test/prod (no tz)
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
    updated_at timestamp without time zone,
    unit character varying,
    deleted boolean not null default false,
    create_time timestamp with time zone not null default CURRENT_TIMESTAMP,
    update_time timestamp with time zone not null default CURRENT_TIMESTAMP
);

--  Add embedding_gemini field for Gemini 1024-dimension vector search (used by indicator_service_v3)
ALTER TABLE th_series_dim ADD COLUMN IF NOT EXISTS embedding_gemini vector(1024);
COMMENT ON COLUMN th_series_dim.embedding_gemini IS 'Gemini embedding (1024 dimensions) for semantic search';

CREATE INDEX IF NOT EXISTS idx_th_series_dim_embedding_gemini
    ON th_series_dim USING hnsw (embedding_gemini vector_cosine_ops);

--  Add embedding_qwen field for Qwen 1024-dimension vector search (selected via EMBEDDING_PROVIDER=qwen)
ALTER TABLE th_series_dim ADD COLUMN IF NOT EXISTS embedding_qwen vector(1024);
COMMENT ON COLUMN th_series_dim.embedding_qwen IS 'Qwen embedding (1024 dimensions) for semantic search';

--  EMBEDDING_PROVIDER=openrouter (qwen/qwen3-embedding-8b) — the shipped
--  default. Same rationale as the fhir_indicators column above: named after
--  the model, added by ALTER for pre-existing databases.
ALTER TABLE th_series_dim ADD COLUMN IF NOT EXISTS embedding_qwen3_8b vector(1024);
COMMENT ON COLUMN th_series_dim.embedding_qwen3_8b IS 'Qwen3-Embedding-8B via OpenRouter (1024 dims, MRL prefix) for semantic search';
CREATE INDEX IF NOT EXISTS idx_th_series_dim_embedding_qwen3_8b
    ON th_series_dim USING hnsw (embedding_qwen3_8b vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_th_series_dim_embedding_qwen
    ON th_series_dim USING hnsw (embedding_qwen vector_cosine_ops);

ALTER TABLE th_series_dim ADD COLUMN IF NOT EXISTS department text NULL;



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


set check_function_bodies = off;

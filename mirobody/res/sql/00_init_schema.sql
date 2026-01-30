CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE TABLE IF NOT EXISTS health_app_user (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    update_at timestamp without time zone not null default CURRENT_TIMESTAMP,
    is_del boolean not null,
    email character varying not null UNIQUE,
    name character varying not null default ''::character varying,
    consultant_id integer not null default 0,
    lang character varying not null default 'en'::character varying,
    apple_sub VARCHAR(255),
    response_lang character varying(64) default NULL::character varying,
    gender integer,
    birth character varying,
    blood character varying,
    tz character varying not null default ''::character varying,
    coins integer default 0
);

CREATE        INDEX IF NOT EXISTS idx_health_app_user_apple_sub ON health_app_user USING btree (apple_sub);
CREATE UNIQUE INDEX IF NOT EXISTS idx_uni_health_app_user_email_active ON health_app_user USING btree (email) WHERE (is_del = false);
CREATE UNIQUE INDEX IF NOT EXISTS idx_uni_health_app_user_apple_sub_active ON health_app_user USING btree (apple_sub) WHERE (is_del = false);

COMMENT ON COLUMN health_app_user.gender IS 'Gender: 0-Unknown 1-Male 2-Female';


CREATE TABLE IF NOT EXISTS th_share_relationship (
    share_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    
    owner_user_id VARCHAR(50) NOT NULL,
    member_user_id VARCHAR(50) NOT NULL,
    owner_email VARCHAR(255),
    member_email VARCHAR(255),
    status VARCHAR(20) DEFAULT 'pending',
    permissions JSONB DEFAULT '{"all": 1}'::jsonb,
    relationship_type VARCHAR(50) DEFAULT 'data_sharing',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_share_relationship UNIQUE (owner_user_id, member_user_id)
);

CREATE INDEX IF NOT EXISTS idx_share_owner_user_id ON th_share_relationship(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_share_member_user_id ON th_share_relationship(member_user_id);
CREATE INDEX IF NOT EXISTS idx_share_status ON th_share_relationship(status);

COMMENT ON TABLE th_share_relationship IS 'Data sharing relationships between users';
COMMENT ON COLUMN th_share_relationship.owner_user_id IS 'The data owner (user sharing their data)';
COMMENT ON COLUMN th_share_relationship.member_user_id IS 'The member (user who can access the data)';
COMMENT ON COLUMN th_share_relationship.status IS 'Status: pending, authorized, revoked';
COMMENT ON COLUMN th_share_relationship.permissions IS 'Permission JSON: {"all": 0/1/2} or {"device": 1, "ehr": 2}';


CREATE TABLE IF NOT EXISTS th_share_user_config (
    config_id SERIAL PRIMARY KEY,
    setter_user_id VARCHAR(50) NOT NULL,
    target_user_id VARCHAR(50) NOT NULL,
    context VARCHAR(50) DEFAULT 'default',
    nickname VARCHAR(255),
    avatar_key VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_share_user_config UNIQUE (setter_user_id, target_user_id, context)
);

CREATE INDEX IF NOT EXISTS idx_share_config_setter ON th_share_user_config(setter_user_id);
CREATE INDEX IF NOT EXISTS idx_share_config_target ON th_share_user_config(target_user_id);

COMMENT ON TABLE th_share_user_config IS 'User-specific configuration for shared relationships (nicknames, avatars)';
COMMENT ON COLUMN th_share_user_config.setter_user_id IS 'The user who sets the nickname/avatar';
COMMENT ON COLUMN th_share_user_config.target_user_id IS 'The user being nicknamed/avatared';
COMMENT ON COLUMN th_share_user_config.context IS 'Context for the configuration (default, family, etc.)';


CREATE TABLE IF NOT EXISTS th_user_avatar_managed (
    user_id character varying(100) NOT NULL,
    owner_user_id character varying(100) NOT NULL,
    avatar_key character varying(500) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT th_user_avatar_managed_pkey PRIMARY KEY (user_id, owner_user_id)
);

CREATE INDEX IF NOT EXISTS idx_th_user_avatar_managed_user_id 
    ON th_user_avatar_managed(user_id);

CREATE INDEX IF NOT EXISTS idx_th_user_avatar_managed_owner_user_id 
    ON th_user_avatar_managed(owner_user_id);

COMMENT ON TABLE th_user_avatar_managed IS 'Manages user avatars updated by authorized users (caregivers, family members, etc.)';
COMMENT ON COLUMN th_user_avatar_managed.user_id IS 'The user whose avatar is being managed';
COMMENT ON COLUMN th_user_avatar_managed.owner_user_id IS 'The user who is managing/updating the avatar';
COMMENT ON COLUMN th_user_avatar_managed.avatar_key IS 'The file key/path of the avatar image in storage (S3/OSS)';
COMMENT ON COLUMN th_user_avatar_managed.created_at IS 'Timestamp when the record was first created';
COMMENT ON COLUMN th_user_avatar_managed.updated_at IS 'Timestamp when the avatar was last updated';

CREATE TABLE IF NOT EXISTS th_share_permission_type (
    permission_id SERIAL PRIMARY KEY,
    permission_key VARCHAR(50) UNIQUE NOT NULL,
    permission_name VARCHAR(100) NOT NULL,
    permission_description TEXT,
    category VARCHAR(50),
    display_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_share_perm_key ON th_share_permission_type(permission_key);
CREATE INDEX IF NOT EXISTS idx_share_perm_active ON th_share_permission_type(is_active);

INSERT INTO th_share_permission_type (permission_key, permission_name, permission_description, category, display_order)
VALUES
    ('all', 'All Data', 'Access to all data types', 'general', 1),
    ('device', 'Device Data', 'Access to device and sensor data', 'specific', 2),
    ('ehr', 'Health Records', 'Access to electronic health records', 'specific', 3),
    ('chat', 'Chat History', 'Access to chat and conversation history', 'specific', 4),
    ('uploadfile', 'upload file', 'Access to upload file', 'specific', 5)
ON CONFLICT (permission_key) DO NOTHING;


CREATE TABLE IF NOT EXISTS health_user_provider
(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL,
    user_id character varying(128) COLLATE pg_catalog."default" NOT NULL DEFAULT 0,
    provider character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    username character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    password character varying COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying,
    llm_access integer DEFAULT 1,
    access_token TEXT DEFAULT NULL,
    access_token_secret TEXT DEFAULT NULL,
    refresh_token TEXT DEFAULT NULL,
    expires_at TIMESTAMP,
    reconnect integer NOT NULL DEFAULT 0,
    connect_info JSONB DEFAULT NULL
);


CREATE INDEX IF NOT EXISTS idx_health_user_provider_user_id ON health_user_provider(user_id);

COMMENT ON COLUMN health_user_provider.reconnect IS 'Reconnection flag: 0=normal, 1=needs reconnect. Pull tasks only process users with reconnect=0';
COMMENT ON COLUMN health_user_provider.connect_info IS 'Additional connection information stored as JSON (e.g., patient_id, device_info, etc.)';


CREATE TABLE IF NOT EXISTS health_user_profile_by_system
(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_time timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted boolean NOT NULL,
    user_id character varying COLLATE pg_catalog."default" NOT NULL,
    version integer NOT NULL,
    name character varying COLLATE pg_catalog."default" NOT NULL,
    last_execute_doc_id integer NOT NULL,
    common_part character varying COLLATE pg_catalog."default" NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_id
    ON health_user_profile_by_system USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_user_id
    ON health_user_profile_by_system USING btree
    (user_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_version
    ON health_user_profile_by_system USING btree
    (version ASC NULLS LAST)
    TABLESPACE pg_default;


CREATE TABLE IF NOT EXISTS th_task_flow
(
    id integer generated always as identity not null,
    task_id character varying(200) COLLATE pg_catalog."default" NOT NULL,
    user_id character varying COLLATE pg_catalog."default" NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone,
    records integer DEFAULT 0,
    indicators integer DEFAULT 0,
    deleted integer NOT NULL DEFAULT 0,
    create_time timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    type character varying(64) COLLATE pg_catalog."default" NOT NULL,
    sort_time timestamp without time zone,
    CONSTRAINT th_task_flow_pkey PRIMARY KEY (id),
    CONSTRAINT uq_task_id UNIQUE (task_id)
);

CREATE INDEX IF NOT EXISTS idx_th_task_flow_user_id ON th_task_flow(user_id);




CREATE TABLE IF NOT EXISTS health_data_garmin (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL DEFAULT false,
    msg_id character varying(200) UNIQUE NOT NULL,
    raw_data jsonb NOT NULL,
    theta_user_id character varying(100),
    external_user_id character varying(100)
);

CREATE INDEX IF NOT EXISTS idx_health_data_garmin_theta_user_id ON health_data_garmin(theta_user_id);



CREATE TABLE IF NOT EXISTS health_data_whoop (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del boolean NOT NULL DEFAULT false,
    msg_id character varying(200),
    raw_data jsonb NOT NULL,
    theta_user_id character varying(100),
    external_user_id character varying(100)
);

CREATE INDEX IF NOT EXISTS idx_health_data_whoop_theta_user_id ON health_data_whoop(theta_user_id);




CREATE OR REPLACE FUNCTION encrypt_info(plain_password TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := COALESCE(current_setting('app.encryption_key', true), 'default_key_2024_holywell_secure');
BEGIN
    IF plain_password IS NULL OR plain_password = '' THEN
        RETURN NULL;
    END IF;
    
    RETURN encode(encrypt(plain_password::bytea, encryption_key::bytea, 'aes'), 'base64');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Password encryption failed: %', SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION decrypt_info(encrypted_password TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := COALESCE(current_setting('app.encryption_key', true), 'default_key_2024_holywell_secure');
BEGIN
    IF encrypted_password IS NULL OR encrypted_password = '' THEN
        RETURN NULL;
    END IF;
    
    RETURN convert_from(decrypt(decode(encrypted_password, 'base64'), encryption_key::bytea, 'aes'), 'UTF8');
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Password decryption failed: %', SQLERRM;
        RETURN encrypted_password;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION has_password(encrypted_password TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN encrypted_password IS NOT NULL AND encrypted_password != '';
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


CREATE OR REPLACE FUNCTION prevent_table_drop()
 RETURNS event_trigger
AS $$
 
BEGIN

END;
 
$$ LANGUAGE plpgsql ;

CREATE OR REPLACE FUNCTION encrypt_content(plain_content TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := COALESCE(current_setting('app.encryption_key', true), 'default_key_2024_holywell_secure');
BEGIN
    IF plain_content IS NULL OR plain_content = '' THEN
        RETURN NULL;
    END IF;

    RETURN CONCAT('gAAAA',encode(encrypt(convert_to(plain_content,'utf8'), encryption_key::bytea, 'aes'), 'base64'));
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Content encryption failed: %', SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION decrypt_content(encrypted_content TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := COALESCE(current_setting('app.encryption_key', true), 'default_key_2024_holywell_secure');
    decoded_data BYTEA;
BEGIN
    IF encrypted_content IS NULL OR encrypted_content = '' THEN
        RETURN NULL;
    END IF;

    IF encrypted_content LIKE 'gAAAA%' THEN
        decoded_data := decode(SUBSTRING(encrypted_content FROM 6), 'base64');
        RETURN convert_from(decrypt(decoded_data, encryption_key::bytea, 'aes'), 'UTF8');
    ELSE
        RETURN encrypted_content;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Content decryption failed: %', SQLERRM;
        RETURN encrypted_content;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

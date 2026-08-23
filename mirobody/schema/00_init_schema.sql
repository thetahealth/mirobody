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


-- `th_share_relationship`, `th_share_user_config` and `th_share_permission_type`
-- used to be defined here. Two tables replace all three:
-- `care_circles` / `care_circle_members` (`a2_care_circles.sql`), with
-- `a3_migrate_share_relationship.sql` carrying existing rows across and dropping
-- the old ones. The old trio modelled a directed grant defaulting to
-- `{"all": 1}` — read everything, on by default, decided by the other party —
-- which is the opposite of what this product promises, and its advertised
-- vocabulary (`th_share_permission_type`, which listed `ehr`) did not even match
-- the names the checker honoured (`health`).

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

ALTER TABLE health_user_profile_by_system
ADD COLUMN IF NOT EXISTS common_part_encrypted text NULL;



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




-- The four crypto helpers below take their key from the `app.encryption_key`
-- session setting, which `utils/config/postgresql.py` puts on every connection
-- it opens. They used to fall back to a hardcoded literal when the setting was
-- absent. In a public repository that fallback IS the key: anyone reading this
-- file could decrypt anything written under it. It is gone, and a missing
-- setting now raises instead.
--
-- The raise has to happen OUTSIDE the crypto `EXCEPTION` handler. Each function
-- ends in `WHEN OTHERS THEN ... RETURN NULL` (or returns its input), so a raise
-- in the same block would be swallowed and `encrypt_*` would silently store
-- NULL — losing the write rather than reporting it. Hence the key check sits in
-- the outer block and the original handling stays in a nested one.

CREATE OR REPLACE FUNCTION encrypt_info(plain_password TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := current_setting('app.encryption_key', true);
BEGIN
    IF encryption_key IS NULL OR encryption_key = '' THEN
        RAISE EXCEPTION 'encrypt_info: app.encryption_key is not set on this connection'
            USING HINT = 'Open the connection through utils/config/postgresql.py, '
                         'or SET app.encryption_key before calling this function.';
    END IF;

    IF plain_password IS NULL OR plain_password = '' THEN
        RETURN NULL;
    END IF;

    BEGIN
        RETURN encode(encrypt(plain_password::bytea, encryption_key::bytea, 'aes'), 'base64');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Password encryption failed: %', SQLERRM;
            RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION decrypt_info(encrypted_password TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := current_setting('app.encryption_key', true);
BEGIN
    IF encryption_key IS NULL OR encryption_key = '' THEN
        RAISE EXCEPTION 'decrypt_info: app.encryption_key is not set on this connection'
            USING HINT = 'Open the connection through utils/config/postgresql.py, '
                         'or SET app.encryption_key before calling this function.';
    END IF;

    IF encrypted_password IS NULL OR encrypted_password = '' THEN
        RETURN NULL;
    END IF;

    BEGIN
        RETURN convert_from(decrypt(decode(encrypted_password, 'base64'), encryption_key::bytea, 'aes'), 'UTF8');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Password decryption failed: %', SQLERRM;
            RETURN encrypted_password;
    END;
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
    encryption_key TEXT := current_setting('app.encryption_key', true);
BEGIN
    IF encryption_key IS NULL OR encryption_key = '' THEN
        RAISE EXCEPTION 'encrypt_content: app.encryption_key is not set on this connection'
            USING HINT = 'Open the connection through utils/config/postgresql.py, '
                         'or SET app.encryption_key before calling this function.';
    END IF;

    IF plain_content IS NULL OR plain_content = '' THEN
        RETURN NULL;
    END IF;

    BEGIN
        RETURN CONCAT('gAAAA',encode(encrypt(convert_to(plain_content,'utf8'), encryption_key::bytea, 'aes'), 'base64'));
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Content encryption failed: %', SQLERRM;
            RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION decrypt_content(encrypted_content TEXT)
RETURNS TEXT AS $$
DECLARE
    encryption_key TEXT := current_setting('app.encryption_key', true);
    decoded_data BYTEA;
BEGIN
    IF encryption_key IS NULL OR encryption_key = '' THEN
        RAISE EXCEPTION 'decrypt_content: app.encryption_key is not set on this connection'
            USING HINT = 'Open the connection through utils/config/postgresql.py, '
                         'or SET app.encryption_key before calling this function.';
    END IF;

    IF encrypted_content IS NULL OR encrypted_content = '' THEN
        RETURN NULL;
    END IF;

    BEGIN
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
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

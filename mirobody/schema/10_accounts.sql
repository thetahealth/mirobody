-- 10_accounts.sql: who a person is, how they sign in, who may see their record.
--
--   health_app_user                 the account
--   webauthn_credentials            passkeys for AAL2
--   health_user_profile_by_system   the generated health profile, versioned
--   care_circles / care_circle_members   the sharing model and the one
--                                   authorization throat (user/care_circle.py)
--
-- Each CREATE carries the full current column list. The ALTERs that follow
-- add the columns that arrived after a table was first created; on a database
-- built from this file they are no-ops.

-- The account. `is_del` is the soft delete; the two partial unique indexes
-- let a deleted account's email or Apple id be registered again.
CREATE TABLE IF NOT EXISTS health_app_user (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del        boolean NOT NULL,
    email         character varying NOT NULL,
    name          character varying NOT NULL DEFAULT ''::character varying,
    consultant_id integer NOT NULL DEFAULT 0,
    lang          character varying NOT NULL DEFAULT 'en'::character varying,
    apple_sub     VARCHAR(255),
    response_lang character varying(64) DEFAULT NULL::character varying,
    gender        integer,                              -- 0 unknown, 1 male, 2 female
    birth         character varying,
    blood         character varying,
    tz            character varying NOT NULL DEFAULT ''::character varying,
    coins         integer DEFAULT 0,
    ethnicity     VARCHAR(128),
    mfa_enabled   BOOLEAN NOT NULL DEFAULT FALSE,       -- per-user WebAuthn AAL2 switch
    password_hash text                                  -- bcrypt via pgcrypto crypt(); NULL = no password
);
ALTER TABLE health_app_user ADD COLUMN IF NOT EXISTS ethnicity VARCHAR(128);
ALTER TABLE health_app_user ADD COLUMN IF NOT EXISTS mfa_enabled BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE health_app_user ADD COLUMN IF NOT EXISTS password_hash text;

CREATE        INDEX IF NOT EXISTS idx_health_app_user_apple_sub ON health_app_user USING btree (apple_sub);
CREATE UNIQUE INDEX IF NOT EXISTS idx_uni_health_app_user_email_active ON health_app_user USING btree (email) WHERE (is_del = false);
-- The column used to carry its own UNIQUE as well, which outranked the partial
-- index above: an address stayed taken after its account was deleted, so the
-- person could never register again. Dropped; the partial index is the rule.
ALTER TABLE health_app_user DROP CONSTRAINT IF EXISTS health_app_user_email_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_uni_health_app_user_apple_sub_active ON health_app_user USING btree (apple_sub) WHERE (is_del = false);

COMMENT ON COLUMN health_app_user.gender IS 'Gender: 0-Unknown 1-Male 2-Female';
COMMENT ON COLUMN health_app_user.password_hash IS
    'bcrypt via pgcrypto crypt()/gen_salt(bf,12); NULL = no password set';

-- Passkeys. One credential id across all users; a deleted one keeps its row.
CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       INTEGER NOT NULL,
    credential_id BYTEA NOT NULL,
    public_key    BYTEA NOT NULL,
    sign_count    INTEGER NOT NULL DEFAULT 0,
    transports    TEXT[],
    aaguid        VARCHAR(36),
    device_name   VARCHAR(255),
    is_del        BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at    TIMESTAMP,
    deleted_by    INTEGER,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at  TIMESTAMP,
    UNIQUE(credential_id)
);
CREATE INDEX IF NOT EXISTS idx_webauthn_credentials_user_id ON webauthn_credentials(user_id);

-- The generated health profile, one row per version. The markdown lives in
-- `common_part_encrypted` (encrypt_content); `common_part` is the plaintext
-- column it replaced, kept nullable so old rows stay readable.
CREATE TABLE IF NOT EXISTS health_user_profile_by_system (
    id                    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_time           timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time      timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted            boolean NOT NULL,
    user_id               character varying COLLATE pg_catalog."default" NOT NULL,
    version               integer NOT NULL,
    name                  character varying COLLATE pg_catalog."default" NOT NULL,
    last_execute_doc_id   integer NOT NULL,
    common_part           character varying COLLATE pg_catalog."default",
    common_part_encrypted text,
    scenario_zh           VARCHAR(512) DEFAULT NULL,
    scenario_en           VARCHAR(512) DEFAULT NULL,
    scenario_image_url    TEXT DEFAULT NULL,
    action_type           VARCHAR(32) DEFAULT NULL             -- add, NULL/delete
);
ALTER TABLE health_user_profile_by_system ADD COLUMN IF NOT EXISTS common_part_encrypted text NULL;
ALTER TABLE health_user_profile_by_system ADD COLUMN IF NOT EXISTS scenario_zh VARCHAR(512) DEFAULT NULL;
ALTER TABLE health_user_profile_by_system ADD COLUMN IF NOT EXISTS scenario_en VARCHAR(512) DEFAULT NULL;
ALTER TABLE health_user_profile_by_system ADD COLUMN IF NOT EXISTS scenario_image_url TEXT DEFAULT NULL;
ALTER TABLE health_user_profile_by_system ADD COLUMN IF NOT EXISTS action_type VARCHAR(32) DEFAULT NULL;
ALTER TABLE health_user_profile_by_system ALTER COLUMN common_part DROP NOT NULL;
-- Rows written before the encrypted column existed.
UPDATE health_user_profile_by_system
   SET common_part_encrypted = encrypt_content(common_part)
 WHERE common_part_encrypted IS NULL AND common_part IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_id
    ON health_user_profile_by_system USING btree (id ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_user_id
    ON health_user_profile_by_system USING btree (user_id COLLATE pg_catalog."default" ASC NULLS LAST) TABLESPACE pg_default;
CREATE INDEX IF NOT EXISTS ix_theta_ai_health_user_profile_by_system_version
    ON health_user_profile_by_system USING btree (version ASC NULLS LAST) TABLESPACE pg_default;

COMMENT ON COLUMN health_user_profile_by_system.action_type IS 'add, NULL/delete';
COMMENT ON COLUMN health_user_profile_by_system.common_part_encrypted IS 'Encrypted profile markdown (encrypt_content/decrypt_content). Replaces legacy plaintext common_part.';

-- The care circle. Sharing is a switch on the sharer's OWN member row
-- (`health_access`), off by default, and nobody else's action can raise it.
-- Integer enums follow the sibling stack that runs this model in production.
CREATE TABLE IF NOT EXISTS care_circles (
    id            INTEGER      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner_user_id INTEGER      NOT NULL REFERENCES health_app_user(id),
    name          VARCHAR(256) NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ,
    deleted_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_care_circles_owner ON care_circles (owner_user_id);

CREATE TABLE IF NOT EXISTS care_circle_members (
    id             INTEGER      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    care_circle_id INTEGER      NOT NULL REFERENCES care_circles(id),
    user_id        INTEGER      NOT NULL REFERENCES health_app_user(id),
    role           SMALLINT     NOT NULL DEFAULT 0 CHECK (role BETWEEN 0 AND 2),          -- 0 member, 1 maintainer, 2 owner
    status         SMALLINT     NOT NULL CHECK (status BETWEEN 1 AND 3),                   -- 1 pending, 2 accepted, 3 declined; no default
    health_access  SMALLINT     NOT NULL DEFAULT 0 CHECK (health_access BETWEEN 0 AND 2),  -- of MY record: 0 none, 1 read, 2 read-write
    nickname       VARCHAR(256),                                                           -- the label this member carries in the circle
    avatar_key     VARCHAR(500),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ,
    deleted_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_care_circle_members_circle ON care_circle_members (care_circle_id);
CREATE INDEX IF NOT EXISTS idx_care_circle_members_user   ON care_circle_members (user_id);
-- One live row per (circle, user); partial, so a removed member can be re-invited.
CREATE UNIQUE INDEX IF NOT EXISTS uq_care_circle_members_active
    ON care_circle_members (care_circle_id, user_id) WHERE deleted_at IS NULL;

COMMENT ON TABLE care_circles IS 'A circle of people, owned by one of them';
COMMENT ON TABLE care_circle_members IS
    'Membership + each member''s own health-sharing switch. THE authorization '
    'throat: user/care_circle.py::accepted_membership reads this to decide '
    'whether one person may act on another''s record.';
COMMENT ON COLUMN care_circle_members.health_access IS
    'What THIS member lets the circle see of THEIR OWN record: 0 none, 1 read, '
    '2 read-write. Not a grant made about them by someone else.';

-- One-way migration from the tables the circle replaced
-- (th_share_relationship, th_share_user_config), then drop them. A no-op when
-- they are gone, which is every fresh database and every second run.
--
-- An old row said "owner O lets member M read O's record". It becomes a circle
-- owned by O, O's own member row carrying the level the old grant expressed
-- (`health` wins over `all`, clamped to 0..2), and M's member row with
-- health_access 0: the old row said nothing about what M shares. Deleted
-- accounts and unrecognised statuses are skipped.
DO $$
DECLARE
    migrated_circles INTEGER := 0;
    migrated_members INTEGER := 0;
BEGIN
    IF to_regclass('th_share_relationship') IS NULL THEN
        RETURN;
    END IF;

    WITH owners AS (
        SELECT DISTINCT r.owner_user_id::integer AS owner_id
          FROM th_share_relationship r
          JOIN health_app_user u
            ON u.id = r.owner_user_id::integer AND u.is_del = false
         WHERE r.status IN ('authorized', 'pending')
           AND r.owner_user_id ~ '^[0-9]+$'
           AND r.member_user_id ~ '^[0-9]+$'
    ), created AS (
        INSERT INTO care_circles (owner_user_id, name)
        SELECT o.owner_id, ''
          FROM owners o
         WHERE NOT EXISTS (
                   SELECT 1 FROM care_circles c
                    WHERE c.owner_user_id = o.owner_id AND c.deleted_at IS NULL)
        RETURNING id
    )
    SELECT count(*) INTO migrated_circles FROM created;

    INSERT INTO care_circle_members (care_circle_id, user_id, role, status, health_access)
    SELECT c.id,
           c.owner_user_id,
           2,   -- owner
           2,   -- accepted
           LEAST(2, GREATEST(0, COALESCE(
               MAX((r.permissions ->> 'health')::integer),
               MAX((r.permissions ->> 'all')::integer),
               0)))
      FROM care_circles c
      JOIN th_share_relationship r
        ON r.owner_user_id ~ '^[0-9]+$' AND r.owner_user_id::integer = c.owner_user_id
     WHERE r.status = 'authorized'
       AND c.deleted_at IS NULL
       AND NOT EXISTS (
               SELECT 1 FROM care_circle_members m
                WHERE m.care_circle_id = c.id AND m.user_id = c.owner_user_id
                  AND m.deleted_at IS NULL)
     GROUP BY c.id, c.owner_user_id;

    INSERT INTO care_circle_members (care_circle_id, user_id, role, status, health_access)
    SELECT c.id,
           r.member_user_id::integer,
           0,                                                    -- member
           CASE r.status WHEN 'authorized' THEN 2 ELSE 1 END,    -- accepted / pending
           0
      FROM th_share_relationship r
      JOIN care_circles c
        ON c.owner_user_id = r.owner_user_id::integer AND c.deleted_at IS NULL
      JOIN health_app_user u
        ON u.id = r.member_user_id::integer AND u.is_del = false
     WHERE r.status IN ('authorized', 'pending')
       AND r.owner_user_id ~ '^[0-9]+$'
       AND r.member_user_id ~ '^[0-9]+$'
       AND r.member_user_id::integer <> c.owner_user_id
       AND NOT EXISTS (
               SELECT 1 FROM care_circle_members m
                WHERE m.care_circle_id = c.id
                  AND m.user_id = r.member_user_id::integer
                  AND m.deleted_at IS NULL);

    -- Labels: the old table held one per viewer, the new model one per member,
    -- so the owner's label wins.
    IF to_regclass('th_share_user_config') IS NOT NULL THEN
        UPDATE care_circle_members m
           SET nickname   = COALESCE(m.nickname, cfg.nickname),
               avatar_key = COALESCE(m.avatar_key, cfg.avatar_key),
               updated_at = now()
          FROM th_share_user_config cfg
          JOIN care_circles c ON c.owner_user_id = cfg.setter_user_id::integer
         WHERE cfg.setter_user_id ~ '^[0-9]+$'
           AND cfg.target_user_id ~ '^[0-9]+$'
           AND m.care_circle_id = c.id
           AND m.user_id = cfg.target_user_id::integer
           AND m.deleted_at IS NULL;
    END IF;

    SELECT count(*) INTO migrated_members FROM care_circle_members;
    RAISE NOTICE 'care-circle migration: % circle(s) created, % member row(s) now present',
                 migrated_circles, migrated_members;

    DROP TABLE th_share_relationship;
    IF to_regclass('th_share_user_config') IS NOT NULL THEN
        DROP TABLE th_share_user_config;
    END IF;
END $$;

-- 62_personal_mcp.sql: personal MCP links, one row each.
--
-- `/mcp/<secret>` is a whole credential in a URL: it reads one person's record
-- with no other sign-in, and it gets pasted into client configs, screenshots
-- and shell history. So the secret itself is never stored, only its SHA-256;
-- and a row names who made the link (`creator_id`) as well as whose record it
-- reads (`subject_id`), because every use re-checks that the creator may
-- still read the subject (mirobody/user/personal_mcp.py). Kept out of
-- th_ephemeral on purpose: a link is listed, attributed and revoked, and a
-- key-value row with a hashed key can be none of those.
CREATE TABLE IF NOT EXISTS th_personal_mcp_url (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    secret_hash   BYTEA NOT NULL UNIQUE,
    creator_id    VARCHAR(255) NOT NULL,
    subject_id    VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    revoked_at    TIMESTAMPTZ,
    revoked_by    VARCHAR(255),
    last_used_at  TIMESTAMPTZ
);

-- One live link per creator and subject: a person's own link and one a
-- family member made for them are two rows, and replacing one leaves the
-- other alone.
CREATE UNIQUE INDEX IF NOT EXISTS uq_th_personal_mcp_url_live
    ON th_personal_mcp_url (creator_id, subject_id) WHERE revoked_at IS NULL;
-- "Who holds a link to my record."
CREATE INDEX IF NOT EXISTS idx_th_personal_mcp_url_subject
    ON th_personal_mcp_url (subject_id) WHERE revoked_at IS NULL;

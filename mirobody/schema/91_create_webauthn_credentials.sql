-- WebAuthn credentials for AAL2 multi-factor authentication
CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id INTEGER NOT NULL,
    credential_id BYTEA NOT NULL,
    public_key BYTEA NOT NULL,
    sign_count INTEGER NOT NULL DEFAULT 0,
    transports TEXT[],
    aaguid VARCHAR(36),
    device_name VARCHAR(255),
    is_del BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    deleted_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    UNIQUE(credential_id)
);

CREATE INDEX IF NOT EXISTS idx_webauthn_credentials_user_id
    ON webauthn_credentials(user_id);

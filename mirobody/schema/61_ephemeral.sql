-- Hashing keys keeps one-time codes and URL secrets out of the table and logs.
-- Values are encrypted by the application before they are written here.
CREATE TABLE IF NOT EXISTS th_ephemeral (
    key_hash BYTEA PRIMARY KEY,
    value_ciphertext TEXT,
    counter BIGINT,
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_th_ephemeral_expiry
ON th_ephemeral (expires_at) WHERE expires_at IS NOT NULL;

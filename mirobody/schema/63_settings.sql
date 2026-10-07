-- 63_settings.sql: what the first-run page saved (1.5.4).
--
-- A deployment set up in the browser instead of in `.env` keeps its choice
-- here: a vendor key, or the addresses of the local model server. Values are
-- encrypted like every other secret (encrypt_content). A name the process
-- environment already sets is never overridden by a row (utils/config/settings.py).

CREATE TABLE IF NOT EXISTS th_setting (
    name        text PRIMARY KEY,
    value       text NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

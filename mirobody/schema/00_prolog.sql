-- 00_prolog.sql: extensions and the functions every other file relies on.
--
-- One file per functional domain, replayed in filename order at every boot
-- (README.md). Every statement is re-runnable: no ledger records what ran.
-- Nothing here names a schema; the connection's search_path decides.

CREATE EXTENSION IF NOT EXISTS vector   WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm  WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

-- Field-level encryption. The key is the session setting `app.encryption_key`,
-- which utils/config/postgresql.py puts on every connection it opens. There is
-- no fallback key: in a public repository a default key IS the key. The check
-- sits outside the crypto block on purpose, because that block ends in
-- `WHEN OTHERS THEN RETURN NULL` and would swallow the raise.
--
--   encrypt_content / decrypt_content   free-text health data ('gAAAA' prefix)
--   encrypt_info / decrypt_info         provider credentials
--   has_password                        a null-safe presence test

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

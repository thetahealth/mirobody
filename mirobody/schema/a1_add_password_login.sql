-- Password login, for the deployment that cannot send mail.
--
-- The email-code path (user/email.py, Mandrill or SMTP) is real and tested, but
-- it needs a mail provider. Someone who clones this to try it out has neither,
-- so the only accounts that could ever sign in were the ones hardcoded in
-- EMAIL_PREDEFINE_CODES. That is a demo, not a sign-up.
--
-- Hashing is bcrypt via pgcrypto's `crypt()` / `gen_salt('bf', 12)`. pgcrypto
-- is already a hard requirement of this schema (00_init_schema creates it), so
-- this adds no Python dependency and no hand-rolled crypto: the hash never
-- leaves the database and the comparison is `stored = crypt(candidate, stored)`,
-- which is constant-time inside pgcrypto.
--
-- NULL means "this account has no password" — the email-code path still works
-- for it, and password login refuses it. That is what keeps this additive:
-- existing accounts are unaffected until someone sets one.
--
-- Named `a1_` rather than `100_`: the bootstrap replays files in filename sort
-- order, and "100" sorts BEFORE "42" as a string, which would run this against
-- a table that does not exist yet.
ALTER TABLE health_app_user ADD COLUMN IF NOT EXISTS password_hash text;

COMMENT ON COLUMN health_app_user.password_hash IS
    'bcrypt via pgcrypto crypt()/gen_salt(bf,12); NULL = no password set';

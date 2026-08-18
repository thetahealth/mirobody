-- Add MFA enabled flag for per-user WebAuthn AAL2 control
ALTER TABLE health_app_user ADD COLUMN IF NOT EXISTS mfa_enabled BOOLEAN NOT NULL DEFAULT FALSE;

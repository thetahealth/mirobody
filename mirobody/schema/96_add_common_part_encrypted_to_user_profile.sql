-- health_user_profile_by_system migrated from plaintext `common_part` to the
-- encrypted `common_part_encrypted` column. The application now only writes
-- common_part_encrypted (via encrypt_content()) and reads it back via
-- decrypt_content(). The encrypted column was missing from the schema, which
-- caused: column "common_part_encrypted" does not exist.

-- 1. The column itself was folded back into the 00_init_schema baseline, so it
--    is not re-added here; what follows is the part that baseline cannot express.

-- 2. The legacy plaintext column is no longer written by the application, so it
--    must not be NOT NULL or inserts that only set common_part_encrypted fail.
ALTER TABLE health_user_profile_by_system
ALTER COLUMN common_part DROP NOT NULL;

-- 3. Backfill the encrypted column from any existing plaintext rows.
UPDATE health_user_profile_by_system
SET common_part_encrypted = encrypt_content(common_part)
WHERE common_part_encrypted IS NULL AND common_part IS NOT NULL;

COMMENT ON COLUMN health_user_profile_by_system.common_part_encrypted IS 'Encrypted profile markdown (encrypt_content/decrypt_content). Replaces legacy plaintext common_part.';

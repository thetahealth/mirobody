-- ============================================================================
-- One-way migration: `th_share_relationship` -> care circles, then drop it.
--
-- Guarded on `to_regclass`, so this is a no-op the second time and on any fresh
-- database (where `00_init_schema.sql` no longer creates the old table at all).
--
-- The translation, and why it is exact rather than approximate. An old row said
-- "owner O lets member M read O's record", with the level in a jsonb bag. The
-- new model says "each member decides what the circle sees of their own record".
-- So one old row becomes:
--
--     a circle owned by O
--     O's own member row:  role=owner,  status=accepted, health_access = the
--                          level the old bag granted (that IS "what O lets the
--                          circle see")
--     M's member row:      role=member, status from the old status,
--                          health_access = 0 (M granted nothing — the old row
--                          carried no such statement, and inventing one would
--                          share M's record without M ever agreeing)
--
-- `health_access` for O is read out of the bag with the same precedence the old
-- checker used: an exact `health` entry wins, else `all`, else 0 — and clamped
-- to 0..2 because the column has a CHECK and the bag had nothing.
--
-- Soft-deleted accounts (`health_app_user.is_del`) are skipped on both sides: a
-- deleted account must not appear in a circle, and the old table had no foreign
-- key to notice one. The live dev database had exactly such a row.
--
-- Only `status = 'authorized'` rows become accepted; 'pending' becomes pending
-- and anything else (including the never-used 'revoked') is skipped, because an
-- unrecognised status in a column with no CHECK cannot be assumed benign.
-- ============================================================================

DO $$
DECLARE
    migrated_circles INTEGER := 0;
    migrated_members INTEGER := 0;
BEGIN
    IF to_regclass('th_share_relationship') IS NULL THEN
        RETURN;
    END IF;

    -- One circle per owner that has at least one usable relationship.
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

    -- The owner joins their own circle as Owner/Accepted, carrying the level the
    -- old grant expressed. Without this row the owner is not a member of their
    -- own circle, and every check reads membership rather than ownership.
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

    -- Each member joins with health_access 0: the old row said nothing about
    -- what THEY share, and a migration must not answer a question the data
    -- never asked.
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

    -- Nicknames and avatars, from the table that held one per viewer. The new
    -- model has one label per member, so the owner's label wins: it is the one
    -- the circle listing showed.
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

-- The advertised-vocabulary table, read only by `/invitation/permissions/list`.
-- Nothing needs it once the grant is a single named column: there is one switch
-- with three values, not a catalogue of scope names. It also disagreed with the
-- checker it was supposed to describe — it listed `ehr`, the checker honoured
-- `health` — so an integrator building against it asked for a scope that always
-- resolved to 0.
DROP TABLE IF EXISTS th_share_permission_type;

-- ============================================================================
-- The care circle: `care_circles` + `care_circle_members`.
--
-- Replaces `th_share_relationship` and `th_share_user_config`, which modelled
-- something the product does not do. That pair stored a DIRECTED grant — owner
-- O lets member M read O's record — in a row whose `permissions` was a
-- schemaless jsonb bag defaulting to `{"all": 1}`. Four separate things were
-- wrong with it, and the README's own diagram (docs/images/your-care-circle.svg)
-- describes the model this file builds instead:
--
--   1. "health stays off until you allow it" / "your switch — off by default".
--      The old default was `{"all": 1}`: read access to everything, on by
--      default, decided by the other party. `health_access` here defaults to 0
--      and lives on YOUR OWN member row — joining a circle is not being seen.
--   2. `status varchar DEFAULT 'pending'` with no CHECK, while the authorization
--      test was `status = 'authorized'` — a value the default is not, and one
--      typo away from silently denying everything. Now a SMALLINT with a CHECK.
--   3. Denormalized `owner_email` / `member_email` copies of
--      `health_app_user.email`, no foreign keys, and `VARCHAR(50)` user ids
--      against an `INTEGER` primary key. Now real integer FKs and no copies.
--   4. `UNIQUE (owner_user_id, member_user_id)` ignored `relationship_type`, so
--      the type column could never hold a second relationship anyway.
--
-- Shape and enum values follow the sibling Python stack's `care_circles` /
-- `care_circle_members`, which has run this model in production; the integers
-- are wire-stable there and are not renumbered here.
--
-- Idempotent, like every file in this directory: it is replayed on every boot.
-- ============================================================================

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

    -- 0 member, 1 maintainer, 2 owner. >= 1 may administer the circle.
    role           SMALLINT     NOT NULL DEFAULT 0 CHECK (role BETWEEN 0 AND 2),

    -- 1 pending, 2 accepted, 3 declined. No default: an invitation and an
    -- acceptance are different events and the writer must say which one it is.
    status         SMALLINT     NOT NULL CHECK (status BETWEEN 1 AND 3),

    -- How much of MY OWN health record I let this circle see: 0 none, 1 read,
    -- 2 read-write. On my row, about my data — which is why it can default to
    -- 0 and still be useful, and why nobody else's action can raise it.
    health_access  SMALLINT     NOT NULL DEFAULT 0 CHECK (health_access BETWEEN 0 AND 2),

    -- The label and picture this member carries inside the circle ("mom").
    -- Was a whole table (`th_share_user_config`, keyed by setter+target+context)
    -- that stored one nickname per viewer; the diagram shows one label per
    -- member, so it is a column on the member.
    nickname       VARCHAR(256),
    avatar_key     VARCHAR(500),

    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ,
    deleted_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_care_circle_members_circle
    ON care_circle_members (care_circle_id);
CREATE INDEX IF NOT EXISTS idx_care_circle_members_user
    ON care_circle_members (user_id);

-- One live row per (circle, user): a repeat invitation refreshes the existing
-- row rather than growing the circle. Partial, so a removed member can be
-- re-invited.
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

"""The care circle: who may act on whose record, and how much of it they see.

`resolve_subject` is the authorization throat. It answers one question — *may
this operator act on this subject's record, and to what depth* — and it is the
check standing between one person's health record and another's.

**Why this replaced `th_share_relationship`.** That table stored a directed
grant, "owner O lets member M read O's record", with the level in a schemaless
`permissions` jsonb defaulting to `{"all": 1}`. The README's own diagram
(`docs/images/your-care-circle.svg`) promises the opposite arrangement:

    "health stays off until you allow it" · "your switch — off by default"
    "mutual — each member controls their own"

A default of `{"all": 1}` is read-everything, on by default, decided by the
other party. So the shipped code contradicted the picture on the one point that
matters. `care_circle_members.health_access` is that switch: it lives on **your
own** member row, it is about **your own** record, it defaults to 0, and nobody
else's action can raise it. Joining a circle is not being seen.

**Three things this module does differently from what it replaced**, each of
which was a real defect rather than a matter of taste:

1. **Denial raises.** `get_query_user_id` returned `{"success": False, ...}`, so
   every caller had to remember to check a dict key; forgetting one meant a
   silent grant. `CareCircleDenied` cannot be mistaken for success, and
   `server/server.py` maps it to a 403 once for every route.

2. **The parameters mean what they are named.** `get_query_user_id`'s first
   parameter was named `user_id` and documented as "owner_user_id, namely the
   data owner", but every one of its eleven call sites passed the *target* there
   and the *caller* second — because the SQL required `owner_user_id = <arg 1>`.
   The function's return value (`query_user_id`) was then ignored by those
   callers, who used their own variable. Names that invert their meaning are how
   an authorization check gets called backwards.

3. **The grant is trimmed to the request.** `require_write=False` against a
   read-write membership still returns read. A caller that only asked to read
   must not silently receive the ability to write just because the underlying
   relationship allows it.

This module must stay framework-free: `mirobody/user/` is engine layer, and
`utils/__init__` imports it transitively. No FastAPI, no HTTP.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..utils.db import execute_query
from .user import get_user

# ── wire values ──────────────────────────────────────────────────────────────
# Integers, and never renumbered: they are written into every row, and the
# sibling stack that has run this model in production uses the same numbers.

STATUS_PENDING = 1
STATUS_ACCEPTED = 2
STATUS_DECLINED = 3

ACCESS_NONE = 0
ACCESS_VIEW = 1
ACCESS_EDIT = 2

ROLE_MEMBER = 0
ROLE_MAINTAINER = 1   # >= this may administer the circle
ROLE_OWNER = 2

STATUS_NAMES = {STATUS_PENDING: "pending", STATUS_ACCEPTED: "accepted", STATUS_DECLINED: "declined"}
ACCESS_NAMES = {ACCESS_NONE: "none", ACCESS_VIEW: "view", ACCESS_EDIT: "edit"}
ROLE_NAMES = {ROLE_MEMBER: "member", ROLE_MAINTAINER: "maintainer", ROLE_OWNER: "owner"}


class CareCircleDenied(Exception):
    """The operator may not act on this subject's record.

    Raised, not returned. `server/server.py` registers the one handler that
    turns it into a 403, so no route has to remember — which is the whole point:
    the shape it replaces was a falsy field in a success-shaped dict.
    """


@dataclass(frozen=True, slots=True)
class Membership:
    """A live, mutually-accepted care-circle relationship.

    Only ever constructed by `accepted_membership`, whose contract is that it
    returns `None` for anything short of that — no shared circle, an invitation
    still pending on either side, a decline, a soft-deleted row. Pending and
    declined states are therefore invisible to the policy: they are filtered at
    the query, not judged here.

    So the only thing left to carry is the level the **subject** granted.
    """

    health_access: int

    @property
    def grants_view(self) -> bool:
        return self.health_access >= ACCESS_VIEW

    @property
    def grants_write(self) -> bool:
        return self.health_access >= ACCESS_EDIT


@dataclass(frozen=True, slots=True)
class Subject:
    """Whose record this request runs against, and what it may do to it.

    `access` is trimmed to what was asked for, not the relationship's ceiling.
    """

    operator_id: int
    subject_id: int
    access: int

    @property
    def is_self(self) -> bool:
        return self.operator_id == self.subject_id

    @property
    def may_write(self) -> bool:
        return self.access >= ACCESS_EDIT


# ── the throat ───────────────────────────────────────────────────────────────

async def accepted_membership(operator_id: int, subject_id: int) -> Membership | None:
    """What `subject_id` grants `operator_id`, or None if nothing does.

    `MAX`, not `LIMIT 1`. Two people can share more than one circle — mutual
    invitations produce exactly that — and `health_access` is per circle, so
    "what B grants A" is a set, not a value. `LIMIT 1` without `ORDER BY` picks
    an arbitrary row, which makes the answer drift with the query plan.
    Taking the maximum means "granted in any shared circle counts", which is the
    same rule `shared_with_me` lists by; if the two disagreed, the UI would show
    someone as sharing with you while your requests came back denied.

    `GROUP BY` is not optional. A bare aggregate always returns one row — NULL
    when there is no relationship — which would collapse "no relationship at
    all" into `Membership(health_access=0)` and throw away the `None`.
    """
    rows = await execute_query(
        """
        SELECT MAX(subject.health_access) AS health_access
          FROM care_circle_members me
          JOIN care_circle_members subject
            ON subject.care_circle_id = me.care_circle_id
         WHERE me.user_id = :operator AND subject.user_id = :subject
           AND me.status = :accepted AND subject.status = :accepted
           AND me.deleted_at IS NULL AND subject.deleted_at IS NULL
         GROUP BY subject.user_id
        """,
        {"operator": int(operator_id), "subject": int(subject_id), "accepted": STATUS_ACCEPTED},
        log_sql=False,
    )
    if not rows:
        return None
    return Membership(health_access=int(rows[0]["health_access"] or 0))


async def resolve_subject(
    operator_id: int | str,
    requested_subject_id: int | str | None = None,
    *,
    require_write: bool = False,
) -> Subject:
    """The record this request runs against. Raises `CareCircleDenied`.

    `requested_subject_id=None`, or the operator's own id, means "my own record"
    and always succeeds with full access — acting on your own data is not proxy
    access, and there is no narrower authorization to express.

    Ids arrive as strings from URL parameters and as ints from the JWT subject,
    so both are accepted and compared as ints. The check they replace compared
    them with `str()` on both sides after a bug where an int JWT subject and a
    string query parameter took the cross-user branch against themselves.
    """
    operator = int(operator_id)
    if requested_subject_id is None or str(requested_subject_id).strip() == "":
        return Subject(operator_id=operator, subject_id=operator, access=ACCESS_EDIT)

    subject = int(requested_subject_id)
    if subject == operator:
        return Subject(operator_id=operator, subject_id=operator, access=ACCESS_EDIT)

    membership = await accepted_membership(operator, subject)
    if membership is None:
        raise CareCircleDenied(
            "no accepted care-circle membership grants access to this record"
        )

    if require_write:
        if not membership.grants_write:
            raise CareCircleDenied("this member has not shared write access to their record")
        return Subject(operator_id=operator, subject_id=subject, access=ACCESS_EDIT)

    if not membership.grants_view:
        raise CareCircleDenied("this member has not shared their health record")
    return Subject(operator_id=operator, subject_id=subject, access=ACCESS_VIEW)


# ── circles ──────────────────────────────────────────────────────────────────

async def own_circle_id(owner_id: int | str) -> int | None:
    """The circle this user owns, if they have one. Read only — never creates."""
    rows = await execute_query(
        "SELECT id FROM care_circles WHERE owner_user_id = :o AND deleted_at IS NULL"
        " ORDER BY id LIMIT 1",
        {"o": int(owner_id)},
        log_sql=False,
    )
    return int(rows[0]["id"]) if rows else None


async def create_circle(owner_id: int | str, name: str = "") -> int:
    """Create a circle and put its owner in it as an accepted Owner member.

    The second half is not bookkeeping. Every administration check reads
    membership, not ownership, so a circle whose owner is not a member rejects
    its own owner.
    """
    owner = int(owner_id)
    rows = await execute_query(
        "INSERT INTO care_circles (owner_user_id, name) VALUES (:o, :n) RETURNING id",
        {"o": owner, "n": (name or "").strip()[:256]},
    )
    circle_id = int(rows[0]["id"])
    await execute_query(
        """
        INSERT INTO care_circle_members (care_circle_id, user_id, role, status, health_access)
        VALUES (:c, :u, :role, :status, :access)
        ON CONFLICT (care_circle_id, user_id) WHERE deleted_at IS NULL DO NOTHING
        """,
        {"c": circle_id, "u": owner, "role": ROLE_OWNER,
         "status": STATUS_ACCEPTED, "access": ACCESS_NONE},
    )
    return circle_id


async def ensure_own_circle(owner_id: int | str, name: str = "") -> int:
    """The user's circle, created on first need."""
    existing = await own_circle_id(owner_id)
    return existing if existing is not None else await create_circle(owner_id, name)


async def member_role(user_id: int | str, circle_id: int) -> int | None:
    """The caller's role in a circle, or None if they are not an accepted member."""
    rows = await execute_query(
        "SELECT role FROM care_circle_members"
        " WHERE user_id = :u AND care_circle_id = :c"
        "   AND status = :accepted AND deleted_at IS NULL",
        {"u": int(user_id), "c": int(circle_id), "accepted": STATUS_ACCEPTED},
        log_sql=False,
    )
    return int(rows[0]["role"]) if rows else None


async def require_maintainer(user_id: int | str, circle_id: int) -> int:
    """The caller's role, or `CareCircleDenied` if they may not administer."""
    role = await member_role(user_id, circle_id)
    if role is None or role < ROLE_MAINTAINER:
        raise CareCircleDenied("only a maintainer or the owner may administer this circle")
    return role


# ── invitations ──────────────────────────────────────────────────────────────

async def invite(circle_id: int, member_id: int | str, *, nickname: str | None = None) -> int:
    """Invite a user into a circle, or refresh a standing invitation.

    Upsert on the partial unique index rather than insert: a repeat invitation
    must not grow the circle or collide. Re-inviting someone who was removed
    works because that index only covers live rows.

    Deliberately does NOT touch `health_access` on an existing row. That is the
    member's own switch, and an inviter re-sending an invitation must not be
    able to reset it.
    """
    rows = await execute_query(
        """
        INSERT INTO care_circle_members
               (care_circle_id, user_id, role, status, health_access, nickname)
        VALUES (:c, :u, :role, :status, :access, :nick)
        ON CONFLICT (care_circle_id, user_id) WHERE deleted_at IS NULL DO UPDATE
           SET status     = CASE WHEN care_circle_members.status = :declined
                                 THEN :status ELSE care_circle_members.status END,
               nickname   = COALESCE(EXCLUDED.nickname, care_circle_members.nickname),
               updated_at = now()
        RETURNING id
        """,
        {"c": int(circle_id), "u": int(member_id), "role": ROLE_MEMBER,
         "status": STATUS_PENDING, "declined": STATUS_DECLINED,
         "access": ACCESS_NONE, "nick": (nickname or None)},
    )
    return int(rows[0]["id"])


async def respond_to_invitation(user_id: int | str, circle_id: int, *, accept: bool) -> bool:
    """Accept or decline an invitation addressed to `user_id`.

    Scoped to the authenticated user's own pending row, so this cannot be used
    to accept on someone else's behalf. Returns whether a row actually moved,
    which is False for a replay.
    """
    result = await execute_query(
        """
        UPDATE care_circle_members
           SET status = :new_status, updated_at = now()
         WHERE care_circle_id = :c AND user_id = :u
           AND status = :pending AND deleted_at IS NULL
        """,
        {"c": int(circle_id), "u": int(user_id),
         "new_status": STATUS_ACCEPTED if accept else STATUS_DECLINED,
         "pending": STATUS_PENDING},
    )
    return bool(result.get("record_count"))


# ── the switch ───────────────────────────────────────────────────────────────

async def set_health_access(user_id: int | str, circle_id: int, access: int) -> bool:
    """Set how much of their own record `user_id` shares with one circle.

    Only ever the caller's own row: there is no argument for whose access this
    sets, because the answer is always "the caller's". That is the invariant the
    old model could not express — its grant lived on a row the *other* party
    wrote, with a default of read-everything.
    """
    if access not in (ACCESS_NONE, ACCESS_VIEW, ACCESS_EDIT):
        raise ValueError(f"health_access must be 0, 1 or 2 — got {access!r}")
    result = await execute_query(
        """
        UPDATE care_circle_members
           SET health_access = :a, updated_at = now()
         WHERE user_id = :u AND care_circle_id = :c
           AND status = :accepted AND deleted_at IS NULL
        """,
        {"a": int(access), "u": int(user_id), "c": int(circle_id), "accepted": STATUS_ACCEPTED},
    )
    return bool(result.get("record_count"))


# ── administration ───────────────────────────────────────────────────────────

async def remove_member(member_row_id: int) -> bool:
    """Soft-delete one membership. Guarded, so a repeated delete converges."""
    result = await execute_query(
        "UPDATE care_circle_members SET deleted_at = now(), updated_at = now()"
        " WHERE id = :m AND deleted_at IS NULL",
        {"m": int(member_row_id)},
    )
    return bool(result.get("record_count"))


async def resolve_member_row(member_row_id: int) -> tuple[int, int] | None:
    """`(circle_id, user_id)` behind a member row id, or None if it is not live.

    The row id is the opaque handle every response carries. It is not the user
    id — they coincide only by accident on early rows.
    """
    rows = await execute_query(
        "SELECT care_circle_id, user_id FROM care_circle_members"
        " WHERE id = :m AND deleted_at IS NULL",
        {"m": int(member_row_id)},
        log_sql=False,
    )
    if not rows:
        return None
    return int(rows[0]["care_circle_id"]), int(rows[0]["user_id"])



async def set_member_label(
    member_row_id: int, *, nickname: str | None = None, avatar_key: str | None = None
) -> bool:
    """Set the label or picture a member carries inside the circle.

    One label per member, not one per viewer — which is what the diagram shows
    ("mom", "dad") and what `th_share_user_config` got wrong by keying on
    (setter, target, context) and then never using the context.
    """
    if nickname is None and avatar_key is None:
        return False
    result = await execute_query(
        """
        UPDATE care_circle_members
           SET nickname   = COALESCE(:nick, nickname),
               avatar_key = COALESCE(:avatar, avatar_key),
               updated_at = now()
         WHERE id = :m AND deleted_at IS NULL
        """,
        {"nick": nickname, "avatar": avatar_key, "m": int(member_row_id)},
    )
    return bool(result.get("record_count"))


# ── listings ─────────────────────────────────────────────────────────────────

async def shared_with_me(user_id: int | str) -> list[dict]:
    """Everyone whose record `user_id` may read: one row each, best grant wins.

    Two people can share several circles with different switches, so this
    aggregates the same way `accepted_membership` does. If the two used
    different rules, this list would advertise access that a request then
    refused.
    """
    return await execute_query(
        """
        SELECT subject.user_id                      AS user_id,
               MAX(subject.health_access)           AS health_access,
               MAX(subject.nickname)                AS nickname,
               MAX(subject.avatar_key)              AS avatar_key,
               MAX(u.name)                          AS name,
               MAX(u.email)                         AS email
          FROM care_circle_members me
          JOIN care_circle_members subject
            ON subject.care_circle_id = me.care_circle_id
           AND subject.user_id <> me.user_id
          JOIN health_app_user u
            ON u.id = subject.user_id AND u.is_del = false
         WHERE me.user_id = :u
           AND me.status = :accepted AND subject.status = :accepted
           AND me.deleted_at IS NULL AND subject.deleted_at IS NULL
           AND subject.health_access >= :view
         GROUP BY subject.user_id
         ORDER BY subject.user_id
        """,
        {"u": int(user_id), "accepted": STATUS_ACCEPTED, "view": ACCESS_VIEW},
    )


async def circle_members(user_id: int | str) -> list[dict]:
    """Every circle the user belongs to, with its full membership.

    Includes the user's own pending invitations — the row that says "you were
    invited" is the same row that says "you are a member", which is why one
    query answers both and the old model needed two endpoints.
    """
    return await execute_query(
        """
        SELECT c.id                AS circle_id,
               c.name              AS circle_name,
               c.owner_user_id     AS owner_user_id,
               m.id                AS member_row_id,
               m.user_id           AS user_id,
               m.role              AS role,
               m.status            AS status,
               m.health_access     AS health_access,
               m.nickname          AS nickname,
               m.avatar_key        AS avatar_key,
               u.name              AS name,
               u.email             AS email
          FROM care_circle_members mine
          JOIN care_circles c
            ON c.id = mine.care_circle_id AND c.deleted_at IS NULL
          JOIN care_circle_members m
            ON m.care_circle_id = c.id AND m.deleted_at IS NULL
          JOIN health_app_user u
            ON u.id = m.user_id AND u.is_del = false
         WHERE mine.user_id = :u AND mine.deleted_at IS NULL
         ORDER BY c.id, m.role DESC, m.id
        """,
        {"u": int(user_id)},
    )


async def beneficiary_users(user_id: int | str, fallback_name: str = "") -> list[dict]:
    """The record switcher: me first, then everyone sharing with me.

    Serves `/api/beneficiary-users`, which is how the web client learns there is
    a second record to look at — the README's demo turns on this one call. The
    shape is the client's, not the table's: `id`, `name`, `nickname`, `gender`
    as "male"/"female", `blood_type`, `age`, `is_current_user`.

    Age is computed here rather than stored, because `health_app_user.birth` is
    a free-text `character varying` that arrives in three formats.
    """
    me = await get_user(user_id=user_id)
    my_name = (me or {}).get("name") or fallback_name or "Current User"
    out = [{
        "id": str(int(user_id)),
        "name": my_name,
        "nickname": fallback_name or None,
        "gender": _gender_name((me or {}).get("gender")),
        "blood_type": (me or {}).get("blood"),
        "age": _age_from((me or {}).get("birth")),
        "is_current_user": True,
    }]
    for row in await shared_with_me(user_id):
        out.append({
            "id": str(row["user_id"]),
            "name": row.get("name") or row.get("nickname") or f"User {row['user_id']}",
            "nickname": row.get("nickname"),
            "gender": _gender_name(row.get("gender")),
            "blood_type": row.get("blood"),
            "age": _age_from(row.get("birth")),
            "is_current_user": False,
        })
    return out


def _gender_name(value) -> str | None:
    return {1: "male", 2: "female"}.get(value)


def _age_from(birth: str | None) -> int | None:
    """Age from `health_app_user.birth`, which is free text in three formats."""
    if not birth:
        return None
    from datetime import date, datetime
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            b = datetime.strptime(str(birth).strip(), fmt).date()
        except ValueError:
            continue
        today = date.today()
        return today.year - b.year - ((today.month, today.day) < (b.month, b.day))
    return None


async def resolve_email_to_user(email: str) -> int | None:
    """The user id behind an address, minting a shell account on first sight.

    Inviting somebody who has never signed in has to work — that is what an
    invitation IS — so the row is created here and the invitee claims it at
    first login. `add_or_get_user` only runs on login, which is why the invite
    path needs its own upsert.
    """
    clean = (email or "").strip().lower()
    if not clean or "@" not in clean:
        return None
    rows = await execute_query(
        """
        WITH ins AS (
            INSERT INTO health_app_user (is_del, email, name)
            VALUES (false, :email, :name)
            ON CONFLICT (email) WHERE (is_del = false) DO NOTHING
            RETURNING id
        )
        SELECT id FROM ins
        UNION ALL
        SELECT id FROM health_app_user WHERE email = :email AND is_del = false
        LIMIT 1
        """,
        {"email": clean, "name": clean.split("@")[0]},
    )
    return int(rows[0]["id"]) if rows else None


async def force_accept_managed_member(
    circle_id: int, member_id: int, *, nickname: str | None = None
) -> int:
    """Put a managed member straight into a circle, accepted and read-write.

    A managed member — the web client calls it a virtual user — is a family
    member who will never sign in: a parent whose readings someone else uploads
    and asks about. There is nobody to accept an invitation and nobody to set
    the health switch, so the person who created them holds both.

    The safety boundary is the circle, not the flags. This writes only into a
    circle the caller owns (the caller is its Owner member, created by
    `create_circle`), so the shortcut cannot reach a stranger's circle. That
    matters because `health_access = 2` here would be wrong for anyone who CAN
    sign in — for them the switch is their own and starts at 0.
    """
    rows = await execute_query(
        """
        INSERT INTO care_circle_members
               (care_circle_id, user_id, role, status, health_access, nickname)
        VALUES (:c, :u, :role, :accepted, :edit, :nick)
        ON CONFLICT (care_circle_id, user_id) WHERE deleted_at IS NULL DO UPDATE
           SET status        = :accepted,
               health_access = :edit,
               nickname      = COALESCE(EXCLUDED.nickname, care_circle_members.nickname),
               updated_at    = now()
        RETURNING id
        """,
        {"c": int(circle_id), "u": int(member_id), "role": ROLE_MEMBER,
         "accepted": STATUS_ACCEPTED, "edit": ACCESS_EDIT, "nick": nickname},
    )
    return int(rows[0]["id"])

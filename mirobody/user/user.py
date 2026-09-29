"""User records, and the rule for which database layer to use.

**Two layers, and the boundary is atomicity.** `utils/db.execute_query` runs ONE
statement inside its own `engine.begin()`, so anything that must commit or roll
back together cannot use it: two calls are two transactions. That is the whole
rule, and in this package it applies to `del_user` (two tables marked deleted
together) and `account_merge` (an explicit `conn.transaction()` over the whole
merge). Those take an injected
`AsyncConnectionPool` and hand-roll `cur.execute(..., %s)`.

Everything else (every single-statement read, wherever it lives) goes through
`execute_query` with `:named` params. Choose by need, never by file.

**And one query, not twenty.** A hand-rolled `SELECT ... FROM health_app_user
WHERE ... AND is_del = false` at every call site is a per-call-site chance to
drop the `is_del` filter, and a lookup without it answers for deleted accounts
name, language, timezone and all. :func:`get_user` is the single lookup;
`test_user_lookup.py` fails if a new hand-rolled one appears.
"""

from __future__ import annotations

import logging

from typing import TYPE_CHECKING

# Type-checking only: `AsyncConnectionPool` appears in two parameter
# annotations, and psycopg_pool lives in the [app] extra. A module-scope
# import here made `import mirobody.user.care_circle` (the pure authorization
# rules examples/06 demonstrates) require the server extra.
if TYPE_CHECKING:
    from psycopg_pool import AsyncConnectionPool

from mirobody.utils.db import execute_query

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

# Every column a caller has asked for across those 20 sites, so the one accessor
# can serve all of them. The table is 15 narrow columns; naming them beats `*`
# because a dropped column then fails here instead of at the first KeyError.
_USER_COLUMNS = ("id, email, name, lang, tz, gender, birth, blood, mfa_enabled, managed_by,"
                 " (password_hash IS NOT NULL) AS has_password")


async def get_user(
    *,
    user_id     : int | str | None = None,
    email       : str | None = None,
) -> dict | None:
    """The one `health_app_user` lookup. Returns the live row, or None.

    Exactly one selector. `is_del = false` is not optional and not a parameter:
    a soft-deleted account must not be findable by either key, which is the
    bug this function exists to make unrepeatable.
    """
    keys = {"id": user_id, "email": email}
    given = {k: v for k, v in keys.items() if v not in (None, "")}
    if len(given) != 1:
        raise ValueError(f"get_user needs exactly one of {list(keys)}, got {list(given)}")

    column, value = next(iter(given.items()))
    if column == "id":
        value = int(value)
    elif column == "email":
        value = str(value).strip().lower()

    rows = await execute_query(
        f"SELECT {_USER_COLUMNS} FROM health_app_user "
        f"WHERE {column} = :value AND is_del = false LIMIT 1;",
        params={"value": value},
    )
    return rows[0] if rows else None

#-----------------------------------------------------------------------------

async def ensure_user(email: str) -> int | None:
    """The live account for an address, created on first sight; None for a
    malformed address, and for a managed account's address: nobody signs in
    to those or is invited as them.

    One statement, so two sign-ins racing on a new address both get the same
    row. This replaced two copies: `add_or_get_user` (sign-in), which read then
    inserted and returned the second racer an IntegrityError as its message,
    and `care_circle.resolve_email_to_user` (invitations), which was this.
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
        SELECT id FROM health_app_user WHERE email = :email AND is_del = false AND managed_by IS NULL
        LIMIT 1
        """,
        {"email": clean, "name": clean.split("@")[0]},
    )
    return int(rows[0]["id"]) if rows else None

#-----------------------------------------------------------------------------

async def update_user_name(
    db_pool : AsyncConnectionPool,
    user_id : int,
    name    : str,
) -> str | None:
    """Update health_app_user.name for the given user. Returns error string
    on failure, None on success. Caller is responsible for trimming / length
    validation."""
    if user_id <= 0:
        return "Invalid user ID."

    if not isinstance(name, str) or not name:
        return "Invalid name."

    if not db_pool:
        return "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE health_app_user"
                    "   SET name=%s, update_at=CURRENT_TIMESTAMP"
                    " WHERE id=%s AND is_del=FALSE;",
                    [name, user_id]
                )
                await conn.commit()
    except Exception as e:
        logger.error(str(e), extra={"user_id": user_id})
        return str(e)

    return None

#-----------------------------------------------------------------------------

async def del_user(
    db_pool : AsyncConnectionPool,
    user_id : int
) -> str:
    if user_id <= 0:
        return "Invalid user ID."

    if not db_pool:
        return "Invalid database connection."

    # Both statements are awaited. They were not, so `/user/del` returned before
    # either ran and no account was ever deleted. `app_user_id` is a varchar: an
    # int there fails the statement and rolls the deletion back.
    try:
        async with db_pool.connection() as conn:
            await conn.execute(
                "UPDATE health_app_user SET is_del=TRUE WHERE id=%s;",
                [user_id]
            )

            await conn.execute(
                "UPDATE health_vital_user SET is_del=TRUE WHERE app_user_id=%s;",
                [str(user_id)]
            )

            await conn.commit()
    except Exception as e:
        return str(e)

    _active.pop(user_id, None)
    _closed.add(user_id)
    return None

#-----------------------------------------------------------------------------

#: How long a live account is taken on trust before it is looked up again. A
#: token outlives the account by at most this long; a closed account is final
#: and is remembered for good.
_ACTIVE_TTL = 30.0
_active: dict[int, float] = {}
_closed: set[int] = set()


async def is_active_account(user_id: int | str) -> bool:
    """Whether a token's subject is an account that still exists. A JWT is
    valid for 30 days and carries nothing a deletion can revoke, so every
    token check asks this. A lookup that fails answers True: an outage must
    not sign everyone out."""
    import time

    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return False
    if uid <= 0 or uid in _closed:
        return False
    now = time.monotonic()
    if _active.get(uid, 0.0) > now:
        return True
    try:
        row = await get_user(user_id=uid)
    except Exception as e:
        logger.warning("account lookup failed; token accepted: error_type=%s", type(e).__name__)
        return True
    if row is None:
        _closed.add(uid)
        return False
    _active[uid] = now + _ACTIVE_TTL
    return True


#-----------------------------------------------------------------------------

class UserInfo:
    def __init__(
        self,
        name: str,
        language: str,
        timezone: str
    ):
        self.name       = name
        self.language   = language
        self.timezone   = timezone


async def get_user_info(
    user_id : int
) -> tuple[UserInfo | None, str | None]:
    if user_id <= 0:
        return None, "Invalid user ID."

    try:
        row = await get_user(user_id=user_id)
        if row:
            return UserInfo(row["name"], row["lang"], row["tz"]), None

    except Exception as e:
        logger.error(str(e), extra={"id": user_id})

        return None, str(e)

    return None, "Not found."

#-----------------------------------------------------------------------------

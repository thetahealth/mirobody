"""User records, and the rule for which database layer to use.

**Two layers, and the boundary is atomicity.** `utils/db.execute_query` runs ONE
statement inside its own `engine.begin()`, so anything that must commit or roll
back together cannot use it — two calls are two transactions. That is the whole
rule, and in this package it applies to exactly three functions: `add_or_get_user`
(SELECT then INSERT-or-UPDATE, a read-then-write that must not interleave),
`del_user` (two tables marked deleted together), and `account_merge` (an explicit
`conn.transaction()` over the whole merge). Those take an injected
`AsyncConnectionPool` and hand-roll `cur.execute(..., %s)`.

Everything else — every single-statement read, wherever it lives — goes through
`execute_query` with `:named` params. Choose by need, never by file.

**And one query, not twenty.** A hand-rolled `SELECT ... FROM health_app_user
WHERE ... AND is_del = false` at every call site is a per-call-site chance to
drop the `is_del` filter, and a lookup without it answers for deleted accounts
— name, language, timezone and all. :func:`get_user` is the single lookup;
`test_user_lookup.py` fails if a new hand-rolled one appears.
"""

from __future__ import annotations

import json, logging

from typing import TYPE_CHECKING

# Type-checking only: `AsyncConnectionPool` appears in three parameter
# annotations, and psycopg_pool lives in the [app] extra. A module-scope
# import here made `import mirobody.user.care_circle` — the pure authorization
# rules examples/06 demonstrates — require the server extra.
if TYPE_CHECKING:
    from psycopg_pool import AsyncConnectionPool

from ..utils.db import execute_query

#-----------------------------------------------------------------------------

# Every column a caller has asked for across those 20 sites, so the one accessor
# can serve all of them. The table is 15 narrow columns; naming them beats `*`
# because a dropped column then fails here instead of at the first KeyError.
_USER_COLUMNS = (
    "id, email, name, lang, response_lang, tz, gender, birth, blood, "
    "apple_sub, mfa_enabled, consultant_id, coins"
)


async def get_user(
    *,
    user_id     : int | str | None = None,
    email       : str | None = None,
    apple_sub   : str | None = None,
) -> dict | None:
    """The one `health_app_user` lookup. Returns the live row, or None.

    Exactly one selector. `is_del = false` is not optional and not a parameter:
    a soft-deleted account must not be findable by any of the three keys, which
    is the bug this function exists to make unrepeatable.
    """
    keys = {"id": user_id, "email": email, "apple_sub": apple_sub}
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

async def add_or_get_user(
    db_pool         : AsyncConnectionPool,
    email           : str,
    name            : str | None = None,
    apple_subject   : str | None = None,
) -> tuple[
    int,        # User ID.
    str | None  # Error message.
]:
    lower_email = email.strip().lower()
    if not lower_email:
        return 0, "Invalid email."

    if name is None or (isinstance(name, str) and name.strip() == ""):
        name = lower_email.split("@")[0]

    if not db_pool:
        return 0, "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM health_app_user WHERE email=%s AND is_del=FALSE;",
                    [lower_email]
                )
                await conn.commit()

                row = await cur.fetchone()
                if row and len(row) == 1:
                    user_id = row[0]

                    if apple_subject:
                        await cur.execute(
                            "UPDATE health_app_user SET apple_sub=%s WHERE id=%s;",
                            [apple_subject, user_id]
                        )
                        await conn.commit()

                    # Return existing user ID.
                    return user_id, None

                #-------------------------------------

                if not apple_subject:
                    apple_subject = None

                await cur.execute(
                    "INSERT INTO health_app_user (is_del,email,name,apple_sub)"
                    " VALUES (FALSE,%s,%s,%s) RETURNING id;",
                    [lower_email, name, apple_subject]
                )
                await conn.commit()

                row = await cur.fetchone()
                if row and len(row) == 1:
                    # Return inserted user ID.
                    return row[0], None

    except Exception as e:
        logging.error(str(e), extra={
            "email": email, "apple": apple_subject
        })

        return 0, str(e)

    return 0, "Not found."

#-----------------------------------------------------------------------------

async def get_user_via_apple_subject(
    apple_subject   : str
) -> tuple[
    int,        # User ID.
    str,        # Email.
    str | None  # Error message.
]:
    if not apple_subject:
        return 0, "", "Invalid Apple sub."

    try:
        row = await get_user(apple_sub=apple_subject)
        if not row:
            return 0, "", "Not found."

        return row["id"], row["email"], None

    except Exception as e:
        logging.error(str(e), extra={"apple": apple_subject})

        return 0, "", str(e)

#-----------------------------------------------------------------------------


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
        logging.error(str(e), extra={"user_id": user_id})
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

    try:
        async with db_pool.connection() as conn:
            conn.execute(
                "UPDATE health_app_user SET is_del=TRUE WHERE id=%s;",
                [user_id]
            )

            conn.execute(
                "UPDATE health_vital_user SET is_del=TRUE WHERE app_user_id=%s;",
                [user_id]
            )

            await conn.commit()
    except Exception as e:
        return str(e)

    return None

#-----------------------------------------------------------------------------

class UserInfo():
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
        logging.error(str(e), extra={"id": user_id})

        return None, str(e)

    return None, "Not found."

#-----------------------------------------------------------------------------

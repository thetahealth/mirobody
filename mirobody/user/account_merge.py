import json
import logging

from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

# Tables where merging is a plain UPDATE col = winning WHERE col = losing:
# no UNIQUE constraint over user_id means no row collision. Each entry is
# (table_name, [column, ...]); user_id is VARCHAR here, so we pass
# str(losing)/str(winning). Tables absent from this list have UNIQUE or
# PRIMARY KEY constraints over user_id and need `_merge_*()` instead.
# The list is deliberately WIDER than `mirobody/schema`, covering tables a
# deployment may have from elsewhere; `_table_exists` guards every access,
# so do not prune it by diffing against our own DDL.

SIMPLE_RELINK_TABLES: list[tuple[str, list[str]]] = [
    # sql/ tables
    ("series_data",                     ["user_id"]),
    ("th_files",                        ["user_id", "query_user_id"]),
    ("th_messages",                     ["user_id", "query_user_id"]),
    ("th_sessions",                     ["user_id", "query_user_id"]),
    ("th_extraction",                   ["user_id"]),
    ("th_check_result",                 ["user_id"]),
    ("th_series_data_genetic",          ["user_id"]),
    ("th_session_share",                ["user_id"]),
    ("th_task_flow",                    ["user_id"]),
    ("user_behavior_insight",           ["user_id"]),
    ("webauthn_credentials",            ["user_id"]),

    # externally provisioned tables (absent in a standalone deployment)
    ("health_data_epic",                ["theta_user_id"]),
    ("health_data_garmin",              ["theta_user_id"]),
    ("health_data_libre",               ["user_id"]),
    ("health_data_oracle",              ["theta_user_id"]),
    ("health_data_oura",                ["theta_user_id"]),
    ("health_user_profile_by_system",   ["user_id"]),
    ("health_user_provider",            ["user_id"]),
    ("health_vital_user",               ["app_user_id"]),
    ("health_vital_webhook",            ["app_user_id", "user_id", "client_user_id"]),
]

#-----------------------------------------------------------------------------

async def _table_exists(cur, table_name: str) -> bool:
    """Skip tables that aren't part of the current deployment. Avoids
    UndefinedTable errors aborting the txn.
    """
    await cur.execute("SELECT to_regclass(%s);", [f"public.{table_name}"])
    row = await cur.fetchone()
    return bool(row and row[0])


async def _relink_simple(cur, table: str, columns: list[str], losing_str: str, winning_str: str) -> int:
    """Plain UPDATE for tables without UNIQUE constraints on user_id columns.

    Returns total rows updated across all named columns.
    """
    if not await _table_exists(cur, table):
        return 0

    total = 0
    for col in columns:
        await cur.execute(
            f"UPDATE {table} SET {col}=%s WHERE {col}=%s;",
            [winning_str, losing_str]
        )
        total += cur.rowcount or 0
    return total


#-----------------------------------------------------------------------------
# Conflict-aware merges. Each handles one table whose UNIQUE / PK covers
# user_id columns, so naive UPDATE may collide with rows already on the
# winning side.





async def _merge_care_circle_members(cur, losing_id: int, winning_id: int) -> int:
    """care_circle_members has a partial UNIQUE (care_circle_id, user_id) on live rows.

    Conflict policy: if the winning account is already a live member of a circle
    the losing account is also in, the losing row is soft-deleted rather than
    moved, and `health_access` is taken as the MAX of the two, because that is
    the rule `care_circle.accepted_membership` already reads by. Anything else
    would let a merge silently revoke access the surviving account had, or grant
    access neither had.

    Replaces the two functions this used to need, `_merge_th_share_relationship`
    and `_merge_th_share_user_config`. The first carried a permissions-bag union
    ("{{\"all\":1}} dominates") which has no meaning now that the grant is one
    named column; the second merged nicknames keyed by (setter, target, context),
    which is a table that no longer exists.
    """
    if not await _table_exists(cur, "care_circle_members"):
        return 0

    # Raise the surviving row to the better grant, then retire the loser.
    await cur.execute(
        """
        UPDATE care_circle_members w
           SET health_access = GREATEST(w.health_access, l.health_access),
               nickname      = COALESCE(w.nickname, l.nickname),
               updated_at    = now()
          FROM care_circle_members l
         WHERE w.user_id = %s AND l.user_id = %s
           AND w.care_circle_id = l.care_circle_id
           AND w.deleted_at IS NULL AND l.deleted_at IS NULL;
        """,
        [winning_id, losing_id],
    )
    total = cur.rowcount or 0

    await cur.execute(
        """
        UPDATE care_circle_members l
           SET deleted_at = now()
         WHERE l.user_id = %s AND l.deleted_at IS NULL
           AND EXISTS (SELECT 1 FROM care_circle_members w
                        WHERE w.user_id = %s
                          AND w.care_circle_id = l.care_circle_id
                          AND w.deleted_at IS NULL);
        """,
        [losing_id, winning_id],
    )
    total += cur.rowcount or 0

    # Whatever is left has no counterpart: move it over.
    await cur.execute(
        "UPDATE care_circle_members SET user_id=%s, updated_at=now()"
        " WHERE user_id=%s AND deleted_at IS NULL;",
        [winning_id, losing_id],
    )
    total += cur.rowcount or 0

    # The circles the losing account OWNED go with it.
    await cur.execute(
        "UPDATE care_circles SET owner_user_id=%s, updated_at=now() WHERE owner_user_id=%s;",
        [winning_id, losing_id],
    )
    total += cur.rowcount or 0
    return total


async def _merge_observations(cur, losing_str: str, winning_str: str) -> int:
    """th_observation has a unique identity that includes user_id, and
    th_series / th_day_authority are keyed by (user_id, series_id[, day]).

    A losing observation whose identity the winner already holds (the same
    report uploaded to both accounts) is dropped; the rest move over. The
    per-user catalogue and day authority are deleted for the loser and
    rebuilt for the winner after the merge commits (`rebuild_series`), so the
    two never hold a row the fact table contradicts.
    """
    if not await _table_exists(cur, "th_observation"):
        return 0

    await cur.execute(
        """
        DELETE FROM th_observation l
         WHERE l.user_id = %(losing)s
           AND EXISTS (
             SELECT 1 FROM th_observation w
              WHERE w.user_id = %(winning)s
                AND w.name_key = l.name_key
                AND w.observed_start = l.observed_start
                AND w.observed_end = l.observed_end
                AND w.source_ref = l.source_ref
                AND COALESCE(w.source_record_id, '') = COALESCE(l.source_record_id, '')
                AND COALESCE(w.member_of, 0) = COALESCE(l.member_of, 0)
                AND COALESCE(w.amends, 0) = COALESCE(l.amends, 0)
           );
        """,
        {"losing": losing_str, "winning": winning_str},
    )
    total = cur.rowcount or 0

    await cur.execute("UPDATE th_observation SET user_id=%s WHERE user_id=%s;", [winning_str, losing_str])
    total += cur.rowcount or 0
    for table in ("th_series", "th_day_authority"):
        if await _table_exists(cur, table):
            await cur.execute(f"DELETE FROM {table} WHERE user_id=%s;", [losing_str])
            total += cur.rowcount or 0
    return total


async def _merge_th_user_avatar_managed(cur, losing_str: str, winning_str: str) -> int:
    """th_user_avatar_managed PRIMARY KEY(user_id, owner_user_id).

    On conflict, drop the losing-side row.
    """
    if not await _table_exists(cur, "th_user_avatar_managed"):
        return 0

    total = 0
    for col_self, col_other in [
        ("user_id", "owner_user_id"),
        ("owner_user_id", "user_id"),
    ]:
        await cur.execute(
            f"""
            DELETE FROM th_user_avatar_managed l
             WHERE l.{col_self}=%(losing)s
               AND EXISTS (
                 SELECT 1 FROM th_user_avatar_managed w
                  WHERE w.{col_self}=%(winning)s
                    AND w.{col_other}=l.{col_other}
               );
            """,
            {"losing": losing_str, "winning": winning_str}
        )
        total += cur.rowcount or 0

        await cur.execute(
            f"UPDATE th_user_avatar_managed SET {col_self}=%s WHERE {col_self}=%s;",
            [winning_str, losing_str]
        )
        total += cur.rowcount or 0

    return total



#-----------------------------------------------------------------------------

async def merge_accounts(
    db_pool             : AsyncConnectionPool,
    losing_user_id      : int,
    winning_user_id     : int,
    reason              : str = "email_link",
) -> tuple[
    dict[str, int],     # affected rows per table
    str | None          # error message
]:
    """Move all data owned by `losing_user_id` to `winning_user_id`, soft-delete
    losing account, and write an audit log row. Atomic.
    """
    if losing_user_id <= 0 or winning_user_id <= 0:
        return {}, "Invalid user IDs."

    if losing_user_id == winning_user_id:
        return {}, "Cannot merge a user into itself."

    if not db_pool:
        return {}, "Invalid database connection."

    losing_str  = str(losing_user_id)
    winning_str = str(winning_user_id)

    affected: dict[str, int] = {}

    try:
        async with db_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:

                    # 1. Simple relinks (UPDATE col = winning WHERE col = losing)
                    for table, columns in SIMPLE_RELINK_TABLES:
                        n = await _relink_simple(cur, table, columns, losing_str, winning_str)
                        if n:
                            affected[table] = n

                    # 2. Conflict-aware merges
                    n = await _merge_care_circle_members(cur, losing_user_id, winning_user_id)
                    if n:
                        affected["care_circle_members"] = n

                    n = await _merge_th_user_avatar_managed(cur, losing_str, winning_str)
                    if n:
                        affected["th_user_avatar_managed"] = n

                    n = await _merge_observations(cur, losing_str, winning_str)
                    if n:
                        affected["th_observation"] = n

                    # 4. Soft-delete the losing health_app_user.
                    await cur.execute(
                        "UPDATE health_app_user SET is_del=TRUE, update_at=CURRENT_TIMESTAMP WHERE id=%s;",
                        [losing_user_id]
                    )
                    affected["health_app_user"] = cur.rowcount or 0

                    # 5. Audit log (skip where the table isn't provisioned,
                    # it is not part of this project's own DDL).
                    if await _table_exists(cur, "user_account_merge_log"):
                        await cur.execute(
                            "INSERT INTO user_account_merge_log"
                            " (losing_user_id, winning_user_id, reason, affected_rows)"
                            " VALUES (%s,%s,%s,%s);",
                            [losing_user_id, winning_user_id, reason, json.dumps(affected)]
                        )

    except Exception as e:
        logger.error(str(e), extra={
            "losing_user_id"  : losing_user_id,
            "winning_user_id" : winning_user_id,
            "reason"          : reason,
            "affected_so_far" : affected,
        })

        return affected, str(e)

    if affected.get("th_observation"):
        # Outside the merge transaction on purpose: the catalogue is derived
        # from the fact table and is rebuilt from it, never carried across.
        try:
            from mirobody.collect.observations import rebuild_series
            await rebuild_series(winning_str)
        except Exception as e:
            logger.warning("series catalogue not rebuilt after merge: error_type=%s", type(e).__name__)

    return affected, None

#-----------------------------------------------------------------------------

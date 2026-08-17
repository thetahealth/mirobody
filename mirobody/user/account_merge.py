import json, logging

from psycopg_pool import AsyncConnectionPool

#-----------------------------------------------------------------------------
# Tables where merging is a plain UPDATE col = winning WHERE col = losing.
# No UNIQUE constraint covering the user_id column means no row collision.
# Each entry: (table_name, [column1, column2, ...]).
#
# user_id values in subordinate tables are stored as VARCHAR (str of the
# health_app_user.id integer); we pass str(losing)/str(winning).
#
# Tables that DON'T appear here have UNIQUE / PRIMARY KEY constraints over
# the user_id column and need conflict-aware handling — see _merge_*().
#
# This list is deliberately WIDER than `mirobody/schema`: it covers whatever
# user-scoped tables the running deployment happens to have, and the holywell
# group below is provisioned by a007-holywell, not by us. Several entries name
# tables our own DDL no longer creates (`th_task_flow`, `health_data_epic`,
# `health_data_oracle`, `health_data_libre`, `health_vital_webhook`) — that is
# correct, not stale: every access is guarded by `_table_exists`, so an entry
# costs one catalogue lookup where the table is absent and keeps a merge honest
# where it is present. Do not prune this list by diffing it against our schema.

SIMPLE_RELINK_TABLES: list[tuple[str, list[str]]] = [
    # sql/ tables
    ("series_data",                     ["user_id"]),
    ("th_files",                        ["user_id", "query_user_id"]),
    ("th_messages",                     ["user_id", "query_user_id"]),
    ("th_sessions",                     ["user_id", "query_user_id"]),
    ("th_series_data",                  ["user_id"]),
    ("th_series_data_genetic",          ["user_id"]),
    ("th_session_share",                ["user_id"]),
    ("th_task_flow",                    ["user_id"]),
    ("user_behavior_insight",           ["user_id"]),
    ("webauthn_credentials",            ["user_id"]),

    # holywell tables (loaded from a007-holywell/backend_db/resource_holywell)
    ("deep_agent_workspace",            ["user_id"]),
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
    """Skip tables that aren't part of the current deployment (mirobody-only
    vs full holywell stack). Avoids UndefinedTable errors aborting the txn.
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

async def _merge_th_share_relationship(cur, losing_str: str, winning_str: str) -> int:
    """th_share_relationship UNIQUE(owner_user_id, member_user_id).

    Conflict policy: if (winning, X) already exists when we'd move (losing, X)
    onto it, prefer the row with status='authorized', then earlier created_at,
    and merge permissions JSONB by taking the union ({"all":1} dominates).
    Self-loops created by the rename ((winning, winning)) are dropped.
    """
    if not await _table_exists(cur, "th_share_relationship"):
        return 0

    # Drop self-loops first: rows where the OTHER side already equals winning.
    await cur.execute(
        "DELETE FROM th_share_relationship"
        " WHERE (owner_user_id=%s AND member_user_id=%s)"
        "    OR (owner_user_id=%s AND member_user_id=%s);",
        [losing_str, winning_str, winning_str, losing_str]
    )
    deleted_loops = cur.rowcount or 0

    # Resolve conflicts on the owner side: same member already shared by winning.
    await cur.execute(
        """
        WITH losing_rows AS (
            SELECT share_id, member_user_id, status, permissions, created_at
            FROM th_share_relationship
            WHERE owner_user_id=%(losing)s
        ),
        conflicts AS (
            SELECT l.share_id AS losing_id, w.share_id AS winning_id,
                   l.status AS l_status, w.status AS w_status,
                   l.permissions AS l_perm, w.permissions AS w_perm,
                   l.created_at AS l_at, w.created_at AS w_at
            FROM losing_rows l
            JOIN th_share_relationship w
              ON w.owner_user_id=%(winning)s AND w.member_user_id=l.member_user_id
        )
        UPDATE th_share_relationship t
           SET status = CASE WHEN c.l_status='authorized' OR c.w_status='authorized'
                             THEN 'authorized' ELSE t.status END,
               permissions = CASE
                 WHEN (c.l_perm->>'all')::int >= 1 OR (c.w_perm->>'all')::int >= 1
                   THEN '{"all":1}'::jsonb
                 ELSE COALESCE(c.l_perm, '{}'::jsonb) || COALESCE(c.w_perm, '{}'::jsonb)
               END,
               created_at = LEAST(c.l_at, c.w_at),
               updated_at = CURRENT_TIMESTAMP
          FROM conflicts c
         WHERE t.share_id = c.winning_id;
        """,
        {"losing": losing_str, "winning": winning_str}
    )

    # Now delete the losing-side conflict rows (their data has been merged in).
    await cur.execute(
        """
        DELETE FROM th_share_relationship l
         WHERE l.owner_user_id=%(losing)s
           AND EXISTS (
             SELECT 1 FROM th_share_relationship w
              WHERE w.owner_user_id=%(winning)s
                AND w.member_user_id=l.member_user_id
           );
        """,
        {"losing": losing_str, "winning": winning_str}
    )
    deleted_owner_conflicts = cur.rowcount or 0

    # Symmetric handling for the member side.
    await cur.execute(
        """
        DELETE FROM th_share_relationship l
         WHERE l.member_user_id=%(losing)s
           AND EXISTS (
             SELECT 1 FROM th_share_relationship w
              WHERE w.member_user_id=%(winning)s
                AND w.owner_user_id=l.owner_user_id
           );
        """,
        {"losing": losing_str, "winning": winning_str}
    )
    deleted_member_conflicts = cur.rowcount or 0

    # Surviving rows just get their user_id columns rewritten.
    await cur.execute(
        "UPDATE th_share_relationship SET owner_user_id=%s WHERE owner_user_id=%s;",
        [winning_str, losing_str]
    )
    updated_owner = cur.rowcount or 0

    await cur.execute(
        "UPDATE th_share_relationship SET member_user_id=%s WHERE member_user_id=%s;",
        [winning_str, losing_str]
    )
    updated_member = cur.rowcount or 0

    return (deleted_loops + deleted_owner_conflicts + deleted_member_conflicts
            + updated_owner + updated_member)


async def _merge_th_share_user_config(cur, losing_str: str, winning_str: str) -> int:
    """th_share_user_config UNIQUE(setter_user_id, target_user_id, context).

    Conflict policy: drop the losing-side row if the winning side already
    has an entry for the same (setter, target, context); permissions /
    nickname / avatar belong to the winning user post-merge.
    """
    if not await _table_exists(cur, "th_share_user_config"):
        return 0

    total = 0
    for col_self, col_other in [
        ("setter_user_id", "target_user_id"),
        ("target_user_id", "setter_user_id"),
    ]:
        # Drop conflicting losing-side rows.
        await cur.execute(
            f"""
            DELETE FROM th_share_user_config l
             WHERE l.{col_self}=%(losing)s
               AND EXISTS (
                 SELECT 1 FROM th_share_user_config w
                  WHERE w.{col_self}=%(winning)s
                    AND w.{col_other}=l.{col_other}
                    AND w.context=l.context
               );
            """,
            {"losing": losing_str, "winning": winning_str}
        )
        total += cur.rowcount or 0

        # Rewrite the survivors.
        await cur.execute(
            f"UPDATE th_share_user_config SET {col_self}=%s WHERE {col_self}=%s;",
            [winning_str, losing_str]
        )
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


async def _merge_unique_user_id_table(cur, table: str, losing_str: str, winning_str: str) -> int:
    """Tables with UNIQUE(user_id) — winning side keeps its config; losing
    is discarded outright. Used for user_agent_prompt and user_mcp_config.
    """
    if not await _table_exists(cur, table):
        return 0

    # If the winning user already has a config row, drop the losing one;
    # otherwise rewrite losing -> winning so the user keeps their settings.
    await cur.execute(
        f"""
        DELETE FROM {table} l
         WHERE l.user_id=%(losing)s
           AND EXISTS (SELECT 1 FROM {table} w WHERE w.user_id=%(winning)s);
        """,
        {"losing": losing_str, "winning": winning_str}
    )
    deleted = cur.rowcount or 0

    await cur.execute(
        f"UPDATE {table} SET user_id=%s WHERE user_id=%s;",
        [winning_str, losing_str]
    )
    updated = cur.rowcount or 0

    return deleted + updated


#-----------------------------------------------------------------------------

async def merge_accounts(
    db_pool             : AsyncConnectionPool,
    losing_user_id      : int,
    winning_user_id     : int,
    reason              : str = "wechat_email_link",
) -> tuple[
    dict[str, int],     # affected rows per table
    str | None          # error message
]:
    """Move all data owned by `losing_user_id` to `winning_user_id`, soft-delete
    losing, repoint auth_wechat, and write an audit log row. Atomic.
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
                    n = await _merge_th_share_relationship(cur, losing_str, winning_str)
                    if n:
                        affected["th_share_relationship"] = n

                    n = await _merge_th_share_user_config(cur, losing_str, winning_str)
                    if n:
                        affected["th_share_user_config"] = n

                    n = await _merge_th_user_avatar_managed(cur, losing_str, winning_str)
                    if n:
                        affected["th_user_avatar_managed"] = n

                    n = await _merge_unique_user_id_table(cur, "user_agent_prompt", losing_str, winning_str)
                    if n:
                        affected["user_agent_prompt"] = n

                    n = await _merge_unique_user_id_table(cur, "user_mcp_config", losing_str, winning_str)
                    if n:
                        affected["user_mcp_config"] = n

                    # 3. Repoint auth_wechat at the winning user.
                    if await _table_exists(cur, "auth_wechat"):
                        await cur.execute(
                            "UPDATE auth_wechat SET app_user_id=%s, update_time=CURRENT_TIMESTAMP"
                            " WHERE app_user_id=%s AND is_del=FALSE;",
                            [winning_user_id, losing_user_id]
                        )
                        if cur.rowcount:
                            affected["auth_wechat"] = cur.rowcount

                    # 4. Soft-delete the losing health_app_user.
                    await cur.execute(
                        "UPDATE health_app_user SET is_del=TRUE, update_at=CURRENT_TIMESTAMP WHERE id=%s;",
                        [losing_user_id]
                    )
                    affected["health_app_user"] = cur.rowcount or 0

                    # 5. Audit log (skip when the table isn't deployed yet —
                    # mirobody-only deployments that don't pull holywell).
                    if await _table_exists(cur, "user_account_merge_log"):
                        await cur.execute(
                            "INSERT INTO user_account_merge_log"
                            " (losing_user_id, winning_user_id, reason, affected_rows)"
                            " VALUES (%s,%s,%s,%s);",
                            [losing_user_id, winning_user_id, reason, json.dumps(affected)]
                        )

    except Exception as e:
        logging.error(str(e), extra={
            "losing_user_id"  : losing_user_id,
            "winning_user_id" : winning_user_id,
            "reason"          : reason,
            "affected_so_far" : affected,
        })

        return affected, str(e)

    return affected, None

#-----------------------------------------------------------------------------

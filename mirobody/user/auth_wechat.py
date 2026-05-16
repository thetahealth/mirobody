import logging

from psycopg_pool import AsyncConnectionPool

from .user import add_or_get_user

#-----------------------------------------------------------------------------

async def find_user_by_wechat_openid(
    db_pool : AsyncConnectionPool,
    openid  : str
) -> tuple[
    int,        # User ID (0 if not found).
    str,        # Email (empty string if not found).
    str | None  # Error message.
]:
    """Resolve a WeChat openid to (app_user_id, email).

    JOINs auth_wechat with health_app_user so the caller gets the JWT email
    claim in one round-trip. Returns (0, "", None) when the openid hasn't
    been seen yet — caller should treat that as "first-time WeChat login"
    rather than an error. Returns (0, "", err) on actual database errors.
    """
    if not openid:
        return 0, "", "Invalid WeChat openid."

    if not db_pool:
        return 0, "", "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT u.id, u.email"
                    "  FROM auth_wechat aw"
                    "  JOIN health_app_user u ON u.id = aw.app_user_id"
                    " WHERE aw.openid=%s AND aw.is_del=FALSE AND u.is_del=FALSE"
                    " LIMIT 1;",
                    [openid]
                )
                await conn.commit()

                row = await cur.fetchone()
                if not row:
                    return 0, "", None

                return row[0], row[1], None

    except Exception as e:
        logging.error(str(e), extra={"openid": openid})

        return 0, "", str(e)

#-----------------------------------------------------------------------------

async def create_wechat_identity(
    db_pool         : AsyncConnectionPool,
    openid          : str,
    app_user_id     : int,
    virtual_email   : str,
    unionid         : str | None = None,
    nickname        : str | None = None,
    headimgurl      : str | None = None,
    refresh_token   : str | None = None,
) -> str | None:
    """Insert an auth_wechat row binding openid to app_user_id."""
    if not openid:
        return "Invalid WeChat openid."

    if app_user_id <= 0:
        return "Invalid app_user_id."

    if not virtual_email:
        return "Invalid virtual_email."

    if not db_pool:
        return "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO auth_wechat"
                    " (openid, unionid, app_user_id, virtual_email, nickname, headimgurl, refresh_token)"
                    " VALUES (%s,%s,%s,%s,%s,%s,%s);",
                    [openid, unionid, app_user_id, virtual_email, nickname, headimgurl, refresh_token]
                )
                await conn.commit()

    except Exception as e:
        logging.error(str(e), extra={"openid": openid, "app_user_id": app_user_id})

        return str(e)

    return None

#-----------------------------------------------------------------------------

async def relink_wechat_identity(
    db_pool             : AsyncConnectionPool,
    openid              : str,
    new_app_user_id     : int
) -> str | None:
    """Re-point an existing auth_wechat row at a different app_user_id.

    Used by the merge flow when a WeChat-only user binds an email that
    already belongs to another (winning) account: their auth_wechat row
    moves to point at the winning user.
    """
    if not openid:
        return "Invalid WeChat openid."

    if new_app_user_id <= 0:
        return "Invalid app_user_id."

    if not db_pool:
        return "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE auth_wechat SET app_user_id=%s, update_time=CURRENT_TIMESTAMP"
                    " WHERE openid=%s AND is_del=FALSE;",
                    [new_app_user_id, openid]
                )
                await conn.commit()

    except Exception as e:
        logging.error(str(e), extra={"openid": openid, "new_app_user_id": new_app_user_id})

        return str(e)

    return None

#-----------------------------------------------------------------------------

PLACEHOLDER_NAMES = ("WeChat User", "")

async def find_or_create_wechat_user(
    db_pool    : AsyncConnectionPool,
    openid     : str,
    unionid    : str | None = None,
    nickname   : str | None = None,
    headimgurl : str | None = None,
) -> tuple[
    int,        # User ID (0 on error).
    str,        # Email (empty on error).
    str | None  # Error message.
]:
    """Resolve an openid to (app_user_id, email), creating both the
    health_app_user row (with synthesized virtual email) and the auth_wechat
    row on first contact.

    nickname / headimgurl come from /sns/userinfo (optional). On first
    contact they seed the user's display name; on returning logins they
    refresh stale placeholder data without overwriting personalized names.

    Single shared helper for any caller that owns the WeChat OAuth dance:
    `wechat_verify_handler` (POST /wechat/verify) and the holywell-side
    `wechat_gateway` (the QR-redirect callback shared across frontends).
    """
    if not openid:
        return 0, "", "Invalid WeChat openid."

    if not db_pool:
        return 0, "", "Invalid database connection."

    # 1. Returning user — auth_wechat hit.
    user_id, email, err = await find_user_by_wechat_openid(db_pool, openid)
    if err:
        return 0, "", err
    if user_id:
        # Best-effort: refresh nickname / avatar in auth_wechat, and only
        # overwrite health_app_user.name when it's still the placeholder
        # (so users who later edit their name aren't stomped on).
        if nickname or headimgurl:
            await _refresh_wechat_meta(db_pool, openid, nickname, headimgurl)
        if nickname:
            await _refresh_placeholder_name(db_pool, user_id, nickname)
        return user_id, email, None

    # 2. First-time login. Create the health_app_user row keyed by virtual
    # email (legacy wechat_openid column intentionally not written), then
    # bind via auth_wechat.
    virtual_email = f"wx_{openid}@wechat.local"

    user_id, err = await add_or_get_user(
        db_pool,
        email = virtual_email,
        name  = nickname or "WeChat User",
    )
    if err:
        return 0, "", err
    if not user_id:
        return 0, "", "Empty user ID."

    err = await create_wechat_identity(
        db_pool,
        openid          = openid,
        app_user_id     = user_id,
        virtual_email   = virtual_email,
        unionid         = unionid,
        nickname        = nickname,
        headimgurl      = headimgurl,
    )
    if err:
        # Best-effort: the user row exists and is reachable by virtual_email,
        # so we still return success. Next login attempt will re-attempt the
        # auth_wechat insert (UNIQUE on openid makes it idempotent).
        logging.warning(
            f"auth_wechat insert failed for openid={openid}, user_id={user_id}: {err}"
        )

    return user_id, virtual_email, None

#-----------------------------------------------------------------------------

async def _refresh_placeholder_name(
    db_pool   : AsyncConnectionPool,
    user_id   : int,
    nickname  : str,
) -> None:
    """Overwrite health_app_user.name only when it's still a placeholder.
    Never stomp on a value the user has personalized through the UI."""
    if not nickname:
        return
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE health_app_user"
                    "   SET name=%s, update_at=CURRENT_TIMESTAMP"
                    " WHERE id=%s AND is_del=FALSE"
                    "   AND (name IS NULL OR name='' OR name='WeChat User');",
                    [nickname, user_id]
                )
                await conn.commit()
    except Exception as e:
        logging.warning(f"_refresh_placeholder_name failed: {e}",
                        extra={"user_id": user_id})


async def _refresh_wechat_meta(
    db_pool    : AsyncConnectionPool,
    openid     : str,
    nickname   : str | None,
    headimgurl : str | None,
) -> None:
    """COALESCE-update auth_wechat.nickname / headimgurl. Always safe to
    re-run; missing fields stay as-is."""
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE auth_wechat"
                    "   SET nickname=COALESCE(%s, nickname),"
                    "       headimgurl=COALESCE(%s, headimgurl),"
                    "       update_time=CURRENT_TIMESTAMP"
                    " WHERE openid=%s AND is_del=FALSE;",
                    [nickname, headimgurl, openid]
                )
                await conn.commit()
    except Exception as e:
        logging.warning(f"_refresh_wechat_meta failed: {e}",
                        extra={"openid": openid})

#-----------------------------------------------------------------------------

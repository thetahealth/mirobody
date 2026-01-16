import json, logging

from psycopg_pool import AsyncConnectionPool

#-----------------------------------------------------------------------------

async def add_or_get_user(
    db_pool         : AsyncConnectionPool,
    email           : str,
    name            : str | None = None,
    apple_subject   : str | None = None
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
                    "INSERT INTO health_app_user (is_del,email,name,apple_sub) VALUES (FALSE,%s,%s,%s) RETURNING id;",
                    [lower_email, name, apple_subject]
                )
                await conn.commit()

                row = await cur.fetchone()
                if row and len(row) == 1:
                    # Return inserted user ID.
                    return row[0], None

    except Exception as e:
        logging.error(str(e), extra={"email": email, "apple": apple_subject})

        return 0, str(e)

    return 0, "Not found."

#-----------------------------------------------------------------------------

async def get_user_via_apple_subject(
    db_pool         : AsyncConnectionPool,
    apple_subject   : str
) -> tuple[
    int,        # User ID.
    str,        # Email.
    str | None  # Error message.
]:
    if not apple_subject:
        return 0, "", "Invalid Apple sub."
    
    if not db_pool:
        return 0, "", "Invalid database connection."
    
    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id,email FROM health_app_user WHERE apple_sub=%s AND is_del=FALSE;",
                    [apple_subject]
                )
                await conn.commit()

                row = await cur.fetchone()
                if not row:
                    return 0, "", "Not found."
            
                return row[0], row[1], None

    except Exception as e:
        logging.error(str(e), extra={"apple": apple_subject})

        return 0, "", str(e)

#-----------------------------------------------------------------------------

async def check_relationship(
    db_pool         : AsyncConnectionPool,
    owner_user_id   : str,
    member_user_id  : str,
    permissions     : list[str]
) -> str | None:    # Error message.
    if not permissions:
        return "Empty permission list."
    
    if not owner_user_id:
        return "Empty owner user ID."
    
    if not member_user_id or owner_user_id == member_user_id:
        return None
    
    if not db_pool:
        return "Invalid database connection."
    
    #-----------------------------------------------------

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT permissions FROM th_share_relationship WHERE owner_user_id=%s AND member_user_id=%s AND status='authorized';",
                    [owner_user_id, member_user_id]
                )
                await conn.commit()

                records = await cur.fetchall()
                if not records:
                    return "Not found."

    except Exception as e:
        logging.error(str(e), extra={"owner_user_id": owner_user_id, "member_user_id": member_user_id, "permissions": permissions})

        return str(e)

    #-----------------------------------------------------

    for record in records:
        if not record or not record[0]:
            continue

        try:
            obj = json.loads(record[0])
        except:
            logging.error(str(e), extra={"owner_user_id": owner_user_id, "member_user_id": member_user_id, "permission": record[0]})
            continue

        if not obj or not isinstance(obj):
            continue

        if "all" in obj and obj["all"] > 0:
            return None
        
        # Check claimed permissions, respectively.
        ok = True
        for permission in permissions:
            if not permission:
                continue
            if permission not in obj or obj[permission] <= 0:
                ok = False
                break
        if ok:
            return None

    #-----------------------------------------------------
    
    return "Not allowed."

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
    db_pool : AsyncConnectionPool,
    user_id : int
) -> tuple[UserInfo | None, str | None]:
    if user_id <= 0:
        return None, "Invalid user ID."

    if not db_pool:
        return None, "Invalid database connection."

    try:
        async with db_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT name,lang,tz FROM health_app_user WHERE id=%s;", [user_id])
                await conn.commit()

                row = await cur.fetchone()
                if row and len(row) == 3:
                    return UserInfo(*row), None

    except Exception as e:
        logging.error(str(e), extra={"id": user_id})

        return None, str(e)

    return None, "Not found."

#-----------------------------------------------------------------------------

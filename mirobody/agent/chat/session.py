import logging, uuid

from datetime import datetime

from ...utils import execute_query
from ...utils.permissions import get_query_user_id

#-----------------------------------------------------------------------------

async def create_session(
    user_id: str,
    query_user_id: str,
    session_id: str | None = None,
) -> dict:
    """
    Create a row in th_sessions.

    `session_id` is optional. When omitted the function behaves as before
    (mints a uuid4). When supplied — e.g. by a client that needs to
    encode pane/group metadata directly into the id (compare mode) —
    the supplied id is used verbatim, after a safety check rejects
    obvious abuse (too long / unsupported chars). Other callers that
    don't send the param continue to get backend-minted uuids, so this
    is a backward-compatible parameter addition.
    """
    try:
        query_user_validation = await get_query_user_id(user_id=query_user_id, query_user_id=user_id, permission=["chat"])
        if not query_user_validation.get("success"):
            return {
                "code"  : -1,
                "msg"   : query_user_validation.get("error"),
                "data"  : {}
            }

        #-------------------------------------------------

        # Validate client-supplied session_id against th_sessions.session_id
        # (varchar(100)) so we never push a value the column would truncate.
        # Character whitelist matches what mirobody normally generates
        # (uuids) plus the underscore/hyphen a structured-id codec uses.
        if session_id:
            import re
            if len(session_id) > 100 or not re.fullmatch(r"[A-Za-z0-9_\-]+", session_id):
                return {
                    "code"  : -3,
                    "msg"   : "Invalid session_id format",
                    "data"  : {},
                }
        else:
            session_id = str(uuid.uuid4())

        created_at = datetime.now()
        
        session_sql = """
            INSERT INTO th_sessions (
                session_id, user_id, query_user_id,summary, created_at, in_use
            )
            VALUES (:session_id, :user_id, :query_user_id, :summary, :created_at, :in_use)
            """
        
        await execute_query(
            session_sql,
            params={
                "session_id"    : session_id,
                "summary"       : "",
                "created_at"    : created_at,
                "user_id"       : user_id,
                "query_user_id" : query_user_id,
                "in_use"        : False,
            }
        )

        return {
            "code"  : 0,
            "msg"   : "ok",
            "data"  : {
                "session_id": session_id,
                "created_at": created_at.isoformat()
            }
        }
        
    except Exception as e:
        logging.error(f"Error creating empty session: {str(e)}", exc_info=True)

        return {
            "code"  : -2,
            "msg"   : str(e),
            "data"  : {},
        }

#-----------------------------------------------------------------------------

async def get_session_summaries(user_id: str) -> list[dict[str, any]]:
    """Get all conversation summaries for user from database, only including sessions with text messages"""
    logging.info(f"get_session_summaries: {user_id}")
    try:
        # Modified SQL to only return sessions that contain text messages
        summary_sql = """
            SELECT DISTINCT ts.session_id, ts.summary, ts.created_at, ts.query_user_id
            FROM th_sessions ts
            INNER JOIN th_messages tm ON ts.session_id = tm.session_id 
                AND ts.user_id = tm.user_id
            WHERE ts.user_id = :user_id 
                AND ts.category IS NULL
                AND tm.message_type = 'text'
            ORDER BY ts.created_at DESC
        """
        result = await execute_query(
            summary_sql,
            params={"user_id": user_id}
        )

        formatted_summaries = []
        for summary in result:
            formatted_summary = {
                "session_id": summary.get("session_id"),
                "timestamp": (
                    summary.get("created_at").isoformat() if summary.get("created_at") else datetime.now().isoformat()
                ),
                "summary": summary.get("summary", ""),
                "query_user_id": summary.get("query_user_id", ""),
            }
            formatted_summaries.append(formatted_summary)

        logging.info(f"Retrieved {len(formatted_summaries)} text conversation summaries for user {user_id}")
        return formatted_summaries
    
    except Exception as e:
        logging.error(f"Error loading conversation summaries: {str(e)}", exc_info=True)
        return []

#-----------------------------------------------------------------------------

async def get_session_summaries_by_person(user_id: str) -> list[dict[str, any]]:
    """Get session summaries by person"""
    logging.info(f"get_session_summaries_by_person: {user_id}")
    try:
        
        summary_sql = """
            SELECT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, tu.name, tu.gender, tu.birth, tu.blood
            FROM th_sessions ts
            INNER JOIN health_app_user tu ON ts.query_user_id::integer = tu.id
            WHERE ts.user_id = :user_id
            ORDER BY created_at DESC
        """
        result = await execute_query(
            summary_sql,
            params={"user_id": user_id}
        )
        
        session_by_person = {
            
        }
        
        nickname_map = {}
        
        for _session in result:
            user_name = _session.get("name", "No name")
            user_gender = "Male" if _session.get("gender") == 1 else "Female" if _session.get("gender") == 2 else "Other"
            user_birth = _session.get("birth", "")
            user_age = ""
            query_user_id = _session.get("query_user_id")

            # Subtracting birth year from current year — which is what stood here —
            # is wrong for everyone whose birthday has not happened yet this year:
            # roughly half of all users at any moment, each reported one year too
            # old, in the profile block that goes into the agent's context.
            # `_calculate_age` compares (month, day) and already existed; this path
            # simply wasn't using it.
            from .user_profile import BasicInfoService
            user_age = BasicInfoService._calculate_age(user_birth)
            if user_age is None:
                user_age = ""

            user_blood = _session.get("blood", "")
            
            if query_user_id != user_id:
                if query_user_id not in nickname_map:
                    query_user_nickname_sql = "select nickname from th_share_user_config where setter_user_id = :user_id and target_user_id = :query_user_id limit 1"
                    query_user_nickname_result = await execute_query(
                        query_user_nickname_sql,
                        params={"user_id": user_id, "query_user_id": query_user_id}
                    )
                    if query_user_nickname_result:
                        user_name = query_user_nickname_result[0].get("nickname")
                        nickname_map[query_user_id] = user_name
                else:
                    user_name = nickname_map[query_user_id]
            
            session_by_person.setdefault((user_name, user_gender, user_age, user_blood), []).append(
                dict(
                    session_id=_session.get("session_id"),
                    query_user_id=_session.get("query_user_id"),
                    timestamp=_session.get("created_at").isoformat() if _session.get("created_at") else datetime.now().isoformat(),
                    summary=_session.get("summary", ""),
                )
            )
            
        return dict(
            code=0,
            msg="ok",
            data=[
                dict(
                    person_name=person_name,
                    gender=user_gender,
                    age=user_age,
                    blood=user_blood,
                    sessions=sessions,
                )
                for (person_name, user_gender, user_age, user_blood), sessions in session_by_person.items()
            ]
        )

    except Exception as e:
        logging.error(f"Error loading conversation summaries: {str(e)}", exc_info=True)

        return dict(
            code=1,
            msg=str(e),
            data=[]
        )

#-----------------------------------------------------------------------------

async def delete_session(user_id: str, session_id: str) -> str | None:
    try:
        # First delete all messages in the session
        delete_messages_sql = """
            DELETE FROM th_messages
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(
            delete_messages_sql,
            params={"user_id": user_id, "session_id": session_id}
        )

        # Then delete the session summary
        delete_session_sql = """
            DELETE FROM th_sessions
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(
            delete_session_sql,
            params={"user_id": user_id, "session_id": session_id}
        )

        # And the agent's own copy. DeepAgent's conversation memory is the
        # LangGraph checkpointer keyed on thread_id = session_id, so the two
        # deletes above would otherwise leave the same turns — the health
        # questions and the tool results answering them — sitting in the
        # checkpoint tables under this session id. Deleting a conversation has
        # to delete it, not just stop listing it. Best-effort by design (see
        # deep.checkpointer.delete_thread): the user-visible rows are already
        # gone, and a checkpoint-cleanup failure must not turn that into an error.
        from ..deep.checkpointer import delete_thread
        await delete_thread(session_id)

        return None

    except Exception as e:
        return str(e)

#-----------------------------------------------------------------------------
# Public session sharing (th_session_share): owner-created share links whose
# history is then served WITHOUT authentication (see session_share_router).
# Formerly a separate session_share.py holding a SessionShareService class of
# three staticmethods and a no-state module instance — plain module functions
# are this module's idiom, and sessions/share-links are one lifecycle.
#-----------------------------------------------------------------------------

async def create_or_get_share_session(user_id: str, session_id: str) -> dict:
    """Create a share link for a session the caller owns, or return the
    existing active one (idempotent — one active share link per session)."""
    try:
        # Check if user owns this session
        session_result = await execute_query(
            "SELECT user_id FROM th_sessions WHERE session_id = :session_id",
            params={"session_id": session_id}
        )

        if not session_result:
            return {"code": -1, "msg": "Session not found", "data": {}}

        if str(session_result[0].get("user_id")) != user_id:
            return {"code": -2, "msg": "Unauthorized: You don't own this session", "data": {}}

        # Check if share session already exists
        result = await execute_query(
            """
            SELECT share_session_id, created_at, is_active
            FROM th_session_share
            WHERE session_id = :session_id AND is_active = TRUE
            """,
            params={"session_id": session_id}
        )

        if result:
            share_session_id = str(result[0].get("share_session_id"))
            created_at = result[0].get("created_at")

            logging.info(f"Returning existing share session {share_session_id} for session {session_id}")

            return {
                "code": 0,
                "msg": "ok",
                "data": {
                    "share_session_id": share_session_id,
                    "session_id": session_id,
                    "created_at": created_at.isoformat() if created_at else None,
                    "is_new": False
                }
            }

        # Create new share session
        share_session_id = str(uuid.uuid4())
        created_at = datetime.now()
        await execute_query(
            """
            INSERT INTO th_session_share (
                share_session_id, session_id, user_id, created_at, updated_at, is_active
            )
            VALUES (:share_session_id, :session_id, :user_id, :created_at, :updated_at, TRUE)
            """,
            params={
                "share_session_id": share_session_id,
                "session_id": session_id,
                "user_id": user_id,
                "created_at": created_at,
                "updated_at": created_at
            }
        )

        logging.info(f"Created new share session {share_session_id} for session {session_id}")

        return {
            "code": 0,
            "msg": "ok",
            "data": {
                "share_session_id": share_session_id,
                "session_id": session_id,
                "created_at": created_at.isoformat(),
                "is_new": True
            }
        }

    except Exception as e:
        logging.error(f"Error creating/getting share session: {str(e)}", exc_info=True)
        return {"code": -3, "msg": f"Internal error: {str(e)}", "data": {}}

#-----------------------------------------------------------------------------

async def get_shared_session_history(share_session_id: str) -> dict:
    """Chat history by share link — the unauthenticated read path.

    Returns the same ``{"history": [...]}`` shape as ``/api/history`` (via the
    same ``get_chat_history``) so the frontend renders shared and own sessions
    with one code path.
    """
    try:
        share_result = await execute_query(
            """
            SELECT session_id, user_id, created_at, is_active
            FROM th_session_share
            WHERE share_session_id = :share_session_id
            """,
            params={"share_session_id": share_session_id}
        )

        if not share_result:
            return {"code": -1, "msg": "Share session not found", "data": {}}

        session_id = share_result[0].get("session_id")
        user_id = share_result[0].get("user_id")

        if not share_result[0].get("is_active"):
            return {"code": -2, "msg": "This share session has been deactivated", "data": {}}

        from .message import get_chat_history

        history = await get_chat_history(user_id, session_id)

        logging.info(f"Retrieved {len(history)} messages for share session {share_session_id}")

        return {"code": 0, "msg": "ok", "data": {"history": history}}

    except Exception as e:
        logging.error(f"Error getting shared session history: {str(e)}", exc_info=True)
        return {"code": -4, "msg": f"Internal error: {str(e)}", "data": {}}

#-----------------------------------------------------------------------------

async def deactivate_share_session(user_id: str, session_id: str) -> dict:
    """Owner turns a share link off; the original session is untouched."""
    try:
        # Check ownership
        result = await execute_query(
            "SELECT user_id FROM th_session_share WHERE session_id = :session_id",
            params={"session_id": session_id}
        )

        if not result:
            return {"code": -1, "msg": "Share session not found", "data": {}}

        if str(result[0].get("user_id")) != user_id:
            return {"code": -2, "msg": "Unauthorized: You don't own this share session", "data": {}}

        await execute_query(
            """
            UPDATE th_session_share
            SET is_active = FALSE, updated_at = :updated_at
            WHERE session_id = :session_id
            """,
            params={"session_id": session_id, "updated_at": datetime.now()}
        )

        logging.info(f"Deactivated share session for session {session_id}")

        return {"code": 0, "msg": "Share session deactivated successfully", "data": {}}

    except Exception as e:
        logging.error(f"Error deactivating share session: {str(e)}", exc_info=True)
        return {"code": -3, "msg": f"Internal error: {str(e)}", "data": {}}

#-----------------------------------------------------------------------------

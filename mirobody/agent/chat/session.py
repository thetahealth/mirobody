import logging
import uuid

from datetime import datetime
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query
from mirobody.user.care_circle import CareCircleDenied, resolve_subject

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

async def create_session(
    user_id: str,
    query_user_id: str,
    session_id: str | None = None,
) -> dict:
    """Create a row in th_sessions, as the `{code, msg, data}` envelope.

    `session_id` is optional: without it a uuid4 is minted. A client that
    encodes pane metadata into the id (compare mode) sends its own, used
    verbatim once it fits the column and the characters such an id uses.
    """
    try:
        # `user_id` is the caller, `query_user_id` the record the session is
        # about. The check this replaced took them in the other order and every
        # call site passed them swapped to compensate.
        try:
            await resolve_subject(user_id, query_user_id)
        except CareCircleDenied:
            return {"code": -1, "msg": "You cannot open a conversation on this person's record.", "data": {}}

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
        logger.error("creating a session failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return {"code": -2, "msg": "Could not create the session.", "data": {}}

#-----------------------------------------------------------------------------

async def get_session_summaries(user_id: str) -> list[dict[str, Any]]:
    """The user's conversations that hold a text message, newest first.

    Raises on a database failure: an empty list would tell the client the
    user has no conversations."""
    result = await execute_query(
        """
        SELECT DISTINCT ts.session_id, ts.summary, ts.created_at, ts.query_user_id
        FROM th_sessions ts
        INNER JOIN th_messages tm ON ts.session_id = tm.session_id
            AND ts.user_id = tm.user_id
        WHERE ts.user_id = :user_id
            AND ts.category IS NULL
            AND tm.message_type = 'text'
        ORDER BY ts.created_at DESC
        """,
        params={"user_id": user_id},
    )
    summaries = [
        {
            "session_id": row.get("session_id"),
            "timestamp": row.get("created_at").isoformat() if row.get("created_at") else datetime.now().isoformat(),
            "summary": row.get("summary", ""),
            "query_user_id": row.get("query_user_id", ""),
        }
        for row in result
    ]
    logger.info("conversation summaries read: user_id=%s count=%d", user_id, len(summaries))
    return summaries

#-----------------------------------------------------------------------------

async def get_session_summaries_by_person(user_id: str) -> dict[str, Any]:
    """The user's conversations grouped by the person each is about, as the
    `{code, msg, data}` envelope `/api/history_by_person` answers with."""

    try:
        result = await execute_query(
            """
            SELECT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, tu.name, tu.gender, tu.birth, tu.blood
            FROM th_sessions ts
            INNER JOIN health_app_user tu ON ts.query_user_id::integer = tu.id
            WHERE ts.user_id = :user_id
            ORDER BY created_at DESC
            """,
            params={"user_id": user_id},
        )

        session_by_person: dict[tuple, list[dict[str, Any]]] = {}
        # Asked once per person, misses included: a person with no label in
        # any shared circle was asked again for every one of their sessions.
        nicknames: dict[str, str | None] = {}

        for _session in result:
            user_name = _session.get("name", "No name")
            user_gender = "Male" if _session.get("gender") == 1 else "Female" if _session.get("gender") == 2 else "Other"
            query_user_id = _session.get("query_user_id")

            # Subtracting birth year from current year (which is what stood here)
            # is wrong for everyone whose birthday has not happened yet this year:
            # roughly half of all users at any moment, each reported one year too
            # old, in the profile block that goes into the agent's context.
            from mirobody.user.user import age_from_birth
            user_age = age_from_birth(_session.get("birth", ""))
            if user_age is None:
                user_age = ""

            user_blood = _session.get("blood", "")

            if query_user_id != user_id:
                if query_user_id not in nicknames:
                    # The label this person carries in a circle the caller
                    # shares with them: one label per member, on the
                    # membership row.
                    rows = await execute_query(
                        "SELECT m.nickname FROM care_circle_members m"
                        " JOIN care_circle_members mine"
                        "   ON mine.care_circle_id = m.care_circle_id"
                        " WHERE mine.user_id = :user_id AND m.user_id = :query_user_id"
                        "   AND m.nickname IS NOT NULL"
                        "   AND mine.deleted_at IS NULL AND m.deleted_at IS NULL"
                        " LIMIT 1",
                        params={"user_id": int(user_id), "query_user_id": int(query_user_id)},
                    )
                    nicknames[query_user_id] = rows[0].get("nickname") if rows else None
                user_name = nicknames[query_user_id] or user_name

            session_by_person.setdefault((user_name, user_gender, user_age, user_blood), []).append(
                {
                    "session_id": _session.get("session_id"),
                    "query_user_id": _session.get("query_user_id"),
                    "timestamp": _session.get("created_at").isoformat() if _session.get("created_at") else datetime.now().isoformat(),
                    "summary": _session.get("summary", ""),
                }
            )

        return {
            "code": 0,
            "msg": "ok",
            "data": [
                {
                    "person_name": person_name,
                    "gender": user_gender,
                    "age": user_age,
                    "blood": user_blood,
                    "sessions": sessions,
                }
                for (person_name, user_gender, user_age, user_blood), sessions in session_by_person.items()
            ]
        }

    except Exception as e:
        logger.error("reading conversations by person failed: user_id=%s error_type=%s", user_id,
                     type(e).__name__, exc_info=not is_driver_exception(e))
        return {"code": 1, "msg": "Could not load the conversations.", "data": []}

#-----------------------------------------------------------------------------

async def delete_session(user_id: str, session_id: str) -> str | None:
    """Delete one conversation everywhere it is kept. Returns None, or the
    sentence to show when it could not be deleted."""
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

        # And the agent's own copy. The agent's conversation memory is the
        # LangGraph checkpointer keyed on `thread_for(owner, session_id)`, so the two
        # deletes above would otherwise leave the same turns (the health
        # questions and the tool results answering them) sitting in the
        # checkpoint tables under this session id. Deleting a conversation has
        # to delete it, not just stop listing it. Best-effort by design (see
        # agent.checkpointer.delete_thread): the user-visible rows are already
        # gone, and a checkpoint-cleanup failure must not turn that into an error.
        from mirobody.agent.checkpointer import delete_thread, thread_for
        await delete_thread(thread_for(user_id, session_id))

        from mirobody.collect import forget_rids
        await forget_rids(thread_for(user_id, session_id))

        return None

    except Exception as e:
        logger.error("deleting a session failed: user_id=%s session_id=%s error_type=%s",
                     user_id, session_id, type(e).__name__, exc_info=not is_driver_exception(e))
        return "Could not delete the conversation."

#-----------------------------------------------------------------------------

#: rids one request may resolve: an answer cites a few dozen at most.
MAX_RIDS = 200


async def resolve_citations(user_id: str, session_id: str, rids: list[str]) -> dict:
    """What the rids an answer cites stand for, as the `{code, msg, data}`
    envelope. Only the conversation's owner, and only while they may still
    read the record it is about: a care-circle share revoked since is a denial."""
    rows = await execute_query(
        "SELECT user_id, query_user_id FROM th_sessions WHERE session_id = :sid",
        {"sid": session_id}, log_sql=False) or []
    if not rows or str(rows[0]["user_id"]) != str(user_id):
        return {"code": -1, "msg": "No such conversation.", "data": []}
    subject = rows[0]["query_user_id"] or user_id
    try:
        await resolve_subject(user_id, subject)
    except CareCircleDenied:
        return {"code": -2, "msg": "You can no longer read this record.", "data": []}
    from mirobody.agent.checkpointer import thread_for
    from mirobody.collect import resolve_rids
    resolved = await resolve_rids(thread_for(user_id, session_id), str(subject), rids[:MAX_RIDS])
    return {"code": 0, "msg": "ok", "data": resolved}

#-----------------------------------------------------------------------------
# Public session sharing (th_session_share): owner-created share links whose
# history is then served WITHOUT authentication (see session_share_router).
# Formerly a separate session_share.py holding a SessionShareService class of
# three staticmethods and a no-state module instance: plain module functions
# are this module's idiom, and sessions/share-links are one lifecycle.
#-----------------------------------------------------------------------------

async def create_or_get_share_session(user_id: str, session_id: str) -> dict:
    """Create a share link for a session the caller owns, or return the
    existing active one (idempotent: one active share link per session)."""
    try:
        session_result = await execute_query(
            "SELECT user_id FROM th_sessions WHERE session_id = :session_id",
            params={"session_id": session_id}
        )

        if not session_result:
            return {"code": -1, "msg": "Session not found", "data": {}}

        if str(session_result[0].get("user_id")) != user_id:
            return {"code": -2, "msg": "Unauthorized: You don't own this session", "data": {}}

        existing = await _active_share(session_id)
        if existing:
            return _share_answer(existing, session_id, is_new=False)

        # `session_id` is UNIQUE, so a link that was turned off still holds the
        # session's row, and a plain INSERT of a new link failed on it: once
        # stopped, a conversation could never be shared again. The row gets a
        # fresh id instead, so the old link stays dead. An active row is left
        # alone: that is another request having just shared it.
        created = await execute_query(
            """
            INSERT INTO th_session_share (
                share_session_id, session_id, user_id, created_at, updated_at, is_active
            )
            VALUES (:share_session_id, :session_id, :user_id, :now, :now, TRUE)
            ON CONFLICT (session_id) DO UPDATE
               SET share_session_id = EXCLUDED.share_session_id,
                   created_at = EXCLUDED.created_at,
                   updated_at = EXCLUDED.updated_at,
                   is_active = TRUE
             WHERE th_session_share.is_active = FALSE
            RETURNING share_session_id, created_at
            """,
            params={"share_session_id": str(uuid.uuid4()), "session_id": session_id,
                    "user_id": user_id, "now": datetime.now()},
        )
        if created:
            logger.info("share link created: session_id=%s", session_id)
            return _share_answer(created[0], session_id, is_new=True)
        return _share_answer(await _active_share(session_id) or {}, session_id, is_new=False)

    except Exception as e:
        logger.error("creating a share link failed: session_id=%s error_type=%s", session_id,
                     type(e).__name__, exc_info=not is_driver_exception(e))
        return {"code": -3, "msg": "Could not create the share link.", "data": {}}


async def _active_share(session_id: str) -> dict | None:
    rows = await execute_query(
        "SELECT share_session_id, created_at FROM th_session_share"
        " WHERE session_id = :session_id AND is_active = TRUE",
        params={"session_id": session_id},
    )
    return rows[0] if rows else None


def _share_answer(row: dict, session_id: str, *, is_new: bool) -> dict:
    created_at = row.get("created_at")
    return {
        "code": 0,
        "msg": "ok",
        "data": {
            "share_session_id": str(row.get("share_session_id") or ""),
            "session_id": session_id,
            "created_at": created_at.isoformat() if created_at else None,
            "is_new": is_new,
        },
    }

#-----------------------------------------------------------------------------

async def get_shared_session_history(share_session_id: str) -> dict:
    """Chat history by share link: the unauthenticated read path.

    Returns the same ``{"history": [...]}`` shape as ``/api/history`` (via the
    same ``get_chat_history``) so the frontend renders shared and own sessions
    with one code path.
    """
    # The column is a UUID: anything else is no link, and used to reach the
    # database and come back as its error text, to an unauthenticated caller.
    try:
        uuid.UUID(str(share_session_id))
    except ValueError:
        return {"code": -1, "msg": "Share session not found", "data": {}}
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
        logger.info("shared history read: session_id=%s message_count=%d", session_id, len(history))
        return {"code": 0, "msg": "ok", "data": {"history": history}}

    except Exception as e:
        logger.error("reading a shared history failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return {"code": -4, "msg": "Could not load the shared conversation.", "data": {}}

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

        logger.info("share link deactivated: session_id=%s", session_id)

        return {"code": 0, "msg": "Share session deactivated successfully", "data": {}}

    except Exception as e:
        logger.error("deactivating a share link failed: session_id=%s error_type=%s", session_id,
                     type(e).__name__, exc_info=not is_driver_exception(e))
        return {"code": -3, "msg": "Could not stop sharing the conversation.", "data": {}}

#-----------------------------------------------------------------------------

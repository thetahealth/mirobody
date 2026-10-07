"""The one-line summary a chat session carries.

Written when a file is uploaded through chat: the first user message becomes
the session's title in the sidebar, so a person scanning their history sees
"blood panel, March" instead of a uuid.
"""

import logging
from datetime import datetime
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


async def generate_summary(user_message: str, provider: str) -> str:
    """Generate session summary"""
    # Simplified implementation, can actually call AI model to generate better summary
    return user_message[:10] + "..." if len(user_message) > 10 else user_message

async def generate_and_save_summary(
    user_id: str, session_id: str, user_message: str, provider: str
) -> dict[str, Any] | None:
    """Asynchronously generate and save summary"""
    try:
        # Generate summary
        summary = await generate_summary(user_message, provider)

        # Save summary
        await save_conversation_summary(user_id, session_id, summary)

        return {"event": "summary_generated", "session_id": session_id}

    except Exception as e:
        logger.error("session summary failed: session_id=%s error_type=%s", session_id, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return None

async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
    """Save conversation summary to database"""
    try:
        # Insert summary, do nothing on conflict
        summary_sql = """
            INSERT INTO th_sessions (
                user_id, session_id, summary, created_at
            )
            VALUES (:user_id, :session_id, :summary, :created_at) 
            ON CONFLICT (session_id) DO NOTHING RETURNING session_id
        """

        await execute_query(
            query=summary_sql,
            params={
                "user_id": user_id,
                "session_id": session_id,
                "summary": summary,
                "created_at": datetime.now(),
            },
        )

        return True

    except Exception as e:
        logger.error("saving a session summary failed: session_id=%s error_type=%s", session_id, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return False

"""The one-line summary a chat session carries, and the message it hangs on.

Written when a file is uploaded through chat: the first user message becomes
the session's title in the sidebar, so a person scanning their history sees
"blood panel, March" instead of a uuid.
"""

import logging
from datetime import datetime
from typing import Any

from mirobody.utils import execute_query
from mirobody.utils.coerce import safe_json_dumps

logger = logging.getLogger(__name__)


async def update_message_content(
    message_id: str,
    content: str = None,
    reasoning: str = None,
    message_type: str = None,
) -> bool:
    """Update message content, reasoning or type.

    There was a fourth field, `comment`, that no caller ever passed, and
    `th_messages.comment` is no longer part of the schema (50_chat.sql), so
    writing it would now fail on a fresh database.
    """
    try:
        update_fields = []
        params = {"message_id": message_id}

        if content is not None:
            update_fields.append("content = encrypt_content(:content)")
            params["content"] = safe_json_dumps(content) if isinstance(content, (dict, list)) else content

        if reasoning is not None:
            update_fields.append("reasoning = :reasoning")
            params["reasoning"] = safe_json_dumps(reasoning) if isinstance(reasoning, (dict, list)) else reasoning

        if message_type is not None:
            update_fields.append("message_type = :message_type")
            params["message_type"] = message_type

        if not update_fields:
            return False

        update_sql = f"""
            UPDATE th_messages SET {", ".join(update_fields)} WHERE id = :message_id
        """

        update_result = await execute_query(
            query=update_sql,
            params=params,
        )

        logger.info(f"[DB] Updated message {message_id} with fields: {', '.join(update_fields)}, result: {update_result}")
        return True
    except Exception as e:
        logger.error(f"Error updating message: {e}", stack_info=True)
        return False

async def generate_summary(user_message: str, provider: str) -> str:
    """Generate session summary"""
    # Simplified implementation, can actually call AI model to generate better summary
    return user_message[:10] + "..." if len(user_message) > 10 else user_message

async def generate_and_save_summary(
    user_id: str, session_id: str, user_message: str, provider: str
) -> dict[str, Any]:
    """Asynchronously generate and save summary"""
    try:
        # Generate summary
        summary = await generate_summary(user_message, provider)

        # Save summary
        await save_conversation_summary(user_id, session_id, summary)

        logger.info(f"session:{session_id}\tSuccessfully saved conversation summary!")
        return {"event": "summary_generated", "session_id": session_id}

    except Exception as e:
        logger.error(f"Error in generate_and_save_summary: {str(e)}", stack_info=True)

        return None

async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
    """Save conversation summary to database"""
    try:
        logger.info(f"save_conversation_summary: {user_id}, {session_id}, {summary}")
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
        logger.error(f"Error saving conversation summary: {str(e)}", stack_info=True)
        return False

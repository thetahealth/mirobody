"""
Chat Summary Generation Module

Provides conversation summary generation using the unified LLM interface.
Automatically selects the best available LLM provider.
"""

import json
import logging
from datetime import datetime
from typing import Any
from ...utils import execute_query
from ...utils.llm import async_get_text_completion
from ...utils.llm_output import strip_code_fence, strip_wrapping

logger = logging.getLogger(__name__)


#-----------------------------------------------------------------------------

def _reply_text(content):
    """Flatten a stored element_list down to just its reply text.

    A message row holds either plain text or the JSON element_list the adapter
    persisted (reply / thinking / tool chunks). For a title we want only what
    was actually said, so non-reply elements are dropped. Anything that is not
    that shape is returned untouched — this runs over very old rows too.
    """
    if not isinstance(content, str):
        return content
    try:
        parsed = json.loads(content)
    except Exception:
        return content
    if not isinstance(parsed, list):
        return content
    return "".join(
        block.get("content", "") for block in parsed if block.get("type") == "reply"
    )
async def generate_summary(conversation_text: str, provider: str | None = None) -> str:
    """Generate a topic title from the user's question only."""
    try:
        if len(conversation_text.strip()) < 5:
            return conversation_text.strip()

        prompt = f"""Write a short topic title for the user's question in this conversation.

                Rules:
                - Title the USER'S QUESTION ONLY. Do NOT include the assistant's
                  answer, diagnosis, conclusion, numbers, or interpretation.
                - If multiple user turns appear, anchor on the first non-trivial one.
                - Max 50 characters.
                - Same language as the user's question.
                - Output only the title text — no quotes, no explanations, no prefixes.

                Conversation:
                {conversation_text[:1000]}

                Title:"""
        result = await async_get_text_completion(
            messages=[{"role": "user", "content": prompt}],
            provider=provider,
            temperature=0,
            max_tokens=100,
        )

        # The prompt says "no quotes, no prefixes"; models add both anyway.
        # Peeled here, by the one implementation, rather than banned there.
        summary = strip_wrapping(strip_code_fence(result))

        if len(summary) > 60:
            summary = summary[:57] + "..."

        logger.info("Generated summary (chars=%d)", len(summary))
        return summary
            
    except Exception as e:
        logger.warning(f"Failed to generate LLM summary, falling back to simple truncation: {str(e)}")

        first_line = conversation_text.split('\n')[0]
        return first_line[:50].replace("User:", " ").replace("Assistant:", " ") + "..." if len(first_line) > 50 else first_line

#-----------------------------------------------------------------------------

async def generate_and_save_summary(user_id: str, session_id: str, provider: str | None = None) -> dict[str, Any]:
    try:
        messages_sql = """
            SELECT role, decrypt_content(content) AS content, created_at
            FROM th_messages
            WHERE session_id = :session_id AND user_id = :user_id
            ORDER BY created_at ASC
            LIMIT 10
        """
        
        messages = await execute_query(
            messages_sql,
            params={"session_id": session_id, "user_id": user_id}
        )
        
        if not messages:
            logger.warning(f"No messages found for session {session_id}")
            return None

        # We only feed user turns into the summarizer so the LLM can't
        # accidentally lift the assistant's answer/diagnosis into the
        # title. In compare mode every pane sees the same user question,
        # so this keeps sibling sessions' summaries aligned.
        conversation_text = ""
        for msg in messages[:10]:
            role = msg.get("role", "")
            if role != "user":
                continue
            content = msg.get("content", "")

            content = _reply_text(content)

            conversation_text += f"User: {content}\n\n"

        # Fallback: if for some reason there were no user-role rows (very old
        # legacy sessions sometimes mis-tag), fall back to the original mixed
        # behaviour so we still produce some title rather than empty string.
        if not conversation_text.strip():
            for msg in messages[:10]:
                role = msg.get("role", "")
                content = msg.get("content", "")
                content = _reply_text(content)
                role_label = "User" if role == "user" else "Assistant"
                conversation_text += f"{role_label}: {content}\n\n"
        
        summary = await generate_summary(conversation_text.strip(), provider=provider)

        await save_conversation_summary(user_id, session_id, summary)

        logger.info(f"session:{session_id}\tSuccessfully saved conversation summary: {summary}")
        return {"event": "summary_generated", "session_id": session_id, "summary": summary}

    except Exception as e:
        logger.error(f"Error in generate_and_save_summary: {str(e)}", exc_info=True)
        return None

#-----------------------------------------------------------------------------

async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
    try:
        logger.info(f"save_conversation_summary: {user_id}, {session_id}, {summary}")

        summary_sql = """
            INSERT INTO th_sessions (
                user_id, session_id, summary, created_at, in_use
            )
            VALUES (:user_id, :session_id, :summary, :created_at, :in_use) 
            ON CONFLICT (session_id) DO UPDATE SET
                summary = EXCLUDED.summary,
                in_use = TRUE
            RETURNING session_id
        """

        await execute_query(
            summary_sql,
            params={
                "user_id": user_id,
                "session_id": session_id,
                "summary": summary,
                "created_at": datetime.now(),
                "in_use": True,
            }
        )

        return True

    except Exception as e:
        logger.error(f"Error saving conversation summary: {str(e)}", exc_info=True)
        return False

#-----------------------------------------------------------------------------

"""`th_messages` reads and writes for the chat layer.

The durable, queryable transcript that `/api/history` and session sharing
render. It is NOT the agent's conversation memory: that is the LangGraph
checkpointer (agent/checkpointer.py), keyed on thread_id = session_id.
"""

import json
import logging
import uuid

from datetime import datetime
from typing import Any

from ...utils import execute_query

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

def parse_stored_content(raw: Any) -> Any:
    """Parse a ``th_messages.content`` value, or return None if it is not JSON.

    ``save_message`` writes this column, and it writes exactly two shapes: a
    ``json.dumps`` of the assistant's element_list / the user's file bubble, or
    a plain string (the user's question). So strict parsing is the whole job —
    valid JSON parses, prose does not, and there is no third case.

    This replaced a 78-line "repair" pair (``repair_json_string`` /
    ``safe_json_parse``) written for *LLM output*: it stripped markdown fences,
    swapped smart quotes, removed trailing commas, and — the damaging part —
    fell back to pulling the first ``[...]``/``{...}`` substring out of the
    text. Nothing here parses model output; the sole caller reads back what
    this module itself wrote. On its own writer's output every repair step was
    a no-op, and on the other shape it was actively wrong: a user asking
    ``这个配置 {"files": [1,2]} 对吗？`` had their question parsed to ``[1, 2]``
    and shipped to the client as that message's ``content_dict``.
    """
    if not isinstance(raw, str):
        return raw if isinstance(raw, (list, dict)) else None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------

async def save_message(
    user_id: str,
    query_user_id: str,
    content: Any,
    role: str,
    session_id: str,  # For WS: trace_id, For HTTP: real session_id
    scene: str,       # 'app' for WS, 'web'/'h5' for HTTP
    agent: str = "default",
    msg_id: str | None = None,
    question_id: str | None = None,
    message_type: str = "text",
    provider: str | None = None,
    **kwargs
) -> str:
    """
    Unified message saving function for all protocols
    
    The key differences between protocols are:
    - session_id: WS uses trace_id, HTTP uses real session_id
    - scene: WS uses 'app', HTTP uses 'web' or other values
    
    Everything else should be the same.
    """
    if msg_id is None or msg_id == "":
        msg_id = f"app_{uuid.uuid4()}"
    
    sql = """
        INSERT INTO th_messages 
        (id, user_id, query_user_id, session_id, role, content, 
         agent, message_type, scene, created_at, question_id, provider)
        VALUES 
        (:id, :user_id, :query_user_id, :session_id, :role, encrypt_content(:content),
         :agent, :message_type, :scene, NOW(), :question_id, :provider)
        ON CONFLICT (id) DO NOTHING RETURNING id
    """
    
    # Handle content serialization
    if isinstance(content, (dict, list)):
        content_str = json.dumps(content, ensure_ascii=False)
    else:
        content_str = str(content)
    
    await execute_query(
        sql,
        params={
            "id": msg_id,
            "user_id": user_id,
            "query_user_id": query_user_id,
            "session_id": session_id,
            "role": role,
            "content": content_str,
            "agent": agent,
            "message_type": message_type,
            "scene": scene,
            "question_id": question_id,
            "provider": provider
        }
    )
    
    logger.info(f"Saved message: id={msg_id}, role={role}, scene={scene}, session_id={session_id}")
    
    return msg_id

#-----------------------------------------------------------------------------

async def set_message_rating(user_id: str, message_id: str, rating: int) -> bool:
    """
    Set the user's rating on a chat message (th_messages.rating).

    The web client rates an assistant response by its message id (responseId,
    as returned by get_chat_history). Scoped by user_id so a user can only
    rate their own messages. Returns True if a row was updated.
    """
    sql = """
        UPDATE th_messages
        SET rating = :rating
        WHERE id = :id AND user_id = :user_id
        RETURNING id
    """
    record = await execute_query(
        sql,
        params={"id": message_id, "user_id": user_id, "rating": rating},
    )
    return bool(record)

#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------

async def _refresh_file_urls_in_content(content_json_obj: Any) -> None:
    """
    Re-sign expired storage URLs in-place for chat-history payloads.

    th_messages.content captures the signed URL at write time, but signed
    URLs expire (S3 default ~24h). file_key is durable, so on the read path
    we re-derive a fresh URL from it.

    Only ONE content shape carries files: the user upload bubble (dict with
    `files: [...]`). Refresh each entry's url_full / url_thumb / file_url, then
    rebuild the top-level url_thumb / url_full arrays the frontend may read
    instead of files[*].

    There used to be a second branch here re-signing assistant "chart bubbles"
    (`{type: "image"}` chunks). Nothing emits those any more: the PNG-rendering
    ChartService tools were removed and the agent charts by writing a fenced
    ```vis-chart``` block inline in its reply text, which needs no URL at all.
    """
    if not content_json_obj:
        return

    from ...pulse.file_parser.services.database_services import FileParserDatabaseService
    from ...pulse.file_parser.services.db_utils import get_mime_type

    async def _sign(file_key: str, file_name: str = "") -> str:
        if not file_key:
            return ""
        content_type = get_mime_type(file_name) if file_name else "application/octet-stream"
        return await FileParserDatabaseService.regenerate_file_url(
            file_key, file_name, content_type
        )

    if isinstance(content_json_obj, dict) and content_json_obj.get("files"):
        files = content_json_obj.get("files") or []
        new_thumbs: list[str] = []
        new_fulls: list[str] = []
        for file_info in files:
            if not isinstance(file_info, dict):
                continue
            file_key = file_info.get("file_key", "")
            file_name = (
                file_info.get("file_name")
                or file_info.get("filename")
                or file_info.get("original_filename")
                or ""
            )
            new_url = await _sign(file_key, file_name)
            if new_url:
                file_info["url_full"] = new_url
                file_info["url_thumb"] = new_url
                file_info["file_url"] = new_url
            new_thumbs.append(file_info.get("url_thumb", "") or "")
            new_fulls.append(file_info.get("url_full", "") or "")
        if "url_thumb" in content_json_obj:
            content_json_obj["url_thumb"] = new_thumbs
        if "url_full" in content_json_obj:
            content_json_obj["url_full"] = new_fulls

#-----------------------------------------------------------------------------

async def get_chat_history(user_id: str, session_id: str) -> list[dict[str, Any]]:
    """Load chat history for given session from database.

    Renderable message types only. There used to be a `filter_message_type`
    flag selecting between this query and an unfiltered one, and it was
    inverted: passing True removed the filter, while the default False applied
    it. No caller ever passed it, so the flag was a trap with one live branch —
    dropped along with the branch.
    """
    history = []
    try:
        # `input_prompt` used to be selected here and surfaced on the response
        # when truthy. Nothing in the project ever writes that column — not
        # save_message, not the one UPDATE path (file_parser's
        # update_message_content, which can set content/reasoning/message_type)
        # — so it is NULL on every row and the branch never fired.
        session_sql = """
            SELECT
                id, decrypt_content(content) AS content, reasoning, role, agent, provider,
                created_at, rating, question_id, message_type
            FROM th_messages
            WHERE user_id = :user_id AND session_id = :session_id
              AND message_type in ('text', 'file', 'pdf', 'image')
            ORDER BY created_at ASC
        """
        db_messages = await execute_query(
            session_sql,
            params={"user_id": user_id, "session_id": session_id}
        )

        if db_messages:
            user_messages = []
            agent_responses = {}

            for msg in db_messages:
                content = msg.get("content", "")
                content_json_obj = parse_stored_content(msg.get("content", ""))
                thinking_chunks = []
                if isinstance(content_json_obj, list) and msg.get("message_type") == "text":
                    blocks = [b for b in content_json_obj if isinstance(b, dict)]
                    content = "".join(b.get("content", "") for b in blocks if b.get("type") == "reply")
                    thinking_chunks = [
                        b for b in blocks
                        if b.get("type") in ("thinking", "queryTitle", "queryArguments", "queryDetail")
                    ]

                try:
                    await _refresh_file_urls_in_content(content_json_obj)
                except Exception as e:
                    logger.error(f"Error regenerating file URLs: {str(e)}")

                message = {
                    "role": msg.get("role", "assistant"),
                    "content": content,
                    "content_dict": content_json_obj if content_json_obj else [],
                    "timestamp": (
                        msg.get("created_at").isoformat() if msg.get("created_at") else datetime.now().isoformat()
                    ),
                    "id": msg.get("id"),
                    "provider": msg.get("provider", ""),  # Always include provider field
                    "thinking_chunks": thinking_chunks,
                }

                if msg.get("reasoning"):
                    message["reasoning"] = msg.get("reasoning")

                if msg.get("agent"):
                    message["agent"] = msg.get("agent")

                if msg.get("rating") is not None:
                    message["rating"] = msg.get("rating")

                if msg.get("message_type"):
                    message["messageType"] = msg.get("message_type")

                question_id = msg.get("question_id")
                if question_id:
                    message["questionId"] = question_id

                if message["role"] == "user":
                    user_messages.append(message)
                else:
                    key = question_id if question_id else msg.get("id")
                    if key not in agent_responses:
                        agent_responses[key] = []
                    agent_responses[key].append(message)

            user_messages.sort(key=lambda x: x["timestamp"])

            # Every user message is returned. There used to be a
            # `if not user_msg.get("provider"): continue` here, labelled
            # "filter duplicate messages": a workaround for a historical
            # duplicate-write bug, which dropped any user message whose
            # `provider` was empty. The bug is gone; the workaround was not,
            # and it silently hid every user message written by a path that
            # does not set `provider` — a filter on the wrong field for a
            # problem that no longer exists.
            for user_msg in user_messages:
                history.append(user_msg)

                question_id = user_msg.get("questionId")
                msg_id = user_msg.get("id")

                if question_id and question_id in agent_responses:
                    history.extend(agent_responses[question_id])
                elif msg_id in agent_responses:
                    history.extend(agent_responses[msg_id])

            logger.info(f"session:{session_id}\tmessage_cnt:{len(history)} Successfully loaded")

    except Exception as e:
        logger.error(f"Error loading conversation history: {str(e)}", exc_info=True)

    return history

#-----------------------------------------------------------------------------

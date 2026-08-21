"""
Unified database operations for chat adapters
All adapters should use these functions to ensure consistency
"""

import json
import logging
import re
import uuid

from datetime import datetime
from typing import Any

from ...utils import execute_query
from ..base.history_replay import fold_trace_into_text

#-----------------------------------------------------------------------------



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
    
    logging.info(f"Saved message: id={msg_id}, role={role}, scene={scene}, session_id={session_id}")
    
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

async def get_last_message(user_id: str, query_user_id: str = None, session_id: str = None, scene="app") -> list:
    """Recent turns of a conversation, flattened to text — BaseAgent's replay.

    DeepAgent does NOT come through here: its history is the LangGraph
    checkpointer (thread_id = session_id). This exists for BaseAgent, which has
    no graph, and so the rows are flattened to ``{role, agent, content}`` — the
    only fields ``compress_messages`` reads — plus ``created_at`` for the
    resume-gap hint.

    Trimmed of three dead parameters/fields in the process: ``include_all``
    (a second, never-requested SQL branch dropping the query_user_id scope) and
    ``db_mode`` (read by nothing at all) were never passed by any caller, and
    the returned ``element_list``/``th_msg_id``/``reference_task_id`` lost their
    only consumer when canonical replay moved to the checkpointer.
    """
    if query_user_id is None:
        query_user_id = user_id

    # session_id comes from the HTTP request (client-controlled) — must be a
    # bind param, never f-string interpolated, to prevent SQL injection.
    session_phrase = "and session_id = :session_id" if session_id else ""

    messages = []
    try:
        sql = f"""
            select role, agent, decrypt_content(content) as content, created_at
            from th_messages
            where user_id = :user_id and query_user_id = :query_user_id
              and scene = :scene and is_del = false {session_phrase}
            order by created_at desc limit 15
        """
        params = {"user_id": user_id, "query_user_id": query_user_id, "scene": scene}
        if session_id:
            params["session_id"] = session_id

        rows = await execute_query(sql, params)

        for i in range(len(rows) - 1, -1, -1):
            m = rows[i]

            # Assistant content is the persisted element_list; flatten it to the
            # `reply` text. A plain string (the user's question, or a legacy row)
            # parses to None and is used as-is.
            element_list = parse_stored_content(m["content"])
            if isinstance(element_list, list):
                content = "".join(
                    e.get("content", "")
                    for e in element_list
                    if isinstance(e, dict) and e.get("type") == "reply"
                )
                # Fold a one-line tool/file trace into the replayed assistant
                # text so the next turn knows it already read /uploads/* and
                # doesn't re-run read_file. Excerpt only, never the full payload.
                if m["role"] == "assistant":
                    content = fold_trace_into_text(content, element_list)
            else:
                content = m["content"]

            messages.append(
                dict(
                    role        = m["role"],
                    agent       = m["agent"],
                    content     = content,
                    created_at  = m["created_at"],
                )
            )

        return messages

    except Exception as e:
        logging.error(str(e), exc_info=True)
        return []

#-----------------------------------------------------------------------------

def compress_messages(agent, messages: list[dict[str, Any]], max_tokens: int = 4000) -> list[dict[str, Any]]:
    """
    Compress message history by keeping only the most recent messages when token limit is exceeded.
    
    Strategy:
    1. First, filter to keep only messages from the specified agent
    2. Calculate total token count for all messages
    3. If limit is exceeded, keep only the most recent messages
    4. Ensure final result is sorted chronologically (oldest to newest)
    
    Args:
        agent: Agent identifier to filter messages by
        messages: List of message dictionaries
        max_tokens: Maximum token limit (default: 4000)
        
    Returns:
        Compressed list of messages sorted chronologically
    """
    if not messages:
        return []
    
    agent_messages = []
    
    # Process messages: user messages are added directly, 
    # consecutive assistant messages are grouped, 
    # from each group prioritize messages matching the agent field, 
    # otherwise take the last message in the group
    i = 0
    while i < len(messages):
        msg = messages[i]
        
        # Add user messages directly
        if msg.get("role") == "user":
            agent_messages.append(msg)
            i += 1
        # Group consecutive assistant messages
        elif msg.get("role") == "assistant":
            # Collect consecutive assistant messages
            assistant_group = []
            while i < len(messages) and messages[i].get("role") == "assistant":
                assistant_group.append(messages[i])
                i += 1
            
            # Select one message from this group
            # Prioritize messages with matching agent field
            selected_msg = None
            for assistant_msg in assistant_group:
                if assistant_msg.get("agent") == agent:
                    selected_msg = assistant_msg
                    break
            
            # If no matching agent found, select the last message in the group
            if selected_msg is None and assistant_group:
                selected_msg = assistant_group[-1]
            
            if selected_msg:
                agent_messages.append(selected_msg)
        else:
            # Skip other message types
            i += 1

    # Estimate token count (rough estimate: ~1.5 tokens per Chinese character, ~1.3 tokens per English word)
    def estimate_tokens(text: str) -> int:
        chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", text))
        english_words = len(re.findall(r"\b\w+\b", text))
        return int(chinese_chars * 1.5 + english_words * 1.3)

    # Calculate token count for a single message
    def count_message_tokens(message: dict[str, Any]) -> int:
        content = message.get("content", "")
        return estimate_tokens(content)

    # Calculate total token count for all messages
    total_tokens = sum(count_message_tokens(msg) for msg in agent_messages)

    # R2: do NOT prefix each message with "[timestamp] ". The current time is
    # already in the system prompt, and prefixing every history line with the
    # *latest* message's timestamp (the old code reused the loop's trailing `msg`,
    # so all lines got the same, newest stamp — a bug) made the history segment
    # change byte-for-byte every turn, defeating prompt caching. Replayed history
    # is now stable across turns so its prefix can be cached.

    # If total tokens exceed limit, keep only the most recent messages
    if total_tokens > max_tokens:
        # Start from the most recent messages and add until approaching token limit
        compressed_messages = []
        current_tokens = 0

        # Iterate from newest to oldest (reversed order)
        for msg in reversed(agent_messages):
            tokens = count_message_tokens(msg)
            if current_tokens + tokens <= max_tokens:
                compressed_messages.append(
                    {
                        "role": msg.get("role", "unknown"),
                        "content": msg.get("content", ""),
                    }
                )
                current_tokens += tokens
            else:
                # If a single message exceeds remaining limit, try truncating content
                if tokens > (max_tokens - current_tokens) * 0.5:
                    content = msg.get("content", "")
                    # Keep the first half of the message
                    truncated_content = content[: len(content) // 2] + "...(truncated)"
                    compressed_messages.append(
                        {
                            "role": msg.get("role", "unknown"),
                            "content": truncated_content,
                        }
                    )
                break

        # Reverse back to chronological order (oldest to newest)
        compressed_messages = list(reversed(compressed_messages))
    else:
        # If limit not exceeded, keep only role and content fields
        compressed_messages = [
            {"role": msg.get("role", "unknown"), "content": msg.get("content", "")} for msg in agent_messages
        ]

    return compressed_messages

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
    ChartService tools were removed and DeepAgent charts by writing a fenced
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
                    logging.error(f"Error regenerating file URLs: {str(e)}")

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

            logging.info(f"session:{session_id}\tmessage_cnt:{len(history)} Successfully loaded")

    except Exception as e:
        logging.error(f"Error loading conversation history: {str(e)}", exc_info=True)

    return history

#-----------------------------------------------------------------------------

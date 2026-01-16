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
from redis.asyncio import Redis

from ..utils import execute_query
from ..utils.config import global_config

#-----------------------------------------------------------------------------

REDIS_CHAT_LIST_KEY_HOLYWELL = "redis_chat_list_hollywell"

class MessageType:
    text = "text"
    external_chat = "external_chat"
    file = "file"

#-----------------------------------------------------------------------------

def repair_json_string(json_str: str) -> str:
    """
    Attempt to repair common JSON formatting issues in LLM responses
    
    Args:
        json_str: The potentially malformed JSON string
        
    Returns:
        Repaired JSON string
    """
    # Remove any markdown code block markers
    clean = json_str.strip()
    if clean.startswith('```json'):
        clean = clean[7:]
    elif clean.startswith('```'):
        clean = clean[3:]
    
    if clean.endswith('```'):
        clean = clean[:-3]
    
    clean = clean.strip()
    
    # Fix common quote issues
    clean = clean.replace('"', '"').replace('"', '"')  # Smart quotes
    clean = clean.replace("'", "'").replace("'", "'")  # Smart apostrophes
    
    # Remove trailing commas before closing brackets/braces
    clean = re.sub(r',(\s*[}\]])', r'\1', clean)
    
    # Fix unescaped quotes in strings (basic attempt)
    # This is a simple heuristic and may not work in all cases
    clean = re.sub(r'(?<!\\)"(?=.*":)', '\\"', clean)
    
    # Remove any control characters that might break JSON
    clean = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', clean)
    
    return clean


def safe_json_parse(json_str: str, fallback_value: Any = None) -> Any:
    """
    Safely parse JSON with repair attempts and fallback
    
    Args:
        json_str: The JSON string to parse
        fallback_value: Value to return if parsing fails
        
    Returns:
        Parsed JSON object or fallback_value
    """
    # Try parsing as-is first
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    
    # Try with basic cleaning
    try:
        cleaned = repair_json_string(json_str)
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    
    # Try to extract JSON from within the text (in case there's extra text)
    try:
        # Look for JSON array or object patterns
        array_match = re.search(r'\[.*\]', json_str, re.DOTALL)
        if array_match:
            return json.loads(repair_json_string(array_match.group()))
            
        obj_match = re.search(r'\{.*\}', json_str, re.DOTALL)
        if obj_match:
            return json.loads(repair_json_string(obj_match.group()))
    except json.JSONDecodeError:
        pass
    
    # If all else fails, return fallback
    return fallback_value

#-----------------------------------------------------------------------------

async def create_message(
    user_id: int,
    trace_id: str,
    content: str,
    agent: str,
    query_user_id: str | None = None,
    role: str = 'user',
    reference_task_id: str | None = None
) -> str:
    """
    Create a new message in the database
    
    Args:
        user_id: The user ID
        trace_id: The trace/session ID
        content: Message content
        agent: Agent identifier
        query_user_id: Query user ID (defaults to user_id if not provided)
        role: Message role ('user' or 'assistant')
    
    Returns:
        The created message ID
    """
    if query_user_id is None:
        query_user_id = user_id
    
    question_id = "app_" + str(uuid.uuid4())
    
    sql = """
        INSERT INTO theta_ai.th_messages 
        (id, user_id, query_user_id, session_id, role, content, agent, message_type, scene, created_at, reference_task_id) 
        VALUES (:id, :user_id, :query_user_id, :session_id, :role, theta_ai.encrypt_content(:content), :agent, :message_type, :scene, now(), :reference_task_id) 
        RETURNING id
    """
    record = await execute_query(
        sql,
        params={
            "id": question_id, 
            "user_id": user_id, 
            "query_user_id": query_user_id,
            "session_id": trace_id, 
            "role": role, 
            "content": content, 
            "agent": agent, 
            "message_type": "text",
            "scene": "app",
            "reference_task_id": reference_task_id
        },
        query_type="insert",
        mode="async",
    )
    return record.get("id")

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
        INSERT INTO theta_ai.th_messages 
        (id, user_id, query_user_id, session_id, role, content, 
         agent, message_type, scene, created_at, question_id, provider)
        VALUES 
        (:id, :user_id, :query_user_id, :session_id, :role, theta_ai.encrypt_content(:content),
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

async def get_last_message(user_id: str, query_user_id: str = None, session_id: str = None, include_all=False, db_mode: str = "sync", scene="app") -> list:
    if query_user_id is None:
        query_user_id = user_id
    
    if session_id:
        session_phrase = f"and session_id = '{session_id}'"
    else:
        session_phrase = ""
    
    messages = []
    try:
        if not include_all:
            sql = f"""
                select id, role, agent, theta_ai.decrypt_content(content) as content, reference_task_id, created_at from theta_ai.th_messages where user_id = :user_id and query_user_id = :query_user_id and scene = :scene and is_del = false {session_phrase} order by created_at desc limit 15
            """
            params = {"user_id": user_id, "query_user_id": query_user_id, "scene": scene}
            rows = await execute_query(sql, params)
        else:
            sql = f"""
                select id, role, agent, theta_ai.decrypt_content(content) as content, reference_task_id, created_at from theta_ai.th_messages where user_id = :user_id and scene = :scene and is_del = false {session_phrase} order by created_at desc limit 15
            """
            params = {"user_id": user_id, "scene": scene}
            rows = await execute_query(sql, params)

        for i in range(len(rows) - 1, -1, -1):
            m = rows[i]
            
            try:
                # TODO: Handle more message types (e.g., food_snap, report)
                content = ""
                element_list = []
                element_list = json.loads(m["content"])
                for e in element_list:
                    if e.get("type") == "reply":
                        content += e.get("content", "")
            except Exception:
                content = m["content"]
            messages.append(
                dict(
                    role                = m["role"],
                    agent               = m["agent"],
                    content             = content,
                    th_msg_id           = m["id"],
                    reference_task_id   = m["reference_task_id"],
                    created_at          = m["created_at"]
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

    timestamp = ""
    created_at = msg.get("created_at")
    if created_at and isinstance(created_at, datetime):
        timestamp = f"[{created_at.strftime('%Y-%m-%d %H:%M:%S')}] "

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
                        "content": f"{timestamp}{msg.get("content", "")}",
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
                            "content": f"{timestamp}{truncated_content}",
                        }
                    )
                break

        # Reverse back to chronological order (oldest to newest)
        compressed_messages = list(reversed(compressed_messages))
    else:
        # If limit not exceeded, keep only role and content fields
        compressed_messages = [
            {"role": msg.get("role", "unknown"), "content": f"{timestamp}{msg.get("content", "")}"} for msg in agent_messages
        ]

    return compressed_messages

#-----------------------------------------------------------------------------

async def create_new_question(trace_id: str, question: str, user_id: int, message_type: str = "text", msg_id: str = None, query_user_id: int = None, scene: str = "app"):
    if query_user_id is None:
        query_user_id = user_id
    
    if msg_id is not None:
        question_id = msg_id
    else:
        question_id = "app_" + str(uuid.uuid4())
    
    sql = "insert into theta_ai.th_messages (id, user_id, query_user_id, session_id, role, content, agent, message_type, scene, created_at) values (:id, :user_id, :query_user_id, :session_id, :role, theta_ai.encrypt_content(:content), :agent, :message_type, :scene, now()) RETURNING id"
    record = await execute_query(
        sql,
        params={
            "id": question_id, 
            "user_id": user_id, 
            "query_user_id": query_user_id,
            "session_id": trace_id, 
            "role": "user", 
            "content": question, 
            "agent": "chat_v3", 
            "message_type": message_type,
            "scene": scene
        },
        query_type="insert",
        mode="async",
    )
    return record.get("id")

#-----------------------------------------------------------------------------

async def get_chat_history(user_id: str, session_id: str) -> list[dict[str, Any]]:
    """Load chat history for given session from database"""
    history = []
    try:
        session_sql = """
            SELECT 
                id, theta_ai.decrypt_content(content) AS content, reasoning, role, agent, provider, 
                input_prompt, created_at, rating, question_id, message_type
            FROM theta_ai.th_messages
            WHERE user_id = :user_id AND session_id = :session_id
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
                content_json_obj = safe_json_parse(msg.get("content", ""))
                thinking_chunks = []
                if isinstance(content_json_obj, list) and msg.get("message_type") == "text":
                    try:
                        content = "".join([block["content"] for block in content_json_obj if block.get("type") == "reply"])
                        thinking_chunks = [block for block in content_json_obj if block.get("type") in ["thinking", "queryTitle", "queryArguments", "queryDetail"]]
                    
                    except Exception as e:
                        logging.error(f"Error parsing content: {str(e)}")
                
                # Regenerate file URLs if content contains files
                if isinstance(content_json_obj, dict) and content_json_obj.get("files"):
                    try:
                        from ..pulse.file_parser.services.database_services import FileParserDatabaseService
                        from ..pulse.file_parser.services.db_utils import get_mime_type
                        files = content_json_obj.get("files", [])
                        for file_info in files:
                            if isinstance(file_info, dict):
                                file_key = file_info.get("file_key", "")
                                if file_key:
                                    file_name = file_info.get("file_name") or file_info.get("filename") or file_info.get("original_filename") or ""
                                    content_type = get_mime_type(file_name)
                                    new_url = await FileParserDatabaseService.regenerate_file_url(
                                        file_key, file_name, content_type
                                    )
                                    if new_url:
                                        file_info["url_full"] = new_url
                                        file_info["url_thumb"] = new_url
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

                if msg.get("input_prompt"):
                    message["input_prompt"] = msg.get("input_prompt")

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

async def query_external_chat_history(user_id: str):
    try:
        group_sql = """
        SELECT DISTINCT group_id 
        FROM theta_ai.th_messages 
        WHERE message_type = :message_type 
        AND user_id = :user_id 
        AND group_id IS NOT NULL
        ORDER BY group_id
        """
        
        group_results = await execute_query(
            group_sql,
            params=dict(user_id=user_id, message_type=MessageType.external_chat)
        )
        
        if not group_results:
            return []
        
        group_ids = [row['group_id'] for row in group_results]
        
        chat_sql = """
        SELECT 
            m.id,
            m.user_id,
            m.group_id,
            theta_ai.decrypt_content(m.content) AS content,
            m.created_at,
            COALESCE(u.name, m.user_id::text) as user_name
        FROM theta_ai.th_messages m
        LEFT JOIN theta_ai.health_app_user u ON m.user_id::int = u.id
        WHERE m.message_type = :message_type 
        AND m.group_id = ANY(:group_ids)
        AND m.content IS NOT NULL 
        AND m.content != ''
        ORDER BY m.group_id, m.created_at DESC
        LIMIT 500
        """
        
        chat_results = await execute_query(
            chat_sql,
            params=dict(
                message_type=MessageType.external_chat, 
                group_ids=group_ids
            )
        )
        
        group_chat_map = {}
        
        for chat in chat_results:
            group_id = chat.get('group_id', '')
            user_name = chat.get('user_name', f"User<{chat['user_id']}>")
            content = chat.get('content', '').strip()
            if content:
                group_chat_map.setdefault(group_id, []).append(f"{user_name}: {content}")
        
        logging.info(f"length of group_chat_map: {len(group_chat_map)}, every group chat length")
        return group_chat_map
        
    except Exception as e:
        logging.error(f"query external chat history for user {user_id} failed: {str(e)}")
        return {}

#-----------------------------------------------------------------------------

_redis_client: Redis = None

async def chat_list_setter_hollywell(user_id: str, msg_id: str, question: str):
    global _redis_client
    if _redis_client is None:
        _redis_client = await global_config().get_redis().get_async_client()
    if _redis_client is None:
        logging.error("Invalid redis client.")
        return

    message = json.dumps(
        {
            "user_id": user_id,
            "msg_id": msg_id,
            "question": question,
        }
    )
    status = await _redis_client.rpush(REDIS_CHAT_LIST_KEY_HOLYWELL, message)
    if status:
        logging.info(f"Redis chat list setter success: {user_id}, {msg_id}, {question}")
    else:
        logging.error(f"Redis chat list setter failed: {user_id}, {msg_id}, {question}", exc_info=True)

#-----------------------------------------------------------------------------

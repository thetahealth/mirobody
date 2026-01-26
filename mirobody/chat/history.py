import json, logging, pytz, time

from datetime import datetime
from typing import Any, Dict, List, Optional

from ..utils import execute_query, get_req_ctx


def safe_json_parse(content: str) -> dict:
    """Safely parse JSON content, return empty dict if fails"""
    try:
        return json.loads(content) if content else {}
    except (json.JSONDecodeError, TypeError):
        return {}


async def get_chat_history(user_id: str, session_id: str) -> List[Dict[str, Any]]:
    """Load chat history for given session from database"""
    history = []
    try:
        session_sql = """
            SELECT 
                id, decrypt_content(content) AS content, reasoning, role, agent, provider, 
                input_prompt, created_at, rating, question_id, message_type
            FROM th_messages
            WHERE user_id = :user_id AND session_id = :session_id AND message_type in ('text', 'file', 'pdf', 'image')
            ORDER BY created_at ASC
        """

        db_messages = await execute_query(session_sql, params={"user_id": user_id, "session_id": session_id})
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
                        thinking_chunks = [block for block in content_json_obj if block.get("type") in ["thinking", "queryTitle", "queryDetail"]]
                    except Exception as e:
                        logging.warning(
                            level="warning",
                            _input=f"Error parsing content: {str(e)}",
                            function="get_chat_history",
                        )
                
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
                # else:
                #     user_timestamp = datetime.fromisoformat(user_msg["timestamp"])
                #     # Find the closest agent response by time
                #     closest_responses = None
                #     min_time_diff = float('inf')

                #     for responses in agent_responses.values():
                #         for response in responses:
                #             response_time = datetime.fromisoformat(response["timestamp"])
                #             time_diff = abs((response_time - user_timestamp).total_seconds())
                #             if time_diff < min_time_diff:
                #                 min_time_diff = time_diff
                #                 closest_responses = responses

                # if closest_responses:
                #     history.extend(closest_responses)

            logging.info(f"session:{session_id}\tmessage_cnt:{len(history)} Successfully loaded")

    except Exception as e:
        logging.error(f"Error loading conversation history: {str(e)}", exc_info=True)

    return history


async def get_session_summaries(
    user_id: str,
    page: int = 0,
    page_size: int = 20,
    category: Optional[List[str]] = None,
    query_user_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get conversation summaries for user with pagination and category filter
    
    Args:
        user_id: User ID
        page: Page number (0-indexed)
        page_size: Number of items per page
        category: Optional list of categories to filter (e.g., ["journal", "food"])
        query_user_id: Optional query_user_id filter
        
    Returns:
        Dictionary containing summaries list and pagination info
    """
    logging.info(f"get_session_summaries: user_id={user_id}, page={page}, page_size={page_size}, category={category}, query_user_id={query_user_id}")
    
    # Record function start time
    start_time = time.time()
    
    try:
        # Build WHERE clause
        where_clauses = ["ts.user_id = :user_id"]
        params = {"user_id": user_id}
        
        if category and len(category) > 0:
            # Build IN clause with placeholders
            placeholders = ", ".join([f":category_{i}" for i in range(len(category))])
            where_clauses.append(f"ts.category IN ({placeholders})")
            # Add each category to params
            for i, cat in enumerate(category):
                params[f"category_{i}"] = cat
        
        if query_user_id:
            where_clauses.append("(ts.query_user_id = :query_user_id OR ts.query_user_id IS NULL)")
            params["query_user_id"] = query_user_id
        
        # Determine query strategy based on category parameter
        has_category = category and len(category) > 0
        
        if not has_category:
            where_clauses.append("ts.category IS NULL")
        
        where_clause = " AND ".join(where_clauses)
        
        # Build SQL queries based on whether category filter exists
        if has_category:
            # When category is specified, query th_sessions directly without JOIN
            count_sql = f"""
                SELECT COUNT(ts.session_id) as total
                FROM th_sessions ts
                WHERE {where_clause}
            """
            
            summary_sql = f"""
                SELECT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, ts.tags, ts.category
                FROM th_sessions ts
                WHERE {where_clause}
                ORDER BY ts.created_at DESC
                LIMIT :limit OFFSET :offset
            """
            query_strategy = "direct query (no JOIN)"
        else:
            # When no category, use INNER JOIN to ensure sessions have messages
            count_sql = f"""
                SELECT COUNT(DISTINCT ts.session_id) as total
                FROM th_sessions ts
                INNER JOIN th_messages tm ON ts.session_id = tm.session_id 
                    AND ts.user_id = tm.user_id
                WHERE {where_clause}
            """
            
            summary_sql = f"""
                SELECT DISTINCT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, ts.tags, ts.category
                FROM th_sessions ts
                INNER JOIN th_messages tm ON ts.session_id = tm.session_id 
                    AND ts.user_id = tm.user_id
                WHERE {where_clause}
                ORDER BY ts.created_at DESC
                LIMIT :limit OFFSET :offset
            """
            query_strategy = "with JOIN"
        
        # Execute count query
        count_query_start = time.time()
        count_result = await execute_query(count_sql, params=params)
        count_query_duration = time.time() - count_query_start
        total = count_result[0].get("total", 0) if count_result else 0
        
        logging.info(f"COUNT query ({query_strategy}) completed in {count_query_duration:.3f}s, total records: {total}")
        
        # Execute data query
        data_query_start = time.time()
        params["limit"] = page_size
        params["offset"] = page * page_size
        
        result = await execute_query(summary_sql, params=params)
        data_query_duration = time.time() - data_query_start
        
        logging.info(f"Data query ({query_strategy}) completed in {data_query_duration:.3f}s, fetched {len(result)} records")

        # Format summaries with timezone conversion
        format_start = time.time()
        formatted_summaries = []
        timezone = get_req_ctx("timezone", "America/Los_Angeles")
        try:
            user_tz = pytz.timezone(timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            user_tz = pytz.timezone("America/Los_Angeles")
        
        for summary in result:
            created_at = summary.get("created_at")
            if created_at:
                # If datetime is naive (no timezone), assume it's UTC
                if created_at.tzinfo is None:
                    created_at = pytz.UTC.localize(created_at)
                # Convert to user timezone
                created_at_local = created_at.astimezone(user_tz)
                timestamp_str = created_at_local.isoformat()
            else:
                timestamp_str = datetime.now().isoformat()
            
            formatted_summary = {
                "session_id": summary.get("session_id"),
                "timestamp": timestamp_str,
                "summary": summary.get("summary") or "",
                "query_user_id": summary.get("query_user_id") or "",
                "tags": summary.get("tags") or [],
                "category": summary.get("category") or "",
            }
            formatted_summaries.append(formatted_summary)
        
        format_duration = time.time() - format_start

        # Calculate pagination info
        has_more = (page + 1) * page_size < total
        
        # Calculate total duration
        total_duration = time.time() - start_time

        logging.info(
            f"Retrieved {len(formatted_summaries)} summaries (total: {total}) for user {user_id} | "
            f"Strategy: {query_strategy} | "
            f"Total: {total_duration:.3f}s (COUNT: {count_query_duration:.3f}s, "
            f"Data: {data_query_duration:.3f}s, Format: {format_duration:.3f}s)"
        )
        
        return {
            "summaries": formatted_summaries,
            "total": total,
            "page": page,
            "page_size": page_size,
            "has_more": has_more
        }
    except Exception as e:
        total_duration = time.time() - start_time
        logging.error(f"Error loading conversation summaries after {total_duration:.3f}s: {str(e)}", exc_info=True)

        return {
            "summaries": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "has_more": False
        }


async def delete_session_history(user_id: str, session_id: str) -> bool:
    """Delete all messages and session data for a specific session"""
    logging.info(f"delete_session_history: user_id={user_id}, session_id={session_id}")
    try:
        # First delete all messages in the session
        delete_messages_sql = """
            DELETE FROM th_messages 
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(delete_messages_sql, params={"user_id": user_id, "session_id": session_id})

        # Then delete the session summary
        delete_session_sql = """
            DELETE FROM th_sessions 
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(delete_session_sql, params={"user_id": user_id, "session_id": session_id})

        logging.info(f"Successfully deleted session {session_id} for user {user_id}")
        return True
        
    except Exception as e:
        logging.error(f"Error deleting session history: {str(e)}", exc_info=True)
        return False

async def get_session_summaries_by_person(user_id: str) -> List[Dict[str, Any]]:
    """Get session summaries by person"""
    logging.info(f"get_session_summaries_by_person: {user_id}")
    try:
        
        summary_sql = """
            SELECT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, tu.name, tu.gender, tu.birth, tu.blood
            FROM th_sessions ts
            INNER JOIN health_app_user tu ON ts.query_user_id::integer = tu.id
            WHERE ts.user_id = :user_id
            AND ts.category IS NULL
            ORDER BY created_at DESC
        """
        result = await execute_query(summary_sql, params={"user_id": user_id})
        
        session_by_person = {
            
        }
        
        nickname_map = {}
        
        for _session in result:
            user_name = _session.get("name", "No name")
            user_gender = "Male" if _session.get("gender") == 1 else "Female" if _session.get("gender") == 2 else "Other"
            user_birth = _session.get("birth", "")
            user_age = ""
            query_user_id = _session.get("query_user_id")
            try:
                user_birth_date = datetime.strptime(user_birth, "%Y-%m-%d")
                user_age = datetime.now().year - user_birth_date.year
            except Exception as e:
                logging.warning(f"Error calculating user age: {str(e)}")
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


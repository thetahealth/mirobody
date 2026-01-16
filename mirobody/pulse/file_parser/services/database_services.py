import asyncio
import json
import logging
from datetime import datetime, timedelta
from hashlib import md5
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote
from zoneinfo import ZoneInfo
from mirobody.utils import execute_query
from mirobody.utils.req_ctx import get_req_ctx

from .db_utils import (
    safe_json_dumps,
    safe_json_loads,
    parse_date,
    parse_iso_datetime,
    get_utc_now,
    extract_first_record,
    get_mime_type,
    get_simple_file_type,
)


class FileParserDatabaseService:
    @staticmethod
    async def log_chat_message(
        id: str,
        user_id: str,
        session_id: str,
        role: str,
        content: str,
        reasoning: str = "",
        agent: str = "",
        provider: str = "",
        input_prompt: str = "",
        question_id: Optional[str] = None,
        user_name: Optional[str] = None,
        message_type: str = "text",
        query_user_id: Optional[str] = None,
    ):
        """Log chat message to database"""
        try:
            message_sql = """
                INSERT INTO theta_ai.th_messages (
                    id, user_id, user_name, session_id, role, content, reasoning,
                    agent, provider, input_prompt, created_at, question_id, rating,
                    message_type, query_user_id
                )
                VALUES (:id, :user_id, :user_name, :session_id, :role, theta_ai.encrypt_content(:content), :reasoning, :agent, :provider, :input_prompt, :created_at, :question_id, :rating, :message_type, :query_user_id)
                ON CONFLICT (id) DO NOTHING RETURNING id
            """

            await execute_query(
                query=message_sql,
                params={
                    "id": id,
                    "user_id": user_id,
                    "user_name": user_name,
                    "session_id": session_id,
                    "role": role,
                    "content": safe_json_dumps(content) if isinstance(content, (dict, list)) else content,
                    "reasoning": safe_json_dumps(reasoning) if isinstance(reasoning, (dict, list)) else reasoning,
                    "agent": agent,
                    "provider": provider,
                    "input_prompt": safe_json_dumps(input_prompt) if isinstance(input_prompt, (dict, list)) else input_prompt,
                    "created_at": datetime.now(),
                    "question_id": question_id,
                    "rating": 5,
                    "message_type": message_type,
                    "query_user_id": query_user_id or user_id,
                },
            )

            logging.info(f"Message logged: {id}")
        except Exception as e:
            logging.error(f"Error logging message: {e}", stack_info=True)

    @staticmethod
    async def get_message_content(message_id: str) -> dict:
        """Get message content by message ID"""
        try:
            query = """
                SELECT theta_ai.decrypt_content(content) AS content FROM theta_ai.th_messages WHERE id = :message_id LIMIT 1
            """
            
            result = await execute_query(
                query=query,
                params={"message_id": message_id},
            )
            
            first_record = extract_first_record(result)
            if not first_record or not first_record.get("content"):
                return {}
            
            content = first_record["content"]
            if isinstance(content, str):
                parsed = safe_json_loads(content, {"raw_content": content})
                return parsed
            elif isinstance(content, dict):
                return content
            return {"content": content}
                
        except Exception as e:
            logging.error(f"Failed to get message content: {message_id}, {e}", "get_message_content", stack_info=True)
            return {}

    @staticmethod
    async def update_message_content(
        message_id: str,
        content: str = None,
        reasoning: str = None,
        comment: str = None,
        message_type: str = None,
    ) -> bool:
        """Update message content, reasoning or comments"""
        try:
            update_fields = []
            params = {"message_id": message_id}

            if content is not None:
                update_fields.append("content = theta_ai.encrypt_content(:content)")
                params["content"] = safe_json_dumps(content) if isinstance(content, (dict, list)) else content

            if reasoning is not None:
                update_fields.append("reasoning = :reasoning")
                params["reasoning"] = safe_json_dumps(reasoning) if isinstance(reasoning, (dict, list)) else reasoning

            if comment is not None:
                update_fields.append("comment = :comment")
                params["comment"] = safe_json_dumps(comment) if isinstance(comment, (dict, list)) else comment

            if message_type is not None:
                update_fields.append("message_type = :message_type")
                params["message_type"] = message_type

            if not update_fields:
                return False

            update_sql = f"""
                UPDATE theta_ai.th_messages SET {", ".join(update_fields)} WHERE id = :message_id
            """

            update_result = await execute_query(
                query=update_sql,
                params=params,
            )

            logging.info(f"💾 [DB] Updated message {message_id} with fields: {', '.join(update_fields)}, result: {update_result}")
            return True
        except Exception as e:
            logging.error(f"Error updating message: {e}", stack_info=True)
            return False

    @staticmethod
    async def get_message_details(response_id: str) -> Dict[str, Any]:
        """Get detailed information of specified message ID"""
        try:
            logging.info(f"Fetching message details for ID: {response_id}")

            message_sql = """
                SELECT 
                    id, user_id, session_id, role, theta_ai.decrypt_content(content) AS content, reasoning,
                    agent, provider, input_prompt, created_at, question_id, 
                    rating, comment, message_type
                FROM theta_ai.th_messages
                WHERE id = :response_id
            """

            result = await execute_query(
                query=message_sql,
                params={"response_id": response_id},
            )

            if not result:
                logging.error(f"No message found with ID: {response_id}")
                return {}

            message = result[0]

            # Create return dictionary with default values to prevent null values
            message_details = {
                "id": message.get("id", ""),
                "user_id": message.get("user_id", ""),
                "sessionId": message.get("session_id", ""),
                "role": message.get("role", "assistant"),
                "content": message.get("content", ""),
                "reasoning": message.get("reasoning", ""),
                "agent": message.get("agent", ""),
                "provider": message.get("provider", ""),
                "inputPrompt": message.get("input_prompt", ""),
                "timestamp": (
                    message.get("created_at").isoformat() if message.get("created_at") else datetime.now().isoformat()
                ),
                "questionId": message.get("question_id", ""),
                "rating": message.get("rating", 0),
                "comment": (message.get("comment") if message.get("comment") is not None else ""),
                "messageType": message.get("message_type", "text"),
            }

            return message_details

        except Exception as e:
            logging.error(f"Error getting message details: {str(e)}", stack_info=True)

            # Return error message instead of empty object on error, frontend can display
            return {
                "error": str(e),
                "id": response_id,
                "content": f"Error getting message details: {str(e)}",
                "role": "system",
                "agent": "system",
                "timestamp": datetime.now().isoformat(),
                "provider": "system",
                "messageType": "text",
            }

    @staticmethod
    async def generate_summary(user_message: str, provider: str) -> str:
        """Generate session summary"""
        # Simplified implementation, can actually call AI model to generate better summary
        return user_message[:10] + "..." if len(user_message) > 10 else user_message

    @staticmethod
    async def generate_and_save_summary(
        user_id: str, session_id: str, user_message: str, provider: str
    ) -> Dict[str, Any]:
        """Asynchronously generate and save summary"""
        try:
            # Generate summary
            summary = await FileParserDatabaseService.generate_summary(user_message, provider)

            # Save summary
            await FileParserDatabaseService.save_conversation_summary(user_id, session_id, summary)

            logging.info(f"session:{session_id}\tSuccessfully saved conversation summary!")
            return {"event": "summary_generated", "session_id": session_id}

        except Exception as e:
            logging.error(f"Error in generate_and_save_summary: {str(e)}", stack_info=True)

            return None

    @staticmethod
    async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
        """Save conversation summary to database"""
        try:
            logging.info(f"save_conversation_summary: {user_id}, {session_id}, {summary}")
            # Insert summary, do nothing on conflict
            summary_sql = """
                INSERT INTO theta_ai.th_sessions (
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
            logging.error(f"Error saving conversation summary: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def get_specific_session_summaries(username: str, session_id: str) -> List[Dict]:
        """Get summary of specific session"""
        try:
            summary_sql = """
                SELECT session_id, summary, created_at
                FROM theta_ai.th_sessions
                WHERE session_id = :session_id
            """

            summaries = await execute_query(
                query=summary_sql,
                params={"session_id": session_id},
            )
            return summaries

        except Exception as e:
            logging.error(f"Error getting session summaries: {str(e)}", stack_info=True)

            return []

    @staticmethod
    async def enable_file_parser_provider(user_id: str, provider_slug: str) -> bool:
        """Enable file parser provider for user"""
        try:
            enable_sql = """
                INSERT INTO health_user_provider (
                    user_id, provider_slug, platform, status, llm_access
                )
                VALUES (:user_id, :provider_slug, 'fileparser', 'enabled', 1)
                ON CONFLICT (user_id, provider_slug, platform) 
                DO UPDATE SET status = 'enabled', llm_access = 1, update_time = NOW()
            """

            await execute_query(
                query=enable_sql,
                params={
                    "user_id": user_id,
                    "provider_slug": provider_slug,
                },
            )

            logging.info(f"Enabled file parser provider {provider_slug} for user {user_id}")
            return True

        except Exception as e:
            logging.error(f"Error enabling file parser provider: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def disable_file_parser_provider(user_id: str, provider_slug: str) -> bool:
        """Disable file parser provider for user"""
        try:
            disable_sql = """
                UPDATE health_user_provider 
                SET status = 'disabled', update_time = NOW()
                WHERE user_id = :user_id AND provider_slug = :provider_slug AND platform = 'fileparser'
            """

            await execute_query(
                query=disable_sql,
                params={
                    "user_id": user_id,
                    "provider_slug": provider_slug,
                },
            )

            logging.info(f"Disabled file parser provider {provider_slug} for user {user_id}")
            return True

        except Exception as e:
            logging.error(f"Error disabling file parser provider: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def get_user_file_parser_providers(user_id: str) -> List[str]:
        """Get enabled file parser providers for user"""
        try:
            providers_sql = """
                SELECT provider_slug
                FROM health_user_provider
                WHERE user_id = :user_id AND platform = 'fileparser' AND status = 'enabled'
            """

            result = await execute_query(
                query=providers_sql,
                params={"user_id": user_id},
            )

            return [row["provider_slug"] for row in result] if result else []

        except Exception as e:
            logging.error(f"Error getting user file parser providers: {str(e)}", stack_info=True)
            return []

    @staticmethod
    async def get_user_file_parser_providers_with_llm_access(user_id: str) -> Dict[str, int]:
        """Get user file parser providers with LLM access information"""
        try:
            providers_sql = """
                SELECT provider_slug, llm_access
                FROM health_user_provider
                WHERE user_id = :user_id AND platform = 'fileparser' AND status = 'enabled'
            """

            result = await execute_query(
                query=providers_sql,
                params={"user_id": user_id},
            )

            return {row["provider_slug"]: row["llm_access"] for row in result} if result else {}

        except Exception as e:
            logging.error(f"Error getting user file parser providers with LLM access: {str(e)}", stack_info=True)
            return {}

    @staticmethod
    async def update_llm_access(user_id: str, provider_slug: str, llm_access: int) -> bool:
        """Update LLM access for file parser provider"""
        try:
            update_sql = """
                UPDATE health_user_provider
                SET llm_access = :llm_access, update_time = NOW()
                WHERE user_id = :user_id AND provider_slug = :provider_slug AND platform = 'fileparser'
            """

            await execute_query(
                query=update_sql,
                params={
                    "user_id": user_id,
                    "provider_slug": provider_slug,
                    "llm_access": llm_access,
                },
            )

            logging.info(f"Updated LLM access for file parser provider {provider_slug}, user {user_id}: {llm_access}")
            return True

        except Exception as e:
            logging.error(f"Error updating LLM access: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def save_raw_text_to_db(user_id: str, file_type: str, raw_text: str) -> int:
        """
        Save file content to database

        Args:
            user_id: User ID
            file_type: File type
            raw_text: Extracted text content

        Returns:
            int: ID of inserted record
        """
        try:
            # Use parameterized queries to avoid SQL injection issues
            sql = """
            INSERT INTO theta_ai.th_health_report_summary (
                user_id, doc_type, doc_text, doc_summary, create_time, update_time
            ) VALUES (
                :user_id, :file_type, :raw_text, :doc_summary, now(), now()
            ) RETURNING id
            """

            result = await execute_query(
                query=sql,
                params={
                    "user_id": user_id,
                    "file_type": file_type,
                    "raw_text": raw_text,
                    "doc_summary": "",
                },
            )

            # Get returned ID
            record_id = result.get("id")
            logging.info(f"File content saved to database, record ID: {record_id}")
            return record_id

        except Exception:
            logging.error("Error saving file content to database", stack_info=True)
            return 0

    @staticmethod
    async def _save_to_series_data(db_params: List[Dict[str, Any]]) -> int:
        """Parallel task: save to th_series_data table"""
        if not db_params:
            return 0

        await execute_query(
            query="""INSERT INTO theta_ai.th_series_data (user_id, indicator, value, start_time, end_time, source_table, source_table_id, comment) 
               VALUES (:user_id, :indicator, :value, :start_time, :end_time, :source_table, :source_table_id, :comment)
               ON CONFLICT DO NOTHING""",
            fieldList=db_params,
        )
        logging.info(f"✅ {len(db_params)} indicator data saved to th_series_data")
        return len(db_params)

    @staticmethod
    async def _save_to_series_dim(dim_params: List[Dict[str, Any]]) -> int:
        """Batch insert to th_series_dim table"""
        if not dim_params:
            return 0

        try:
            # Prepare parameters for batch insert, add updated_at field
            batch_params = []
            for dim_param in dim_params:
                batch_param = dim_param.copy()
                batch_param["updated_at"] = datetime.now()
                batch_params.append(batch_param)

            # Execute batch insert
            insert_query = """
            INSERT INTO theta_ai.th_series_dim 
            (original_indicator, standard_indicator, category_group, category, updated_at)
            VALUES 
            (:original_indicator, :standard_indicator, :category_group, :category, :updated_at)
            ON CONFLICT (original_indicator) 
            DO UPDATE SET 
                standard_indicator = EXCLUDED.standard_indicator,
                category_group = EXCLUDED.category_group,
                category = EXCLUDED.category,
                updated_at = EXCLUDED.updated_at
            WHERE theta_ai.th_series_dim.updated_at < EXCLUDED.updated_at
            """

            await execute_query(query=insert_query, fieldList=batch_params)

            logging.info(f"✅ {len(batch_params)} indicator dimensions batch saved to th_series_dim")
            return len(batch_params)

        except Exception as e:
            logging.error(f"Batch save indicator dimensions failed: {len(dim_params)} records", stack_info=True)
            return 0
    
    @staticmethod
    def generate_source_table_id(msg_id: str, file_key: str) -> str:
        """
        Generate new format source_table_id for th_series_data
        
        Args:
            msg_id: Message ID
            file_key: File key from files array
            
        Returns:
            str: Generated source_table_id in format: msg_id_#_hash[:10]
        """
        if not file_key:
            # Fallback to old format if no file_key
            return msg_id
        
        # Generate MD5 hash of file_key and take first 10 characters
        file_key_hash = md5(file_key.encode()).hexdigest()[:10]
        return f"{msg_id}_#_{file_key_hash}"

    @staticmethod
    async def get_user_current_time_with_timezone(user_id: str) -> datetime:
        """Get current time in user's timezone, falls back to UTC"""
        try:
            query = "SELECT tz FROM theta_ai.health_app_user WHERE id = :user_id AND is_del = FALSE"
            result = await execute_query(
                query=query,
                params={"user_id": int(user_id)},
            )
            
            first_record = extract_first_record(result)
            if not first_record:
                return get_utc_now()
            
            user_tz = (first_record.get("tz") or "").strip()
            if not user_tz:
                return get_utc_now()
            
            try:
                return datetime.now(ZoneInfo(user_tz)).replace(tzinfo=None)
            except Exception:
                return get_utc_now()
                
        except Exception:
            return get_utc_now()

    @staticmethod
    async def save_indicators_to_db(
        user_id: str,
        indicators: List[Dict[str, Any]],
        exam_date: str,
        msg_id: str,
        comment: str = "",
        source_table: str = "theta_ai.th_messages",
        file_key: str = None,
    ) -> int:
        """Batch save health indicators to th_series_data table"""
        try:
            # Parse exam date or use current time
            if not exam_date or not exam_date.strip():
                start_time = await FileParserDatabaseService.get_user_current_time_with_timezone(user_id)
            else:
                start_time = parse_date(exam_date)
                if start_time is None:
                    raise ValueError(f"Failed to parse date format: {exam_date}")

            end_time = start_time
            db_params = []

            for indicator in indicators:
                # Check required fields
                original_indicator = indicator.get("original_indicator")

                if not original_indicator:
                    continue

                # Generate source_table_id with file-level precision
                source_table_id = FileParserDatabaseService.generate_source_table_id(msg_id, file_key)
                
                # Build comment JSON with unit, reference_range, and detection_method
                try:
                    comment_data = {
                        "unit": indicator.get("unit", ""),
                        "reference_range": indicator.get("reference_range", ""),
                        "detection_method": indicator.get("detection_method", ""),
                    }
                    comment_json = json.dumps(comment_data, ensure_ascii=False)
                except Exception as e:
                    logging.warning(f"Failed to build comment JSON for indicator {original_indicator}: {str(e)}")
                    comment_json = ""
                
                # Build th_series_data parameters
                db_params.append(
                    {
                        "user_id": str(user_id),
                        "indicator": original_indicator,
                        "value": indicator.get("value", ""),
                        "start_time": start_time,
                        "end_time": end_time,
                        "source_table": source_table,
                        "source_table_id": source_table_id,
                        "comment": comment_json,
                    }
                )

            # Execute database write tasks
            if db_params:
                await FileParserDatabaseService._save_to_series_data(db_params)

            logging.info(f"🚀 Write complete: {len(db_params)} records, user_id: {user_id}")

            return len(db_params)

        except Exception:
            logging.error(f"Failed to save indicators to database, user_id: {user_id}", stack_info=True)
            return 0

    @staticmethod
    async def delete_genetic_data_by_source(user_id: str, source_table: str, source_table_id: str) -> bool:
        """
        Delete genetic data by source table and ID

        Args:
            user_id: User ID
            source_table: Source table name
            source_table_id: Source table record ID

        Returns:
            bool: Whether deletion was successful
        """
        try:
            # Delete genetic data
            sql = """
                DELETE FROM theta_ai.th_series_data_genetic 
                WHERE user_id = :user_id 
                AND source_table = :source_table 
                AND source_table_id = :source_table_id
            """

            params = {
                "user_id": user_id,
                "source_table": source_table,
                "source_table_id": source_table_id,
            }

            await execute_query(query=sql, params=params,)

            logging.info(f"Genetic data deleted successfully, user_id: {user_id}, source_table: {source_table}, source_table_id: {source_table_id}")
            return True

        except Exception:
            logging.error(f"Failed to delete genetic data, user_id: {user_id}, source_table: {source_table}, source_table_id: {source_table_id}", stack_info=True)
            return False

    @staticmethod
    async def delete_genetic_data_by_message_id(user_id: str, message_id: str) -> bool:
        """
        Delete genetic data by message ID (convenience method for file upload scenarios)

        Args:
            user_id: User ID
            message_id: Message ID

        Returns:
            bool: Whether deletion was successful
        """
        return await FileParserDatabaseService.delete_genetic_data_by_source(user_id, "theta_ai.th_messages", message_id)

    @staticmethod
    def generate_file_hash(content: str) -> str:
        """
        Generate MD5 hash value of file content

        Args:
            content: File content

        Returns:
            str: MD5 hash value
        """
        return md5(content.encode("utf-8")).hexdigest()

    @staticmethod
    async def get_indicator_dict() -> Dict[str, str]:
        """
        Get indicator dictionary
        """
        sql = """
            select indicator_id, indicator_name_cn from theta_ai.indicator_dimension_all
        """
        rows = await execute_query(query=sql, params={})
        return {row.get("indicator_name_cn"): row.get("indicator_id") for row in rows}

    @staticmethod
    async def get_health_data_by_type(
        user_id: str,
        message_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve health management data from theta_ai.th_messages table by message type

        Args:
            user_id: User ID
            message_type: Type of message to filter
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            limit: Maximum number of records to return

        Returns:
            List of health data records
        """
        try:
            # Build SQL query
            base_sql = """
                SELECT id, theta_ai.decrypt_content(content) AS content, reasoning, created_at, session_id
                FROM theta_ai.th_messages 
                WHERE user_id = :user_id 
                    AND message_type = :message_type
                    AND (is_del = false OR is_del IS NULL)
            """

            params = {"user_id": user_id, "message_type": message_type, "limit": limit}

            if start_date:
                try:
                    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                    base_sql += " AND DATE(created_at) >= :start_date"
                    params["start_date"] = start_date_obj
                except ValueError:
                    pass  # Skip invalid date filter

            if end_date:
                try:
                    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                    base_sql += " AND DATE(created_at) <= :end_date"
                    params["end_date"] = end_date_obj
                except ValueError:
                    pass  # Skip invalid date filter

            base_sql += " ORDER BY created_at DESC LIMIT :limit"

            # Execute query
            results = await execute_query(
                query=base_sql,
                params=params,
            )

            # Parse and format results
            health_data = []
            for row in results:
                try:
                    content = json.loads(row["content"]) if isinstance(row["content"], str) else row["content"]

                    health_data.append(
                        {
                            "id": row["id"],
                            "content": content,
                            "notes": row["reasoning"] or "",
                            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                            "session_id": row["session_id"],
                        }
                    )
                except (json.JSONDecodeError, KeyError, AttributeError):
                    # Skip invalid records
                    continue

            logging.info(f"Retrieved {len(health_data)} health data records for user {user_id}, type {message_type}")

            return health_data

        except Exception as e:
            logging.error(f"Error retrieving health data by type: {str(e)}", stack_info=True)
            return []

    @staticmethod
    async def get_health_series_data_by_filters(
        user_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        indicators: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get health series data from th_series_data table with filters

        Args:
            user_id: User ID
            start_time: Start time filter in ISO format
            end_time: End time filter in ISO format
            indicators: Comma-separated list of indicators
            limit: Maximum number of records to return

        Returns:
            List of health data points
        """
        try:
            # Build the base query
            query = """
            SELECT 
                id,
                user_id,
                indicator,
                value,
                start_time,
                end_time,
                source_table,
                source_table_id,
                comment,
                update_time
            FROM theta_ai.th_series_data 
            WHERE user_id = :user_id
            """

            params = {"user_id": user_id}

            # Add time filters
            if start_time:
                query += " AND start_time >= :start_time"
                parsed = parse_iso_datetime(start_time)
                if parsed is None:
                        raise ValueError(f"Invalid start_time format: {start_time}")
                params["start_time"] = parsed

            if end_time:
                query += " AND start_time <= :end_time"
                parsed = parse_iso_datetime(end_time)
                if parsed is None:
                        raise ValueError(f"Invalid end_time format: {end_time}")
                params["end_time"] = parsed

            # Add indicator filters
            if indicators:
                indicator_list = [ind.strip() for ind in indicators.split(",")]
                placeholders = ",".join([f":indicator_{i}" for i in range(len(indicator_list))])
                query += f" AND indicator IN ({placeholders})"
                for i, indicator in enumerate(indicator_list):
                    params[f"indicator_{i}"] = indicator

            # Add ordering and limit
            query += " ORDER BY start_time DESC, indicator"
            query += f" LIMIT {limit}"

            logging.info(f"Fetching health series data for user {user_id} with query: {query[:100]}...")

            # Execute query
            result = await execute_query(
                query=query,
                params=params,
            )

            # Process results
            data = []
            if result and hasattr(result, "__iter__"):
                for row in result:
                    try:
                        # Convert row to dict if it's not already
                        if hasattr(row, "_asdict"):
                            row_dict = row._asdict()
                        elif hasattr(row, "keys"):
                            row_dict = dict(row)
                        else:
                            # Assume it's a tuple and map to expected columns
                            row_dict = {
                                "id": row[0],
                                "user_id": row[1],
                                "indicator": row[2],
                                "value": row[3],
                                "start_time": row[4],
                                "end_time": row[5],
                                "source_table": row[6],
                                "source_table_id": row[7],
                                "comment": row[8],
                                "update_time": row[9] if len(row) > 9 else None,
                            }

                        # Convert datetime objects to ISO strings
                        for key in ["start_time", "end_time", "update_time"]:
                            if key in row_dict and row_dict[key]:
                                if isinstance(row_dict[key], datetime):
                                    row_dict[key] = row_dict[key].isoformat()

                        data.append(row_dict)

                    except Exception as e:
                        logging.warning(f"Error processing row: {str(e)}")
                        continue

            logging.info(f"Successfully fetched {len(data)} health data points for user {user_id}")

            return data

        except Exception as e:
            logging.error(f"Error fetching health series data for user {user_id}: {str(e)}", stack_info=True)
            raise Exception(f"Failed to fetch health series data: {str(e)}")

    @staticmethod
    async def get_health_records_paginated(
        user_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Get health records from theta_ai.th_messages table with pagination

        Args:
            user_id: User ID
            limit: Maximum number of records to return
            offset: Offset for pagination

        Returns:
            List of health record entries
        """
        try:
            query = """
                SELECT id, theta_ai.decrypt_content(content) AS content, created_at, session_id
                FROM theta_ai.th_messages 
                WHERE user_id = :user_id 
                AND message_type = 'health_record'
                AND (is_del IS NULL OR is_del = FALSE)
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """

            result = await execute_query(
                query=query,
                params={"user_id": user_id, "limit": limit, "offset": offset},
            )

            records = []
            for row in result:
                try:
                    content_data = json.loads(row["content"])
                    records.append(
                        {
                            "id": row["id"],
                            "session_id": row["session_id"],
                            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                            "template_name": content_data.get("template_name"),
                            "records": content_data.get("records", []),
                            "timestamp": content_data.get("timestamp"),
                        }
                    )
                except json.JSONDecodeError:
                    continue

            logging.info(f"Retrieved {len(records)} health records for user {user_id} with limit {limit}, offset {offset}")

            return records

        except Exception as e:
            logging.error(f"Error getting health records: {str(e)}", stack_info=True)
            raise Exception(f"Failed to get health records: {str(e)}")

    @staticmethod
    def get_simple_type(file_type: str) -> str:
        """Get simplified type for compatibility"""
        return get_simple_file_type(file_type)

    @staticmethod
    def _get_content_type_by_filename(filename: str) -> str:
        """Get MIME content type from filename extension"""
        return get_mime_type(filename)

    @staticmethod
    def extract_file_key_from_url(url: str) -> str:
        """
        Extract file key from URL
        
        Args:
            url: The URL to extract key from
            
        Returns:
            str: Extracted file key, or empty string if extraction fails
        """
        if not url:
            return ""
            
        try:
            parsed = urlparse(url)
            path = unquote(parsed.path)
            
            # Remove leading slash
            if path.startswith('/'):
                path = path[1:]
            
            # For S3/OSS URLs, the path is typically the key
            # Remove query parameters and fragments
            key = path.split('?')[0].split('#')[0]
            
            logging.debug(f"Extracted file key: {key} from URL: {url[:100]}...")
            
            return key
            
        except Exception as e:
            logging.warning(f"Failed to extract file key from URL: {str(e)}")
            return ""

    @staticmethod
    async def regenerate_file_url(file_key: str, original_filename: str = "", content_type: str = "application/octet-stream") -> str:
        """
        Regenerate file URL using unified storage client
        
        Args:
            file_key: The file key/path
            original_filename: Original filename (unused, kept for backward compatibility)
            content_type: MIME type of the file
            
        Returns:
            str: Regenerated signed URL, or empty string if regeneration fails
        """
        if not file_key:
            return ""
            
        try:
            from mirobody.utils.config.storage import get_storage_client
            
            storage = get_storage_client()
            storage_type = storage.get_storage_type()
            
            logging.debug(f"Using {storage_type} storage for URL regeneration, key: {file_key}")
            
            # Generate signed URL with 24 hours expiration
            url = await storage.generate_signed_url(
                key=file_key, 
                expires=24 * 3600, 
                content_type=content_type
            )
            
            if url:
                return url
            else:
                logging.warning(f"URL generation returned empty for key: {file_key}")
                return ""
                    
        except Exception as e:
            logging.error(f"URL regeneration failed for key {file_key}: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    def _extract_file_info_from_content(content: dict, row: dict, default_id: str) -> list:
        """
        Extract file info from content JSON
        
        Args:
            content: Parsed content JSON dict
            row: Database row dict
            default_id: Default ID prefix for fallback filename
            
        Returns:
            List of dicts containing extracted file info for each file
        """
        files_array = content.get("files", [])
        file_sizes_list = content.get("file_sizes", [])
        original_filenames_list = content.get("original_filenames", [])
        default_type = content.get("type", row.get("message_type"))
        default_file_key = content.get("file_key", "")
        
        result = []
        
        # If files_array has valid file dicts, iterate over all of them
        if files_array and isinstance(files_array[0], dict):
            for idx, file_obj in enumerate(files_array):
                if not isinstance(file_obj, dict):
                    continue
                    
                # Extract filename
                filename = file_obj.get("file_name") or file_obj.get("filename") or file_obj.get("original_filename")
                original_name = file_obj.get("original_filename") or file_obj.get("filename") or file_obj.get("file_name")
                if not filename:
                    filename = f"File_{default_id[:8]}_{idx}" if idx > 0 else f"File_{default_id[:8]}"
                    original_name = filename
                
                # Extract file size
                try:
                    file_size = int(file_obj.get("file_size") or file_obj.get("size", 0))
                except (ValueError, TypeError):
                    file_size = 0
                
                # Extract file type
                extracted_type = file_obj.get("type") or default_type
                
                # Extract file key
                file_key = file_obj.get("file_key", "") or default_file_key
                
                result.append({
                    "file_name": filename,
                    "original_name": original_name,
                    "file_size": file_size,
                    "file_type": extracted_type,
                    "type": FileParserDatabaseService.get_simple_type(extracted_type),
                    "contentType": FileParserDatabaseService._get_content_type_by_filename(filename),
                    "file_key": file_key,
                })
        else:
            # Fallback: use top-level arrays (original_filenames, file_sizes, etc.)
            # Determine how many files based on available arrays
            num_files = max(
                len(original_filenames_list),
                len(file_sizes_list),
                1  # At least one file
            )
            
            for idx in range(num_files):
                # Extract filename
                filename = original_filenames_list[idx] if idx < len(original_filenames_list) else None
                original_name = filename
                if not filename:
                    filename = f"File_{default_id[:8]}_{idx}" if idx > 0 else f"File_{default_id[:8]}"
                    original_name = filename
                
                # Extract file size
                if idx < len(file_sizes_list):
                    try:
                        file_size = int(file_sizes_list[idx])
                    except (ValueError, TypeError):
                        file_size = 0
                else:
                    file_size = 0
                
                result.append({
                    "file_name": filename,
                    "original_name": original_name,
                    "file_size": file_size,
                    "file_type": default_type,
                    "type": FileParserDatabaseService.get_simple_type(default_type),
                    "contentType": FileParserDatabaseService._get_content_type_by_filename(filename),
                    "file_key": default_file_key,
                })
        
        return result

    @staticmethod
    def _determine_upload_status(content: dict, message_type: str) -> dict:
        """Determine upload status from content"""
        # First check the status field (works for all file types including genetic)
        status = content.get("status", "")
        
        # Check files array for status (genetic files store status in files[0].status)
        files = content.get("files", [])
        if files and isinstance(files, list) and len(files) > 0:
            first_file = files[0]
            if isinstance(first_file, dict):
                file_status = first_file.get("status", "")
                file_success = first_file.get("success")
                if file_status == "completed" or file_success is True:
                    return {"upload_status": "complete", "progress": 100, "message": "Upload and processing completed"}
                elif file_status == "failed" or first_file.get("error"):
                    return {"upload_status": "failed", "progress": 0, "message": first_file.get("error", "Processing failed")}
                elif file_status == "processing":
                    return {"upload_status": "processing", "progress": first_file.get("progress", 0), "message": first_file.get("progress_message", "Processing...")}
        
        # Check top-level status
        if status == "completed" or content.get("success") is True:
            return {"upload_status": "complete", "progress": 100, "message": "Upload and processing completed"}
        elif status == "failed" or content.get("error"):
            return {"upload_status": "failed", "progress": 0, "message": content.get("error", "Processing failed")}
        elif status == "processing":
            return {"upload_status": "processing", "progress": content.get("progress", 0), "message": content.get("message", "Processing...")}
        
        # Fallback: check message content (with priority: completed > failed > processing)
        message_lower = content.get("message", "").lower()
        if "completed" in message_lower or "successfully" in message_lower:
            return {"upload_status": "complete", "progress": 100, "message": "Upload and processing completed"}
        elif "failed" in message_lower or "error" in message_lower:
            return {"upload_status": "failed", "progress": 0, "message": content.get("message", "Processing failed")}
        elif "processing" in message_lower:
            return {"upload_status": "processing", "progress": content.get("progress", 0), "message": content.get("message", "Processing...")}
        
        # Default to complete if no status indicators found
        return {"upload_status": "complete"}

    @staticmethod
    async def _regenerate_urls(file_info: dict) -> None:
        """Regenerate URLs for file_info in place"""
        file_key = file_info.get("file_key", "")
        
        if not file_key:
            return
            
        try:
            new_url = await FileParserDatabaseService.regenerate_file_url(
                file_key, "", file_info.get("contentType", "application/octet-stream")
            )
            if new_url:
                file_info["url_full"] = new_url
        except Exception as e:
            logging.warning(f"Failed to regenerate URL for {file_key}: {str(e)}", "_regenerate_urls")

    @staticmethod
    async def get_uploaded_files_paginated(
        uploader_user_id: str,
        target_user_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get user's uploaded file history with pagination
        
        Args:
            uploader_user_id: Current user ID (for permission checking)
            target_user_id: Target user ID to query files (None = query uploader_user_id)
            limit: Maximum number of files to return
            offset: Pagination offset
            
        Returns:
            Dict containing files list, total count, and total size
        """
        try:
            query_user_id = target_user_id or uploader_user_id
            query_user_id_str = str(query_user_id)
            
            # Define both queries
            list_sql = """
            SELECT id, user_id, session_id, message_type,
                   theta_ai.decrypt_content(content) AS content, created_at
            FROM theta_ai.th_messages 
            WHERE message_type IN ('image', 'pdf', 'excel', 'genetic', 'file', 'csv')
                AND COALESCE(query_user_id, user_id) = :target_query_user_id
                AND (is_del IS NULL OR is_del = false)
                AND scene = 'web'
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
            """
            
            total_sql = """
            WITH decrypted AS (
                SELECT theta_ai.decrypt_content(content)::jsonb AS cj
                FROM theta_ai.th_messages 
                WHERE message_type IN ('image', 'pdf', 'excel', 'genetic', 'file', 'csv')
                    AND COALESCE(query_user_id, user_id) = :target_query_user_id
                    AND (is_del IS NULL OR is_del = false)
                    AND scene = 'web'
            )
            SELECT 
                COALESCE(SUM(
                    CASE 
                        WHEN cj->'files' IS NOT NULL AND jsonb_typeof(cj->'files') = 'array' 
                             AND jsonb_array_length(cj->'files') > 0 
                        THEN jsonb_array_length(cj->'files')
                        ELSE 1 
                    END
                ), 0)::bigint AS total_files
            FROM decrypted
            """
            
            # Execute both queries in parallel
            list_task = execute_query(query=list_sql, params={
                "target_query_user_id": query_user_id_str,
                "limit": limit, "offset": offset
            })
            total_task = execute_query(query=total_sql, params={"target_query_user_id": query_user_id_str})
            
            result, total_result = await asyncio.gather(list_task, total_task)

            files = []
            is_for_others = target_user_id and target_user_id != uploader_user_id
            timezone = get_req_ctx("timezone", "America/Los_Angeles")
            
            for row in result:
                row_id = row.get("id", "")
                created_at = row.get("created_at")
                # Convert created_at to user's timezone
                if created_at:
                    try:
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=ZoneInfo("UTC"))
                        local_time = created_at.astimezone(ZoneInfo(timezone))
                        create_time = local_time.isoformat()
                    except Exception:
                        create_time = created_at.isoformat()
                else:
                    create_time = None
                message_type = row.get("message_type")
                
                file_info = {
                    "id": row_id, "user_id": row.get("user_id"), "session_id": row.get("session_id"),
                    "message_type": message_type, "create_time": create_time,
                    "upload_status": "complete", "file_size": 0, "file_key": "", "is_uploaded_for_others": False,
                }
                
                content = None
                try:
                    content = json.loads(row.get("content", "{}"))
                    if isinstance(content, dict):
                        extracted_files = FileParserDatabaseService._extract_file_info_from_content(content, row, row_id)
                        upload_status = FileParserDatabaseService._determine_upload_status(content, message_type)
                        
                        for idx, extracted in enumerate(extracted_files):
                            file_entry = file_info.copy()
                            # Add sub-index for multiple files from same row
                            if len(extracted_files) > 1:
                                file_entry["id"] = f"{row_id}#{idx}"
                            file_entry.update(extracted)
                            file_entry["upload_time"] = create_time
                            file_entry.update(upload_status)
                            if is_for_others:
                                file_entry.update({"target_user_id": target_user_id, "is_uploaded_for_others": True, "target_user_name": f"User{target_user_id[:8]}"})
                            files.append(file_entry)
                        continue  # Skip the default append below
                    else:
                        content = None  # Treat non-dict as parse failure
                except Exception as e:
                    logging.warning(f"Parse failed: {e}, row_id={row_id}")
                
                # Apply defaults if content parsing failed
                if not isinstance(content, dict):
                    file_info.update({
                        "file_name": f"File_{row_id[:8]}", "original_name": f"File_{row_id[:8]}",
                        "file_type": message_type, "type": FileParserDatabaseService.get_simple_type(message_type),
                        "contentType": "application/octet-stream", "upload_time": create_time,
                    })
                    files.append(file_info)
            
            # Regenerate URLs concurrently for files with valid file_key
            files_with_keys = [f for f in files if f.get("file_key")]
            if files_with_keys:
                await asyncio.gather(*[FileParserDatabaseService._regenerate_urls(f) for f in files_with_keys], return_exceptions=True)

            # Extract total from parallel query result
            total_files = total_result[0].get("total_files", 0) if total_result else 0

            logging.info(f"Success: query_user_id={query_user_id}, total={total_files}")

            return {"files": files, "total": total_files, "limit": limit, "offset": offset}

        except Exception as e:
            logging.error(f"Get uploaded files failed: {str(e)}", "get_uploaded_files_paginated", stack_info=True)
            raise Exception(f"Failed to get uploaded files: {str(e)}")

    @staticmethod
    async def delete_uploaded_file(
        file_id: str,
        user_id: str,
        target_user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Soft delete uploaded file
        
        Args:
            file_id: File ID (message ID)
            user_id: Current user ID (who is performing the deletion)
            target_user_id: Target user ID (whose file is being deleted, if different from user_id)
            
        Returns:
            Dict containing deletion result
        """
        try:
            # Handle file_id with sub-index suffix (e.g., "msg_id#0" -> "msg_id")
            # This suffix is added by get_uploaded_files_paginated when a message contains multiple files
            original_file_id = file_id
            if "#" in file_id:
                file_id = file_id.split("#")[0]
                logging.info(f"Parsed file_id: original={original_file_id}, actual_msg_id={file_id}")
            
            # Determine the actual file owner (target user if specified, otherwise current user)
            file_owner_id = target_user_id if target_user_id else user_id
            
            logging.info(f"Delete file request: file_id={file_id}, operator_user_id={user_id}, file_owner_id={file_owner_id}")

            # Query file info - check if file exists and belongs to the expected owner
            # Use query_user_id to determine file ownership (with fallback to user_id for old data)
            query_sql = """
            SELECT theta_ai.decrypt_content(content) AS content, message_type, user_id
            FROM  theta_ai.th_messages 
            WHERE id = :file_id 
                AND COALESCE(query_user_id, user_id) = :file_owner_id
                AND message_type <> 'text'
                AND (is_del IS NULL OR is_del = false)
            """

            query_params = {
                "file_id": file_id,
                "file_owner_id": file_owner_id,
            }

            file_info = await execute_query(
                query=query_sql,
                params=query_params,
            )

            if not file_info or len(file_info) == 0:
                logging.warning(f"File not found or not owned by expected user: file_id={file_id}, expected_owner={file_owner_id}")
                return {
                    "success": False,
                    "code": "NOT_FOUND",
                    "message": "File not found",
                }

            # Parse file type for cleanup purposes
            file_type = "unknown"
            try:
                content = file_info[0].get("content", "")
                if content:
                    content_json = json.loads(content)
                    file_type = content_json.get("type", "unknown")
            except (json.JSONDecodeError, Exception) as e:
                logging.warning(f"Failed to parse file content, using default type: {str(e)}")

            # Update message record, mark as deleted
            update_sql = """
            UPDATE  theta_ai.th_messages 
            SET is_del = true,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :file_id 
                AND user_id = :user_id
                AND message_type <> 'text'
            RETURNING id
            """

            result = await execute_query(
                query=update_sql,
                params={"file_id": file_id, "user_id": str(file_owner_id)},
            )

            if result and len(result) > 0:
                # Execute cascade delete based on file type
                cascade_result = await FileParserDatabaseService._cascade_delete_by_file_type(
                    file_type, file_id, str(file_owner_id)
                )

                logging.info(f"File delete success: file_id={file_id}, file_type={file_type}, file_owner_id={file_owner_id}")

                return {
                    "success": True,
                    "code": "SUCCESS",
                    "message": "File deleted successfully",
                    "data": {
                        "file_id": file_id,
                        "cascade_deleted": cascade_result,
                    }
                }
            else:
                logging.warning(f"File not found or no permission to delete: file_id={file_id}, user_id={user_id}")

                return {
                    "success": False,
                    "code": "UPDATE_FAILED",
                    "message": "File not found or already deleted",
                }

        except Exception as e:
            logging.error(f"Delete file failed: {str(e)}", stack_info=True)

            return {
                "success": False,
                "code": "INTERNAL_ERROR",
                "message": f"Delete file failed: {str(e)}",
            }

    async def _cascade_delete_by_file_type(file_type: str, file_id: str, user_id: str) -> None:
        """
        Execute cascade delete operations based on file type

        Args:
            file_type: File type (excel, genetic, image, pdf, etc.)
            file_id: File ID (message ID)
            user_id: Operating user ID
            target_user_id: Target user ID (for files uploaded for others)
        """
        try:
            # Since files are stored under target user ID, data_user_id is always the same as user_id from th_messages
            data_user_id = user_id

            if file_type == "excel":
                # Excel files: delete data in th_series_data table with source_table='excel'
                await FileParserDatabaseService._delete_th_series_data(data_user_id, "excel", file_id)

            elif file_type == "genetic":
                # Genetic files: use specialized genetic data deletion method
                await FileParserDatabaseService._delete_genetic_data(data_user_id, file_id)

            else:
                # Other file types: delete data in th_series_data table with source_table='theta_ai.th_messages'
                await FileParserDatabaseService._delete_th_series_data(data_user_id, "theta_ai.th_messages", file_id)

            logging.info(f"Cascade delete successful: file_type={file_type}, file_id={file_id}, data_user_id={data_user_id}")

        except Exception as e:
            logging.warning(f"Cascade delete failed: file_type={file_type}, file_id={file_id}, error={str(e)}")


    async def _delete_th_series_data(user_id: str, source_table: str, source_table_id: str) -> None:
        """
        Delete related data in th_series_data table
        """
        try:
            delete_sql = """
            DELETE FROM theta_ai.th_series_data 
            WHERE user_id = :user_id 
            AND source_table = :source_table 
            AND source_table_id = :source_table_id
            """

            result = await execute_query(
                delete_sql,
                {
                    "user_id": user_id,
                    "source_table": source_table,
                    "source_table_id": source_table_id,
                },
            )

            delete_count = len(result) if result else 0
            logging.info(f"Deleted th_series_data successfully: user_id={user_id}, source_table={source_table}, source_table_id={source_table_id}, deleted_records={delete_count}")

        except Exception as e:
            logging.warning(f"Failed to delete th_series_data: {str(e)}", stack_info=True)
            raise


    async def _delete_genetic_data(user_id: str, message_id: str) -> None:
        """
        Delete genetic data
        """
        try:

            delete_success = await FileParserDatabaseService.delete_genetic_data_by_message_id(user_id, message_id)

            if delete_success:
                logging.info(f"Genetic data deleted successfully: user_id={user_id}, message_id={message_id}")
            else:
                logging.info(f"Delete genetic data: user_id={user_id}, message_id={message_id}, no related data found")

        except Exception as e:
            logging.warning(f"Failed to delete genetic data: {str(e)}", stack_info=True)
            raise

    @staticmethod
    async def get_weekly_health_stats(
        user_id: str,
        end_date: str,
        period: int = 7,
    ) -> Dict[str, Any]:
        """
        Get weekly health statistics for dashboard display
        
        Args:
            user_id: User ID
            end_date: End date in YYYY-MM-DD format
            period: Number of days to analyze (defaults to 7)
            
        Returns:
            Dict containing weekly health statistics
        """
        try:
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")

            # Calculate date range for the last N days
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            start_date_obj = end_date_obj - timedelta(days=period - 1)  # period-1 to include end_date

            # SQL query to get daily data for the last N days
            sql = """
                SELECT
                    uis_inner.indicator,
                    DATE(uis_inner.start_time) AS date,
                    
                    -- Choose sum or avg based on indicator type
                    ROUND(AVG(
                        CASE
                        WHEN uis_inner.indicator IN (
                            'daily_stats_cyclingDistances',
                            'daily_stats_exerciseMinutes',
                            'daily_stats_floors',
                            'daily_stats_steps',
                            'daily_stats_walkingRunningDistances',
                            'daily_stats_sleepAnalysis_Awake',
                            'daily_stats_sleepAnalysis_Asleep(REM)',
                            'daily_stats_sleepAnalysis_Asleep(Deep)',
                            'daily_stats_sleepAnalysis_Asleep(Core)'
                        )
                        THEN (uis_inner.value::json ->> 'sum')::numeric
                        ELSE (uis_inner.value::json ->> 'avg')::numeric
                        END
                    ), 2) AS daily_value,
                    
                    idim.unit AS unit,
                    COALESCE(idim.standard_indicator, uis_inner.indicator) as display_name,
                    COALESCE(idim.category, 'other') as category
                FROM theta_ai.th_series_data uis_inner
                LEFT JOIN theta_ai.th_series_dim idim
                ON uis_inner.indicator = idim.original_indicator
                WHERE uis_inner.user_id = :user_id
                AND DATE(uis_inner.start_time) BETWEEN :start_date AND :end_date
                AND uis_inner.value LIKE '{%'
                AND uis_inner.deleted = 0
                GROUP BY uis_inner.indicator, DATE(uis_inner.start_time), idim.unit, display_name, category
                ORDER BY uis_inner.indicator, DATE(uis_inner.start_time);
            """

            result = await execute_query(
                query=sql,
                params={
                    "user_id": user_id,
                    "start_date": start_date_obj,
                    "end_date": end_date_obj,
                },
            )

            # Convert Decimal values to strings for JSON serialization
            for row in result:
                if "daily_value" in row and row["daily_value"] is not None:
                    row["daily_value"] = str(row["daily_value"])

            # Organize data by indicator for chart display
            indicators_data = {}

            for row in result:
                indicator = row["indicator"]
                date_str = row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])
                value = float(row["daily_value"]) if row["daily_value"] else 0

                if indicator not in indicators_data:
                    indicators_data[indicator] = {
                        "indicator": indicator,
                        "display_name": row.get("display_name", indicator),
                        "unit": row.get("unit", ""),
                        "category": row.get("category", ""),
                        "daily_data": [],
                        "total_value": 0,
                        "avg_value": 0,
                        "max_value": 0,
                        "min_value": float("inf"),
                    }

                # Add daily data point
                indicators_data[indicator]["daily_data"].append({"date": date_str, "value": value})

                # Update statistics
                indicators_data[indicator]["total_value"] += value
                indicators_data[indicator]["max_value"] = max(indicators_data[indicator]["max_value"], value)
                indicators_data[indicator]["min_value"] = min(indicators_data[indicator]["min_value"], value)

            # Calculate averages and prepare final data
            for indicator, data in indicators_data.items():
                data_count = len(data["daily_data"])
                if data_count > 0:
                    data["avg_value"] = round(data["total_value"] / data_count, 2)
                    if data["min_value"] == float("inf"):
                        data["min_value"] = 0

                # Sort daily data by date
                data["daily_data"].sort(key=lambda x: x["date"])

            # Generate date range for the period
            date_range = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                date_range.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)

            response_data = {
                "indicators": list(indicators_data.values()),
                "date_range": date_range,
                "period": {
                    "start_date": start_date_obj.strftime("%Y-%m-%d"),
                    "end_date": end_date_obj.strftime("%Y-%m-%d"),
                    "days": period,
                },
                "summary": {
                    "total_indicators": len(indicators_data),
                    "date_range_days": len(date_range),
                },
            }

            logging.info(f"Get weekly health stats success: user_id={user_id}, indicators={len(indicators_data)}")

            return response_data

        except Exception as e:
            logging.error(f"Get weekly health stats failed: {str(e)}", stack_info=True)
            raise Exception(f"Failed to get weekly health stats: {str(e)}")

    @staticmethod
    async def get_user_data_distribution(user_id: str) -> Dict[str, Any]:
        """
        Get user data distribution

        Args:
            user_id: User ID

        Returns:
            Dictionary containing data distribution information

        Raises:
            Exception: Thrown when query fails
        """
        try:
            # Ensure user_id is string type
            user_id = str(user_id)

            logging.info(f"Getting user data distribution: user_id={user_id}")

            # Build query SQL - split departments by comma and calculate count for each department, null values go to "Other"
            query = """
            SELECT * FROM (
                -- Handle null department data
                SELECT 
                    'Other' as category,
                    MAX(update_time) as last_update_time,
                    COUNT(1) as record_count
                FROM theta_ai.v_th_series_data
                WHERE user_id = :user_id
                  AND (department IS NULL OR TRIM(department) = '')
                
                UNION ALL
                
                -- Handle non-empty department data using LATERAL split
                SELECT 
                    TRIM(dept_expanded.dept) as category,
                    MAX(v.update_time) as last_update_time,
                    COUNT(1) as record_count
                FROM theta_ai.v_th_series_data v
                CROSS JOIN LATERAL unnest(string_to_array(v.department, ',')) AS dept_expanded(dept)
                WHERE v.user_id = :user_id
                  AND v.department IS NOT NULL 
                  AND TRIM(v.department) != ''
                  AND TRIM(dept_expanded.dept) != ''
                GROUP BY TRIM(dept_expanded.dept)
                
                UNION ALL
                
                -- Handle genetic data
                SELECT 
                    'genetic' as category,
                    MAX(update_time) as last_update_time,
                    COUNT(1) as record_count
                FROM theta_ai.th_series_data_genetic
                WHERE user_id = :user_id
                
            ) AS data_distribution
            WHERE category IS NOT NULL 
              AND TRIM(category) != ''
            ORDER BY 3 DESC, 2 DESC
            """

            params = {"user_id": user_id}


            # Execute query
            results = await execute_query(
                query=query,
                params=params,
            )

            # Process results
            distribution_data = []

            for row in results:
                if isinstance(row, dict):
                    item = {
                        "category": row["category"],
                        "last_update_time": row["last_update_time"].isoformat() if row["last_update_time"] else None,
                        "record_count": row["record_count"],
                    }
                else:
                    # Handle tuple format
                    item = {
                        "category": row[0],
                        "last_update_time": row[1].isoformat() if row[1] else None,
                        "record_count": row[2],
                    }

                distribution_data.append(item)

            # Get actual total records from th_series_data and th_series_data_genetic tables
            # This should NOT be the sum of category counts to avoid double-counting when records have multiple departments
            total_records_query = """
            SELECT 
                (SELECT COUNT(1) FROM theta_ai.th_series_data WHERE user_id = :user_id AND deleted = 0) +
                (SELECT COUNT(1) FROM theta_ai.th_series_data_genetic WHERE user_id = :user_id AND is_deleted = false)
                AS total_records
            """

            # logging.info(f"total_records_query: {total_records_query}")
            
            total_records_result = await execute_query(
                query=total_records_query,
                params={"user_id": user_id},
            )
            
            # logging.info(f"total_records_result: {total_records_result}")

            # Get total records from the query result
            if total_records_result and len(total_records_result) > 0:
                if isinstance(total_records_result[0], dict):
                    total_records = total_records_result[0]["total_records"] or 0
                else:
                    total_records = total_records_result[0][0] or 0
            else:
                total_records = 0

            logging.info(f"Query completed: user={user_id}, categories={len(distribution_data)}, total_records={total_records}")

            return {
                "user_id": user_id,
                "total_categories": len(distribution_data),
                "total_records": total_records,
                "distribution": distribution_data,
            }

        except Exception as e:
            logging.error(f"Failed to query data distribution: {str(e)}", stack_info=True)
            raise Exception(f"Failed to query data distribution: {str(e)}")

    @staticmethod
    async def save_file_upload_message(
        msg_id: str,
        user_id: str,
        session_id: str,
        content: Dict[str, Any],
        message_type: str,
        query_user_id: str = None
    ) -> bool:
        """
        Save file upload message to th_messages table
        
        Args:
            msg_id: Message ID
            user_id: User ID
            session_id: Session ID 
            content: Message content dictionary
            message_type: Message type (file, image, pdf, excel, etc.)
            query_user_id: Query user ID (optional, defaults to user_id)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Default query_user_id to user_id if not provided
            if query_user_id is None:
                query_user_id = user_id
            
            # Ensure content is JSON string
            if isinstance(content, dict):
                content_json = json.dumps(content, ensure_ascii=False)
            else:
                content_json = content

            insert_sql = """
                INSERT INTO theta_ai.th_messages (
                    id, 
                    user_id,
                    query_user_id,
                    session_id, 
                    role, 
                    content, 
                    message_type,
                    scene,
                    created_at
                ) VALUES (
                    :id,
                    :user_id,
                    :query_user_id,
                    :session_id,
                    :role,
                    theta_ai.encrypt_content(:content),
                    :message_type,
                    :scene,
                    NOW()
                )
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            """
            
            result = await execute_query(
                query=insert_sql,
                params={
                    "id": msg_id,
                    "user_id": user_id,
                    "query_user_id": query_user_id,
                    "session_id": session_id,
                    "role": "user",
                    "content": content_json,
                    "message_type": message_type,
                    "scene": "web"
                },
            )
            
            if result:
                logging.info(f"File upload message saved to database with msg_id: {msg_id}")
                return True
            else:
                logging.info(f"File upload message already exists or insert skipped for msg_id: {msg_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error saving file upload message: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def update_message_processed_files(
        msg_id: str,
        processed_files: List[Dict[str, Any]]
    ) -> bool:
        """
        Update th_messages with processed file information
        
        Args:
            msg_id: Message ID
            processed_files: List of processed file information
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First get current content to merge with processed files
            get_sql = """
                SELECT theta_ai.decrypt_content(content) AS content
                FROM theta_ai.th_messages 
                WHERE id = :msg_id
            """
            
            current_result = await execute_query(
                query=get_sql,
                params={"msg_id": msg_id},
            )
            
            if not current_result or not current_result[0]:
                logging.error(f"Message not found for msg_id: {msg_id}")
                return False
            
            current_content = current_result[0]["content"]
            if isinstance(current_content, str):
                current_content = json.loads(current_content)
            
            # Merge processed files with existing files info
            existing_files = current_content.get("files", [])
            merged_files = []
            
            for existing_file in existing_files:
                # Find matching processed file by filename
                processed_file = next(
                    (pf for pf in processed_files if pf["filename"] == existing_file["filename"]),
                    None
                )
                
                if processed_file:
                    # Merge existing file info with processed info
                    merged_file = {**existing_file, **processed_file}
                    merged_files.append(merged_file)
                else:
                    # Keep existing file info if not processed
                    merged_files.append(existing_file)
            
            # Update content with merged files
            current_content["files"] = merged_files
            current_content["processed"] = True
            current_content["processing_completed_at"] = datetime.now().isoformat()
            
            # Convert to JSON string
            updated_content_json = json.dumps(current_content, ensure_ascii=False)
            
            # Update content field with merged data
            update_sql = """
                UPDATE theta_ai.th_messages 
                SET content = theta_ai.encrypt_content(:content)
                WHERE id = :msg_id
                RETURNING id
            """
            
            result = await execute_query(
                query=update_sql,
                params={
                    "msg_id": msg_id,
                    "content": updated_content_json
                },
            )
            
            if result:
                logging.info(f"Updated processed files for msg_id: {msg_id}, files_count: {len(processed_files)}")
                return True
            else:
                logging.error(f"Failed to update processed files for msg_id: {msg_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error updating processed files: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def update_message_llm_analysis(
        msg_id: str,
        llm_analysis: Dict[str, Any]
    ) -> bool:
        """
        Update th_messages with LLM analysis result
        
        Args:
            msg_id: Message ID
            llm_analysis: LLM analysis result
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # First get current content to merge with LLM analysis
            get_sql = """
                SELECT theta_ai.decrypt_content(content) AS content
                FROM theta_ai.th_messages 
                WHERE id = :msg_id
            """
            
            current_result = await execute_query(
                query=get_sql,
                params={"msg_id": msg_id},
            )
            
            if not current_result or not current_result[0]:
                logging.error(f"Message not found for msg_id: {msg_id}")
                return False
            
            current_content = current_result[0]["content"]
            if isinstance(current_content, str):
                current_content = json.loads(current_content)
            
            # Add LLM analysis to content
            current_content["llm_analysis"] = llm_analysis
            current_content["llm_analyzed_at"] = datetime.now().isoformat()
            
            # Update the message
            update_sql = """
                UPDATE theta_ai.th_messages
                SET content = theta_ai.encrypt_content(:content),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :msg_id
            """
            
            update_result = await execute_query(
                query=update_sql,
                params={
                    "msg_id": msg_id,
                    "content": json.dumps(current_content, ensure_ascii=False)
                },
            )
            
            if update_result:
                logging.info(f"Successfully updated LLM analysis for msg_id: {msg_id}")
                return True
            else:
                logging.error(f"Failed to update LLM analysis for msg_id: {msg_id}")
                return False
                
        except Exception as e:
            logging.error(f"Error updating LLM analysis: {str(e)}", stack_info=True)
            return False


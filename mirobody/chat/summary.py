"""
Chat Summary Generation Module

Provides conversation summary generation using the unified LLM interface.
Automatically selects the best available LLM provider.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from mirobody.utils import execute_query
from mirobody.utils.llm import async_get_text_completion


#-----------------------------------------------------------------------------

async def generate_summary(conversation_text: str) -> str:
    try:
        if len(conversation_text.strip()) < 5:
            return conversation_text.strip()

        prompt = f"""Based on this conversation, generate a concise topic summary (max 50 characters in Chinese or English).
                Only output the summary text, no explanations or quotes. You shall mainly focus on the user's request.
                Please note that the conversation may contain multiple messages from different users. You shall mainly focus on the user's request.
                The summary should be in the same language as the user's question.
                
                Conversation:
                {conversation_text[:1000]}

                Summary:"""
        result = await async_get_text_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=100,
        )

        summary = result.strip() if result else ""
        
        if len(summary) > 60:
            summary = summary[:57] + "..."

        logging.info(f"Generated summary: {summary})")
        return summary
            
    except Exception as e:
        logging.warning(f"Failed to generate LLM summary, falling back to simple truncation: {str(e)}")

        first_line = conversation_text.split('\n')[0]
        return first_line[:50].replace("User:", " ").replace("Assistant:", " ") + "..." if len(first_line) > 50 else first_line

#-----------------------------------------------------------------------------

async def generate_and_save_summary(user_id: str, session_id: str, provider: Optional[str] = None) -> Dict[str, Any]:
    try:
        messages_sql = """
            SELECT role, theta_ai.decrypt_content(content) AS content, created_at
            FROM theta_ai.th_messages
            WHERE session_id = :session_id AND user_id = :user_id
            ORDER BY created_at ASC
            LIMIT 10
        """
        
        messages = await execute_query(
            messages_sql,
            params={"session_id": session_id, "user_id": user_id}
        )
        
        if not messages:
            logging.warning(f"No messages found for session {session_id}")
            return None

        conversation_text = ""
        for msg in messages[:10]:
            role = msg.get("role", "")
            content = msg.get("content", "")
            
            if isinstance(content, str):
                try:
                    content_obj = json.loads(content)
                    if isinstance(content_obj, list):
                        content = "".join([
                            block.get("content", "") 
                            for block in content_obj 
                            if block.get("type") == "reply"
                        ])
                except:
                    pass
            
            role_label = "User" if role == "user" else "Assistant"
            conversation_text += f"{role_label}: {content}\n\n"
        
        summary = await generate_summary(conversation_text.strip())

        await save_conversation_summary(user_id, session_id, summary)

        logging.info(f"session:{session_id}\tSuccessfully saved conversation summary: {summary}")
        return {"event": "summary_generated", "session_id": session_id, "summary": summary}

    except Exception as e:
        logging.error(f"Error in generate_and_save_summary: {str(e)}", exc_info=True)
        return None

#-----------------------------------------------------------------------------

async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
    try:
        logging.info(f"save_conversation_summary: {user_id}, {session_id}, {summary}")

        summary_sql = """
            INSERT INTO theta_ai.th_sessions (
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
        logging.error(f"Error saving conversation summary: {str(e)}", exc_info=True)
        return False

#-----------------------------------------------------------------------------

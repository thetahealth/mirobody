"""
Personal Memory Service
Provides MCP tool for searching user's personal memories.

Only registered when EVERMEMOS_API_KEY is configured.
"""

import logging

from typing import Any, Dict, Optional

from mirobody.chat.memory import get_profile, search_memory

#-----------------------------------------------------------------------------

class MemoryService:
    """Memory Service"""

    def __init__(self):
        self.name = "Personal Memory Service"
        self.version = "1.0.0"

    @staticmethod
    def _enabled() -> bool:
        """Only register when EVERMEMOS_API_KEY is configured."""
        from mirobody.utils import global_config
        return bool(global_config().get_str("EVERMEMOS_API_KEY"))

    async def search_user_memories(
        self,
        user_info: Dict[str, Any],
        query: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Search user's personal memories by semantic query.
        Use this tool to recall user's past experiences, preferences, habits, or any personal information.

        Args:
            query: A descriptive query to search memories. e.g. "exercise habits", "dietary preferences"
            start_time: Start date filter ("YYYY-MM-DD").
            end_time: End date filter ("YYYY-MM-DD").
        """
        try:
            user_id = user_info.get("user_id")
            if not user_id or not isinstance(user_id, str):
                return {"success": False, "error": "Authorization required."}

            if not query or not isinstance(query, str):
                return {"success": False, "error": "Query cannot be empty."}

            query = query.strip()
            if not query:
                return {"success": False, "error": "Query cannot be empty."}

            memories, err = await search_memory(
                user_id=user_id,
                query=query,
                start_time=start_time,
                end_time=end_time,
            )

            if err:
                logging.error(f"[GetPersonalMemories] Error: {err}")
                return {"success": False, "error": err}

            return {
                "success": True,
                "message": "Ok" if memories else "No memories found",
                "memories": memories if memories else None,
            }

        except Exception as e:
            logging.error(f"[GetPersonalMemories] Error: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    #-------------------------------------------------------------------------

    async def get_user_memory_profile(
        self,
        user_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Get user's memory-based profile: preferences, habits, and personal traits extracted from past conversations.
        This is NOT the health profile — use get_user_health_profile for that.

        Args:
            no args needed.
        """
        try:
            user_id = user_info.get("user_id")
            if not user_id or not isinstance(user_id, str):
                return {"success": False, "error": "Authorization required."}

            profile, err = await get_profile(user_id=user_id)

            if err:
                logging.error(f"[GetUserMemoryProfile] Error: {err}")
                return {"success": False, "error": err}

            return {
                "success": True,
                "message": "Ok" if profile else "No profile found",
                "profile": profile if profile else None,
            }

        except Exception as e:
            logging.error(f"[GetUserMemoryProfile] Error: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

#-----------------------------------------------------------------------------

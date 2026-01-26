"""
Database service for Vital providers
"""

import logging
import traceback
from typing import Dict

from ....utils import execute_query


class AppleDatabaseService:

    def __init__(self):
        pass

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> bool:
        """
        Update LLM access permission for a user's provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier (without vital prefix)
            llm_access: Access level (0: no access, 1: limited access, 2: full access)

        Returns:
            Whether update was successful
        """
        try:
            existing_query = """
            SELECT id, username, create_at
            FROM health_user_provider
            WHERE user_id = :user_id AND provider = :provider AND is_del = FALSE
            ORDER BY create_at DESC
            LIMIT 1
            """

            existing_result = await execute_query(
                query=existing_query,
                params={"user_id": user_id, "provider": provider_slug},
            )

            if existing_result and len(existing_result) > 0:
                update_query = """
                UPDATE health_user_provider
                SET llm_access = :llm_access, update_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """

                await execute_query(
                    query=update_query,
                    params={"id": existing_result[0]["id"], "llm_access": llm_access},
                )

            else:
                insert_query = """
                INSERT INTO health_user_provider 
                (user_id, provider, username, password, llm_access, is_del, reconnect, create_at, update_at)
                VALUES (:user_id, :provider, :username, :password, :llm_access, :is_del, :reconnect, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """

                await execute_query(
                    query=insert_query,
                    params={
                        "user_id": user_id,
                        "provider": provider_slug,
                        "username": '',
                        "password": '',
                        "llm_access": llm_access,
                        "is_del": False,
                        "reconnect": 0,  # Always start with normal status
                    },
                )

            logging.info(f"Successfully updated LLM access to {llm_access} for user {user_id}, provider {provider_slug}")
            return True

        except Exception as e:
            logging.error(f"Error updating LLM access for user {user_id}, provider {provider_slug}: {str(e)}", extra={"error": traceback.format_exc()})
            return False

    async def get_user_apple_providers_with_llm_access(self, user_id: str) -> Dict[str, int]:
        """
        Get user's apple providers with their LLM access permissions

        Args:
            user_id: User ID

        Returns:
            Dict mapping provider_slug (without apple prefix) to llm_access level
        """
        try:
            query = """
            SELECT provider, llm_access
            FROM health_user_provider
            WHERE user_id = :user_id AND provider in ('apple_health', 'cda') AND is_del = FALSE
            """

            result = await execute_query(
                query=query,
                params={"user_id": user_id},
            )

            provider_llm_map = {}
            if result:
                for row in result:
                    provider_slug = row["provider"]
                    llm_access = row["llm_access"]
                    provider_llm_map[provider_slug] = llm_access

            logging.info(f"Retrieved LLM access for {len(provider_llm_map)} apple providers for user {user_id}")
            return provider_llm_map

        except Exception as e:
            logging.error(f"Error getting apple providers LLM access for user {user_id}: {str(e)}", extra={"error": traceback.format_exc()})
            return {}

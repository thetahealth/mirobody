"""The Apple platform's link rows: an LLM-access flag per pushed provider."""

import logging

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class AppleDatabaseService:

    def __init__(self):
        pass

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> bool:
        """
        Update LLM access permission for a user's provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier (apple_health or cda)
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

            return True

        except Exception as e:
            logger.error("LLM access update failed: user_id=%s provider=%s error_type=%s", user_id, provider_slug,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return False

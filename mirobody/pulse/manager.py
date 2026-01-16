"""Platform manager module for coordinating multiple platforms"""

import logging

from typing import Any, Dict, List, Optional

from .base import LinkRequest, Platform, ProviderInfo, UserProvider
from .core import LinkType
from .core.database import ManageDatabaseService


class PlatformManager:
    """
    Platform Manager for coordinating multiple health data platforms

    Manages platform lifecycle and provides unified interface for platform operations
    """

    def __init__(self):
        self._platforms: Dict[str, Platform] = {}
        self.db_service = ManageDatabaseService()  # Shared database service instance, maintains cache

    def register_platform(self, platform: Platform) -> None:
        """Register a platform"""
        self._platforms[platform.name] = platform
        logging.info(f"Registered platform: {platform.name}")

    def get_platform(self, platform_name: str) -> Optional[Platform]:
        """Get platform by name"""
        return self._platforms.get(platform_name)

    async def get_all_providers(self, nocache: bool = False) -> List[ProviderInfo]:
        """Get all providers from all platforms"""
        all_providers = []
        for platform_name, platform in self._platforms.items():
            try:
                providers = await platform.get_providers(nocache=nocache)
                all_providers.extend(providers)
                logging.info(f"Got {len(providers)} providers from platform: {platform_name}")
            except Exception as e:
                logging.error(f"Error getting providers from platform {platform_name}: {str(e)}")
                continue

        logging.info(f"Total providers from all platforms: {len(all_providers)}")
        return all_providers

    async def get_user_providers(self, user_id: str) -> List[UserProvider]:
        """Get user providers from all platforms"""
        all_providers = []
        for platform_name, platform in self._platforms.items():
            try:
                providers = await platform.get_user_providers(user_id)
                all_providers.extend(providers)
                logging.info(f"Got {len(providers)} providers from platform {platform_name} for user {user_id}")
            except Exception as e:
                logging.error(f"Error getting user providers from platform {platform_name}: {str(e)}")
                continue

        logging.info(f"Total providers for user {user_id}: {len(all_providers)}")
        return all_providers

    async def link_provider(
            self,
            user_id: str,
            provider_slug: str,
            platform: str,
            auth_type: str,
            credentials: Dict[str, Any],
            options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Link provider through platform"""
        # 1. Validate authentication type
        auth_type_map = {
            "oauth": LinkType.OAUTH,  # Legacy OAuth, treated as OAuth2 in router (default)
            "oauth1": LinkType.OAUTH1,  # OAuth 1.0a
            "oauth2": LinkType.OAUTH2,  # OAuth 2.0
            "password": LinkType.PASSWORD,
            "token": LinkType.TOKEN,
            "email": LinkType.EMAIL,
            "link_token": LinkType.LINK_TOKEN,
            "api_key": LinkType.API_KEY,
            "customized": LinkType.CUSTOMIZED,  # Customized connection with dynamic fields
        }

        if auth_type not in auth_type_map:
            raise ValueError(f"Unsupported auth type: {auth_type}")

        link_type = auth_type_map[auth_type]

        # 2. Validate credential completeness
        if auth_type == "password":
            if not credentials.get("username") or not credentials.get("password"):
                raise ValueError("Username and password are required for password auth")
        elif auth_type == "token" or auth_type == "link_token":
            # token/link_token type must provide token
            if not credentials.get("token"):
                raise ValueError("Token is required for token auth")
        elif auth_type == "oauth":
            # OAuth type allows not providing token (step 1 generate token) or providing token (step 2 complete connection)
            pass
        elif auth_type == "email":
            if not credentials.get("email"):
                raise ValueError("Email is required for email auth")
        elif auth_type == "customized":
            if not credentials.get("connect_info"):
                raise ValueError("connect_info is required for customized auth")
            # Customized type: fields are directly in credentials (e.g., host, port, database)
            # No specific validation here - let provider validate
            pass

        # 3. Build standard request object
        request = LinkRequest(
            user_id=user_id,
            provider_slug=provider_slug,
            auth_type=link_type,
            credentials=credentials,
            options=options or {},
            platform=platform,
        )

        target_platform = self.get_platform(request.platform)
        if not target_platform:
            logging.warning(f"Platform not found: {request.platform}")
            raise ValueError(f"Platform {request.platform} not found")

        try:
            result_data = await target_platform.link(request)
            logging.info(f"Link successful for provider {request.provider_slug}")
            return result_data

        except Exception as e:
            logging.error(f"Error linking provider {request.provider_slug}: {str(e)}")
            raise e

    async def unlink_provider(self, user_id: str, provider_slug: str, platform: str) -> Dict[str, Any]:
        """
        Unlink Provider connection

        Args:
            user_id: User ID
            provider_slug: Provider identifier
            platform: Platform identifier

        Returns:
            Unlink result data, throws exception on failure
        """
        target_platform = self.get_platform(platform)
        if not target_platform:
            logging.warning(f"Platform not found: {platform}")
            raise ValueError(f"Platform {platform} not found")

        try:
            result_data = await target_platform.unlink(user_id, provider_slug)
            logging.info(f"Unlink successful for provider {provider_slug}")
            return result_data

        except Exception as e:
            logging.error(f"Error unlinking provider {provider_slug}: {str(e)}")
            raise RuntimeError(f"Failed to unlink provider: {str(e)}")

    async def post_data(
            self,
            platform: str,
            provider_slug: str,
            data: Dict[str, Any],
            msg_id: Optional[str] = None,
    ) -> bool:
        target_platform = self.get_platform(platform)
        if not target_platform:
            logging.warning(f"Platform not found: {platform}")
            return False

        try:
            result = await target_platform.post_data(provider_slug, data, msg_id)
            logging.info(f"Post data result for provider {provider_slug}: {result}")
            return result
        except Exception as e:
            logging.error(f"Error posting data to provider {provider_slug}: {str(e)}")
            return False

    async def update_llm_access(
            self, user_id: str, provider_slug: str, platform: str, llm_access: int
    ) -> Dict[str, Any]:
        """
        Update LLM access permission for a user's provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier
            platform: Platform identifier
            llm_access: Access level (0: no access, 1: limited access, 2: full access)

        Returns:
            Update result data

        Raises:
            ValueError: When platform not found or invalid parameters
            RuntimeError: When update fails
        """
        # Validate llm_access value
        if llm_access not in [0, 1, 2]:
            raise ValueError(f"Invalid llm_access value: {llm_access}. Must be 0, 1, or 2")

        target_platform = self.get_platform(platform)
        if not target_platform:
            logging.warning(f"Platform not found: {platform}")
            raise ValueError(f"Platform {platform} not found")

        try:
            result_data = await target_platform.update_llm_access(user_id, provider_slug, llm_access)
            logging.info(f"Updated LLM access successful for provider {provider_slug}")
            return result_data

        except Exception as e:
            logging.error(f"Error updating LLM access for provider {provider_slug}: {str(e)}")
            raise RuntimeError(f"Failed to update LLM access: {str(e)}")

    async def populate_provider_stats(self, user_id: str, providers: List[UserProvider]) -> List[UserProvider]:
        """
       Populate provider statistics for all providers at once using cached query

       Args:
           user_id: User ID
           providers: List of UserProvider objects to populate
       """
        try:
            stats_dict = await self.db_service.get_user_provider_stats_cached(user_id)
            logging.info(f"Got cached stats for {len(stats_dict)} sources for user {user_id} of {len(providers)}")

            for provider in providers:
                if provider.slug in stats_dict:
                    stats = stats_dict[provider.slug]
                    provider.record_count = stats.get("record_count", 0)
                    sync_time = stats.get("last_sync_time")
                    provider.last_sync_at = sync_time.isoformat() if sync_time else None
                    continue
                if f"{provider.platform}.{provider.slug}" in stats_dict:
                    stats = stats_dict[f"{provider.platform}.{provider.slug}"]
                    provider.record_count = stats.get("record_count", 0)
                    sync_time = stats.get("last_sync_time")
                    provider.last_sync_at = sync_time.isoformat() if sync_time else None
                    continue

        except Exception as e:
            logging.error(f"Error populating provider stats for user {user_id}: {str(e)}")

        return providers


# Global singleton instance
platform_manager = PlatformManager()

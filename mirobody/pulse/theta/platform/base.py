"""
Base classes for Theta providers
"""

import logging
import uuid
from abc import abstractmethod
from typing import Any, Dict, List, Optional

from mirobody.pulse import LinkRequest
from mirobody.pulse.base import Provider
from mirobody.pulse.core import LinkType
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.core.user import ThetaUserService
from mirobody.pulse.data_upload.models.requests import StandardPulseData, StandardPulseMetaInfo
from mirobody.pulse.theta.platform.database_service import ThetaDatabaseService


class BaseThetaProvider(Provider):
    """
    Base class for Theta providers

    Provides common functionality for Theta providers
    """

    def __init__(self):
        self.db_service = ThetaDatabaseService()
        self.user_service = ThetaUserService()

    @classmethod
    def create_provider(cls, config: Dict[str, Any]) -> Optional['BaseThetaProvider']:
        """
        Factory method to create provider instance from config
        
        Subclasses should override this method to:
        1. Check if required config keys exist
        2. Return None if config is insufficient
        3. Return provider instance if config is valid
        
        Default implementation: call no-arg constructor
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Provider instance if config is valid, None otherwise
        """
        try:
            return cls()
        except Exception as e:
            logging.warning(f"Failed to create provider {cls.__name__}: {e}")
            return None

    def register_pull_task(self) -> bool:
        """
        Register pull task for Theta Provider
        """
        return True

    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        user_id = request.user_id
        provider_slug = request.provider_slug
        auth_type = request.auth_type

        try:
            # OAuth types should use callback flow, not direct link
            if auth_type in (LinkType.OAUTH1, LinkType.OAUTH2, LinkType.OAUTH):
                raise ValueError(f"{auth_type} should use OAuth callback flow")
            
            # Validate credentials
            await self._validate_credentials_v2(request.credentials)
            
            # Build credentials based on auth_type
            connect_info = request.credentials.get("connect_info")
            if auth_type == LinkType.CUSTOMIZED:
                if not connect_info:
                    raise ValueError("connect_info is required for customized auth type")
                username = ""
                password = ""
            elif auth_type == LinkType.PASSWORD:
                username = request.credentials.get("username", "")
                password = request.credentials.get("password", "")
                if not username or not password:
                    raise ValueError("Username and password are required for PASSWORD auth type")
            else:
                raise ValueError(f"Unsupported auth type: {auth_type}")
            
            # Save to database
            creds = self.db_service.ThetaCredentials(username=username, password=password, connect_info=connect_info)
            success = await self.db_service.save_user_theta_provider(user_id, provider_slug, auth_type, creds)
            
            if not success:
                raise RuntimeError(f"Failed to link Theta provider {provider_slug}")
            
            logging.info(f"Successfully linked theta provider {provider_slug} ({auth_type.value}) for user {user_id}")
            result = {"provider_slug": provider_slug, "msg": "ok", "connected": True}
            if username:
                result["username"] = username
            return result

        except Exception as e:
            logging.error(f"Error linking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(str(e))

    async def unlink(self, user_id: str) -> Dict[str, Any]:
        provider_slug = self.info.slug

        try:
            success = await self.db_service.delete_user_theta_provider(user_id, provider_slug)

            if success:
                logging.info(f"Successfully unlinked theta provider {provider_slug} for user {user_id}")
                return {"provider_slug": provider_slug}
            else:
                raise RuntimeError(f"Failed to unlink Theta provider {provider_slug}")

        except Exception as e:
            logging.error(f"Error unlinking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(str(e))

    async def _validate_credentials(self, username: str, password: str) -> None:
        """
        This is the v1 interface. New providers should override _validate_credentials_v2 instead.

        Args:
            username: username
            password: password

        Raises:
            Exception:
        """
        pass

    async def _validate_credentials_v2(self, credentials: Dict[str, Any]) -> None:
        username = credentials.get("username", "")
        password = credentials.get("password", "")
        await self._validate_credentials(username, password)

    async def _get_user_timezone(self, user_id: str) -> str:
        try:
            user_info = await self.user_service.get_user_by_id(user_id)
            if user_info and user_info.get("tz"):
                user_timezone = user_info.get("tz").strip()
                if user_timezone:
                    logging.info(f"Retrieved user timezone from database: user_id={user_id}, timezone={user_timezone}")
                    return user_timezone

            logging.info(f"No timezone found for user {user_id}, using default UTC")
            return "UTC"

        except Exception as e:
            logging.warning(f"Failed to get user timezone for user {user_id}: {str(e)}, using default UTC")
            return "UTC"

    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        raise NotImplementedError("Subclasses must implement format_data method")

    def generate_request_id(self) -> str:
        """Generate request ID"""
        return str(uuid.uuid4())

    def _create_empty_response(self, request_id: str, user_id: str) -> StandardPulseData:
        meta_info = StandardPulseMetaInfo(userId=user_id or "", requestId=request_id, source="theta", timezone="UTC")

        return StandardPulseData(metaInfo=meta_info, healthData=[])

    # ========== Pull Related Methods ==========

    async def get_all_user_credentials(self) -> List[Dict[str, Any]]:
        try:
            # Explicitly pass link_type based on current provider's authentication method
            link_type = self.info.auth_type
            if link_type == self.info.auth_type.SERVICE:
                return []
            return await self.db_service.get_all_user_credentials_for_provider(self.info.slug, link_type)
        except Exception as e:
            logging.warning(f"Error getting user credentials for provider {self.info.slug}: {str(e)}")
            return []

    @abstractmethod
    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
        pass

    async def pull_and_push(self) -> bool:
        try:
            credentials = await self.get_all_user_credentials()
            if not credentials:
                logging.info(f"No users found for provider {self.info.slug}")
                return True

            success_count = 0
            error_count = 0

            for cred in credentials:
                try:
                    user_success = await self._pull_and_push_for_user(cred)
                    if user_success:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logging.error(f"Error processing user {cred['user_id']}: {str(e)}")
                    error_count += 1

            logging.info(
                f"Pull and push completed for provider {self.info.slug}: {success_count} success, {error_count} errors"
            )
            return error_count == 0

        except Exception as e:
            logging.error(f"Error in pull_and_push for provider {self.info.slug}: {str(e)}")
            return False

    async def _pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool:
        """
        Execute pull and push for a single user

        Args:
            credentials: User credentials

        Returns:
            Whether successful
        """
        try:
            user_id = credentials["user_id"]
            username = credentials["username"]
            password = credentials["password"]

            # 1. Pull data from vendor API with optimization
            # Use optimized version if available, fallback to regular version
            if hasattr(self, "pull_from_vendor_api_optimized"):
                raw_data_list = await self.pull_from_vendor_api_optimized(username, password, user_id)
            else:
                raw_data_list = await self.pull_from_vendor_api(username, password)

            if not raw_data_list:
                logging.info(f"No data pulled for user {user_id}")
                return True  # No data is not an error

            success_count = 0

            # 2. Process each data record
            for raw_data in raw_data_list:
                try:
                    # Add user ID to raw data
                    raw_data["user_id"] = user_id

                    # Check if already processed
                    if await self.is_data_already_processed(raw_data):
                        logging.info(f"Data already processed for user {user_id}")
                        continue

                    # Push data (function call)
                    push_success = await push_service.push_data(
                        platform="theta",
                        provider_slug=self.info.slug,
                        data=raw_data,
                        msg_id=str(uuid.uuid4()),
                    )

                    if push_success:
                        success_count += 1
                        logging.info(f"Successfully pushed data for user {user_id}")
                    else:
                        logging.error(f"Failed to push data for user {user_id}")

                except Exception as e:
                    logging.error(f"Error processing data for user {user_id}: {str(e)}")
                    continue

            logging.info(f"Processed {success_count} records for user {user_id}")
            return True

        except Exception as e:
            logging.error(f"Error in _pull_and_push_for_user: {str(e)}")
            return False

    async def pull_from_vendor_api(self, username: str, password: str) -> List[Dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement pull_from_vendor_api method")

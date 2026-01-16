"""
Base classes and interfaces for Pulse system
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

# from . import BaseThetaProvider  # Remove circular import
# === Enum definitions ===
from .core import (
    LinkRequest,
    LinkType,
    ProviderInfo,
    UserProvider,
)
from .data_upload.models.requests import StandardPulseData

# Create AuthType alias for API compatibility
AuthType = LinkType


class Provider(ABC):
    """
    Provider abstract base class

    Defines interfaces that all Providers must implement
    Constraint: Provider must format raw data to StandardPulseData unified format
    """

    def __init__(self, platform: "Platform"):
        """
        Initialize Provider

        Args:
            platform: The Platform instance this provider belongs to
        """
        self.platform = platform
        self.platform_slug: Optional[str] = None

    def set_platform(self, platform_slug: str) -> None:
        """
        Set the Platform identifier this Provider belongs to

        Args:
            platform_slug: Platform identifier
        """
        self.platform_slug = platform_slug

    @property
    @abstractmethod
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        pass

    @abstractmethod
    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        """
        Connect Provider

        Args:
            request: Connection request

        Returns:
            Connection result data, throws exception on failure
        """
        pass

    @abstractmethod
    async def unlink(self, user_id: str) -> Dict[str, Any]:
        """
        Disconnect

        Args:
            user_id: User ID

        Returns:
            Disconnection result data, throws exception on failure
        """
        pass

    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> list[dict[str, any]]:
        """
        Args:
            raw_data: Raw data

        Returns:
            Whether save succeeded
        """

    pass

    @abstractmethod
    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        """
        Format raw data to StandardPulseData format

        **Core constraint**: All Providers must implement this method to convert
        platform-specific raw data to unified StandardPulseData format,
        subsequently processed uniformly by StandardHealthService

        Args:
            raw_data: Raw data

        Returns:
            StandardPulseData: Standardized platform data format
        """
        pass


class Platform(ABC):
    def __init__(self):
        self._providers: Dict[str, Provider] = {}

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def supports_registration(self) -> bool:
        pass

    @property
    def solo(self) -> bool:
        """
        Whether this platform is a solo platform
        
        Solo platforms return a single virtual provider instead of listing all providers.
        Subclasses can override this property to return True.
        
        Returns:
            bool: False by default, can be overridden by subclasses
        """
        return False

    def register_provider(self, provider: Provider) -> None:
        if not self.supports_registration:
            raise RuntimeError(f"Platform {self.name} does not support provider registration")

        provider.set_platform(self.name)
        self._providers[provider.info.slug] = provider

    def get_provider(self, provider_slug: str) -> Optional[Provider]:
        return self._providers.get(provider_slug)

    @abstractmethod
    async def get_providers(self, nocache: bool = False) -> List[ProviderInfo]:
        pass

    @abstractmethod
    async def get_user_providers(self, user_id: str) -> List[UserProvider]:
        pass

    @abstractmethod
    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def unlink(self, user_id: str, provider_slug: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def post_data(self, provider_slug: str, data: Dict[str, Any], msg_id: str) -> bool:
        pass

    @abstractmethod
    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> Dict[str, Any]:
        pass

    async def get_webhooks(
        self,
        page: int = 1,
        page_size: int = 20,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get webhook data with pagination and filters
        
        This is an optional method that platforms can implement if they support webhook management.
        Platforms that don't support webhooks should raise NotImplementedError.
        
        Args:
            page: Page number (starting from 1)
            page_size: Number of records per page
            event_type: Optional filter for event type
            user_id: Optional filter for user ID
            status: Optional filter for status (e.g., 'success', 'pending', 'error')
            
        Returns:
            Dictionary containing paginated webhook data with metadata
            
        Raises:
            NotImplementedError: If the platform doesn't support webhook management
        """
        raise NotImplementedError(f"Platform {self.name} does not support webhook management")

    async def check_format(self, webhook_id: int) -> Dict[str, Any]:
        """
        Check webhook format by simulating event provider processing
        
        This is an optional method that platforms can implement if they support format checking.
        Platforms that don't support format checking should raise NotImplementedError.
        
        Args:
            webhook_id: Webhook ID from database
            
        Returns:
            Dictionary containing original webhook data and formatted result
            
        Raises:
            NotImplementedError: If the platform doesn't support format checking
        """
        raise NotImplementedError(f"Platform {self.name} does not support format checking")

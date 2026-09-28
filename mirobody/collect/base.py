"""
Base classes and interfaces for Pulse system
"""

from abc import ABC, abstractmethod
from typing import Any

# from . import BasePullProvider  # Remove circular import
# === Enum definitions ===
from .core import (
    LinkRequest,
    LinkType,
    ProviderInfo,
    UserProvider,
)
from .ingest.models.requests import FormatDataInput, StandardPulseData

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
        self.platform_slug: str | None = None

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

    @abstractmethod
    async def link(self, request: LinkRequest) -> dict[str, Any]:
        """
        Connect Provider

        Args:
            request: Connection request

        Returns:
            Connection result data, throws exception on failure
        """

    @abstractmethod
    async def unlink(self, user_id: str) -> dict[str, Any]:
        """
        Disconnect

        Args:
            user_id: User ID

        Returns:
            Disconnection result data, throws exception on failure
        """

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Save raw data to database (optional, subclasses can override)

        Args:
            raw_data: Raw data

        Returns:
            List of saved records
        """
        return []

    @abstractmethod
    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Vendor payload -> `StandardPulseData`, the format every platform converges on.

        `fmt_input.payload` is the source's data untouched; `fmt_input.context`
        is what the caller already resolved (internal user id, vendor user id,
        timezone, msg_id), so the transformation needs no I/O of its own.
        StandardHealthService then processes the result uniformly.
        """



class Platform(ABC):
    def __init__(self):
        self._providers: dict[str, Provider] = {}

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

    def get_provider(self, provider_slug: str) -> Provider | None:
        return self._providers.get(provider_slug)

    @abstractmethod
    async def get_providers(self, nocache: bool = False) -> list[ProviderInfo]:
        pass

    @abstractmethod
    async def get_user_providers(self, user_id: str) -> list[UserProvider]:
        pass

    @abstractmethod
    async def link(self, request: LinkRequest) -> dict[str, Any]:
        pass

    @abstractmethod
    async def unlink(self, user_id: str, provider_slug: str) -> dict[str, Any]:
        pass

    @abstractmethod
    async def post_data(self, provider_slug: str, data: dict[str, Any], msg_id: str) -> bool:
        pass

    @abstractmethod
    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> dict[str, Any]:
        pass

"""
Pulse Module

Provides unified health data platform management architecture, supporting multiple data source platforms and providers

Supports dynamic loading - Theta providers are automatically loaded based on configuration, deleting files will take them offline
"""

from .apple import (
    AppleHealthPlatform,
    AppleHealthProvider,
    CDAProvider,
)
from .base import LinkRequest, Platform, Provider, ProviderInfo, UserProvider
from .manager import PlatformManager, platform_manager

# from .router.public_router import pulse_public_router  # User interface routing
from .router import public_router as pulse_public_router
from .router.manage_router import router as manage_router  # Management interface routing
from .setup import get_platform_manager, setup_platform_system, setup_platform_system_async
from .theta import (
    BaseThetaProvider,
    ThetaPlatform,
)

__all__ = [
    # Base classes and models
    "setup_platform_system_async",
    "Platform",
    "Provider",
    "ProviderInfo",
    "UserProvider",
    "LinkRequest",
    # Managers
    "PlatformManager",
    "platform_manager",
    # Data processors
    # Routing
    "pulse_public_router",
    "manage_router",  # Management interface routing
    # Concrete implementations
    "ThetaPlatform",
    "BaseThetaProvider",
    # Note: Specific Theta providers (ThetaGarminProvider, etc.) are auto-loaded
    # and can be imported from .theta if needed
    # Apple Health implementations
    "AppleHealthPlatform",
    "AppleHealthProvider",
    "CDAProvider",
    # Setup functions
    "setup_platform_system",
    "get_platform_manager",
]

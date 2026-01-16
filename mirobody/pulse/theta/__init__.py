"""
Theta Platform Module

This module provides the Theta platform implementation with pluggable providers.

Architecture:
- Platform (1): Unified interface and scheduling layer
- Providers (n): Pluggable provider implementations in mirobody_* directories

Each provider is self-contained and can be added/removed by simply adding/removing its directory.
"""

# Export platform classes for external use
from .platform.platform import ThetaPlatform
from .platform.base import BaseThetaProvider

# Export utility modules
from .platform import database_service, utils, pull_task, startup

__all__ = [
    # Main classes
    "ThetaPlatform",
    "BaseThetaProvider",
    # Utility modules
    "database_service",
    "utils",
    "pull_task",
    "startup",
]


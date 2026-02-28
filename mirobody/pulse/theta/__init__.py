"""
Theta Platform Module

Provides the Theta platform implementation with pluggable providers for direct
device/service integrations (Garmin, Whoop, PostgreSQL, etc.).

Architecture:
    ThetaPlatform (platform/platform.py)
        — manages provider lifecycle, pull scheduling, and data routing
    BaseThetaProvider (platform/base.py)
        — abstract base for all Theta providers; subclasses implement
          create_provider(), info, format_data(), pull_from_vendor_api(),
          save_raw_data_to_db(), is_data_already_processed()
    Providers (mirobody_*/provider_*.py)
        — one directory per device/service, self-contained implementations

Provider loading:
    ThetaPlatform.load_providers() scans mirobody_*/provider_*.py, calls
    create_provider(config) on each, and registers successful instances.
    Each provider is self-contained and can be added/removed by simply
    adding/removing its directory.
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


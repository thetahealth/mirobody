"""
Pulse Module

Provides unified health data platform management architecture, supporting multiple data source platforms and providers

Supports dynamic loading - Theta providers are automatically loaded based on configuration, deleting files will take them offline

Exports resolve lazily (PEP 562). ``import mirobody.pulse`` is the ENGINE —
it must not eagerly construct the platform singletons, and it carries no HTTP
routers at all: those were platform assembly, not engine logic, and now live
where they always belonged — ``mirobody/server/routers/``. Every existing
``from mirobody.pulse import X`` keeps working unchanged; each export simply
pays its own import cost at first use.
"""

from typing import TYPE_CHECKING

# name -> submodule that defines it
_EXPORTS = {
    # Base classes and models
    "Platform": "base",
    "Provider": "base",
    "ProviderInfo": "base",
    "UserProvider": "base",
    "LinkRequest": "base",
    # Managers
    "PlatformManager": "manager",
    "platform_manager": "manager",
    # Setup functions
    "setup_platform_system": "setup",
    "setup_platform_system_async": "setup",
    "get_platform_manager": "setup",
    # Concrete implementations
    "ThetaPlatform": "theta",
    "BaseThetaProvider": "theta",
    # Apple Health implementations
    "AppleHealthPlatform": "apple",
    "AppleHealthProvider": "apple",
    "CDAProvider": "apple",
    # Note: Specific Theta providers (ThetaGarminProvider, etc.) are auto-loaded
    # and can be imported from .theta if needed
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .apple import AppleHealthPlatform, AppleHealthProvider, CDAProvider
    from .base import LinkRequest, Platform, Provider, ProviderInfo, UserProvider
    from .manager import PlatformManager, platform_manager
    from .setup import get_platform_manager, setup_platform_system, setup_platform_system_async
    from .theta import BaseThetaProvider, ThetaPlatform


def __getattr__(name: str):
    import importlib

    if name in _EXPORTS:
        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

"""
Setup functions for the Pulse system
"""

import asyncio
import logging

from .apple.platform import AppleHealthPlatform

from .manager import platform_manager
from mirobody.pulse.providers.platform.base import BasePullProvider
from mirobody.pulse.providers.platform.platform import ProviderPlatform
from ..utils.config import global_config

logger = logging.getLogger(__name__)


async def setup_platform_system_async(providers: list[BasePullProvider] | None = None):
    """
    Asynchronously initialize Platform system

    Register all Platforms and Providers (async version)
    
    Args:
        providers: List of additional BasePullProvider instances to register (optional)
    """
    logger.info("Starting platform system setup...")

    # 1. Create and register Platforms
    # Config.init() has already run by this point (server startup); this is
    # the accessor, not a loader. It previously took `config_file_path` and
    # silently dropped it.
    cfg = global_config()
    
    theta_platform = ProviderPlatform(cfg)
    apple_platform = AppleHealthPlatform()

    # Register Platforms
    platform_manager.register_platform(theta_platform)
    platform_manager.register_platform(apple_platform)

    # 3. Load providers using ProviderPlatform's method
    theta_providers = theta_platform.load_providers()

    # 4. Append additional providers if provided
    if providers:
        for provider in providers:
            theta_providers.append(provider)

    # 5. Register all providers to the platform
    for provider in theta_providers:
        try:
            theta_platform.register_provider(provider)
            logger.info(f"Loaded provider: [{provider.info.slug}]")
        except Exception as e:
            logger.error(f"Error registering provider {provider.info.slug}: {str(e)}")
            continue

    # 6. Initialize FHIR mapping (optional, config-driven)
    from .standardize.fhir_mapping import FhirMapping
    fhir_mapping = await FhirMapping.initialize()
    if fhir_mapping:
        logger.info("  - FHIR mapping initialized")

    logger.info("Platform system setup completed:")
    logger.info(f"  - provider platform loaded {len(theta_providers)} providers")
    logger.info("  - Apple Health platform initialized with built-in providers")


def setup_platform_system():
    """
    Initialize Platform system (sync version)

    Register all Platforms and Providers
    """
    # Get current event loop, create new one if none exists
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Run async version
    loop.run_until_complete(setup_platform_system_async())


def get_platform_manager():
    """
    Get Platform manager instance

    Returns:
        PlatformManager instance
    """
    return platform_manager

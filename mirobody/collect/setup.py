"""Registering the platforms and their providers at server start."""

import logging

from .providers.apple.platform import AppleHealthPlatform

from .manager import platform_manager
from mirobody.collect.providers._platform.platform import ProviderPlatform
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config import global_config

logger = logging.getLogger(__name__)


async def setup_platform_system_async() -> None:
    """Register the provider platform and the Apple Health platform with
    `platform_manager`, and every provider the provider platform loads.
    `Config.init()` has run by then: this reads the loaded configuration."""
    logger.info("Starting platform system setup...")

    theta_platform = ProviderPlatform(global_config())
    apple_platform = AppleHealthPlatform()
    platform_manager.register_platform(theta_platform)
    platform_manager.register_platform(apple_platform)

    theta_providers = theta_platform.load_providers()
    for provider in theta_providers:
        try:
            theta_platform.register_provider(provider)
            logger.info(f"Loaded provider: [{provider.info.slug}]")
        except Exception as e:
            logger.error("provider registration failed: provider=%s error_type=%s", provider.info.slug,
                         type(e).__name__, exc_info=not is_driver_exception(e))

    logger.info("Platform system setup completed:")
    logger.info(f"  - provider platform loaded {len(theta_providers)} providers")
    logger.info("  - Apple Health platform initialized with built-in providers")

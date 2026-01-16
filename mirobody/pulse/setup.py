"""
Setup functions for the Pulse system
"""

import asyncio
import logging
from typing import List, Optional

from .apple.platform import AppleHealthPlatform

from .manager import platform_manager
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.theta.platform.platform import ThetaPlatform
from ..utils.config import global_config


async def setup_platform_system_async(config_file_path=None, providers: Optional[List[BaseThetaProvider]] = None):
    """
    Asynchronously initialize Platform system

    Register all Platforms and Providers (async version)
    
    Args:
        config_file_path: Path to configuration file (optional)
        providers: List of additional ThetaProvider instances to register (optional)
    """
    logging.info("Starting platform system setup...")

    # 1. Create and register Platforms
    cfg = global_config(config_file_path)
    
    theta_platform = ThetaPlatform(cfg)
    apple_platform = AppleHealthPlatform()

    # Register Platforms
    platform_manager.register_platform(theta_platform)
    platform_manager.register_platform(apple_platform)

    # 3. Load Theta Providers using ThetaPlatform's method
    theta_providers = theta_platform.load_providers()

    # 4. Append additional providers if provided
    if providers:
        for provider in providers:
            theta_providers.append(provider)

    # 5. Register all Theta Providers to platform
    for provider in theta_providers:
        try:
            theta_platform.register_provider(provider)
            logging.info(f"✅ Loaded provider: [{provider.info.slug}]")
        except Exception as e:
            logging.error(f"Error registering provider {provider.info.slug}: {str(e)}")
            continue

    logging.info("Platform system setup completed:")
    logging.info(f"  - Theta platform loaded {len(theta_providers)} providers")
    logging.info(f"  - Apple Health platform initialized with built-in providers")


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

"""
Provider scheduler startup functions
"""

import logging

from mirobody.collect.manager import platform_manager
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


async def start_theta_pull_scheduler() -> None:
    try:
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            logger.info("provider platform not found, skipping pull scheduler startup")
            return

        await theta_platform.start_pull_scheduler()
        logger.info("provider pull scheduler started successfully")

    except Exception as e:
        logger.error("pull scheduler failed to start: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))


"""
Provider scheduler startup functions
"""

import logging


from mirobody.pulse.manager import platform_manager

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
        logger.info(f"Failed to start theta pull scheduler: {str(e)}")





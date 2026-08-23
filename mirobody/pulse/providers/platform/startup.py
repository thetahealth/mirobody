"""
Provider scheduler startup functions
"""

import logging

from typing import Any

from mirobody.pulse.manager import platform_manager


async def start_theta_pull_scheduler() -> None:
    try:
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            logging.info("provider platform not found, skipping pull scheduler startup")
            return

        await theta_platform.start_pull_scheduler()
        logging.info("provider pull scheduler started successfully")

    except Exception as e:
        logging.info(f"Failed to start theta pull scheduler: {str(e)}")





"""
Theta scheduler startup functions
"""

import logging

from typing import Any

from mirobody.pulse.manager import platform_manager


async def start_theta_pull_scheduler() -> None:
    try:
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            logging.info("Theta platform not found, skipping pull scheduler startup")
            return

        await theta_platform.start_pull_scheduler()
        logging.info("Theta pull scheduler started successfully")

    except Exception as e:
        logging.info(f"Failed to start theta pull scheduler: {str(e)}")


async def stop_theta_pull_scheduler() -> None:
    try:
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            logging.info("Theta platform not found, skipping pull scheduler shutdown")
            return

        await theta_platform.stop_pull_scheduler()
        logging.info("Theta pull scheduler stopped successfully")

    except Exception as e:
        logging.info(f"Failed to stop theta pull scheduler: {str(e)}")


def get_theta_pull_task_status() -> dict[str, Any]:
    try:
        theta_platform = platform_manager.get_platform("theta")
        if not theta_platform:
            return {"tasks": {}, "total_tasks": 0, "message": "Theta platform not available"}

        return theta_platform.get_all_pull_task_status()

    except Exception as e:
        logging.info(f"Failed to get theta pull task status: {str(e)}")
        return {"tasks": {}, "total_tasks": 0, "error": str(e)}

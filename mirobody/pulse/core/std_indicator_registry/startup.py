"""
Startup hook for RegisterStandardIndicatorsTask.

Registers the catalog publisher task with the unified scheduler so it
runs once a day, picking up any IndicatorInfo / aggregation-rule changes
that ship with a deploy.
"""

import logging

from .task import RegisterStandardIndicatorsTask
from ..scheduler import scheduler

_registry_task = None


async def start_std_indicator_registry():
    """Register the standard indicator registry task with the scheduler."""
    global _registry_task

    if _registry_task is not None:
        logging.warning("Std indicator registry task already registered")
        return

    logging.info("Registering std indicator registry task with scheduler...")

    _registry_task = RegisterStandardIndicatorsTask()
    scheduler.register_task(_registry_task)

    logging.info("Std indicator registry task registered successfully")


async def get_std_indicator_registry_full_status() -> dict:
    """Return full task status (async — includes cached stats)."""
    if _registry_task:
        return await _registry_task.get_task_info()
    return {"status": "not_initialized"}

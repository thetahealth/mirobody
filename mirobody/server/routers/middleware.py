"""Backing-service bootstrap for the HTTP server.

Everything else that used to live here (`register_middleware`, `lifespan`,
and eight `get_*` request helpers) was dead: the only symbol any caller ever
imported was `init` (`server/server.py`). The helpers were also a near-verbatim
duplicate of the live copies in `server/middlewares.py`, so the file was both
unused and a second source of truth for the same logic.
"""

import logging

from mirobody.collect import setup_platform_system_async
from mirobody.collect import start_theta_pull_scheduler

logger = logging.getLogger(__name__)


async def init():
    logger.info("start init db...")
    await setup_platform_system_async()
    await start_theta_pull_scheduler()
    
    # Start aggregate indicator scheduler
    try:
        from mirobody.translate import start_aggregate_indicator_scheduler
        await start_aggregate_indicator_scheduler(False)
        from mirobody.translate import start_derived_scheduler
        await start_derived_scheduler()
        logger.info("Aggregate indicator scheduler started")
    except Exception as e:
        logger.error(f"Failed to start aggregate indicator scheduler: {str(e)}")
        raise  # Re-raise to prevent service from starting if tests fail


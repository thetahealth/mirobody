"""Backing-service bootstrap for the HTTP server.

Everything else that used to live here — `register_middleware`, `lifespan`,
and eight `get_*` request helpers — was dead: the only symbol any caller ever
imported was `init` (`server/server.py`). The helpers were also a near-verbatim
duplicate of the live copies in `server/middlewares.py`, so the file was both
unused and a second source of truth for the same logic.
"""

import logging

from ...pulse import setup_platform_system_async
from ...pulse.providers.platform.startup import start_theta_pull_scheduler


async def init():
    logging.info("start init db...")
    await setup_platform_system_async()
    await start_theta_pull_scheduler()
    
    # Start aggregate indicator scheduler
    try:
        from ...pulse.aggregate.startup import start_aggregate_indicator_scheduler
        await start_aggregate_indicator_scheduler(False)
        logging.info("Aggregate indicator scheduler started")
    except Exception as e:
        logging.error(f"Failed to start aggregate indicator scheduler: {str(e)}")
        raise  # Re-raise to prevent service from starting if tests fail

    # Start standard indicator registry task — publishes in-code
    # StandardIndicator enum + derived aggregation rules to
    # standard_indicators_device once a day.
    try:
        from ...pulse.standardize.std_indicator_registry.startup import start_std_indicator_registry
        await start_std_indicator_registry()
        logging.info("Std indicator registry task started")
    except Exception as e:
        logging.error(f"Failed to start std indicator registry task: {str(e)}")

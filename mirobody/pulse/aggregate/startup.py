"""
Startup functions for Aggregate Indicator

Integrates with the unified scheduler system.
"""

import logging

from .task import AggregateIndicatorTask
from .derived_task import DerivedCalculationTask
from ..core.scheduler import scheduler

logger = logging.getLogger(__name__)

# Global task instances
_aggregate_task = None
_derived_task = None


async def start_aggregate_indicator_scheduler(run_integration_test: bool = False):
    """
    Register aggregate indicator task with the unified scheduler
    
    Called during application startup.
    
    Args:
        run_integration_test: Whether to run integration test on initialization (default: False)
    """
    global _aggregate_task

    if _aggregate_task is not None:
        logger.warning("Aggregate indicator task already registered")
        return

    # Run integration test if requested (before creating task)
    if run_integration_test:
        logger.info("Running integration test before starting scheduler...")
        try:
            from .test_aggregator import AggregatorTester
            tester = AggregatorTester()
            await tester.run_all_tests()
            logger.info("Integration test passed, continuing with scheduler startup")
        except Exception as e:
            logger.error(f"Integration test failed: {e}")
            raise RuntimeError(f"Integration test failed, aborting scheduler startup: {e}")

    logger.info("Registering aggregate indicator task with scheduler...")

    # Create task instance
    _aggregate_task = AggregateIndicatorTask()

    # Register with global scheduler
    scheduler.register_task(_aggregate_task)

    logger.info("Aggregate indicator task registered successfully")

    # Register derived indicator task (TH-174 W2.2)
    global _derived_task
    if _derived_task is None:
        _derived_task = DerivedCalculationTask()
        scheduler.register_task(_derived_task)
        logger.info("Derived indicator task registered successfully")







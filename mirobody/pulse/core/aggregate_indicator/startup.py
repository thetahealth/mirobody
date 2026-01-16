"""
Startup functions for Aggregate Indicator

Integrates with the unified scheduler system.
"""

import logging

from .task import AggregateIndicatorTask
from ..scheduler import scheduler

# Global task instance
_aggregate_task = None


async def start_aggregate_indicator_scheduler(run_integration_test: bool = False):
    """
    Register aggregate indicator task with the unified scheduler
    
    Called during application startup.
    
    Args:
        run_integration_test: Whether to run integration test on initialization (default: False)
    """
    global _aggregate_task

    if _aggregate_task is not None:
        logging.warning("Aggregate indicator task already registered")
        return

    # Run integration test if requested (before creating task)
    if run_integration_test:
        logging.info("Running integration test before starting scheduler...")
        try:
            from .test_aggregator import AggregatorTester
            tester = AggregatorTester()
            await tester.run_all_tests()
            logging.info("✅ Integration test passed, continuing with scheduler startup")
        except Exception as e:
            logging.error(f"❌ Integration test failed: {e}")
            raise RuntimeError(f"Integration test failed, aborting scheduler startup: {e}")

    logging.info("Registering aggregate indicator task with scheduler...")

    # Create task instance
    _aggregate_task = AggregateIndicatorTask()

    # Register with global scheduler
    scheduler.register_task(_aggregate_task)

    logging.info("Aggregate indicator task registered successfully")


async def stop_aggregate_indicator_scheduler():
    """
    Stop is handled by the unified scheduler
    
    This function is kept for API compatibility but doesn't need to do anything.
    The unified scheduler will handle stopping all tasks.
    """
    logging.info("Aggregate indicator task will be stopped by unified scheduler")


def get_aggregate_task_status() -> dict:
    """
    Get aggregate indicator task status (synchronous - scheduler info only)
    
    Returns:
        Dict with task status information
    """
    if _aggregate_task:
        return _aggregate_task.get_status()
    else:
        return {
            "status": "not_initialized",
            "message": "Aggregate indicator task not registered"
        }


async def get_aggregate_task_full_status() -> dict:
    """
    Get aggregate indicator task full status (async - includes cached data)
    
    Returns:
        Dict with full task status including cached timestamp and stats
    """
    if _aggregate_task:
        return await _aggregate_task.get_task_info()
    else:
        return {
            "status": "not_initialized",
            "message": "Aggregate indicator task not registered"
        }

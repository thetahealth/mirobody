"""
Unified task scheduler
"""

import asyncio
import logging
from datetime import datetime, timedelta
from enum import Enum

from mirobody.utils.tasks import spawn

from .distributed_lock import pull_task_lock_manager

logger = logging.getLogger(__name__)


class ScheduleType(str, Enum):
    INTERVAL = "interval"  # Interval scheduling
    HOURLY = "hourly"  # Hourly scheduling at the top of each hour


class PullTask:
    """Pull task base class with distributed lock support and configurable intervals"""

    def __init__(
        self,
        provider_slug: str,
        schedule_type: ScheduleType = ScheduleType.HOURLY,
        interval_minutes: int = 30,
        execution_interval_hours: float = 1.0,  # New: actual execution interval
        lock_duration_hours: float | None = None,  # New: lock duration
    ):
        """
        Initialize Pull Task

        Args:
            provider_slug: Provider identifier
            schedule_type: Schedule type (hourly/interval)
            interval_minutes: Schedule check interval in minutes (only effective when schedule_type=INTERVAL)
            execution_interval_hours: Actual execution interval (hours), determines task real execution frequency
            lock_duration_hours: Distributed lock duration (hours), defaults to execution_interval_hours - 0.5
        """
        self.provider_slug = provider_slug
        self.schedule_type = schedule_type
        self.interval_minutes = interval_minutes
        self.execution_interval_hours = execution_interval_hours

        # Lock duration defaults to execution interval minus 0.5 hours buffer time
        if lock_duration_hours is None:
            self.lock_duration_hours = max(0.1, execution_interval_hours - 0.5)
        else:
            self.lock_duration_hours = lock_duration_hours

        self.last_run: datetime | None = None
        self.next_run: datetime | None = None
        self.is_running = False
        self.last_failed = False  # an INTERVAL task waits twice as long after a failure

        # Calculate initial run time
        self._calculate_next_run()

    async def execute(self) -> bool:
        """Execute pull task"""
        raise NotImplementedError("Subclasses must implement execute method")

    def should_run(self) -> bool:
        """Check if should run based on schedule type and execution interval"""
        if self.is_running:
            return False

        if self.next_run is None:
            return True

        # Check if schedule time is reached
        schedule_ready = datetime.now() >= self.next_run

        # Check if actual execution time is reached (based on execution_interval_hours)
        if self.last_run is None:
            execution_ready = True
        else:
            execution_ready = datetime.now() >= (self.last_run + timedelta(hours=self.execution_interval_hours))

        return schedule_ready and execution_ready

    async def try_execute_with_lock(self, force: bool = False) -> bool:
        """
        Execute task with distributed lock

        Args:
            force: Whether to force execution (ignore locks)

        Returns:
            True if executed successfully, False if skipped or failed
        """
        # Try to acquire distributed lock
        execution_id = await pull_task_lock_manager.try_acquire_execution_lock(
            self.provider_slug,
            lock_duration_hours=self.lock_duration_hours,
            force=force,
        )

        if execution_id is None:
            if not force:
                logger.info(f"Skipping execution for {self.provider_slug} - lock held by another instance")
                return False
            logger.error(f"Failed to acquire lock for {self.provider_slug} even in force mode")
            return False

        try:
            logger.info(f"Starting execution for {self.provider_slug} (execution: {execution_id})")
            return await self._execute_internal()
        finally:
            await pull_task_lock_manager.release_execution_lock(self.provider_slug, execution_id)

    async def _execute_internal(self) -> bool:
        """Internal execution logic without lock handling"""
        if self.is_running:
            logger.warning(f"Task {self.provider_slug} is already running")
            return False

        self.is_running = True
        self.last_run = datetime.now()
        # TH-416: persist last_run so a service restart doesn't reset the
        # execution window of long-interval tasks (renpho/whoop @ 24h, etc.)
        try:
            await pull_task_lock_manager.set_last_run(
                self.provider_slug, self.last_run
            )
        except Exception as e:
            logger.warning(
                f"Failed to persist last_run for {self.provider_slug}: {e}"
            )

        try:
            success = await self.execute()

            self.last_failed = not success
            if success:
                logger.info(f"Task {self.provider_slug} completed successfully")
            else:
                logger.error(f"Task {self.provider_slug} failed")

            self._calculate_next_run()
            return success

        except Exception as e:
            self.last_failed = True
            logger.error(
                f"Task {self.provider_slug} execution error: {str(e)}",
                exc_info=True,
            )
            self._calculate_next_run()
            return False
        finally:
            self.is_running = False

    def _calculate_next_run(self):
        """Calculate next run time based on schedule type"""
        now = datetime.now()

        if self.schedule_type == ScheduleType.HOURLY:
            # Run at the top of each hour
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            self.next_run = next_hour
        elif self.schedule_type == ScheduleType.INTERVAL:
            # Run by interval
            if self.last_run is None:
                self.next_run = now + timedelta(minutes=self.interval_minutes)
            else:
                if self.last_failed:
                    # If there was an error, wait double time
                    self.next_run = self.last_run + timedelta(minutes=self.interval_minutes * 2)
                else:
                    self.next_run = self.last_run + timedelta(minutes=self.interval_minutes)

    async def get_last_execution_timestamp(self) -> float | None:
        """Where the task's incremental work stopped (Unix seconds), or None."""
        return await pull_task_lock_manager.get_last_execution_timestamp(self.provider_slug)

    async def update_last_execution_timestamp(self, timestamp: float) -> bool:
        """Record where the task's incremental work stopped (Unix seconds)."""
        return await pull_task_lock_manager.update_last_execution_timestamp(self.provider_slug, timestamp)


class Scheduler:
    """Unified background task scheduler with distributed lock support"""

    def __init__(self):
        self.tasks: dict[str, PullTask] = {}
        self.running = False
        self._scheduler_task: asyncio.Task | None = None

    def register_task(self, task: PullTask):
        """Register a new task"""
        self.tasks[task.provider_slug] = task

    async def start(self):
        """Start the scheduler as a background task"""
        if self.running:
            logger.warning("Scheduler is already running")
            return

        self.running = True
        logger.info("Starting scheduler...")

        # TH-416: restore each task's last_run from Postgres before scheduling,
        # so a service restart doesn't reset long-interval tasks
        # (renpho/whoop @ 24h) to execute immediately.
        for task in self.tasks.values():
            try:
                persisted = await pull_task_lock_manager.get_last_run(
                    task.provider_slug
                )
                if persisted is not None:
                    task.last_run = persisted
                    logger.info(
                        f"Restored last_run for {task.provider_slug}: "
                        f"{persisted.isoformat()}"
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to restore last_run for {task.provider_slug}: {e}"
                )

        # Start scheduler as a background task to avoid blocking startup
        self._scheduler_task = asyncio.create_task(self._run_scheduler())
        logger.info("Scheduler started as background task")

    async def _run_scheduler(self):
        """Main scheduler loop"""
        logger.info("Scheduler main loop started")

        while self.running:
            try:
                current_time = datetime.now()
                logger.debug(f"Scheduler check at {current_time.isoformat()}")

                # Check all tasks
                for task in self.tasks.values():
                    if task.should_run():
                        logger.info(f"Executing scheduled task: {task.provider_slug}")
                        # Execute task with distributed lock
                        spawn(task.try_execute_with_lock(force=False))

                # Wait 1 minute before next check
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                logger.info("Scheduler loop cancelled")
                break
            except Exception as e:
                logger.error(f"Scheduler loop error: {str(e)}")
                await asyncio.sleep(60)


# Global scheduler instance
scheduler = Scheduler()

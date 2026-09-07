"""
Unified task scheduler
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from ...utils.tasks import spawn

logger = logging.getLogger(__name__)

# Import distributed lock manager
try:
    from .distributed_lock import pull_task_lock_manager
except ImportError:
    # Fallback if distributed_lock is not available
    pull_task_lock_manager = None
    logger.warning("Distributed lock manager not available, running without lock protection")


class ScheduleType(str, Enum):
    INTERVAL = "interval"  # Interval scheduling
    HOURLY = "hourly"  # Hourly scheduling at the top of each hour
    MANUAL = "manual"  # Manual trigger only


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
            schedule_type: Schedule type (hourly/interval/manual)
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
        self.error_count = 0
        self.success_count = 0
        self.last_error: str | None = None
        # Subclass-captured exception detail (TH-331). Subclasses that swallow
        # exceptions internally (try-except + return False) should call
        # self._capture_error(e) so the scheduler can surface a real traceback
        # in last_error instead of the generic "Task execution returned False".
        self.last_internal_error: str | None = None
        self.current_execution_id: str | None = None

        # Calculate initial run time
        self._calculate_next_run()

    async def execute(self) -> bool:
        """Execute pull task"""
        raise NotImplementedError("Subclasses must implement execute method")

    def _capture_error(self, e: BaseException) -> None:
        """Record an exception with full traceback for diagnostic visibility.

        Subclasses that catch exceptions internally and return False should call
        this in their except block. The scheduler will then prefer this detailed
        message over the generic fallback when populating ``last_error``.

        Truncates to 4096 chars to keep the field bounded for Redis / status
        endpoints.
        """
        import traceback
        try:
            msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        except Exception:
            msg = f"{type(e).__name__}: {e}"
        self.last_internal_error = msg[:4096]

    def should_run(self) -> bool:
        """Check if should run based on schedule type and execution interval"""
        if self.is_running:
            return False

        if self.schedule_type == ScheduleType.MANUAL:
            return False  # Manual tasks only run when triggered

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
        if not pull_task_lock_manager:
            logger.warning(f"No lock manager available for {self.provider_slug}, executing without lock")
            return await self._execute_internal()

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
            self.current_execution_id = execution_id
            logger.info(f"Starting execution for {self.provider_slug} (execution: {execution_id})")
            return await self._execute_internal()
        finally:
            # Ensure lock is released
            if execution_id:
                await pull_task_lock_manager.release_execution_lock(self.provider_slug, execution_id)
                self.current_execution_id = None

    async def _execute_internal(self) -> bool:
        """Internal execution logic without lock handling"""
        if self.is_running:
            logger.warning(f"Task {self.provider_slug} is already running")
            return False

        self.is_running = True
        self.last_run = datetime.now()
        # TH-416: persist last_run so a service restart doesn't reset the
        # execution window of long-interval tasks (renpho/whoop @ 24h, etc.)
        if pull_task_lock_manager:
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

            if success:
                self.success_count += 1
                self.last_error = None
                self.last_internal_error = None
                logger.info(f"Task {self.provider_slug} completed successfully")
            else:
                self.error_count += 1
                # Prefer the subclass-captured traceback (set via _capture_error)
                # over the generic fallback. See TH-331.
                self.last_error = (
                    self.last_internal_error
                    or "Task execution returned False"
                )
                logger.error(f"Task {self.provider_slug} failed")

            self._calculate_next_run()
            return success

        except Exception as e:
            self.error_count += 1
            self._capture_error(e)
            self.last_error = self.last_internal_error or str(e)
            logger.error(
                f"Task {self.provider_slug} execution error: {str(e)}",
                exc_info=True,
            )
            self._calculate_next_run()
            return False
        finally:
            self.is_running = False

    async def manual_trigger(self, force: bool = False) -> bool:
        """
        Manual trigger task

        Args:
            force: Whether to force execution (ignore locks and execution intervals)
                  If True, also clears last execution timestamp to trigger 24h lookback

        Returns:
            True if executed successfully
        """
        logger.info(f"Manual trigger for {self.provider_slug} (force: {force})")

        if force:
            # Clear last execution timestamp to trigger 24h lookback
            await self.clear_last_execution_timestamp()
            # Force mode executes directly, ignoring execution interval check
            return await self.try_execute_with_lock(force=True)
        # Check execution interval
        if self.last_run is not None:
            time_since_last = datetime.now() - self.last_run
            if time_since_last < timedelta(hours=self.execution_interval_hours):
                logger.info(f"Skipping manual trigger for {self.provider_slug} - execution interval not reached")
                return False

        return await self.try_execute_with_lock(force=False)

    def _calculate_next_run(self):
        """Calculate next run time based on schedule type"""
        now = datetime.now()

        if self.schedule_type == ScheduleType.MANUAL:
            self.next_run = None
        elif self.schedule_type == ScheduleType.HOURLY:
            # Run at the top of each hour
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            self.next_run = next_hour
        elif self.schedule_type == ScheduleType.INTERVAL:
            # Run by interval
            if self.last_run is None:
                self.next_run = now + timedelta(minutes=self.interval_minutes)
            else:
                if self.last_error:
                    # If there was an error, wait double time
                    self.next_run = self.last_run + timedelta(minutes=self.interval_minutes * 2)
                else:
                    self.next_run = self.last_run + timedelta(minutes=self.interval_minutes)

    def get_status(self) -> dict:
        """Get task status information (synchronous - scheduler info only)"""
        status = {
            "provider_slug": self.provider_slug,
            "schedule_type": self.schedule_type.value,
            "interval_minutes": self.interval_minutes,
            "execution_interval_hours": self.execution_interval_hours,
            "lock_duration_hours": self.lock_duration_hours,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "is_running": self.is_running,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "current_execution_id": self.current_execution_id,
        }
        return status

    async def get_lock_status(self) -> dict:
        """Get distributed lock status for this task"""
        if not pull_task_lock_manager:
            return {"error": "Lock manager not available"}

        return await pull_task_lock_manager.get_lock_status(self.provider_slug)
    
    # ==================== Cache Service Interface ====================
    
    async def get_last_execution_timestamp(self) -> float | None:
        """
        Get last execution timestamp for incremental processing

        Calls distributed_lock's timestamp service.

        Returns:
            Unix timestamp (float, sub-second precision) or None
        """
        if not pull_task_lock_manager:
            logger.warning(
                f"Lock manager not available for {self.provider_slug}"
            )
            return None
        
        return await pull_task_lock_manager.get_last_execution_timestamp(
            self.provider_slug
        )
    
    async def clear_last_execution_timestamp(self) -> bool:
        """
        Clear last execution timestamp (for force refresh)
        
        Calls distributed_lock's timestamp clearing service.
        This will cause next execution to use 24h lookback.
        
        Returns:
            True if successful
        """
        if not pull_task_lock_manager:
            logger.warning(
                f"Lock manager not available for {self.provider_slug}"
            )
            return False
        
        return await pull_task_lock_manager.clear_last_execution_timestamp(
            self.provider_slug
        )
    
    async def update_last_execution_timestamp(self, timestamp: float) -> bool:
        """
        Update last execution timestamp

        Calls distributed_lock's timestamp service.

        Args:
            timestamp: Unix timestamp as float (sub-second precision retained)

        Returns:
            True if successful
        """
        if not pull_task_lock_manager:
            logger.warning(
                f"Lock manager not available for {self.provider_slug}"
            )
            return False
        
        return await pull_task_lock_manager.update_last_execution_timestamp(
            self.provider_slug,
            timestamp
        )
    
    def _get_stats_redis_key(self) -> str:
        """Get Redis key for task statistics"""
        return f"task_stats:{self.provider_slug}"
    
    async def get_task_stats(self) -> dict[str, Any] | None:
        """
        Get task execution statistics from cache
        
        Subclasses can override to provide custom stats structure.
        
        Returns:
            Dict with statistics or None
        """
        if not pull_task_lock_manager:
            return None
        
        try:
            from .distributed_lock import get_redis_client
            redis_client = await get_redis_client()
            if redis_client is None:
                return None
            
            stats_key = self._get_stats_redis_key()
            stats_json = await redis_client.get(stats_key)
            
            if stats_json:
                # Handle bytes returned from Redis
                if isinstance(stats_json, bytes):
                    stats_json = stats_json.decode('utf-8')
                return json.loads(stats_json)
            return None
            
        except Exception as e:
            logger.error(
                f"Error getting stats for {self.provider_slug}: {e}"
            )
            return None
    
    async def save_task_stats(
        self, 
        stats: dict[str, Any], 
        ttl: int = 86400
    ) -> bool:
        """
        Save task execution statistics to cache
        
        Subclasses define their own stats structure.
        
        Args:
            stats: Statistics dictionary (structure defined by subclass)
            ttl: Time to live in seconds (default: 24 hours)
            
        Returns:
            True if successful
        """
        if not pull_task_lock_manager:
            return False
        
        try:
            from .distributed_lock import get_redis_client
            redis_client = await get_redis_client()
            if redis_client is None:
                return False
            
            stats_key = self._get_stats_redis_key()
            stats_json = json.dumps(stats)
            await redis_client.set(stats_key, stats_json, ex=ttl)
            
            logger.debug(f"Saved stats for {self.provider_slug}")
            return True
            
        except Exception as e:
            logger.error(
                f"Error saving stats for {self.provider_slug}: {e}"
            )
            return False
    
    async def get_full_status(self) -> dict:
        """
        Get full task status including cached data (async)
        
        Includes:
        - Scheduler info (from get_status)
        - Last execution timestamp
        - Task statistics
        - Lock status
        """
        base_status = self.get_status()
        
        # Add timestamp
        last_timestamp = await self.get_last_execution_timestamp()
        base_status["last_execution_timestamp"] = last_timestamp
        
        # Add task stats
        task_stats = await self.get_task_stats()
        base_status["task_stats"] = task_stats
        
        # Add lock status
        lock_status = await self.get_lock_status()
        base_status["lock_status"] = lock_status
        
        return base_status


class Scheduler:
    """Unified background task scheduler with distributed lock support"""

    def __init__(self):
        self.tasks: dict[str, PullTask] = {}
        self.running = False
        self._scheduler_task: asyncio.Task | None = None

    def register_task(self, task: PullTask):
        """Register a new task"""
        self.tasks[task.provider_slug] = task

    def get_task(self, provider_slug: str) -> PullTask | None:
        """Get task by provider slug"""
        return self.tasks.get(provider_slug)

    async def trigger_task(self, provider_slug: str, force: bool = False) -> bool:
        """Manually trigger a specific task"""
        task = self.get_task(provider_slug)
        if not task:
            logger.error(f"Task not found: {provider_slug}")
            return False

        return await task.manual_trigger(force=force)

    def get_tasks_status(self) -> dict:
        """Get status of all tasks"""
        return {
            "total_tasks": len(self.tasks),
            "tasks": {slug: task.get_status() for slug, task in self.tasks.items()},
        }

    async def get_task_status(self, provider_slug: str) -> dict | None:
        """Get status of a specific task"""
        task = self.get_task(provider_slug)
        if not task:
            return None

        status = task.get_status()
        # Add lock status information
        lock_status = await task.get_lock_status()
        status["lock_status"] = lock_status

        return status

    async def start(self):
        """Start the scheduler as a background task"""
        if self.running:
            logger.warning("Scheduler is already running")
            return

        self.running = True
        logger.info("Starting scheduler...")

        # TH-416: restore each task's last_run from redis before scheduling,
        # so a service restart doesn't reset long-interval tasks
        # (renpho/whoop @ 24h) to execute immediately.
        if pull_task_lock_manager:
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

    async def stop(self):
        """Stop the scheduler"""
        self.running = False
        logger.info("Stopping scheduler...")

        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                logger.info("Scheduler task cancelled successfully")

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

"""
Distributed lock manager for Theta Pull tasks
"""

import logging, uuid
import redis.asyncio

from datetime import datetime
from typing import Optional

#-----------------------------------------------------------------------------

_global_redis_client = None

async def get_redis_client() -> redis.asyncio.Redis | None:
    global _global_redis_client

    if not _global_redis_client:
        from ...utils.config import global_config
        _global_redis_client = await global_config().get_redis().get_async_client()

    return _global_redis_client

#-----------------------------------------------------------------------------

class PullTaskLockManager:
    """
    Pull task distributed lock manager

    Resolves task duplication issues in multi-docker instance environments, supporting:
    - Provider-based distributed locking
    - Configurable lock expiration time
    - Force execution option
    - Lock status monitoring
    """

    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]  # Instance identifier

    def _get_lock_key(self, provider_slug: str) -> str:
        """Get Redis key for the lock"""
        return f"theta_pull_execution_lock:{provider_slug}"

    def _get_lock_value(self, execution_id: str) -> str:
        """Get lock value containing instance information"""
        timestamp = datetime.now().isoformat()
        return f"{self.instance_id}:{timestamp}:{execution_id}"
    
    def _get_timestamp_key(self, provider_slug: str) -> str:
        """Get Redis key for execution timestamp"""
        return f"task_execution_timestamp:{provider_slug}"

    async def try_acquire_execution_lock(
            self, provider_slug: str, lock_duration_hours: float = 23.5, force: bool = False
    ) -> Optional[str]:
        """
        Try to acquire execution lock

        Args:
            provider_slug: Provider identifier
            lock_duration_hours: Lock duration in hours
            force: Whether to force execution (ignore locks)

        Returns:
            execution_id if lock acquired, None if failed
        """
        execution_id = str(uuid.uuid4())
        lock_key = self._get_lock_key(provider_slug)
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.warning(f"Redis client not initialized for {provider_slug} return true")
            return execution_id
        # Force execution mode
        if force:
            logging.warning(f"Force execution mode enabled for {provider_slug}, ignoring existing locks")
            # In force mode, delete existing lock first, then acquire new lock
            await redis_client.delete(lock_key)

        try:
            lock_value = self._get_lock_value(execution_id)
            lock_timeout_seconds = int(lock_duration_hours * 3600)

            # Try to acquire lock
            acquired = await redis_client.set(lock_key, lock_value, ex=lock_timeout_seconds, nx=True)

            if acquired:
                logging.info(
                    f"Execution lock acquired for {provider_slug} "
                    f"(instance: {self.instance_id}, execution: {execution_id}, "
                    f"duration: {lock_duration_hours}h)"
                )
                return execution_id
            else:
                # Failed to acquire lock, check existing lock info
                existing_lock = await redis_client.get(lock_key)
                if existing_lock:
                    logging.info(
                        f"Execution lock already exists for {provider_slug}, "
                        f"existing: {existing_lock.decode() if isinstance(existing_lock, bytes) else existing_lock}"
                    )
                else:
                    logging.warning(f"Failed to acquire lock for {provider_slug}, unknown reason")
                return None

        except Exception as e:
            logging.error(f"Error acquiring execution lock for {provider_slug}: {str(e)}")
            return None

    async def release_execution_lock(self, provider_slug: str, execution_id: str) -> bool:
        """
        Release execution lock

        Args:
            provider_slug: Provider identifier
            execution_id: Execution ID

        Returns:
            True if released successfully
        """
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.warning("No redis return true")
            return True

        lock_key = self._get_lock_key(provider_slug)

        try:
            # Get current lock value to verify ownership
            current_lock = await redis_client.get(lock_key)
            if current_lock is None:
                logging.warning(f"Lock {lock_key} does not exist or already expired")
                return True

            # Parse lock value to check ownership
            current_lock_str = current_lock.decode() if isinstance(current_lock, bytes) else current_lock
            if execution_id in current_lock_str and self.instance_id in current_lock_str:
                # We own this lock, safe to delete
                await redis_client.delete(lock_key)
                logging.info(f"Released execution lock for {provider_slug} (execution: {execution_id})")
                return True
            else:
                logging.warning(
                    f"Lock ownership mismatch for {provider_slug}, "
                    f"expected execution: {execution_id}, current: {current_lock_str}"
                )
                return False

        except Exception as e:
            logging.error(f"Error releasing execution lock for {provider_slug}: {str(e)}")
            return False

    async def get_last_execution_timestamp(
        self, 
        provider_slug: str
    ) -> Optional[int]:
        """
        Get last execution timestamp for incremental processing
        
        Args:
            provider_slug: Task identifier (e.g., "aggregate_indicator")
            
        Returns:
            Unix timestamp or None if not set
        """
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.warning(f"Redis not available for {provider_slug}")
            return None
        
        try:
            key = self._get_timestamp_key(provider_slug)
            timestamp_str = await redis_client.get(key)
            
            if timestamp_str:
                # Handle bytes returned from Redis
                if isinstance(timestamp_str, bytes):
                    timestamp_str = timestamp_str.decode('utf-8')
                return int(timestamp_str)
            return None
            
        except Exception as e:
            logging.error(
                f"Error getting execution timestamp for {provider_slug}: {e}"
            )
            return None
    
    async def clear_last_execution_timestamp(
        self,
        provider_slug: str
    ) -> bool:
        """
        Clear last execution timestamp (for force refresh scenarios)
        
        Args:
            provider_slug: Task identifier
            
        Returns:
            True if cleared successfully
        """
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.warning(f"Redis not available for {provider_slug}")
            return False
        
        try:
            key = self._get_timestamp_key(provider_slug)
            await redis_client.delete(key)
            logging.info(f"Cleared last execution timestamp for {provider_slug}")
            return True
            
        except Exception as e:
            logging.error(
                f"Error clearing execution timestamp for {provider_slug}: {e}"
            )
            return False
    
    async def update_last_execution_timestamp(
        self,
        provider_slug: str,
        timestamp: int
    ) -> bool:
        """
        Update last execution timestamp
        
        Args:
            provider_slug: Task identifier
            timestamp: Unix timestamp
            
        Returns:
            True if successful
        """
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.warning(f"Redis not available for {provider_slug}")
            return False
        
        try:
            key = self._get_timestamp_key(provider_slug)
            await redis_client.set(
                key,
                str(timestamp),
                ex=604800  # 7 days TTL
            )
            logging.debug(
                f"Updated execution timestamp for {provider_slug}: {timestamp}"
            )
            return True
            
        except Exception as e:
            logging.error(
                f"Error updating execution timestamp for {provider_slug}: {e}"
            )
            return False

    async def get_lock_status(self, provider_slug: str) -> dict:
        """
        Get lock status information

        Args:
            provider_slug: Provider identifier

        Returns:
            Lock status information
        """
        redis_client = await get_redis_client()
        if redis_client is None:
            logging.error(f"Redis client not initialized for {provider_slug}")
            return {"locked": False, "error": "Redis client not initialized"}

        try:
            lock_key = self._get_lock_key(provider_slug)
            existing_lock = await redis_client.get(lock_key)
            ttl = await redis_client.ttl(lock_key)

            if existing_lock:
                lock_value = existing_lock.decode() if isinstance(existing_lock, bytes) else existing_lock
                parts = lock_value.split(":")

                return {
                    "locked": True,
                    "lock_value": lock_value,
                    "holder_instance": parts[0] if len(parts) > 0 else "unknown",
                    "lock_timestamp": parts[1] if len(parts) > 1 else "unknown",
                    "execution_id": parts[2] if len(parts) > 2 else "unknown",
                    "ttl_seconds": ttl if ttl > 0 else 0,
                    "is_current_instance": parts[0] == self.instance_id if len(parts) > 0 else False,
                }
            else:
                return {
                    "locked": False,
                    "lock_value": None,
                    "holder_instance": None,
                    "lock_timestamp": None,
                    "execution_id": None,
                    "ttl_seconds": 0,
                    "is_current_instance": False,
                }

        except Exception as e:
            logging.error(f"Error getting lock status for {provider_slug}: {str(e)}")
            return {"locked": False, "error": str(e)}


# Global lock manager instance
pull_task_lock_manager = PullTaskLockManager()

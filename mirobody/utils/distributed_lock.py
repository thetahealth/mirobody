"""Session-bound Postgres advisory locks for provider pulls."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config import global_config

logger = logging.getLogger(__name__)


class PullTaskLockManager:
    """Hold each advisory lock on its own connection until the pull finishes."""

    def __init__(self) -> None:
        self.instance_id = str(uuid.uuid4())[:8]
        self._connections: dict[str, tuple[str, Any]] = {}

    @staticmethod
    def _lock_key(provider_slug: str) -> str:
        return f"theta_pull_execution_lock:{provider_slug}"

    @staticmethod
    def _timestamp_key(provider_slug: str) -> str:
        return f"task_execution_timestamp:{provider_slug}"

    @staticmethod
    def _last_run_key(provider_slug: str) -> str:
        return f"pull_task:last_run:{provider_slug}"

    async def try_acquire_execution_lock(
        self, provider_slug: str, lock_duration_hours: float = 23.5, force: bool = False
    ) -> str | None:
        # Force bypasses the scheduler interval, but never mutual exclusion.
        del lock_duration_hours, force
        conn = None
        try:
            conn = await global_config().get_postgresql().get_async_client(cursor_factory=None)
            await conn.set_autocommit(True)
            row = await (await conn.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (self._lock_key(provider_slug),),
            )).fetchone()
            if not row[0]:
                await conn.close()
                return None
            execution_id = str(uuid.uuid4())
            self._connections[execution_id] = (provider_slug, conn)
            logger.info("provider lock acquired: provider=%s execution_id=%s",
                        provider_slug, execution_id)
            return execution_id
        except Exception as exc:
            if conn is not None:
                await conn.close()
            logger.error("provider lock acquire failed: provider=%s error_type=%s",
                         provider_slug, type(exc).__name__,
                         exc_info=not is_driver_exception(exc))
            return None

    async def release_execution_lock(self, provider_slug: str, execution_id: str) -> bool:
        entry = self._connections.get(execution_id)
        if entry is None or entry[0] != provider_slug:
            return False
        _, conn = self._connections.pop(execution_id)
        try:
            row = await (await conn.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (self._lock_key(provider_slug),),
            )).fetchone()
            return bool(row[0])
        except Exception as exc:
            logger.error("provider lock release failed: provider=%s error_type=%s",
                         provider_slug, type(exc).__name__,
                         exc_info=not is_driver_exception(exc))
            return False
        finally:
            await conn.close()

    async def get_last_execution_timestamp(self, provider_slug: str) -> float | None:
        try:
            value = await global_config().get_ephemeral().get(self._timestamp_key(provider_slug))
            return float(value) if value else None
        except Exception as exc:
            logger.warning("execution timestamp read failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return None

    async def clear_last_execution_timestamp(self, provider_slug: str) -> bool:
        try:
            await global_config().get_ephemeral().delete(self._timestamp_key(provider_slug))
            return True
        except Exception as exc:
            logger.warning("execution timestamp clear failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return False

    async def update_last_execution_timestamp(self, provider_slug: str,
                                              timestamp: float) -> bool:
        try:
            await global_config().get_ephemeral().set(
                self._timestamp_key(provider_slug), str(timestamp), ex=604800
            )
            return True
        except Exception as exc:
            logger.warning("execution timestamp write failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return False

    async def get_last_run(self, provider_slug: str) -> datetime | None:
        try:
            value = await global_config().get_ephemeral().get(self._last_run_key(provider_slug))
            return datetime.fromisoformat(value) if value else None
        except Exception as exc:
            logger.warning("last run read failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return None

    async def set_last_run(self, provider_slug: str, ts: datetime) -> bool:
        try:
            await global_config().get_ephemeral().set(
                self._last_run_key(provider_slug), ts.isoformat(), ex=604800
            )
            return True
        except Exception as exc:
            logger.warning("last run write failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return False

    async def get_lock_status(self, provider_slug: str) -> dict:
        conn = None
        try:
            conn = await global_config().get_postgresql().get_async_client(cursor_factory=None)
            await conn.set_autocommit(True)
            row = await (await conn.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (self._lock_key(provider_slug),),
            )).fetchone()
            if row[0]:
                await conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    (self._lock_key(provider_slug),),
                )
            own = next((execution_id for execution_id, (slug, _) in self._connections.items()
                        if slug == provider_slug), None)
            return {
                "locked": not row[0],
                "lock_value": None,
                "holder_instance": self.instance_id if own else None,
                "lock_timestamp": None,
                "execution_id": own,
                "ttl_seconds": 0,
                "is_current_instance": bool(own),
            }
        except Exception as exc:
            logger.warning("provider lock status failed: provider=%s error_type=%s",
                           provider_slug, type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
            return {"locked": False, "error": type(exc).__name__}
        finally:
            if conn is not None:
                await conn.close()


pull_task_lock_manager = PullTaskLockManager()

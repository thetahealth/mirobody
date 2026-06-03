"""
Apple Health platform implementation
"""

import asyncio
import logging

from typing import Any, Dict, List

from .provider import AppleHealthProvider
from .services.database_service import AppleDatabaseService
from ..base import LinkRequest, Platform, ProviderInfo
from ..core import (
    CacheConfig,
    UserProvider,
    ProviderStatus
)
from ..core.aggregate_indicator.service import AggregateIndicatorService
from ..core.distributed_lock import pull_task_lock_manager
from ..data_upload.services import VitalHealthService


class AppleHealthPlatform(Platform):
    """
    Apple Health Platform implementation

    Handles Apple Health data import, supporting two data sources:
    1. Apple Health export data
    2. CDA (Clinical Document Architecture) documents
    """

    def __init__(self):
        """Initialize Apple Health Platform"""
        super().__init__()
        self.db_service = AppleDatabaseService()
        self.vital_health_service = VitalHealthService()

        self._register_built_in_providers()

        self._cache = None
        self._cache_time = None
        self.cache_ttl = CacheConfig.PROVIDER_CACHE_TTL

    @property
    def name(self) -> str:
        """Platform name"""
        return "apple"

    @property
    def supports_registration(self) -> bool:
        return False

    def _register_built_in_providers(self) -> None:
        apple_provider = AppleHealthProvider(self)
        self._providers[apple_provider.info.slug] = apple_provider

        logging.info(f"Registered built-in providers for {self.name} platform")

    async def get_providers(self, nocache: bool = False) -> List[ProviderInfo]:
        return []

    async def get_user_providers(self, user_id: str) -> List[UserProvider]:
        return []

    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        provider_slug = request.provider_slug

        provider = self.get_provider(provider_slug)
        if not provider:
            raise ValueError(f"Provider {provider_slug} not found in apple platform")

        result = await provider.link(request)
        return result

    async def unlink(self, user_id: str, provider_slug: str) -> Dict[str, Any]:
        provider = self.get_provider(provider_slug)
        if not provider:
            raise ValueError(f"Provider {provider_slug} not found in apple platform")

        result = await provider.unlink(user_id)
        return result

    async def post_data(self, provider_slug: str, data: Dict[str, Any], msg_id: str) -> bool:
        try:
            provider = self.get_provider(provider_slug)
            if not provider:
                logging.error(f"Provider {provider_slug} not found in apple platform")
                return False

            user_id = data.get("user_id")

            if not user_id:
                logging.error("Missing user_id in data")
                return False

            try:
                standard_data = await provider.format_data(data)

                if not standard_data or not standard_data.healthData:
                    logging.info(f"No data formatted by provider {provider_slug}")
                    return True

                success = await self.vital_health_service.process_standard_data(standard_data, user_id)

                logging.info(f"Apple platform processed {len(standard_data.healthData)} records for user {user_id}, "
                    f"success: {success}")

                # Fire-and-forget: kick incremental aggregation so the frontend
                # sees fresh th_series_data without waiting for the 4-min
                # scheduled AggregateIndicatorTask. Errors are swallowed in the
                # background task; the scheduled task is the safety net.
                if success:
                    asyncio.create_task(self._trigger_aggregation_after_ingest(user_id))

                return success

            except Exception as e:
                logging.error(f"Error processing data: {str(e)}", stack_info=True)
                return False

        except Exception as e:
            logging.error(f"Error in post_data for provider {provider_slug}: {str(e)}", stack_info=True)
            return False

    async def _trigger_aggregation_after_ingest(self, user_id: str) -> None:
        """Run an incremental aggregation in the background after an apple_health ingest.

        Runs all-users incremental aggregation to keep the call path simple;
        the distributed lock inside AggregateIndicatorService de-duplicates
        concurrent ingest bursts. Any failure here is logged and swallowed —
        AggregateIndicatorTask (every 4 min) will catch up.
        """
        # --- DEBOUNCE (DISABLED) -----------------------------------------------
        # Re-enable when concurrent uploads from many users cause the
        # distributed lock to back up. Keying by 'all' because we currently
        # run a global incremental; switch the key to user_id if this method
        # is later changed to per-user aggregation.
        #
        # from ..utils.utils_redis import redis_client
        # debounce_key = "agg:debounce:apple_health:all"
        # if redis_client is not None:
        #     acquired = await redis_client.set(debounce_key, "1", ex=5, nx=True)
        #     if not acquired:
        #         logging.info("[AppleHealth] Post-ingest aggregation debounced")
        #         return
        # -----------------------------------------------------------------------

        try:
            service = AggregateIndicatorService()
            # Reuse AggregateIndicatorTask's cursor so we run an actual
            # incremental delta. Without this, process_incremental falls
            # back to a 24h cold_start scan (see service.py: last_timestamp
            # is None branch), which on dev volume costs ~150s instead of
            # the <2s a normal incremental cycle takes.
            # We deliberately do NOT update the cursor here; the scheduled
            # AggregateIndicatorTask owns it. Re-running the same window
            # is idempotent (ON CONFLICT DO UPDATE on th_series_data).
            last_ts = await pull_task_lock_manager.get_last_execution_timestamp(
                "aggregate_indicator"
            )
            result = await service.process_incremental(
                last_timestamp=last_ts,
                user_id=None,
            )
            logging.info(
                f"[AppleHealth] Post-ingest aggregation triggered for user {user_id}: "
                f"status={result.get('status')}, "
                f"summaries={result.get('summaries_created', 0)}, "
                f"users_affected={result.get('users_affected', 0)}, "
                f"time_ms={result.get('execution_time_ms', 0):.1f}, "
                f"cursor={last_ts}"
            )
        except Exception as e:
            logging.warning(
                f"[AppleHealth] Post-ingest aggregation failed (ignored, "
                f"scheduled task will catch up): {e}"
            )

    async def update_llm_access(self, user_id: str, provider_slug: str, llm_access: int) -> Dict[str, Any]:
        """
        Update LLM access permission for a apple provider

        Args:
            user_id: User ID
            provider_slug: Provider identifier
            llm_access: Access level (0: no access, 1: limited access, 2: full access)

        Returns:
            Update result data
        """
        success = await self.db_service.update_llm_access(user_id, provider_slug, llm_access)

        if not success:
            raise RuntimeError(f"Failed to update LLM access for provider {provider_slug}")

        return {
            "provider_slug": provider_slug,
            "platform": self.name,
            "llm_access": llm_access,
            "updated": True,
        }

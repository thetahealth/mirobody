"""In-process push of pulled provider data to its platform.

`push_data` hands a provider's payload to the registered platform's `post_data`,
the shape a webhook delivery would have taken. There used to be an HTTP twin
(`_push_via_http`, POSTing to a hardcoded `http://localhost:18060`) behind a
`use_function_call` switch; the switch was constructed True and the two methods
that flipped it had no callers, so the HTTP branch was unreachable.
"""

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)

class PushService:
    async def push_data(
        self,
        platform: str,
        provider_slug: str,
        data: dict[str, Any],
        msg_id: str | None = None,
    ) -> bool:
        """
        Push data to specified platform

        Args:
            platform: Platform identifier
            provider_slug: Provider identifier
            data: Raw data
            msg_id: Message ID, auto-generated if None

        Returns:
            Whether push succeeded
        """
        if not msg_id:
            msg_id = str(uuid.uuid4())

        try:
            return await self._push_via_function_call(platform, provider_slug, data, msg_id)

        except Exception as e:
            logger.error(f"Push data failed for {platform}/{provider_slug}: {str(e)}")
            return False

    async def _push_via_function_call(
        self, platform: str, provider_slug: str, data: dict[str, Any], msg_id: str
    ) -> bool:
        """
        Push data via function call

        Uses platformManager to get registered platform instance
        """
        try:
            # Use platformManager to get registered platform instance
            from ...pulse.manager import platform_manager

            platform_instance = platform_manager.get_platform(platform)
            if not platform_instance:
                logger.error(f"platform not found in platformManager: {platform}")
                return False

            # Call platform's post_data method
            success = await platform_instance.post_data(provider_slug, data, msg_id)

            if success:
                logger.info(f"Function call push successful: {platform}/{provider_slug}, msg_id: {msg_id}")
            else:
                logger.error(f"Function call push failed: {platform}/{provider_slug}, msg_id: {msg_id}")

            return success

        except Exception as e:
            logger.error(f"Function call push error for {platform}/{provider_slug}: {str(e)}")
            return False


# Global push service instance
push_service = PushService()

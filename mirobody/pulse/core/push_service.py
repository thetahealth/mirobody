"""
Encapsulated function call push service

Used to simulate webhook pushes, avoiding HTTP overhead while providing the ability to switch to HTTP
"""

import aiohttp, logging, uuid
from typing import Any, Dict, Optional

class PushService:
    """
    Push service

    Encapsulates function call and HTTP push, providing unified push interface
    """

    def __init__(self, use_function_call: bool = True):
        """
        Initialize push service

        Args:
            use_function_call: Whether to prioritize function call, False for HTTP
        """
        self.use_function_call = use_function_call
        self._market_cache: Dict[str, Any] = {}

    async def push_data(
        self,
        platform: str,
        provider_slug: str,
        data: Dict[str, Any],
        msg_id: Optional[str] = None,
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
            if self.use_function_call:
                return await self._push_via_function_call(platform, provider_slug, data, msg_id)
            else:
                return await self._push_via_http(platform, provider_slug, data, msg_id)

        except Exception as e:
            logging.error(f"Push data failed for {platform}/{provider_slug}: {str(e)}")
            return False

    async def _push_via_function_call(
        self, platform: str, provider_slug: str, data: Dict[str, Any], msg_id: str
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
                logging.error(f"platform not found in platformManager: {platform}")
                return False

            # Call platform's post_data method
            success = await platform_instance.post_data(provider_slug, data, msg_id)

            if success:
                logging.info(f"Function call push successful: {platform}/{provider_slug}, msg_id: {msg_id}")
            else:
                logging.error(f"Function call push failed: {platform}/{provider_slug}, msg_id: {msg_id}")

            return success

        except Exception as e:
            logging.error(f"Function call push error for {platform}/{provider_slug}: {str(e)}")
            return False

    async def _push_via_http(self, platform: str, provider_slug: str, data: Dict[str, Any], msg_id: str) -> bool:
        """
        Push data via HTTP

        As an alternative to function call
        """
        try:
            base_url = "http://localhost:18060"
            webhook_url = f"{base_url}/api/v1/pulse/{platform}/webhook"

            headers = {
                "Content-Type": "application/json",
                "X-Message-ID": msg_id,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=data, headers=headers) as response:
                    if response.status == 200:
                        logging.info(f"HTTP push successful: {platform}/{provider_slug}, msg_id: {msg_id}")
                        return True
                    else:
                        response_text = await response.text()

                        logging.error(
                            f"HTTP push failed: {platform}/{provider_slug}, status: {response.status}, response: {response_text}"
                        )
                        return False

        except Exception as e:
            logging.error(f"HTTP push error for {platform}/{provider_slug}: {str(e)}")
            return False

    def use_http_push(self):
        """Switch to HTTP push mode"""
        self.use_function_call = False
        logging.info("Switched to HTTP push mode")

    def use_function_call_push(self):
        """Switch to function call push mode"""
        self.use_function_call = True
        logging.info("Switched to function call push mode")


# Global push service instance
push_service = PushService(use_function_call=True)

"""In-process push of pulled provider data to its platform.

`push_data` hands a provider's payload to the registered platform's `post_data`
through `PlatformManager`, the shape a webhook delivery would have taken. It is
the name a provider plugin imports (`docs/provider-guide.md`), so it stays a
module-level instance.
"""

import uuid
from typing import Any

from mirobody.collect.manager import platform_manager


class PushService:
    async def push_data(
        self,
        platform: str,
        provider_slug: str,
        data: dict[str, Any],
        msg_id: str | None = None,
    ) -> bool:
        """Deliver `data` to `platform`'s `post_data` for `provider_slug`.
        `msg_id` is generated when not given. Returns whether the platform
        accepted it; a missing platform or a failure is `False`, logged by
        `PlatformManager.post_data`."""
        return await platform_manager.post_data(platform, provider_slug, data, msg_id or str(uuid.uuid4()))


push_service = PushService()

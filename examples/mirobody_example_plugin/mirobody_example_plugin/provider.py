"""A device provider from a plugin.

The contract is `mirobody.pulse.providers.platform.base.BasePullProvider`:
`create_provider(config)` returns an instance or None (declining because a
credential is absent is normal), `format_data(fmt_input)` turns the vendor's
payload into `StandardPulseData`. This one declines unless
`ENABLE_EXAMPLE_PROVIDER` is set, so installing the plugin is harmless.
"""

from __future__ import annotations

import os
from typing import Any

from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.ingest.models.requests import FormatDataInput, StandardPulseData
from mirobody.pulse.providers.platform.base import BasePullProvider


class ExampleProvider(BasePullProvider):
    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> ExampleProvider | None:
        if not os.environ.get("ENABLE_EXAMPLE_PROVIDER"):
            return None
        return cls()

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            slug="example",
            name="Example device",
            description="A provider that ships as a pip package",
            auth_type=LinkType.PASSWORD,
            status=ProviderStatus.AVAILABLE,
        )

    # The pull half. A real provider talks to the vendor here and stores the raw
    # payload in its own table; this one has nothing to pull.
    async def pull_from_vendor_api(self, username: str, password: str) -> list[dict[str, Any]]:
        return []

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    async def is_data_already_processed(self, raw_data: dict[str, Any]) -> bool:
        return True

    # The pure half: vendor payload -> StandardPulseData.
    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        return self._create_empty_response(self.generate_request_id(), fmt_input.context.theta_user_id or "")

"""A device provider from a plugin.

The contract is `mirobody.collect.BasePullProvider`, and every name a
provider needs comes from that one import:
`create_provider(config)` returns an instance or None (declining because a
credential is absent is normal), `format_data(fmt_input)` turns the vendor's
payload into `StandardPulseData`. This one declines unless
`ENABLE_EXAMPLE_PROVIDER` is set, so installing the plugin is harmless.
"""

from __future__ import annotations

import os
from typing import Any

from mirobody.collect import (
    BasePullProvider,
    FormatDataInput,
    LinkType,
    ProviderInfo,
    ProviderStatus,
    StandardPulseData,
)



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

    # The pull half. A real provider asks the vendor for the account's last
    # `days` here (`credentials` holds `username` and `password` for this
    # auth type) and sets `raw_table` to keep payloads as received; this one
    # has nothing to pull.
    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        return []

    # The pure half: vendor payload -> StandardPulseData.
    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        return self._create_empty_response(self.generate_request_id(), fmt_input.context.theta_user_id or "")

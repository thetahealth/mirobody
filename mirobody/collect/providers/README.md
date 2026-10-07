# Device and health-platform providers

A provider connects Mirobody to one external data source. This directory holds
the shipped ones and the machinery they stand on:

    _platform/              BasePullProvider, the loader, credential storage,
                            the OAuth2 client, the HTTP helpers, the pull task
    mirobody_garmin_connect/  Garmin (OAuth 1.0a, pushes; pulled once after a link)
    mirobody_oura/          Oura (OAuth 2.0, pulled hourly)
    mirobody_whoop/         WHOOP (OAuth 2.0, pulled daily)
    apple/                  Apple Health and CDA documents, pushed by a client app

The translation of a vendor's JSON into facts is not here: it is the pure
decode table in `mirobody/kernel/decoders/<vendor>.py`, with its samples. A
provider here is the IO shell around it.

## Where a provider can live

`ProviderPlatform.load_providers()` scans each directory in `PROVIDER_DIRS`
(config.yaml lists `mirobody/collect/providers`; add your own directory to the
list) for `mirobody_*/provider_*.py`, and also loads any installed package that
declares a `mirobody.providers` entry point
(`examples/mirobody_example_plugin/` is a complete one). A directory not in
`PROVIDER_DIRS` is not scanned. The module file name need not match the
directory slug: the shipped Garmin provider is
`mirobody_garmin_connect/provider_garmin.py`.

The directory slug is also what `installed.py` reports, without importing
anything; `test_installed.py` guards the convention, so a `mirobody_*/`
directory holding no `provider_*.py` fails the suite instead of silently
loading nothing.

## The contract

```python
from typing import Any

from mirobody.collect import (
    BasePullProvider, FormatDataInput, LinkType, ProviderInfo, ProviderStatus, StandardPulseData,
)


class MyDeviceProvider(BasePullProvider):
    pull_interval_hours = 6.0              # how often the scheduler pulls
    raw_table = "health_data_mydevice"     # keep payloads as received; "" keeps none

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "MyDeviceProvider | None":
        return cls()                       # None when a credential it needs is not configured

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(slug="theta_mydevice", name="My Device",
                            auth_type=LinkType.PASSWORD, status=ProviderStatus.AVAILABLE)

    async def _validate_credentials(self, credentials: dict) -> None:
        ...                                # raise if username/password cannot work

    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        ...                                # the last `days`, as {"data_type", "data", "timestamp"} packages;
                                           # raise PermissionError when the vendor refuses the credential

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        ...                                # decoders.decode + records_from_facts
```

The base class runs the loop: every linked account every `pull_interval_hours`,
`pull_days` back; each package pushed to the platform, saved by
`save_raw_data_to_db` and formatted. Three refusals in a row stop it trying an
account until the person links again or the worker restarts; a timeout or a
vendor outage does not count. A `LinkType.CUSTOMIZED` provider declares its fields with
`ConnectInfoField` and reads them from `credentials["connect_info"]`, which is
stored encrypted.

The full guide is [docs/provider-guide.md](../../../docs/provider-guide.md);
setting up the shipped three is [docs/provider-setup.md](../../../docs/provider-setup.md).

"""Polar — direct client for the Polar heart-rate / watch brand, which exposes a
free, self-serve public API (Polar Open AccessLink). A direct integration for
deployments (or individual developers who own a Polar device) that want Polar
data without an aggregator. See src/health/README.md.

Polar Open AccessLink v3 (https://www.polar.com/accesslink-api/):
* base_url defaults to the documented host https://www.polaraccesslink.com
(override via POLAR_BASE_URL).
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header; the
token is exchanged out of band at https://polarremote.com/v2/oauth2/token.
* authorize_url() builds the authorization-code consent URL at
https://flow.polar.com/oauth2/authorization from config.client_id.
* revoke() DELETEs the registered user (/v3/users/{user-id}), disconnecting them.

fetch() is only PARTLY expressible on this interface, because AccessLink splits
data into two shapes:
* Sleep (and Nightly Recharge) are NON-transactional direct GETs by user —
/v3/users/{user-id}/sleep returns the recent nights — so Sleep is honored.
* Training sessions and daily activity use a TRANSACTION pull model (create a
transaction, list its resource URLs, GET each, then commit) that returns
"new data since the last pull" — there is no [start,end] query. That does not
fit fetch(user_id, domain, start, end), so Activity/HeartRate throw a
VendorError explaining the model rather than fabricating a date-range call.
(Same honest-stub stance as garmin.cpp's push model.)

list_providers / handle_webhook stay inherited stubs: Polar is a single brand,
and its webhook ("ping") contract is a separate scheme not wired here.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/polar.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="polar",
    display_name="Polar",
    positioning="Heart-rate / sports-watch brand with a free, self-serve OAuth2 API",
    target_customers="Apps (and individual developers) serving Polar device users",
    data_source_coverage="Polar Flow: training sessions, daily activity, sleep, Nightly Recharge",
    integration_method="\"REST Open AccessLink v3 (OAuth2)",
    compliance_summary="User-consented OAuth scopes",
    differentiator="Free self-serve API — a Polar Flow account registers a client, no approval",
    docs_url="https://www.polar.com/accesslink-api/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.HEART_RATE, DataDomain.SLEEP,),
    integrations=(Integration.REST,),
    category="device",
    status=VendorStatus.METADATA,
)


class PolarVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

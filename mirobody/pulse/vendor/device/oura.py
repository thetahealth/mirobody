"""Oura (Oura Ring) — direct client for a consumer wearable brand with a free,
self-serve public cloud API. Like Fitbit/Withings this is a direct integration
for deployments (or individual developers who own the ring) that want Oura data
without an aggregator. See src/health/README.md.

Oura API v2 (https://cloud.ouraring.com/v2/docs):
* base_url defaults to the documented host https://api.ouraring.com
(override via OURA_BASE_URL).
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header on
fetch(); the token is exchanged out of band at https://api.ouraring.com/oauth/token.
(Personal Access Tokens were deprecated Dec 2025 — OAuth2 only for new apps.)
* authorize_url() builds the standard authorization-code consent URL at
https://cloud.ouraring.com/oauth/authorize from config.client_id.
* fetch() issues the documented v2 usercollection GETs. Daily summaries take a
[start_date, end_date] day range (YYYY-MM-DD); the heartrate series takes a
[start_datetime, end_datetime] ISO-8601 range instead — handled per domain.

revoke / list_providers / handle_webhook stay inherited stubs: Oura is a single
brand (no provider catalogue), documents no token-revoke endpoint on the public
API, and its webhook subscription API is a separate contract not wired here.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/oura.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="oura",
    display_name="Oura",
    positioning="Smart-ring wearable brand with a free, self-serve OAuth2 cloud API",
    target_customers="Apps (and individual developers) serving Oura Ring users",
    data_source_coverage="Oura Ring: sleep, readiness, activity, heart rate / HRV",
    integration_method="REST v2 Web API (OAuth2)",
    compliance_summary="User-consented OAuth scopes",
    differentiator="Free self-serve API (<=10 users before approval), developer-friendly",
    docs_url="https://cloud.ouraring.com/v2/docs",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.SLEEP, DataDomain.ACTIVITY, DataDomain.HEART_RATE,),
    integrations=(Integration.REST,),
    category="device",
    status=VendorStatus.METADATA,
)


class OuraVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

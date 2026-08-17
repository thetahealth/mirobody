"""Fitbit (Google) — direct client for a consumer wearable brand. Brands like
this are normally reached through an aggregator (Terra/Validic/…); this is a
direct integration for deployments that want to talk to Fitbit's public Web
API without one. See src/health/README.md.

Fitbit Web API (https://dev.fitbit.com/build/reference/web-api/):
* base_url defaults to the documented host https://api.fitbit.com
(override via MIROBODY_VENDOR_FITBIT_BASE_URL).
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header on
fetch(); the token is exchanged out of band at /oauth2/token.
* authorize_url() builds the standard authorization-code consent URL at
www.fitbit.com/oauth2/authorize from config.client_id (no network call; a
confidential server client authenticates with its secret at the token step,
so PKCE is not used). revoke() POSTs the token to /oauth2/revoke with HTTP
Basic client_id:client_secret. handle_webhook() verifies the subscription
notification's X-Fitbit-Signature (base64(HMAC-SHA1(raw_body)) keyed by the
client secret + "&"). All confirmed at dev.fitbit.com/build/reference/web-api.
* fetch() issues the documented per-domain time-series GETs over a
[startDate, endDate] day range (yyyy-MM-dd). The user path segment is the
Fitbit user id, or "-" (the API's alias for the token's own user).

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/fitbit.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="fitbit",
    display_name="Fitbit",
    positioning="Consumer wearable brand (Google) with a public OAuth2 Web API",
    target_customers="Apps serving Fitbit tracker / smartwatch users",
    data_source_coverage="Fitbit trackers and smartwatches",
    integration_method="REST Web API (OAuth2) + subscription webhooks",
    compliance_summary="User-consented OAuth scopes",
    differentiator="Mature, public, well-documented consumer wearable API",
    docs_url="https://dev.fitbit.com/build/reference/web-api/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.HEART_RATE, DataDomain.SLEEP, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST, Integration.WEBHOOK,),
    category="device",
    status=VendorStatus.METADATA,
)


class FitbitVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

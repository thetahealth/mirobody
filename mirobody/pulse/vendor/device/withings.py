"""Withings (Health Mate) — direct client for a consumer health-device brand,
normally reachable through an aggregator; this talks to Withings' own API for
deployments that want it directly. See src/health/README.md.

Withings API (https://developer.withings.com/api-reference/):
* base_url defaults to the documented host https://wbsapi.withings.net
(override via MIROBODY_VENDOR_WITHINGS_BASE_URL).
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header on
fetch(); minted out of band via Withings' authorization-code flow.
* authorize_url() builds the standard authorization-code consent URL at
account.withings.com/oauth2_user/authorize2 from config.client_id (no
network call). revoke() and handle_webhook() stay inherited stubs: Withings
documents no app-initiated token-revoke endpoint (only the user can revoke,
or Notify's `revoke` action which just drops a webhook subscription), and its
Notify webhook carries no inbound signature to verify — the documented
pattern is to treat a notification as an untrusted trigger and re-fetch via
the Data API.
* fetch() POSTs form-encoded requests with an `action` selector, the way the
API is shaped. Domains map onto the documented services: body measures
(Measure v1 getmeas, unix-second window), heart list (Heart v2, unix-second
window), activity and sleep summaries (v2, yyyy-MM-dd window).
The token already scopes the request to its owner, so `user_id` is accepted for
interface parity but is not sent.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/withings.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="withings",
    display_name="Withings Health Mate",
    positioning="Consumer health-device brand (scales, BP monitors, watches) with an OAuth2 API",
    target_customers="Apps serving Withings device users",
    data_source_coverage="Withings scales, blood-pressure monitors, sleep mats, watches",
    integration_method="REST API (OAuth2, form-encoded actions) + notify webhooks",
    compliance_summary="\"User-consented OAuth scopes",
    differentiator="Strong body-measure / blood-pressure / sleep coverage from medical-grade home devices",
    docs_url="https://developer.withings.com/api-reference/",
    region=Region.EU,
    open_source=False,
    compliance=("GDPR",),
    domains=(DataDomain.ACTIVITY, DataDomain.HEART_RATE, DataDomain.SLEEP, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST, Integration.WEBHOOK,),
    category="device",
    status=VendorStatus.METADATA,
)


class WithingsVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

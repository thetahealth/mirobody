"""WHOOP — direct client for the WHOOP band, which has a free, self-serve public
developer API. A direct integration for deployments (or individual developers who
own a WHOOP — a device + membership is required to use the platform at all) that
want WHOOP data without an aggregator. See src/health/README.md.

WHOOP Developer Platform (https://developer.whoop.com/):
* base_url defaults to the documented host https://api.prod.whoop.com
(override via WHOOP_BASE_URL).
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header on
fetch(); the token is exchanged out of band at /oauth/oauth2/token.
* authorize_url() builds the authorization-code consent URL at
{base}/oauth/oauth2/auth from config.client_id.
* fetch() issues the documented collection GETs over a [start, end] ISO-8601
range (the API paginates via `nextToken`; this returns the first page — a
caller wanting more follows nextToken). WHOOP has no single "heart rate"
collection: HR/HRV live inside the Recovery record, so HeartRate maps there.

The API version is centralized in kApiVersion. WHOOP's v1 was deprecated in favor
of v2; the v2 collection paths (activity/sleep, recovery, cycle) are used here.
Reconcile against developer.whoop.com when onboarding.

revoke / list_providers / handle_webhook stay inherited stubs: WHOOP is a single
brand, documents no self-serve token-revoke endpoint, and its webhook signing
contract is a separate scheme not wired here.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/whoop.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="whoop",
    display_name="WHOOP",
    positioning="Recovery/strain band with a free, self-serve OAuth2 developer API",
    target_customers="Apps (and individual developers) serving WHOOP members",
    data_source_coverage="WHOOP band: recovery (HRV, resting HR), sleep, strain (cycles), workouts",
    integration_method="REST v2 Web API (OAuth2), cursor-paginated",
    compliance_summary="User-consented OAuth scopes",
    differentiator="Free self-serve developer platform (device + membership required to use)",
    docs_url="https://developer.whoop.com/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.SLEEP, DataDomain.HEART_RATE, DataDomain.ACTIVITY,),
    integrations=(Integration.REST,),
    category="device",
    status=VendorStatus.METADATA,
)


class WhoopVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

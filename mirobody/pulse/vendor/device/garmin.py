"""Garmin Health API — premium device brand. Garmin migrated the Health/Connect
APIs from OAuth 1.0a to OAuth 2.0 + PKCE, and the OAuth flow is public (the
"OAuth2.0 PKCE Specification" PDF on developerportal.garmin.com). The DATA side
is still partner-gated and push-based, which shapes what this client can do from
public docs:
1. Partner-gated data: the Health REST API spec (endpoints/payloads) needs
Garmin's program approval; there is no self-serve key, and Garmin retains
only ~7 days of data for pull. So fetch() explains the model rather than
faking a synchronous pull.
2. OAuth 2.0 + PKCE (PKCE mandatory): authorize at
https://connect.garmin.com/oauth2Confirm, token at
https://diauth.garmin.com/di-oauth2-service/oauth/token. The code_verifier
minted at authorize-time must be persisted and replayed at the token
exchange, but Vendor::authorize_url returns only a URL with nowhere to stash
it — so PKCE consent belongs at a stateful layer (cf. ehr_connect.cpp, which
caches the verifier+state), and authorize_url stays an inherited stub here.
3. Push delivery: Garmin POSTs summaries to a registered webhook; backfill
endpoints return 202 and trigger async push rather than returning data. The
pushes carry NO documented signature, so handle_webhook cannot verify
authenticity from public docs and stays an inherited stub.

revoke() IS wired: the public PKCE spec documents the required account-disconnect
hook, DELETE /wellness-api/rest/user/registration (OAuth2 Bearer). list_providers
is N/A (single brand). See https://developer.garmin.com/gc-developer-program/health-api/.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/garmin.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="garmin",
    display_name="Garmin Health",
    positioning="\"Premium wearable brand",
    target_customers="Approved partners serving Garmin device users",
    data_source_coverage="Garmin watches, bike computers, and health sensors",
    integration_method="\"OAuth2.0 + PKCE",
    compliance_summary="\"Partner-program approval",
    differentiator="Deep multisport / endurance metrics from premium devices",
    docs_url="https://developer.garmin.com/gc-developer-program/health-api/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.HEART_RATE, DataDomain.SLEEP, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST, Integration.WEBHOOK,),
    category="device",
    status=VendorStatus.METADATA,
)


class GarminVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

"""Rook Health — unified wearable data API. See README.md (Segment A).

Implemented against the public reference at https://docs.tryrook.io/api/ —
Rook splits into a Connect API (data extraction) and a Score API (analytics),
with REST + cross-platform SDK + webhook delivery. This client covers the
Connect side: the per-pillar processed-data summary reads (the data the
platform leads with), the consumer connection/authorize flow, and the HTTP
Basic auth Rook gates every REST call behind.

CONFIRMED from the public docs (https://docs.tryrook.io/api/):
* base URL        https://api.rook-connect.com   (sandbox host:
https://api.rook-connect.review)
* auth            HTTP Basic auth — client_uuid as the username, the
portal-issued secret key as the password, on every
protected REST endpoint
* data fetch      GET /v2/processed_data/<pillar>/summary with `user_id`
and `date` (YYYY-MM-DD) query params; pillars are
physical_health, sleep_health, body_health
* connect flow    /api/v1/client_uuid/<uuid>/user_id/<user>/data_sources/
authorizers?redirect_url=... presents the connections
page a user authorizes data sources through
* webhook         deliveries carry an HMAC signature in the `X-ROOK-Hash`
header (data-delivery guide)

INFERRED — the field-level reference is partially gated, so these are
best-effort and centralized below for easy reconciliation:
* the credential mapping onto Basic auth uses client_id=client_uuid and
client_secret=secret key; api_key, if set alone, is treated as a
pre-formed "uuid:secret" pair.
The exact HMAC computation behind X-ROOK-Hash (algorithm, key, encoding) is
documented only in a gated changelog/support guide, so handle_webhook is
intentionally left as an inherited stub rather than fabricating a signature
scheme we cannot confirm.

list_providers and revoke are also left as stubs — not for lack of docs, but
because the documented contracts do not fit the Vendor interface signatures
(verified against https://docs.tryrook.io/api/):
* list_providers — every "list data sources" endpoint is USER-scoped (needs
user_id in the path: GET /api/v2/user_id/{user_id}/data_sources/authorized
lists only what a user already authorized; the full-catalogue authorizers
endpoint is DEPRECATED). There is no parameterless provider catalogue to
back a list_providers() call, so it stays a stub.
* revoke — POST /api/v1/user_id/{user_id}/data_sources/revoke_auth revokes a
SINGLE data source (body {"data_source": ...}); there is no whole-user
disconnect, so revoke(user_id) can't be honored without iterating sources
(which needs the authorized-sources list above). Left a stub.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/rook.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="rook",
    display_name="Rook Health",
    positioning="Unified wearable data API",
    target_customers="Digital health, fitness apps, insurtech",
    data_source_coverage="Mainstream wearables (health/activity/sleep)",
    integration_method="REST API, cross-platform SDK, webhook",
    compliance_summary="Not publicly disclosed",
    differentiator="Modular architecture (Connect for data extraction + Score for analytics/scoring)",
    docs_url="https://docs.tryrook.io/docs/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE,),
    integrations=(Integration.REST, Integration.SDK, Integration.WEBHOOK,),
    category="platform",
    status=VendorStatus.METADATA,
)


class RookVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

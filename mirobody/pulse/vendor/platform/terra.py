"""Terra API — all-scenario unified fitness & health API. See README.md (Segment A).

Implemented against the public reference at https://docs.tryterra.co — 500+
devices, 5,000+ metrics, REST + multi-language SDK + real-time WebSocket
streams + webhooks. This client covers the three operations the platform leads
with: the Terra Connect widget session (consent flow), the per-resource
historical-data fetch endpoints, and inbound webhook signature verification.

CONFIRMED from the public docs (docs.tryterra.co):
* base URL        https://api.tryterra.co/v2
* auth            two headers on every call: `dev-id` and `x-api-key`
(plus Content-Type: application/json on POSTs)
* widget session  POST /v2/auth/generateWidgetSession with a JSON body of
{ reference_id, language, auth_success_redirect_url,
auth_failure_redirect_url }; response carries the widget
link in the "url" field (+ session_id, status, expires_in)
* data fetch      GET /v2/{resource} where resource is one of
activity / sleep / body / daily / menstruation / nutrition,
with query params user_id (required, Terra UUID),
start_date (required), end_date (optional), and
to_webhook=false to receive the data inline in the response
* webhooks        terra-signature header is "t=<unix>,v1=<hex>"; v1 is the
hex HMAC-SHA256 of the signed payload "<t>.<raw_body>"
keyed by the endpoint's signing secret. Verification uses a
constant-time compare of the recomputed digest.

CONFIG MAPPING (see VendorConfig): Terra splits its credentials across three
fields, which we map onto VendorConfig as:
* client_id     -> dev-id        (developer id, sent on every request)
* api_key       -> x-api-key     (API key, sent on every request)
* client_secret -> signing secret (webhook HMAC key; only handle_webhook
needs it — the dashboard's per-destination signing secret)
This mapping is an INFERRED local convention (the docs name the three secrets
dev-id / x-api-key / signing-secret; VendorConfig has no dedicated slot for the
last, so it rides on client_secret). The header *names* and the secrets they
carry are confirmed; only which VendorConfig field holds the signing secret is
our choice. list_providers and revoke are implemented against confirmed public
endpoints (docs.tryterra.co/reference):
* list_providers -> GET /v2/integrations — the provider/integration catalogue.
Per the docs this endpoint takes NO authentication; we send it without the
dev-id/x-api-key headers and return the JSON verbatim.
* revoke         -> DELETE /v2/auth/deauthenticateUser?user_id=<uuid> — keyed
by the Terra user_id (UUID), authenticated with the usual dev-id/x-api-key.
On success Terra deletes the user's records and emits a deauth webhook.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/terra.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="terra",
    display_name="Terra API",
    positioning="All-scenario unified fitness & health API",
    target_customers="Developers, clinical research, insurance, corporate wellness",
    data_source_coverage="500+ devices, 5,000+ metrics (incl. CGM, menstruation, blood)",
    integration_method="REST API, multi-language SDK, WebSocket, webhook",
    compliance_summary="HIPAA, GDPR, SOC 2 Type II",
    differentiator="\"Real-time WebSocket data streams",
    docs_url="https://tryterra.co",
    region=Region.GLOBAL,
    open_source=False,
    compliance=("HIPAA", "GDPR", "SOC2_TYPE_II",),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE, DataDomain.GLUCOSE, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST, Integration.SDK, Integration.WEBSOCKET, Integration.WEBHOOK,),
    category="platform",
    status=VendorStatus.METADATA,
)


class TerraVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

"""Spike API — health data gateway + AI nutrition. See README.md (Segment A).

Implemented against the public reference at https://docs.spikeapi.com — a
health-data gateway (activity, sleep, heart rate, lab reports) with a built-in
Nutrition AI (food-image + nutrition-label recognition) and webhook delivery.
This client covers the operations the platform leads with: the per-user data
queries (timeseries / sleeps / workouts / lab reports), the defining Nutrition
AI image+label endpoints, inbound webhook verification, and disconnect via the
provider-integration delete endpoint — all behind Spike's per-user JWT auth.

CONFIRMED from the public docs (docs.spikeapi.com/api-docs/* + api-reference/*):
* base URL          https://app-api.spikeapi.com/v3
* auth              POST /auth/hmac with {application_id, application_user_id,
signature}; signature = HMAC-SHA256 of application_user_id
under the console shared secret. Response carries an
access_token (JWT), sent as Authorization: Bearer on every
subsequent call. The JWT is *per end user*, so user_id maps
onto application_user_id at auth time.
* data fetch        GET /queries/timeseries?metric=&from_timestamp=&to_timestamp=
GET /queries/sleeps?from_date=&to_date=
GET /queries/workouts?from_timestamp=&to_timestamp=
GET /lab_reports?from_timestamp=&to_timestamp=
(timeseries/workouts/labs take UTC date-time bounds;
sleeps take local-date bounds — published per-endpoint)
* nutrition AI      POST /nutrition_records/image            {body|body_url,...}
POST /nutrition_records/ingredients/label {body|body_url,...}
* webhook           events POSTed as a JSON array; HMAC-SHA256 of the raw body
under the shared secret, delivered in the X-Body-Signature
header; endpoint acknowledges with HTTP 200.
* revoke            DELETE /providers/{provider_slug}/integration

INFERRED — Spike documents the signature as "HMAC-SHA256 of the body using the
shared secret" but does not pin down the wire ENCODING of that digest (hex vs
base64). We use lowercase hex (the codebase's storage::hmac_sha256 + hex_encode
chain, and the more common convention); it is centralized in kSigEncodeHex /
sign_hmac() below so it can be flipped to base64 in one place if the console's
emitted format differs. Everything else above is from the published reference.

authorize_url IS implemented: GET /providers/{provider}/integration/init_url
?redirect_uri=&state= -> { path } (docs.spikeapi.com/api-docs/provider_integration).
The provider slug is a required path segment, now carried by the interface's
`provider` argument; `user_id` selects whose per-user JWT signs the call.

list_providers stays a stub (verified, not a research gap): there is NO list
endpoint. The OpenAPI spec exposes only per-slug provider operations; the
supported-provider set is published only as a static documentation table (the
"provider matrix"), not a queryable REST resource.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/spike.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="spike",
    display_name="Spike API",
    positioning="Health data gateway + AI nutrition",
    target_customers="App developers, fitness apps, labs",
    data_source_coverage="Activity, sleep, heart rate, nutrition, lab reports",
    integration_method="REST API, mobile SDK, webhook",
    compliance_summary="Not publicly disclosed",
    differentiator="Built-in Nutrition AI (food-image and nutrition-label recognition)",
    docs_url="https://docs.spikeapi.com/overview",
    region=Region.GLOBAL,
    open_source=False,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE, DataDomain.NUTRITION, DataDomain.LABS,),
    integrations=(Integration.REST, Integration.SDK, Integration.WEBHOOK,),
    category="platform",
    status=VendorStatus.METADATA,
)


class SpikeVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

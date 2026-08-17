"""Metriport — open-source medical data interoperability platform. See README.md
(Strategic Option A).

Implemented against the public developer docs at https://docs.metriport.com
and the open-source TypeScript SDK (github.com/metriport/metriport,
packages/api-sdk). Metriport runs two products behind one host and one API
key, so this client routes by domain:
* Clinical            -> Medical API: consolidated FHIR R4 data for a patient.
* wearable domains     -> Devices API: per-day metric reads for a connected user.

CONFIRMED from packages/api-sdk/src/shared.ts and the medical/devices clients:
* production base       https://api.metriport.com           (BASE_ADDRESS)
* sandbox base          https://api.sandbox.metriport.com    (BASE_ADDRESS_SANDBOX)
selected via base_url override (Metriport is also
self-hostable, so base_url is meaningful either way).
* auth header           x-api-key: <key>                     (API_KEY_HEADER)
* Medical API path      BASE_PATH = "/medical/v1"
GET /medical/v1/patient/{id}/consolidated
?resources=<csv>&dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD
(the SDK's getPatientConsolidated() — returns the patient's consolidated
FHIR Bundle directly. The newer async POST .../consolidated/query +
webhook flow is left as the inherited stub; our fetch() contract is
synchronous request/response.)
* Devices API paths     GET /activity | /sleep | /biometrics | /body |
/nutrition  ?userId=<id>&date=YYYY-MM-DD
(the SDK's getActivityData/getSleepData/getBiometricsData/getBodyData/
getNutritionData — all take userId + a single date in YYYY-MM-DD.)

NOTHING here is inferred: every path, parameter name, header, and host below is
taken verbatim from the open-source SDK. Two contract mismatches are handled
honestly rather than papered over:
* Devices reads are single-day; our fetch() takes a [start,end] range. We
pass start_iso as the `date` and ignore end_iso (documented at the call
site) rather than invent an undocumented range parameter.
* Metriport dates are YYYY-MM-DD; ISO-8601 timestamps are truncated to their
date component before being sent.
authorize_url, handle_webhook, and revoke are implemented against the
open-source Devices API (server routes + packages/api-sdk):
* authorize_url  -> POST /user?appUserId=<user_id> then GET /user/connect/token,
returning the Connect Widget URL https://connect.metriport.com/?token=…
(user_id carries your appUserId; redirect_uri sets the success/failure
redirects; sandbox flag added when base_url is the sandbox host).
* handle_webhook -> verifies the `x-metriport-signature` header (lowercase-hex
HMAC-SHA256 over the raw body, keyed by your webhook key in
config.client_secret — api_key is taken by x-api-key). ping_pong() builds
the {"pong":…} answer to Metriport's ping handshake.
* revoke         -> DELETE /user/{userId} (deletes the connected user, revoking
all their providers; revoke_provider() drops a single provider via
DELETE /user/{userId}/revoke?provider=).
list_providers stays an inherited stub: the open source exposes NO supported-
provider CATALOGUE endpoint — only the user-scoped GET /user/{userId}/
connected-providers, which doesn't fit list_providers(). The supported set is
the ProviderSource enum (apple, cronometer, dexcom, fitbit, garmin, google,
oura, tenovi, whoop, withings).

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/metriport.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="metriport",
    display_name="Metriport",
    positioning="Open-source medical data interoperability platform",
    target_customers="Digital health startups, health-system developers",
    data_source_coverage="Major U.S. healthcare IT systems (EHR clinical data)",
    integration_method="Modern API, developer dashboard, FHIR R4 format",
    compliance_summary="HIPAA, open-source self-hosted compliance",
    differentiator="\"Open-source",
    docs_url="https://www.metriport.com",
    region=Region.US,
    open_source=True,
    compliance=("HIPAA",),
    domains=(DataDomain.CLINICAL,),
    integrations=(Integration.REST, Integration.FHIR,),
    category="platform",
    status=VendorStatus.METADATA,
)


class MetriportVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

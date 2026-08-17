"""Vitalera — next-generation health data API platform. See README.md (Segment B).

Implemented against the public reference at https://docs-v2.vitalera.io — a
FHIR-R5 health-data platform (500+ wearables + medical devices, ECG, blood
glucose) certified as medical software (SaMD). This client covers the two
operations the platform leads with: the native device-fetch path (per-metric
REST list endpoints) and FHIR output (FHIR R5 Observation), plus the JWT auth
it gates every call behind, and disconnect via the auth deactivate endpoint.

CONFIRMED from the public docs (docs-v2.vitalera.io/platform-api/authentication):
* base URL            https://api.vitalera.io/api/
* auth                OAuth 2.0 + JWT Bearer in the Authorization header.
Tokens minted at POST /api/auth/tokens/ with a body of
{ grant_type: "client_credentials", client_id,
client_secret }; the response carries the token in the
"access_token" field. Tokens last 3600s; refresh at
/api/auth/tokens/refresh/, validate at
/api/auth/tokens/validate/.
* device-fetch path   GET /<metric>/ list endpoints (heart-rate, blood-glucose,
blood-pressure, oxygen-saturation, temperature, step-count,
calories, workouts), DRF pagination (count/next/previous)
* FHIR                FHIR R5 resources (Patient, Observation, …)
* list_providers      GET /api/connected-accounts/services/ — the provider
catalogue (entitlement-filtered to the caller's org).
Method + path are public; the response shape is in the
gated reference, so we return the JSON verbatim.
* handle_webhook      outbound webhooks POST a JSON envelope
{ event_type, timestamp, organization_id, data }; the
signature is in the `x-webhook-signature` header as
lowercase-hex HMAC-SHA256 over the RAW request body,
keyed by the webhook ENDPOINT secret. The body's
`timestamp` is NOT part of the signed string. This whole
scheme is public (docs-v2.vitalera.io/webhooks/overview).
The endpoint secret is a DIFFERENT credential from the
OAuth client_secret; since VendorConfig has one slot, it
rides on config.client_secret — so a deployment that also
mints tokens via client-credentials should set api_key to
a pre-issued bearer (the fetch path falls back to it when
client_id is empty), leaving client_secret for the webhook.

authorize_url stays a stub: the connect flow is documented at the path level
(POST /api/connected-accounts/connect-session/ then a hosted GET
/api/connected-accounts/connect/?token=…, or per-provider POST
/api/connected-accounts/{service_id}/oauth/initiate/), but whether it accepts a
redirect_uri/state and what URL field it returns live in the gated field-level
reference — so a faithful authorize_url(redirect_uri, state) can't be built from
public docs yet.

INFERRED — the field-level reference is gated (Vitalera issues it after sign-up
via info@vitalera.io), so these names are best-effort and centralized in the
constants below for easy reconciliation against that reference:
* the native list endpoints' patient + date-range query parameters,
* the FHIR base path segment ("fhir/"),
* the token-deactivation path used by revoke (the auth family is confirmed
to expose a deactivate op, but its exact route is not public).
The FHIR *search* semantics (patient=, date=ge.../date=le...) are the R5
standard, not a guess.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/vitalera.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="vitalera",
    display_name="Vitalera",
    positioning="Next-generation health data API platform",
    target_customers="Healthcare providers, RPM developers, digital health",
    data_source_coverage="500+ wearable and medical devices (incl. ECG, blood glucose)",
    integration_method="REST API, automatic code generator, FHIR connectivity",
    compliance_summary="HIPAA, GDPR, ISO 27001, SaMD",
    differentiator="Auto-generates integration code, supports FHIR, certified as medical software",
    docs_url="https://www.vitalera.io",
    region=Region.GLOBAL,
    open_source=False,
    compliance=("HIPAA", "GDPR", "ISO_27001", "SaMD",),
    domains=(DataDomain.HEART_RATE, DataDomain.GLUCOSE, DataDomain.CLINICAL,),
    integrations=(Integration.REST, Integration.FHIR,),
    category="platform",
    status=VendorStatus.METADATA,
)


class VitaleraVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

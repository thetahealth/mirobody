"""Redox — healthcare interoperability platform. See README.md (Segment C).

Implemented against the public reference at https://docs.redoxengine.com — a
deep-EHR interoperability platform that exposes a FHIR R4 API plus proprietary
real-time message streams over HL7/FHIR. This client implements the STANDARD
path Redox publishes: FHIR R4 reads against the deployer's destination, gated
behind the OAuth2 bearer Redox issues.

CONFIRMED from the public docs:
* auth token endpoint POST https://api.redoxengine.com/v2/auth/token, body
grant_type=client_credentials &
client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer &
client_assertion=<signed JWT>; response JSON carries the
bearer in "access_token" (+ token_type / expires_in,
5-minute lifetime). This is SMART Backend Services Auth.
* bearer use          Authorization: Bearer <access_token> on every API call.
* FHIR R4 base        https://api.redoxengine.com/fhir/R4/{destination-slug}/{environment}
e.g. .../redox-fhir-sandbox/Development/Patient/123
* FHIR search         patient= and the date= range (ge/le prefixes) are the
FHIR R4 standard search parameters, not a guess.

AUTH HANDLING — the Redox token flow requires a JWT signed with the partner's
PRIVATE KEY (referenced by `kid`, published via a JWKS URL). VendorConfig holds
no private key or signing material, so minting that assertion cannot be done
here. Following vitalera/healthconnect's honest approach, we accept a
pre-obtained bearer access token via config.api_key; acquiring it (signing the
client assertion and POSTing it to /v2/auth/token) runs out of band. We do NOT
attempt the token POST with the plain client_id/client_secret in VendorConfig:
Redox does not accept a client_secret grant, so that would only ever fail.

BASE URL — there is no usable public default: the FHIR base embeds the
deployer's destination-slug and environment, which only the deployer knows. So
base_url is REQUIRED (the full FHIR R4 base, through the environment segment),
and we refuse rather than fabricate one.

The proprietary real-time message-stream API (Redox's defining operation) and
the consent flow stay inherited stubs: those contracts are not cleanly
expressible from VendorConfig, and fabricating them would be worse than an
honest "not implemented".

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/redox.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="redox",
    display_name="Redox",
    positioning="Healthcare interoperability platform",
    target_customers="Healthcare IT architects, integration engineers, hospitals",
    data_source_coverage="Deep EHR systems (HL7, FHIR standard data)",
    integration_method="API-driven, real-time message streams, standardized data exchange",
    compliance_summary="HIPAA, SOC 2 Type II, HITRUST",
    differentiator="High-speed, high-reliability data exchange between EHR systems and cloud apps",
    docs_url="https://www.redoxengine.com",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA", "SOC2_TYPE_II", "HITRUST",),
    domains=(DataDomain.CLINICAL,),
    integrations=(Integration.REST, Integration.HL7, Integration.FHIR,),
    category="platform",
    status=VendorStatus.METADATA,
)


class RedoxVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

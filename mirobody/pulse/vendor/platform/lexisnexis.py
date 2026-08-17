"""LexisNexis Health Intelligence EHR — life-insurance underwriting EHR
intelligence. See README.md (Segment C).

LexisNexis® Health Intelligence (risk.lexisnexis.com, formerly Human API
Health Intelligence) is a contract-gated enterprise underwriting platform —
NOT a self-serve public API. As of the 2026 report it publishes no developer
reference for this product: the product pages are marketing-only and route
every integration question to sales ("Talk to an Expert"). There is no
publicly documented base URL, no proprietary endpoint paths, no auth spec,
and the pages do not even commit to a wire standard (FHIR/QHIN are described
as capabilities, not as a documented contract). Access — base URL,
credentials, the exact request shape — is handed to a carrier under a signed
agreement. See https://risk.lexisnexis.com/products/health-intelligence-ehr.

So rather than invent a proprietary contract, this client refuses to guess:
* base_url is REQUIRED and supplied by the deployer (the contracted
LexisNexis Health Intelligence endpoint) — there is no public host to
default to, so we refuse rather than fabricate one.
* auth is a bearer credential (config.api_key) sent in the Authorization
header; acquisition runs out of band against the contracted auth server,
which is not publicly documented.
* fetch() does NOT invent endpoint paths. It issues a deployer-driven
request: the caller supplies the resource path their contract documents
via start_iso (overloaded here as the path/query override — see fetch()),
and we attach the bearer and pass through the response verbatim. With no
path supplied there is nothing safe to call, so it errors with guidance.
The consumer-mediated consent flow (QHIN authorize), the APS-handoff /
Medical Insights extraction, webhooks, and revoke stay honest "not
implemented" stubs: those contracts are gated and undocumented, and
fabricating them would be worse than an honest stub.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/lexisnexis.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="lexisnexis",
    display_name="LexisNexis EHR",
    positioning="Life-insurance underwriting EHR intelligence platform",
    target_customers="Life insurance carriers, distributors",
    data_source_coverage="\"30,000+ U.S. data sources",
    integration_method="API access with QHINs consumer-mediated consent",
    compliance_summary="HIPAA authorized network",
    differentiator="\"Medical Insights underwriting-attribute extraction",
    docs_url="https://risk.lexisnexis.com",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA", "QHIN",),
    domains=(DataDomain.CLINICAL, DataDomain.LABS, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST,),
    category="platform",
    status=VendorStatus.METADATA,
)


class LexisnexisVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

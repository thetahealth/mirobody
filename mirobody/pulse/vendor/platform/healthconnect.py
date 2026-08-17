"""HealthConnect (CoPilot) — dual-track EHR + wearable interoperability platform.
See README.md (supplementary competitor).

HealthConnect CoPilot (by Mindbowser) is a B2B interoperability *accelerator*,
not a self-serve public API: as of the 2026 report it publishes no developer
reference — no base URL, no proprietary endpoint paths, no auth spec. Its pages
confirm only the standards it speaks: OAuth2 bearer auth and FHIR (it stores
metrics as "FHIR-compliant Observation resources" and bridges Epic/Cerner via
HL7 V2 + FHIR). See https://www.mindbowser.com/healthconnect-copilot/.

So rather than invent a proprietary contract, this client implements the
STANDARD path the product exposes: FHIR R4 reads against the deployer's tenant.
* base_url is REQUIRED and supplied by the deployer (the HealthConnect/FHIR
tenant endpoint) — there is no public host to default to, so we refuse
rather than guess one.
* auth is an OAuth2 bearer access token (config.api_key); token acquisition
runs out of band against the tenant's authorization server, which is not
publicly documented.
* fetch() issues standard FHIR R4 `Observation` searches — `patient`,
`category`, and the `date` range are published FHIR search parameters, not
guesses.
The dual-track real-time sync (its defining feature, i.e. FHIR Subscription /
webhooks) and the consent flow stay stubs: those contracts are not documented,
and fabricating them would be worse than an honest "not implemented".

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/healthconnect.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="healthconnect",
    display_name="HealthConnect CoPilot",
    positioning="Dual-track EHR + wearable interoperability platform",
    target_customers="Healthcare providers, RPM developers, AI decision systems",
    data_source_coverage="Epic/Cerner EHR + mainstream wearables",
    integration_method="HL7/FHIR standard API, real-time data sync",
    compliance_summary="HIPAA, medical-grade security",
    differentiator="Dual-track real-time sync purpose-built for RPM and AI clinical decision support",
    docs_url="https://www.mindbowser.com/healthconnect-copilot/",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA",),
    domains=(DataDomain.CLINICAL, DataDomain.ACTIVITY, DataDomain.HEART_RATE,),
    integrations=(Integration.REST, Integration.HL7, Integration.FHIR,),
    category="platform",
    status=VendorStatus.METADATA,
)


class HealthconnectVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

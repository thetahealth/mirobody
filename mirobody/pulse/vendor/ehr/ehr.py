"""Generic EHR client — one SMART-on-FHIR reader for every ONC-certified EHR
(Epic, Oracle Health/Cerner, athenahealth, MEDITECH, Veradigm, …). Because the
US ONC Cures Act forces these systems onto the SMART App Launch + FHIR R4
standard, a single client parameterized by the tenant's base_url covers them
all — the same way s3() with an endpoint override covers every S3-compatible
store, rather than a near-identical file per vendor.

* base_url is REQUIRED and per-tenant: each hospital/clinic has its own FHIR
service base URL (discoverable from the public Service Base URL directories
— see directory.hpp). There is no single host to default to, so we refuse
rather than guess one.
* auth is a SMART-on-FHIR OAuth2 access token (config.api_key) sent as a
Bearer header; the authorize/token dance is per-tenant (discovered from
{base}/.well-known/smart-configuration) and runs out of band, which is why
authorize_url stays a stub rather than a half-built flow.
* fetch() issues standard FHIR R4 `Observation` searches — `patient`,
`category`, and the `date` range are published FHIR search parameters.
See the SMART App Launch spec at https://hl7.org/fhir/smart-app-launch/.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/ehr/ehr.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="ehr",
    display_name="EHR (SMART on FHIR)",
    positioning="Direct EHR-system access via SMART on FHIR (Epic, Oracle Health/Cerner, athenahealth, …)",
    target_customers="Apps integrating directly with a patient's hospital/clinic EHR",
    data_source_coverage="Any ONC-certified EHR exposing a SMART on FHIR R4 API",
    integration_method="\"SMART on FHIR (OAuth2 + FHIR R4)",
    compliance_summary="\"HIPAA",
    differentiator="One client for every certified EHR — tenant base_url is discovered from public directories (see ehr/directory.hpp)",
    docs_url="https://hl7.org/fhir/smart-app-launch/",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA",),
    domains=(DataDomain.CLINICAL, DataDomain.LABS, DataDomain.GLUCOSE, DataDomain.HEART_RATE, DataDomain.BODY_METRICS, DataDomain.ACTIVITY,),
    integrations=(Integration.REST, Integration.FHIR,),
    category="ehr",
    status=VendorStatus.METADATA,
)


class EhrVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

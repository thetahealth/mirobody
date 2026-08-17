"""Particle Health — API-driven clinical data platform. See README.md (Segment C).

Implemented against the public reference at https://docs.particlehealth.com — a
query-based clinical-data network that retrieves longitudinal patient records
from nationwide healthcare networks and returns them as FHIR R4. This client
covers the documented Main Query Flow end-to-end: mint a JWT, register the
patient, fire a one-time query, poll it to COMPLETE, then read the FHIR R4
Patient $everything bundle.

CONFIRMED from the public docs (docs.particlehealth.com):
* base host          https://api.particlehealth.com
* auth               GET /auth mints a 60-minute JWT from a Client ID/Secret;
every other call carries it as "Authorization: Bearer <jwt>"
* register patient   POST /api/v2/patients  -> a particle_patient_id (PPID)
* fire query         POST /api/v2/patients/{ppid}/query  (one-time network search)
* query status       GET  /api/v2/patients/{ppid}/query  (poll until COMPLETE)
* FHIR R4 retrieval   GET /api/v2/patients/{ppid}/r4/Patient/{ppid}/$everything
(Patient $everything — all resources in one Bundle; $everything,
the _since/_count paging are FHIR R4 standard, not a guess)

INFERRED — the request-level reference (auth header spelling, the JWT response
envelope, the exact FHIR sub-path under the patient resource) is gated behind a
signed-in docs portal / a Client ID issued by a Particle representative, so the
few names below are best-effort and centralized in the constants block for easy
reconciliation against that reference once credentials are in hand.

SCOPE: fetch(DataDomain::Clinical) implements the final retrieval leg — it
reads the FHIR R4 Patient $everything bundle for a particle_patient_id whose
one-time query has already reached COMPLETE. The two pre-fetch legs Particle
documents — register the patient (POST /api/v2/patients) and fire + poll the
network query (POST/GET /api/v2/patients/{ppid}/query) — are intentionally left
to the caller for now: they take a caller-built demographics body and a polling
loop that don't fit the value-returning fetch(user_id, domain, …) signature.
Their confirmed paths are recorded above so they slot in cleanly later.

The proprietary ADT (admit/discharge/transfer) event stream — Particle's other
headline feature — is an asynchronous push contract that is not publicly
documented at the field level; rather than invent a webhook signature/envelope,
handle_webhook stays the inherited "not implemented" stub.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/particle_health.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="particle_health",
    display_name="Particle Health",
    positioning="API-driven clinical data platform",
    target_customers="Health systems, payers, value-based care teams",
    data_source_coverage="Longitudinal patient clinical records from nationwide healthcare networks",
    integration_method="API, supports ADT event streams",
    compliance_summary="HIPAA, SOC 2 Type II",
    differentiator="Aggregates, de-duplicates, and standardizes longitudinal patient clinical records",
    docs_url="https://www.particlehealth.com",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA", "SOC2_TYPE_II",),
    domains=(DataDomain.CLINICAL,),
    integrations=(Integration.REST,),
    category="platform",
    status=VendorStatus.METADATA,
)


class ParticleHealthVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

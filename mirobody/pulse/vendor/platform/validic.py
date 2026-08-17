"""Validic — enterprise PGHD infrastructure. See README.md (Segment B).

Implemented against the public Validic Inform REST API reference at
https://developer.validic.com (formerly helpdocs.validic.com). Inform is the
current platform; the legacy V1 API (docs.validic.com) is distinct and not
targeted here. This client covers the two operations the REST API leads with:
provisioning an organization-scoped user (the device-connect prerequisite) and
fetching a user's per-metric data, plus the organization access-token auth that
gates every call.

CONFIRMED from the public Inform REST API docs:
* base URL   https://api.v2.validic.com
* auth       token-based: the Organization ID is a path segment and the
Organization Access Token is the `token` query parameter on
every request (HTTPS only).
* provision  POST /organizations/:org_id/users?token=:token  with body
{"uid": "<your_user_id>"} — returns id, uid, marketplace info
(incl. the per-user access/marketplace token + connect URL),
and created_at. Provision when a user is ready to connect.
* fetch      GET /organizations/:org_id/users/:uid/:object_type
?token=:token&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
object_type ∈ {measurements, intraday, cgm, nutrition, sleep,
summaries, workouts}. 30-day max window per request.

Config mapping: api_key = Organization Access Token (the `token` param);
client_id = Organization ID (the :org_id path segment). base_url defaults to
the confirmed public host.

NOT IMPLEMENTED (left as inherited stubs — contracts not in the public
reference, so fabricating them would be worse than an honest "not implemented"):
* the EHR write-back into Epic/Cerner — Validic's headline moat, but no
public POST/PUT write-back endpoint is documented;
* authorize_url / list_providers — the consumer marketplace connect URL is
returned by provisioning, not via a documented authorize endpoint;
* handle_webhook (Inform Streaming API) and revoke (no documented user-delete
endpoint in the public reference).

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/validic.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="validic",
    display_name="Validic",
    positioning="Enterprise PGHD infrastructure",
    target_customers="Health systems, health plans/insurers, wellness",
    data_source_coverage="700+ health devices (BP cuffs, pulse oximeters, and other PGHD)",
    integration_method="REST API, iOS/Android SDK, direct EHR write-back",
    compliance_summary="HIPAA, SOC 2 Type II",
    differentiator="\"Deep integration with EHRs like Epic/Cerner",
    docs_url="https://developer.validic.com",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA", "SOC2_TYPE_II",),
    domains=(DataDomain.BODY_METRICS, DataDomain.HEART_RATE, DataDomain.GLUCOSE, DataDomain.CLINICAL,),
    integrations=(Integration.REST, Integration.SDK,),
    category="platform",
    status=VendorStatus.METADATA,
)


class ValidicVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

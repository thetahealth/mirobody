"""Human API — consumer-controlled health data aggregator. See README.md
(Segment C).

Implemented against the public reference at https://reference.humanapi.co — a
consumer-mediated aggregator that brokers wearable/"wellness" data plus
clinical EHR ("Medical") records from a large share of U.S. hospitals, behind
a unified RESTful Data API and the Human Connect authorization widget.
(Human API was acquired by LexisNexis; the v2.x developer portal layers an
order-management "Health Intelligence Platform" on top of the classic Data
API, but the v1 Data API documented below is the consumer-data path this
client targets.)

CONFIRMED from the public docs:
* Data API base       https://api.humanapi.co/v1/human
* auth                user access token as "Authorization: Bearer <token>";
the token is config.api_key here. It is minted out of
band by exchanging a Human Connect sessionTokenObject
(+ client_secret) at POST https://user.humanapi.co/
v1/connect/tokens, which returns {humanId, accessToken,
publicToken}. That exchange needs the browser-side
widget's session object, so it is not reproduced here.
* wellness endpoints  GET /activities, /sleeps, /heart_rate, /blood_glucose,
/weight, /bmi, /blood_pressure under the base above
* time filtering      since / until (ISO-8601), plus updated_since for
incremental sync (Patterns & Conventions reference)
* Human Connect       a CLIENT-SIDE JavaScript popup (connect.humanapi.co/
connect.js, HumanConnect.open({clientId, clientUserId,
publicToken})) — there is NO server-issued hosted
redirect/authorize URL, so authorize_url() cannot honor
the redirect_uri/state contract and stays a stub.

INFERRED — the field-level Medical (Clinical) API reference is gated behind the
developer portal post-acquisition, so the clinical resource path below is a
best-effort placeholder centralized in the constants block; the wellness paths
and the Bearer/since/until conventions above are documented, not guessed.

STUBS (honest "not implemented", like healthconnect.cpp): authorize_url (no
server-side redirect contract — widget is client-side JS), list_providers,
handle_webhook, and revoke — none have a publicly documented contract for the
v1 Data API, and fabricating one would be worse than reporting "not implemented".

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/human_api.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="human_api",
    display_name="Human API",
    positioning="Consumer-controlled health data aggregator",
    target_customers="Insurers, digital health, clinical research",
    data_source_coverage="EHR from 90% of U.S. hospitals + 300+ wearables",
    integration_method="Unified RESTful API, consumer-authorization widget",
    compliance_summary="HIPAA, SOC 2",
    differentiator="\"Dual-track aggregation of clinical EHR and wearable data",
    docs_url="https://www.humanapi.co",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA", "SOC2",),
    domains=(DataDomain.CLINICAL, DataDomain.ACTIVITY,),
    integrations=(Integration.REST,),
    category="platform",
    status=VendorStatus.METADATA,
)


class HumanApiVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

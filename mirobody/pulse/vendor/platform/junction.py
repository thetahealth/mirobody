"""Junction — diagnostic data integration platform. See README.md (Segment B).

Junction (formerly "Vital" / tryvital) brokers data from 300+ wearables AND a
nationwide U.S. lab-testing network through a single API, multi-language SDK,
and a white-label embed ("Link"). Implemented against the public reference at
https://docs.junction.com (the legacy https://docs.tryvital.io host 301s here).

This client covers the operations the platform leads with: fetching wearable
summaries (activity/sleep/heart-rate/body metrics) and lab orders/results, and
listing the providers a user can connect.

CONFIRMED from the public docs:
* base URLs        prod US   https://api.us.junction.com/
prod EU   https://api.eu.junction.com/
sandbox   https://api.sandbox.us.junction.com/  (and .eu)
Legacy *.tryvital.io hosts remain supported.
* auth             Team API Key in the `X-Vital-API-Key` header (server to
server). Key prefixes pk_us_*/pk_eu_* (prod), sk_us_*/sk_eu_*
(sandbox) — the prefix, not the host, picks the environment,
so override base_url to match the key you provision.
* wearable fetch   GET /v2/summary/<resource>/<user_id>?start_date=&end_date=
(resources: activity, sleep, body, sleep_stream, …); dates
accept YYYY-MM-DD or ISO datetimes.
* providers        GET /v2/providers
* lab orders       GET /v3/orders?user_id=&start_date=&end_date= (Labs domain)
GET /v3/order/<order_id>/result (parsed JSON + PDF metadata)
* lab catalogue    GET /v3/lab_tests/labs
* link / connect   POST /v2/user            (body {"client_user_id": ...})
POST /v2/link/token      (body {"user_id": ..., "provider"?})
-> { link_token, link_web_url }
The hosted widget is launched at the returned link_web_url.
* webhooks         delivered via Svix. Each request carries svix-id,
svix-timestamp and svix-signature headers; the signature is
base64(HMAC-SHA256("<svix-id>.<svix-timestamp>.<raw_body>"))
keyed by the base64-decoded portion of the endpoint signing
secret (the part after the "whsec_" prefix). svix-signature
is a space-separated list of "v1,<sig>" entries; a match on
any entry (constant-time) verifies. (docs.junction.com/
webhooks/introduction + docs.svix.com verification scheme.)

authorize_url() mints a Link Token for an existing Junction user_id (carried by
the interface's user_id arg) and returns the hosted-widget link_web_url; an
optional provider pre-selects a data source. Create the user first via
create_user() (its own step). redirect_uri / state have no slot in the Link
Token request (the landing is configured Junction-side), so they are ignored.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/junction.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="junction",
    display_name="Junction",
    positioning="Diagnostic data integration platform",
    target_customers="Virtual clinics, digital health, healthcare SaaS",
    data_source_coverage="300+ wearables + nationwide U.S. lab testing network",
    integration_method="Single API, multi-language SDK, white-label embed",
    compliance_summary="U.S. healthcare compliance",
    differentiator="Integrated wearables and lab testing, no test markup, supports at-home blood draws",
    docs_url="https://www.junction.com",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA",),
    domains=(DataDomain.ACTIVITY, DataDomain.LABS,),
    integrations=(Integration.REST, Integration.SDK, Integration.WHITE_LABEL,),
    category="platform",
    status=VendorStatus.METADATA,
)


class JunctionVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

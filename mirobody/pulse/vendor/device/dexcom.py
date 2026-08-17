"""Dexcom — direct client for the Dexcom CGM (continuous glucose monitor) brand.
Dexcom sensors (G6, G7, One+, Stelo) speak a proprietary encrypted BLE protocol
to the official Dexcom app; that app uploads to Dexcom's cloud, and THIS client
reads from that cloud over the public Dexcom v3 Web API. So the reachable data
is what Dexcom's cloud serves — not a device/BLE path (a third party cannot read
the sensor over Bluetooth). See src/health/README.md.

Dexcom v3 Web API (https://developer.dexcom.com/):
* base_url defaults to the SANDBOX host https://sandbox-api.dexcom.com — the
free, immediate, fake-data environment every registered developer gets, so a
fresh deploy touches no real PHI. Point at production via DEXCOM_ENVIRONMENT
(us => https://api.dexcom.com, eu/ous => https://api.dexcom.eu) or an explicit
DEXCOM_BASE_URL. See src/health/vendor_link.cpp for the mapping.
* auth is an OAuth 2.0 access token (config.api_key) as a Bearer header on
fetch(); the token is exchanged out of band at /v2/oauth2/token.
* authorize_url() builds the standard authorization-code consent URL at
{base}/v2/oauth2/login from config.client_id (scope offline_access; a
confidential server client authenticates with its secret at the token step).
* fetch() reads Estimated Glucose Values: GET /v3/users/self/egvs over a
[startDate, endDate] window. The API is user-scoped by the token ("self"),
so the user_id argument is unused (data belongs to the token holder). Note
the API is RETROSPECTIVE: EGVs are delayed ~1h (US) / ~3h (outside US) by
regulatory design — there is no real-time push here.

Left as inherited stubs, deliberately (see vendor.hpp):
* revoke          — Dexcom documents no token-revocation endpoint on the
partner API; revoking is done in the user's Dexcom account.
* list_providers  — Dexcom is a single brand, not a provider aggregator, so
there is no provider catalogue to list.
* handle_webhook  — the standard partner API delivers no signed webhook, so
there is no signature scheme to verify faithfully.

Access tiers (all start free): Sandbox is immediate; production Limited Access
(<=5 real users) needs only app registration; serving more real users requires a
Dexcom commercial partnership + Data Licensing Agreement. See config.example.yml.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/device/dexcom.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="dexcom",
    display_name="Dexcom",
    positioning="CGM brand with a public OAuth2 cloud Web API (retrospective glucose)",
    target_customers="Apps serving Dexcom CGM users (diabetes management, RPM)",
    data_source_coverage="Dexcom CGM: G6, G7/One+, Stelo — estimated glucose values, events, devices",
    integration_method="\"REST v3 Web API (OAuth2)",
    compliance_summary="\"User-consented OAuth",
    differentiator="Direct Dexcom cloud API — free sandbox + <=5-user production without a partnership",
    docs_url="https://developer.dexcom.com/",
    region=Region.US,
    open_source=False,
    compliance=("HIPAA",),
    domains=(DataDomain.GLUCOSE,),
    integrations=(Integration.REST,),
    category="device",
    status=VendorStatus.METADATA,
)


class DexcomVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

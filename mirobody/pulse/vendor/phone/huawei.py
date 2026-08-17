"""Huawei Health Kit — the one device-native health platform with a server-side
cloud REST API (Apple HealthKit, Google Health Connect, and Xiaomi/Mi Fitness
are on-device only; their data reaches mirobody by being read on the phone and
POSTed to the FHIR endpoint, not fetched here — see src/health/README.md).

Huawei exposes two surfaces: an on-device Health Kit SDK (Android/HarmonyOS)
and a Health Kit *Cloud* REST API a server can call with a user's OAuth token.
This client implements the cloud REST read path:
* base_url defaults to the documented host https://health-api.cloud.huawei.com
(override via MIROBODY_VENDOR_HUAWEI_BASE_URL for a regional endpoint).
* auth is a Huawei Account Kit OAuth 2.0 access token (config.api_key) carrying
Health Kit scopes; token acquisition / refresh runs out of band against
Huawei's authorization server (oauth-login.cloud.huawei.com), which is why
authorize_url / revoke stay stubs rather than half-built flows.
* fetch() issues the documented `sampleSet:polymerize` query, which aggregates
a data type's sample points over [startTime, endTime] (epoch ms) into daily
buckets. DataDomains map onto Huawei's `com.huawei.*` atomic data types.
The consent OAuth flow and the subscription/webhook push are not implemented:
those contracts need the deployer's client_id + registered scopes, so they
throw "not implemented" rather than fabricate a flow. See the Health Kit REST
API reference at https://developer.huawei.com/consumer/en/hms/huawei-healthkit/.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/phone/huawei.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="huawei",
    display_name="Huawei Health Kit",
    positioning="Device-native wearable platform with a server-side cloud REST API",
    target_customers="Apps serving Huawei / Honor device users",
    data_source_coverage="Huawei Health app: Huawei/Honor wearables and phone sensors",
    integration_method="Health Kit Cloud REST API (OAuth 2.0 via Huawei Account Kit) + on-device Health Kit SDK",
    compliance_summary="\"User-consented OAuth scopes",
    differentiator="The one major device-native platform offering a server-to-server cloud REST API (Apple/Xiaomi are on-device only)",
    docs_url="https://developer.huawei.com/consumer/en/hms/huawei-healthkit/",
    region=Region.GLOBAL,
    open_source=False,
    compliance=("GDPR",),
    domains=(DataDomain.ACTIVITY, DataDomain.HEART_RATE, DataDomain.SLEEP, DataDomain.GLUCOSE, DataDomain.BODY_METRICS,),
    integrations=(Integration.REST, Integration.SDK,),
    category="phone",
    status=VendorStatus.METADATA,
)


class HuaweiVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

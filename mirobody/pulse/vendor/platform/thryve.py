"""Thryve — European medical-grade wearable API. See README.md (Segment C, EU).

Implemented against the public reference at https://docs.thryve.health — a
GDPR-first, EU-hosted wearable aggregation API (500+ devices, 250+ metrics)
exposing a "unified data format" over two timeseries endpoints: daily
aggregates and intraday ("epoch") values. This client covers the lifecycle a
server needs: create an end user (the access token that *is* the user id),
hand off to the hosted connection widget for consent, fetch unified daily /
epoch data, and delete the user on revoke.

CONFIRMED from the public docs (https://docs.thryve.health):
* base host    https://api.thryve.de  (EU-hosted, per the GDPR positioning)
* auth         TWO HTTP Basic headers on every request —
Authorization:    Basic base64(username:password)
AppAuthorization: Basic base64(authID:authSecret)
We map client_id:client_secret -> Authorization and
api_key (formatted "authID:authSecret") -> AppAuthorization.
* create user  POST /v5/accessToken  -> plain-text access token == endUserId
* connect      POST /widget/v6/connection  (JSON {endUserId, locale}) ->
hosted Connection Widget URL to redirect the user to
* daily data   POST /v5/dailyDynamicValues  (x-www-form-urlencoded;
authenticationToken + startDay/endDay or *Unix range)
* epoch data   POST /v5/dynamicEpochValues  (x-www-form-urlencoded;
authenticationToken + startTimestamp/endTimestamp range)
* revoke       DELETE /v5/userInformation  (deletes the Thryve user)
Request bodies are application/x-www-form-urlencoded and the documented form
keys (authenticationToken, startDay/endDay, startTimestamp/endTimestamp,
dataSources, valueTypes) are taken verbatim from the reference.

INFERRED — the numeric Thryve DataType IDs used to filter by metric are only
partially published, so per-domain valueTypes filtering is centralized in the
constants below and applied only where the IDs are confirmed; for domains
without a confirmed ID (e.g. glucose) we omit the filter and return the full
unified payload for the window rather than guess an ID. The credential->header
split (client pair -> Authorization, api_key -> AppAuthorization) is the one
mapping choice not spelled out by the docs and is flagged here too.

STUBS (verified against the public reference, not gaps in research):
* list_providers — there is NO "list data sources" endpoint. The v5 reference
enumerates only user/data endpoints; connecting a source is delegated to the
hosted Connection Widget (POST /widget/v6/connection), which renders an
iframe, not a machine-readable provider catalogue. Left a stub.
* handle_webhook — the push PAYLOAD is documented (POST JSON, zstd-encoded,
event types event.data.{epoch,daily}.{create,update}), but the SIGNATURE
scheme is NOT public: the docs only say "an HMAC secret can be configured"
without naming the header, the HMAC variant, or what is signed (raw zstd
bytes vs decompressed JSON). Verification cannot be implemented faithfully
from public docs, so handle_webhook stays a stub rather than guess a scheme.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/thryve.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="thryve",
    display_name="Thryve",
    positioning="European medical-grade wearable API",
    target_customers="Insurers, digital health, clinical trials, pharma",
    data_source_coverage="500+ devices, 250+ metrics (incl. cardiovascular, diabetes)",
    integration_method="Plug-and-play API, unified data format",
    compliance_summary="GDPR, HIPAA, ISO 9001/27001",
    differentiator="\"Fully developed and hosted in Europe",
    docs_url="https://www.thryve.health",
    region=Region.EU,
    open_source=False,
    compliance=("GDPR", "HIPAA", "ISO_9001", "ISO_27001",),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE, DataDomain.GLUCOSE,),
    integrations=(Integration.REST,),
    category="platform",
    status=VendorStatus.METADATA,
)


class ThryveVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

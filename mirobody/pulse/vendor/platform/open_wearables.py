"""Open Wearables — open-source self-hosted wearable API platform. See README.md
(Strategic Option A).

Open Wearables (by The Momentum, https://github.com/the-momentum/open-wearables)
is an MIT-licensed, self-hosted FastAPI service that unifies wearable data
(Apple Health, Samsung/Google Health Connect, Garmin, Polar, Oura, Whoop,
Fitbit, …) behind one normalized REST API. Because every deployment serves a
single organization on the operator's own infrastructure, there is NO public
host: base_url is REQUIRED and points at the operator's server (e.g.
http://localhost:8000) — we refuse rather than guess one.

CONFIRMED from the public docs (https://openwearables.io/docs, API Reference):
* base path           /api/v1 under the deployer's host
* auth                a custom API-key header, NOT a bearer token:
X-Open-Wearables-API-Key: <key>
(the docs explicitly say "Do not use Bearer token
format … a custom header, not the standard
Authorization header"). We send config.api_key here.
* per-user summaries  GET /api/v1/users/{user_id}/summaries/{activity|sleep|body|recovery}
* timeseries          GET /api/v1/users/{user_id}/timeseries (granular series)
* date range params   start_date / end_date, ISO-8601 accepted
* providers list      GET /api/v1/oauth/providers

INFERRED — the per-endpoint field reference (the Swagger UI / openapi.json
served by a running deployment) is not statically published, so the timeseries
metric-selector parameter name and its heart-rate value are best-effort and
centralized in the constants below for easy reconciliation against a live
/docs. The summary paths, the date params, and the providers path are
confirmed; only the timeseries metric selector is a guess.

Mapped operations: fetch() (Activity→activity summary, Sleep→sleep summary,
HeartRate→timeseries), list_providers() (GET oauth/providers — a global
catalogue, so the user_id arg is ignored), and authorize_url(), which builds the
per-provider consent URL GET /api/v1/oauth/{provider}/authorize?user_id=… now
that the interface carries provider + user_id (the endpoint takes neither
redirect_uri nor state, so those are ignored). revoke() / handle_webhook() stay
inherited stubs: there is no documented disconnect endpoint, and the webhook
signature scheme is not published.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/open_wearables.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="open_wearables",
    display_name="Open Wearables",
    positioning="Open-source self-hosted wearable API platform",
    target_customers="Startups, growth-stage companies, indie developers",
    data_source_coverage="Apple Health, Samsung, Garmin, Polar, and more",
    integration_method="Flutter/React Native SDK, AI-ready endpoints",
    compliance_summary="Self-hosted control (compliance depends on deployment environment)",
    differentiator="Open-source and free, no SaaS seat fees, built-in AI interface (can connect to Claude)",
    docs_url="https://www.themomentum.ai",
    region=Region.SELF_HOSTED,
    open_source=True,
    compliance=(),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE,),
    integrations=(Integration.SDK, Integration.REST,),
    category="platform",
    status=VendorStatus.METADATA,
)


class OpenWearablesVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

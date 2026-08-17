"""WeFitter — health gamification API platform. See README.md (Segment A).

Implemented against the public reference at
https://www.wefitter.com/en-us/developers/documentation/ (plus the official
generated client at https://github.com/funxtionatics/wefitter-client, which
pins exact paths and parameter names). WeFitter aggregates 250+/300+ wearable
brands behind a REST API, layering a gamified challenge engine and an AI
biological-age ("Bio Age") score on top. This client covers the data-fetch
path (per-metric profile summary endpoints), the connection/authorize flow,
challenge listing, and disconnect, all behind the platform's JWT auth.

CONFIRMED from the public docs + generated client:
* base URL   https://api.wefitter.com/api/v1.1/
* auth       two-legged. Basic auth (base64 client_id:client_secret) ->
POST /token/ returns a JSON object with a "bearer" field (a
24h administrator JWT). That JWT is sent as `Authorization:
Bearer <jwt>` on subsequent calls.
* data fetch GET /profile/{profile_public_id}/<metric>_summary/ with
date_start / date_end query params, e.g. daily_summary (steps/
activity), heartrate_summary, sleep_summary, biometric.
Activity workouts live at .../workout/.
* connect    GET /profile/{public_id}/connections/ returns the per-provider
connection URLs; a `redirect` query param sets the post-connect
return URL. (Apple/Samsung Health are SDK-only, no web URL.)
* challenges GET /profile/{public_id}/challenge/
* disconnect DELETE /profile/{public_id}/ removes the profile.

INFERRED / not part of a confirmed contract:
* The POST /token/ *request body* field names are not published (the Token
model documents only the "bearer" *response* field), so the credentials
ride in the Basic-auth header — which IS confirmed — and the body is empty.
* Bio Age exists ("Bio Age V0.1", changelog 2023-02-20) but its endpoint
path is not published anywhere public; rather than invent one, fetch()
surfaces an honest VendorError for any domain without a confirmed endpoint,
and the bio-age score is not exposed as a fabricated path.
handle_webhook stays a stub (verified, not a research gap): WeFitter DOES have
webhooks with a documented payload (activity / challenge events, ~5-min batched
push to a dashboard-configured URL), but publishes NO signature/verification
scheme — no header name, no HMAC algorithm, no signing secret. Verification
cannot be implemented faithfully from public docs, so we do not fabricate one.
The SDK-mediated Apple/Samsung connect flow likewise stays a stub.

---
Ported from the archived C++ implementation (private repo ``mirobody-on-device``,
``src/health/vendor/platform/wefitter.cpp``). The comment block above is the original port spec:
confirmed endpoints, auth style, and the per-operation rationale for anything
left a stub. Status: **metadata** — every network operation is an honest stub
until the wire contract is implemented (and then verified) here in Python.
"""

from ..base import DataDomain, Integration, Region, Vendor, VendorInfo, VendorStatus

INFO = VendorInfo(
    id="wefitter",
    display_name="WeFitter",
    positioning="Health gamification API platform",
    target_customers="Corporate wellness, digital fitness, insurers",
    data_source_coverage="Data from 300+ mainstream wearable brands",
    integration_method="REST API, mobile SDK",
    compliance_summary="European compliance standards",
    differentiator="Gamified challenge engine (team competitions) + AI biological-age scoring",
    docs_url="https://www.wefitter.com/en-us/",
    region=Region.EU,
    open_source=False,
    compliance=("GDPR",),
    domains=(DataDomain.ACTIVITY, DataDomain.SLEEP, DataDomain.HEART_RATE,),
    integrations=(Integration.REST, Integration.SDK,),
    category="platform",
    status=VendorStatus.METADATA,
)


class WefitterVendor(Vendor):
    """Stub client. ``info`` works without credentials; every network operation
    raises ``VendorError`` until implemented — see the module docstring."""

    INFO = INFO

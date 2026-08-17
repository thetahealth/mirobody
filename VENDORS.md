# Data-Source Vendors

The transport layer under `mirobody/pulse/vendor/` speaks to **24 health-data
sources** behind one contract (`Vendor`): B2B aggregators, device brands, phone
vendors, and a generic SMART-on-FHIR EHR client. Ported from our archived C++
implementation — each module's docstring carries the confirmed endpoints, auth
style, and the per-operation rationale for anything left a stub.

## Status levels — we grade ourselves honestly

| Level | Meaning |
|---|---|
| **verified** | Exercised end-to-end with real credentials. |
| **implemented** | Coded against the public wire docs, not yet run against a live account (most aggregators gate keys behind a sales contract). |
| **metadata** | Profile only; every network operation is an honest stub that says so. |

Three sources (`garmin`, `oura`, `whoop`) additionally have deep, **production**
pipeline providers under `pulse/theta/` — those are the battle-tested paths Theta
runs on; their vendor-layer entries here await the transport consolidation.

**Want a source moved up a level?** If you hold credentials for any
`implemented` or `metadata` vendor, verifying it is one of the most valuable
contributions you can make — see [Contributing](README.md#-contributing).

## Registry

List programmatically (no credentials needed):

```python
from mirobody.pulse.vendor import all_vendor_info
for v in all_vendor_info():
    print(v.id, v.status.value, v.display_name)
```

| id | Name | Category | Region | Data domains | Status | Notes |
|---|---|---|---|---|---|---|
| `rook` | Rook Health | B2B Aggregator | global | `activity` `sleep` `heart_rate` | **metadata** |  [docs](https://docs.tryrook.io/docs/) |
| `spike` | Spike API | B2B Aggregator | global | `activity` `sleep` `heart_rate` `nutrition` `labs` | **metadata** |  [docs](https://docs.spikeapi.com/overview) |
| `terra` | Terra API | B2B Aggregator | global | `activity` `sleep` `heart_rate` `glucose` `body_metrics` | **metadata** |  [docs](https://tryterra.co) |
| `junction` | Junction | B2B Aggregator | us | `activity` `labs` | **metadata** |  [docs](https://www.junction.com) |
| `wefitter` | WeFitter | B2B Aggregator | eu | `activity` `sleep` `heart_rate` | **metadata** |  [docs](https://www.wefitter.com/en-us/) |
| `lexisnexis` | LexisNexis EHR | B2B Aggregator | us | `clinical` `labs` `body_metrics` | **metadata** |  [docs](https://risk.lexisnexis.com) |
| `thryve` | Thryve | B2B Aggregator | eu | `activity` `sleep` `heart_rate` `glucose` | **metadata** |  [docs](https://www.thryve.health) |
| `validic` | Validic | B2B Aggregator | us | `body_metrics` `heart_rate` `glucose` `clinical` | **metadata** |  [docs](https://developer.validic.com) |
| `human_api` | Human API | B2B Aggregator | us | `clinical` `activity` | **metadata** |  [docs](https://www.humanapi.co) |
| `vitalera` | Vitalera | B2B Aggregator | global | `heart_rate` `glucose` `clinical` | **metadata** |  [docs](https://www.vitalera.io) |
| `open_wearables` | Open Wearables | B2B Aggregator | self-hosted | `activity` `sleep` `heart_rate` | **metadata** |  [docs](https://www.themomentum.ai) |
| `redox` | Redox | B2B Aggregator | us | `clinical` | **metadata** |  [docs](https://www.redoxengine.com) |
| `particle_health` | Particle Health | B2B Aggregator | us | `clinical` | **metadata** |  [docs](https://www.particlehealth.com) |
| `healthconnect` | HealthConnect CoPilot | B2B Aggregator | us | `clinical` `activity` `heart_rate` | **metadata** |  [docs](https://www.mindbowser.com/healthconnect-copilot/) |
| `metriport` | Metriport | B2B Aggregator | us | `clinical` | **metadata** |  [docs](https://www.metriport.com) |
| `huawei` | Huawei Health Kit | Phone Vendor | global | `activity` `heart_rate` `sleep` `glucose` `body_metrics` | **metadata** |  [docs](https://developer.huawei.com/consumer/en/hms/huawei-healthkit/) |
| `fitbit` | Fitbit | Device Brand | global | `activity` `heart_rate` `sleep` `body_metrics` | **metadata** |  [docs](https://dev.fitbit.com/build/reference/web-api/) |
| `withings` | Withings Health Mate | Device Brand | eu | `activity` `heart_rate` `sleep` `body_metrics` | **metadata** |  [docs](https://developer.withings.com/api-reference/) |
| `garmin` | Garmin Health | Device Brand | global | `activity` `heart_rate` `sleep` `body_metrics` | **metadata** | production provider in `pulse/theta` [docs](https://developer.garmin.com/gc-developer-program/health-api/) |
| `dexcom` | Dexcom | Device Brand | us | `glucose` | **metadata** |  [docs](https://developer.dexcom.com/) |
| `oura` | Oura | Device Brand | global | `sleep` `activity` `heart_rate` | **metadata** | production provider in `pulse/theta` [docs](https://cloud.ouraring.com/v2/docs) |
| `whoop` | WHOOP | Device Brand | global | `sleep` `heart_rate` `activity` | **metadata** | production provider in `pulse/theta` [docs](https://developer.whoop.com/) |
| `polar` | Polar | Device Brand | global | `activity` `heart_rate` `sleep` | **metadata** |  [docs](https://www.polar.com/accesslink-api/) |
| `ehr` | EHR (SMART on FHIR) | EHR | us | `clinical` `labs` `glucose` `heart_rate` `body_metrics` `activity` | **metadata** |  [docs](https://hl7.org/fhir/smart-app-launch/) |

## Credentials convention

```
MIROBODY_VENDOR_<ID>_API_KEY / _CLIENT_ID / _CLIENT_SECRET / _BASE_URL
```

where `<ID>` is the vendor id upper-cased (e.g. `HUMAN_API`). Self-hosted
vendors (`open_wearables`) require `_BASE_URL` — there is no public host to
default to.

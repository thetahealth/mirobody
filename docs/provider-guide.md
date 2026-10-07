# Provider Integration Guide

## Before you write a provider: do you need one?

A provider integration is two very different things glued together — the OAuth
dance, token storage, rate limits and pull windows (IO, per deployment) and the
translation of a vendor's JSON into standardised facts (a pure function of the
payload). This project owns the second half properly and the first half only as
far as its own reference application needs.

**If you need broad device access** — eight or more vendors, a phone SDK,
webhooks, multi-account fan-out — put
[open-wearables](https://github.com/the-momentum/open-wearables) in front:
twelve provider strategies, iOS/Android/Flutter/React Native SDKs, a developer
portal, Celery sync. Then decode its API here (`mirobody.kernel.decoders.open_wearables`)
and you get UCUM units, LOINC where the code is public, day windows that survive
daylight saving, quality gates, corrections, medications and the two query
tools on top. **Run open-wearables to connect devices; run
mirobody to make the data mean one thing and answer questions about it.**

Write a provider here when you need a vendor neither covers, or when the
deployment must hold its own credentials.

## What a provider is made of

| Piece | Pure? | Where |
|---|---|---|
| **the decode table** | yes | `mirobody/kernel/decoders/<vendor>.py` |
| **the IO shell** | no | `mirobody/collect/providers/mirobody_<vendor>/` |
| **the samples** | data | `mirobody/kernel/decoders/samples/<vendor>/` |
| **the coverage** | generated | `decoders.coverage_of("<vendor>")` |

A provider does not have to live in this repository. Ship it as a package that
declares a `mirobody.providers` entry point pointing at the module with your
`BasePullProvider` subclass, `pip install` it next to mirobody, and the pull
loop picks it up at boot with no config change — or drop the `mirobody_<slug>/`
directory into a folder listed in `PROVIDER_DIRS`. Both paths call the same
`create_provider(config)`. `examples/mirobody_example_plugin/` is a
complete, installable example of the first.

### 1. The decode table

A dict from the vendor's field path to `(catalogue metric, a function that
converts INTO the catalogue's unit)`:

```python
MAPPING = {
    "sleep": {
        "total_sleep_duration": ("dailyTotalSleepTime", s_to_ms),
        "efficiency":           ("sleepEfficiency",     _same),
    },
}
```

**The unit is never written in the table.** It comes from the catalogue, so a
decoder cannot disagree with the aggregator about what `ms` means. The table
says only how to get there. (The one bug this rule would have prevented in this
repository: a kJ→kcal conversion applied the wrong way round, invisible because
the number stayed plausible.)

Three more rules:

* **Never fabricate a time.** A record without one decodes to nothing. A
  synthetic timestamp is indistinguishable from a measured one afterwards.
* **Never guess a metric.** A key the catalogue does not know returns nothing
  and is quarantined. `metrics.mapping_for` returning `None` is an answer.
* **Emit intervals as intervals.** A night's stages are spans, not one total;
  two syncs of the same night overlap and adding their totals counts it twice.

`decode(data_type, item, tz, *, pulled_at_ms, source_record_id, ingested_at_ms)
-> list[series.Fact]`, pure — no clock, no database, no config.

### 2. The IO shell

`format_data(self, fmt_input: FormatDataInput) -> StandardPulseData` calls
`decoders.decode` and wraps the facts with `records_from_facts`. Everything else
in the class is authentication, pagination and storage. The credential state
machine is `mirobody.kernel.connect`: the base pull loop backs off after the
vendor refuses a credential and stops after a threshold, so a changed password
does not become an account lockout. Only a refusal counts (raise
`PermissionError` for one); a timeout or a vendor outage does not.

### 3. The samples

`mirobody/kernel/decoders/samples/<vendor>/*.json`: payloads shaped like the vendor's
PUBLIC documentation, each with its expected facts **computed by hand**. Never
by running the decoder — a sample generated from the code under test asserts
that the code does what it does.

They ship in the wheel, so a downstream repository can run
`mirobody.testing.samples.run_samples` against its own decoders.

### 4. Coverage — what the connector ACTUALLY carries

"This platform supports Oura" and "this platform brings you your blood
pressure" are different claims, and a person choosing a device wants the
second. `connect.Coverage` is generated from the decode table, so a page built
from it cannot promise data the code does not produce.

<!-- coverage:start -->
| Provider | Metrics | Data types |
|---|---|---|
| `apple` | 58 | `HKCategoryTypeIdentifierCervicalMucusQuality`, `HKCategoryTypeIdentifierContraceptive`, `HKCategoryTypeIdentifierIntermenstrualBleeding`, `HKCategoryTypeIdentifierLactation`, `HKCategoryTypeIdentifierMenstrualFlow`, `HKCategoryTypeIdentifierOvulationTestResult`, `HKCategoryTypeIdentifierPregnancy`, `HKCategoryTypeIdentifierPregnancyTestResult`, `HKCategoryTypeIdentifierProgesteroneTestResult`, `HKCategoryTypeIdentifierSexualActivity`, `HKCategoryTypeIdentifierSleepAnalysis`, `HKCorrelationTypeIdentifierBloodPressure`, `HKQuantityTypeIdentifierActiveEnergyBurned`, `HKQuantityTypeIdentifierAppleExerciseTime`, `HKQuantityTypeIdentifierAppleSleepingWristTemperature`, `HKQuantityTypeIdentifierBasalBodyTemperature`, `HKQuantityTypeIdentifierBasalEnergyBurned`, `HKQuantityTypeIdentifierBloodGlucose`, `HKQuantityTypeIdentifierBloodPressureDiastolic`, `HKQuantityTypeIdentifierBloodPressureSystolic`, `HKQuantityTypeIdentifierBodyFatPercentage`, `HKQuantityTypeIdentifierBodyMass`, `HKQuantityTypeIdentifierBodyMassIndex`, `HKQuantityTypeIdentifierBodyTemperature`, `HKQuantityTypeIdentifierCyclingSpeed`, `HKQuantityTypeIdentifierDietaryCarbohydrates`, `HKQuantityTypeIdentifierDietaryEnergyConsumed`, `HKQuantityTypeIdentifierDietaryFatTotal`, `HKQuantityTypeIdentifierDietaryProtein`, `HKQuantityTypeIdentifierDietaryWater`, `HKQuantityTypeIdentifierDistanceCycling`, `HKQuantityTypeIdentifierDistanceWalkingRunning`, `HKQuantityTypeIdentifierFlightsClimbed`, `HKQuantityTypeIdentifierHeartRate`, `HKQuantityTypeIdentifierHeartRateRecoveryOneMinute`, `HKQuantityTypeIdentifierHeartRateVariabilitySDNN`, `HKQuantityTypeIdentifierHeight`, `HKQuantityTypeIdentifierLeanBodyMass`, `HKQuantityTypeIdentifierOxygenSaturation`, `HKQuantityTypeIdentifierPeripheralPerfusionIndex`, `HKQuantityTypeIdentifierRespiratoryRate`, `HKQuantityTypeIdentifierRestingHeartRate`, `HKQuantityTypeIdentifierSixMinuteWalkTestDistance`, `HKQuantityTypeIdentifierStairAscentSpeed`, `HKQuantityTypeIdentifierStairDescentSpeed`, `HKQuantityTypeIdentifierStepCount`, `HKQuantityTypeIdentifierUVExposure`, `HKQuantityTypeIdentifierVO2Max`, `HKQuantityTypeIdentifierWaistCircumference`, `HKQuantityTypeIdentifierWalkingAsymmetryPercentage`, `HKQuantityTypeIdentifierWalkingDoubleSupportPercentage`, `HKQuantityTypeIdentifierWalkingHeartRateAverage`, `HKQuantityTypeIdentifierWalkingSpeed` |
| `garmin` | 46 | `activities`, `allDayRespiration`, `bloodPressures`, `bodyComps`, `dailies`, `hrv`, `pulseox`, `skinTemp`, `sleeps`, `stressDetails`, `userMetrics` |
| `open_wearables` | 53 | `sleep`, `timeseries`, `workouts` |
| `oura` | 36 | `daily_activity`, `daily_cardiovascular_age`, `daily_readiness`, `daily_sleep`, `daily_spo2`, `daily_stress`, `heartrate`, `personal_info`, `sleep`, `vo2_max`, `workout` |
| `whoop` | 26 | `body`, `cycle`, `recovery`, `sleep`, `workout` |
<!-- coverage:end -->

Regenerate with `mirobody.testing.coverage.gen_coverage(decoders.coverage_matrix())`
and embed with `coverage.embed`, in the same change as the decoder.
`coverage.stale` returns the difference between this table and a fresh one; no
CI job runs it, so a decoder change that forgets the table goes unnoticed
until someone does.

A metric listed here is one the decoder can EMIT. Whether a given person's
device records it is a further question, and no table can answer it.

---

## Writing the IO shell

Everything below is the IO half: configuration, where the class lives, the
contract it implements, the OAuth flows and what the base class already does.
The shipped providers are the reference implementation and are short enough to
read whole: `mirobody_oura/provider_oura.py` for OAuth 2.0 (the smallest),
`mirobody_whoop/provider_whoop.py` for the same with a once-a-day pull, and
`mirobody_garmin_connect/provider_garmin.py` for OAuth 1.0a and pushes.

### Configuration

A provider reads its settings with `safe_read_cfg` in `__init__`, and its
`create_provider` returns `None` when one it cannot run without is missing:
that is the usual self-hosted state, not a fault, and it is logged at INFO.
The shipped providers' keys are in `config.devices.yaml` and
[provider-setup.md](provider-setup.md); a deployment puts its secrets in its
`config.{env}.yaml` overlay.

A provider that keeps its payloads as received names a table for them:

```sql
CREATE TABLE IF NOT EXISTS health_data_<vendor> (
    id               INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    create_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_at        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_del           boolean NOT NULL DEFAULT false,
    msg_id           character varying(200),
    raw_data         jsonb NOT NULL,
    theta_user_id    character varying(100),
    external_user_id character varying(100)
);
```

The shipped three are in `mirobody/schema/40_devices.sql`.

### Where it lives

`ProviderPlatform.load_providers()` scans each directory in `PROVIDER_DIRS`
for `mirobody_*/provider_*.py` and also loads every installed package that
declares a `mirobody.providers` entry point. A directory not in
`PROVIDER_DIRS` is not scanned. A provider outside this package is loaded by
file location, so its imports are absolute. See
[`mirobody/collect/providers/README.md`](../mirobody/collect/providers/README.md).

### The class

```python
from typing import Any

from mirobody.collect import (
    BasePullProvider,
    FormatDataInput,
    LinkType,
    ProviderInfo,
    ProviderStatus,
    StandardPulseData,
    StandardPulseMetaInfo,
    records_from_facts,
)
from mirobody.kernel import decoders


class AcmeProvider(BasePullProvider):
    pull_interval_hours = 1.0          # the scheduler's interval; under an hour runs hourly
    pull_days = 2                      # what a scheduled pull asks for
    backfill_days = 30                 # what the pull right after a link asks for
    raw_table = "health_data_acme"     # where payloads are kept as received; "" keeps none

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "AcmeProvider | None":
        provider = super().create_provider(config)
        if provider is None or not provider.client_id:
            return None
        return provider

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(slug="theta_acme", name="Acme", auth_type=LinkType.OAUTH2,
                            status=ProviderStatus.AVAILABLE)

    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        ...

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        ...
```

`format_data` and `pull_from_vendor_api` are abstract: a class without either
does not load (`create_provider` declines it and the boot log says so).
Everything not shown is inherited: the pull loop, the credential back-off, the
push into the platform, the raw save and the format context.

### `info`

`slug` is unique across providers and is the string in every URL and stored
link (`theta_<vendor>` for the shipped three). `auth_type` decides the link
flow and which credentials the pull receives. `logo` is optional: the web
client ships the built-in providers' logos itself
(`src/pages/Drive/Vital/logos.js` in mirobody-web), so for your own provider
give an https URL you host, or leave it `None` if that request should not
happen. A `LinkType.CUSTOMIZED` provider lists the fields it asks for in
`connect_info_fields` (`ConnectInfoField`).

### Linking

**PASSWORD and CUSTOMIZED** need nothing but `_validate_credentials`:

```python
async def _validate_credentials(self, credentials: dict[str, Any]) -> None:
    connect_info = credentials.get("connect_info") or {}
    if not await self.client.check(connect_info.get("api_key")):
        raise ValueError("the vendor refused this key")
```

`BasePullProvider.link` then stores the credential under the provider's own
`auth_type`, whatever the request said. A password is stored encrypted, and so
is the whole of `connect_info`.

**OAuth 2.0** composes `OAuth2Client` (`_platform/oauth2.py`):

```python
async def link(self, request: Any) -> dict[str, Any]:
    return await self.oauth.generate_authorization_url(request.user_id, request.options or {})

async def callback(self, code: str, state: str) -> dict[str, Any]:
    result = await self.oauth.exchange_code_for_tokens(code, state, self.db_service, self.info.slug)
    self._relinked(result["user_id"])
    spawn(self._pull_and_push_for_user({
        "user_id": result["user_id"],
        "access_token": result["access_token"],
        "refresh_token": result["refresh_token"],
        "expires_at": result.get("expires_at"),
    }, days=self.backfill_days))
    return {"provider_slug": self.info.slug, "stage": "completed", "return_url": result.get("return_url")}
```

The account the link belongs to comes from the temporary state the
authorization step wrote, never from the callback's query string, which the
browser controls. `_relinked` forgets the old credential's refusals.

**A callback returns no token, not even part of one.** The route hands what it
returns to the browser, in a message to the page that opened the link, and a
token has no business in a browser. `provider_slug`, `stage` and `return_url`
are the whole answer.

**OAuth 1.0a** is Garmin's flow: a request token, the person's approval, then
the access-token exchange. Ask the vendor anything about the account only with
the access token; asked with the request token, Garmin's `/user/id` answers
nothing. `requests_oauthlib` is synchronous, so each call runs in
`asyncio.to_thread`.

`unlink` removes the stored link (`db_service.delete_user_theta_provider`); a
vendor that offers revocation is asked first, and the link goes whether or not
it answers.

### `pull_from_vendor_api(credentials, days)`

One linked account's last `days`, as a list of packages:

```python
{"data_type": "sleep", "data": [record, ...], "timestamp": pulled_at_ms}
```

`credentials` holds `user_id` and, by `auth_type`: `username` and `password`;
`access_token`, `refresh_token` and `expires_at`; `access_token` and
`access_token_secret`; or `connect_info`. An OAuth 2.0 provider gets a usable
token from `self.oauth.get_valid_access_token(credentials, self.info.slug,
self.db_service)`, which refreshes one within five minutes of expiry; a
refused refresh marks the link for reconnecting and raises. Fetch with
`_platform/http.py`:

```python
async with aiohttp.ClientSession() as session:
    records = await get_pages(session, url, headers=headers, params=params,
                              records_key="data", token_param="next_token")
```

`get_json` retries a 429, a 5xx, a timeout or a dropped connection a bounded
number of times with a capped wait, raises `VendorAuthError` (a
`PermissionError`) at once for a 401 or 403, and `VendorError` for any other
status. Let a refusal propagate: it is what the back-off counts. Skip a
collection that fails some other way and return the rest.

### The pull loop

`BasePullProvider.pull_and_push` runs every `pull_interval_hours` for every
account whose link is not marked for reconnecting, `pull_days` back; a
callback runs `_pull_and_push_for_user` once, `backfill_days` back. For each
account the loop checks the credential's state, calls
`pull_from_vendor_api`, adds the account id to each package as
`theta_user_id`, and pushes it to the platform. Three refusals in a row stop
the loop trying that account until the person links again (or the worker
restarts); the run fails for an account when a push fails.

### `save_raw_data_to_db`

The platform calls it with each pushed package, `msg_id` added. With
`raw_table` set, the base class inserts the package there and returns it for
formatting. A payload without `theta_user_id` came from outside the pull loop
(the webhook route strips that key from every push) and is not kept: nothing
in it names an account this provider can verify. A provider that accepts
vendor pushes overrides this method and maps the vendor's own user id to a
linked account, as Garmin does through the Garmin id it stores as the link's
username.

### `format_data(fmt_input)`

`fmt_input.payload` is the saved package, untouched. `fmt_input.context`
carries what needed the database (the account id, the vendor's user id, the
person's timezone), so the method is a pure transformation:

```python
async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
    ctx, payload = fmt_input.context, fmt_input.payload
    tz = ctx.user_timezone or "UTC"
    records = []
    for item in payload.get("data") or []:
        record_id = str(item.get("id") or "")
        facts = decoders.decode("acme", payload["data_type"], item, tz,
                                pulled_at_ms=int(payload.get("timestamp") or 0), source_record_id=record_id)
        records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=record_id))
    return StandardPulseData(
        metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=self.generate_request_id(),
                                       source="theta", timezone=tz),
        healthData=records,
    )
```

**`source_id` is the vendor's own id for the record, or `""`.** It becomes part
of the stored reading's identity, so the same record pulled twice is one
reading, amended if its value changed. The pull's `msg_id` is different on
every pull, and a provider that passed it stored a new copy of every reading
on every run.

### Data flow

```
link() → the person approves at the vendor → callback()
    → stores the credential, spawns _pull_and_push_for_user(days=backfill_days)
scheduler, every pull_interval_hours → pull_and_push() → _pull_and_push_for_user(days=pull_days)
    → pull_from_vendor_api() → push_service.push_data()
vendor push → POST /api/v1/pulse/theta/<slug>/webhook
    → ProviderPlatform.post_data()
both → save_raw_data_to_db() → build_format_context() → format_data()
    → StandardHealthService.process_standard_data()
```

`post_data` answers False when a payload that carries data saves nothing, so
a vendor that retries on an error answer retries it.

### Testing

The decode table is tested by its samples (`samples/<vendor>/*.json`, expected
facts worked by hand), which `mirobody.testing.samples.run_samples` checks.
`format_data` is tested with no database at all:

```python
from mirobody.collect import FormatDataContext, FormatDataInput

async def test_a_sleep_document_becomes_records():
    provider = AcmeProvider.__new__(AcmeProvider)   # no config, no database
    ctx = FormatDataContext(theta_user_id="u1", user_timezone="UTC", msg_id="m1")
    payload = {"data_type": "sleep", "data": [SLEEP_DOCUMENT], "timestamp": 0}
    out = await provider.format_data(FormatDataInput(context=ctx, payload=payload))
    assert {r.source_id for r in out.healthData} == {SLEEP_DOCUMENT["id"]}
```

For the pull, fake the vendor at the session: give `get_json` and `get_pages`
an object whose `get()` returns canned responses, and check what
`pull_from_vendor_api` returns and that a 401 raises.

### Rules that bite

- **Log ids, counts, durations, status codes and type names; never a value.**
  No token (not a prefix of one either), no reading, no vendor response body,
  no `str(e)`. In a broad `except`:

  ```python
  logger.error("Acme pull failed: user_id=%s error_type=%s", user_id, type(e).__name__,
               exc_info=not is_driver_exception(e))
  ```

  with `logger = logging.getLogger(__name__)` at module top, never the root
  logger. `is_driver_exception` is `mirobody.kernel.ops`'.
- **Never fabricate a time.** A record without one decodes to nothing, never
  to now() or to the pull instant.
- **The unit is the catalogue's.** A decode table converts into
  `metrics.METRICS[name]`'s unit and never names one.
- **Metric names are catalogue names**: the `name` column of
  `mirobody/res/catalog/metrics.tsv` (`heartRates`, `dailyTotalSleepTime`).

### Common issues

**The callback answers 404 or "provider not available".** The provider
declined at startup, so nothing is registered under the slug. Check its keys;
the callback is downstream of registration.

**The vendor rejects the redirect URI.** It must match what you registered
byte for byte, slug included.

**A link stopped pulling.** A refused refresh marks it for reconnecting; the
app shows "reconnect" and linking again clears it. A refresh that only timed
out is retried on the next run.

**The same reading appears twice.** Check that `format_data` passes the
vendor's record id as `source_id`, not the `msg_id`.

---

## Support & Resources

- **Example providers** (read these first: they are the real thing):
   - Oura: `mirobody/collect/providers/mirobody_oura/provider_oura.py`
   - WHOOP: `mirobody/collect/providers/mirobody_whoop/provider_whoop.py`
   - Garmin: `mirobody/collect/providers/mirobody_garmin_connect/provider_garmin.py`
- **Platform internals**: `mirobody/collect/providers/_platform/`: `base.py` is
  the contract you implement and the pull loop, `platform.py` does discovery and
  pull scheduling, `oauth2.py` and `http.py` are the shared clients.
- **Testing**: `docs/testing.md`.
- **Data contract**: `mirobody/collect/ingest/models/requests.py`:
  `StandardPulseData` and friends, the shape every provider produces.

**Maintenance note**: this guide is prose without a test gate. When it and the
code disagree, the code wins; the shipped providers are the executable
reference.

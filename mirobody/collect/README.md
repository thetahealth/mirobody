# `mirobody.collect` — ① Collect

① Collect is how a reading gets in and how it is kept. Wearables and health
platforms (Garmin, Oura, WHOOP, Apple Health) arrive through a
**Platform-Provider** plugin architecture; lab reports, photos and genetic
files arrive as uploads. Everything converges on `StandardPulseData` and is
written, verbatim, by one writer. What a value MEANS is ② Translate's.

## Architecture

### Platform-Provider Pattern

```
PlatformManager (singleton)
  ├── ProviderPlatform        — direct device integrations (Garmin, Whoop, Renpho, etc.)
  │     └── BasePullProvider subclasses (one per device)
  ├── AppleHealthPlatform  — Apple Health data import + CDA documents
  └── (future platforms)
```

**Data flow (pull-provider path)**:
```
Vendor API → Provider.pull_from_vendor_api()
  → Provider.save_raw_data_to_db()     (raw JSON → health_data_<name>)
  → Provider.format_data()             (raw → StandardPulseData)
  → StandardHealthService.process()    (StandardPulseData → observations.ingest)
  → translate/aggregate                (a day of points → one published number)
```

All platforms converge at `StandardPulseData` — the universal exchange format defined in `ingest/models/requests.py`.

### Plugin System (Current: file-scan autoload)

providers are discovered at startup by `ProviderPlatform._load_providers_from_directory()`:
1. Glob `providers/mirobody_*/provider_*.py`
2. Import module, find class matching `*Provider(BasePullProvider)`
3. Call `create_provider(config)` — returns instance or `None` (graceful skip)
4. Register with `ProviderPlatform.register_provider()` which also schedules pull tasks

> **Planned**: migrate to `__init_subclass__` registry (see todo: `init-subclass-provider-loading`).

## Subsystem Map

| Subsystem | Directory | Size | Responsibility | Entry Point |
|-----------|-----------|------:|---------------|-------------|
| *— where data comes from —* | | | | |
| **Providers** | `providers/` | ~5.8k | Devices and health platforms: Garmin, Oura and WHOOP pulled on a schedule, Apple and CDA documents pushed | `providers/_platform/platform.py` |
| **Files** | `files/` | ~7.8k | A file is a source too: upload, storage, PDF/CSV/Excel/Office/image/text/genetic | `files/file_upload_manager.py` |
| *— what happens to it —* | | | | |
| **Ingest** | `ingest/` | ~1.1k | `StandardPulseData` → the writer. Every source above converges here | `ingest/services/upload_health.py` |
| *— how it is kept, and read back —* | | | | |
| **Observations** | `observations.py` | ~1.1k | THE writer. One transaction: freeze the extraction, fold, parse, place the local day, insert, code, refresh the series | `observations.ingest` |
| **Query** | `query.py` | ~0.6k | THE reader, over `v_observation`: the Postgres half of `kernel.query.HealthQuery` | `PostgresHealthQuery` |
| *— what they all stand on —* | | | | |
| **Core** | `core/` | ~0.5k | The provider contract types, DB base classes, push | `core/constants.py`, `core/models.py` |
| **Meds** | `meds/` | ~0.4k | A Postgres store for `kernel.meds`, not a collector | `meds/__init__.py` |

What a value MEANS is not here. The indicator catalogue, units, value ranges
and fhir_id are `mirobody/translate/` since 1.4.4, which is what makes "collect
only collects" something the tree shows. The scheduler and the distributed lock
left `core/` in the same release, to `mirobody/utils/`: both are generic
infrastructure that `translate` needed too, and a package importing back into
`collect` for a scheduler was a cycle.

Read top to bottom and the table is the data flow: two source shapes, one
convergence point, one writer, one reader. The directory listing cannot show
that ordering — `core/` sorts before `providers/` — which is why it is spelled
out here.

A source is a TRANSPORT, not a kind of data. Medications are not a third
source: they arrive either in a CDA document through `providers/apple` or in an
uploaded file, and `collect/meds/` is a Postgres store for
`mirobody.kernel.meds`, not a collector. An EHR integration will split the same
way, a FHIR API being a provider and an exported document being a file.

`ingest/` was called `data_upload/` until it was renamed for saying the
opposite of what it does: it holds `StandardPulseRecord` and
`StandardHealthService`, the normalized-record core, while the directory that
actually handles file *uploads* is `files/`. The old name reliably sent readers
to the wrong place. `files/` was `file_parser/` until 1.4.4, renamed for the
same reason in reverse: it does uploads, storage and genetic processing, not
only parsing.

Two subsystems are gone rather than moved: `insight/` (a recipe + LLM engine
writing rows nothing displayed) and `monitor/` (ingestion counters read only by
the admin API). An insight is what the agent produces when you ask it about your
own data, over the same series; a second scheduled path to the same answer was a
duplicate with its own tables and prompts.

There is no `router/` here any more — the HTTP endpoints moved to
`mirobody/server/routers/`, where importing the agent layer is legal.

## Key Files by Task

### Adding a new provider
- `providers/_platform/base.py` — `BasePullProvider` (inherit from this)
- `providers/mirobody_garmin_connect/` — full OAuth reference (OAuth1)
- `providers/mirobody_whoop/` — OAuth2 reference
- `providers/__init__.py` — add import here after creating provider

### Adding a new health indicator
**Not in this package** since 1.4.4:
- `mirobody/translate/indicators_info.py` — `StandardIndicator` + `IndicatorInfo`
- `mirobody/translate/canonical_units.py` — unit conversion definitions
- `mirobody/kernel/decoders/apple.py` — the HealthKit identifier, if it comes
  from Apple. One table serves both the push endpoint and `mirobody import apple`.

### Modifying API endpoints
**Not in this package.** The HTTP layer moved to `mirobody/server/routers/`;
`collect/` has no FastAPI routes, which is what keeps ① Collect importable without
a web framework.
- `mirobody/server/routers/public_router.py` — main user-facing API
- `mirobody/server/routers/file_router.py` — file upload endpoints

### Data processing pipeline
- `ingest/models/requests.py` — `StandardPulseData`, `StandardPulseRecord`, `StandardPulseMetaInfo`
- `ingest/services/upload_health.py` — `StandardHealthService.process_standard_data()`
- `ingest/repositories/health_data.py` — DB queries for health data

### Aggregate indicators
**Not in this package** since 1.4.4: a daily total is the same quantity on a
different time axis, which is a LOINC axis change, so it went to ② Translate.
- `mirobody/translate/aggregate/service.py` — aggregation orchestrator
- `mirobody/translate/derive/rules.py` — quantities nothing measured

### Writing and reading a reading
- `observations.py` — `ingest`, `amend`, `retract`, `redate`, `erase`, `recode`
- `query.py` — `PostgresHealthQuery`, the only read path
- `migrate_observations.py` — `mirobody migrate-observations`, the one-time move

## Framework Protection

These files form the framework skeleton. Modifying them affects ALL providers and platforms.

| Risk | File | What It Does |
|------|------|-------------|
| :red_circle: | `base.py` | `Provider` / `Platform` ABCs — contract for all plugins |
| :red_circle: | `manager.py` | `PlatformManager` singleton — orchestrates all platforms |
| :red_circle: | `core/constants.py` | Shared enums (`LinkType`, `ProviderStatus`) — used everywhere |
| :red_circle: | `setup.py` | Platform registration sequence — startup order matters |
| :yellow_circle: | `providers/_platform/platform.py` | `ProviderPlatform` — provider loading and registration |
| :yellow_circle: | `providers/_platform/base.py` | `BasePullProvider` — shared provider logic |

## Coding Constraints

### All content must use English
Comments, docstrings, API messages, error messages, variable names, exception messages — all English.

### Token verification (CRITICAL SECURITY)
STRICTLY FORBIDDEN: creating mock token verification, returning fixed user IDs, bypassing real verification, duplicating verification functions.

```python
# ONLY correct import:
from ..auth import verify_token, verify_token_optional  # mirobody/server/auth.py

@router.get("/endpoint")
async def endpoint(current_user: str = Depends(verify_token)):
    pass
```

### async/await — ALWAYS await async calls
```python
# BAD:  result = async_function()     # returns coroutine object!
# GOOD: result = await async_function()
```

### Imports — prefer top-level, avoid lazy import unless necessary
Always use top-level imports. Lazy imports (inside functions) hide import errors until runtime and bypass startup validation. A wrong relative path in a lazy import won't be caught until that code path executes, which may not happen during testing.

**Only use lazy import when**:
1. Breaking a circular dependency (document which cycle it breaks)
2. Optional heavy dependency that may not be installed (e.g. `openpyxl`)

```python
# BAD: lazy import hides path errors
def process():
    from ..translate import get_standard_unit  # wrong path won't be caught at startup
    return get_standard_unit(indicator)

# GOOD: top-level import, fails fast at startup if path is wrong
from mirobody.translate import get_standard_unit

def process():
    return get_standard_unit(indicator)
```

**Incident**: TH-126 introduced `from ..core.fhir_mapping` as a lazy import
inside `_prepare_summary_record()`. The relative path was wrong — it resolved
to `ingest/core/`, and the module was `core/fhir_mapping.py` at the time — but
nothing said so, because the bug only ran on the SUMMARY path and the tests
only exercised SERIES. A top-level import would have failed at startup.
`fhir_mapping` itself was retired in 1.5.0 with the table it indexed; the
lesson is about lazy imports, not about that module.

### Sleep data uses 18:00-18:00 time window
Sleep data uses previous-day 18:00 to current-day 18:00, NOT 00:00-24:00. This affects `data_begin` calculation in SQL. See `mirobody/translate/aggregate/windows.py` for the implementation.

Related sleep indicators missed by `LIKE '%sleep%'`: `napDuration`, `inBedStartTime`, `endSleepReportTimeOffset`, `startSleepReportTimeOffset`.

### Query timing — query BEFORE insert
```python
# BAD:  await save(data); max_key = await get_max_key()  # includes new data!
# GOOD: max_key = await get_max_key(); await save(data)
```

### Boundary conditions — don't change casually
Think carefully before changing `<=`, `>=`, `<`, `>` comparisons. Understand the logic first, document reasoning.

### Circular dependency prevention
Follow layered architecture: Application -> Service -> Core. Avoid lazy imports unless absolutely necessary. Test imports before committing.

### Import timing — fetch runtime-initialized objects when you use them

Anything built by `Config.init()` does not exist at import time. Bind it at the
call site, not at module scope:

```python
# BAD — evaluated once, at import, before Config.init() has run
redis = global_config().get_redis()

# GOOD — evaluated per call, after startup
async def _redis():
    return await global_config().get_redis().get_async_client()
```

(The previous example here imported `...utils.utils_redis`, a module that has
never existed in this repo. The lesson was right; the code was not.)

### Type annotations
```python
# BAD:  def func(items=List[str]):     # uses type object as default!
# GOOD: def func(items: Optional[List[str]] = None):
```

## Commands

```bash
# Start the server (see the repo README for the full Quick Start)
mirobody serve

# Confirm providers were discovered
docker compose logs mirobody | grep "Loaded provider"
```

## Common Patterns

### Standard imports for a provider
```python
from mirobody.collect.base import ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.translate.indicators_info import StandardIndicator
from mirobody.collect.ingest.models.requests import (
    StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord,
)
from mirobody.collect.providers._platform.base import BasePullProvider
from mirobody.collect.providers._platform.normalize import DataFormatter, TimeUtils
from mirobody.utils.config import safe_read_cfg, global_config
```

### Provider factory method pattern
```python
@classmethod
def create_provider(cls, config: Dict[str, Any]) -> Optional['XxxProvider']:
    try:
        if not safe_read_cfg("XXX_API_KEY"):
            logging.info("XxxProvider disabled: missing config")
            return None
        return cls()
    except Exception as e:
        logging.warning(f"Failed to create provider: {e}")
        return None
```

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `coroutine object is not iterable` | Forgot `await` | Add `await` before async calls |
| `'NoneType' has no attribute 'setnx'` | Static import of `redis_client` before init | Use dynamic getter function |
| No data pulled but API returns data | `max_stored_key` queried after insert | Query before saving raw data |
| `UNMAPPED_HEALTH_TYPE` validation error | New health type not in enum | Add to enum or use permissive mode |
| Circular import on startup | Module dependency cycle | Follow layered architecture, refactor shared code |
| Provider not loading despite config | `create_provider` returns `None` | Check logs for "provider disabled" messages |
| Router endpoints not responding | Router not imported in `__init__.py` | Add import to `mirobody/server/routers/__init__.py` — a router module that exists but is not re-exported there makes the whole server fail to start, not just that route 404 |


---

## Longer guides

These moved to `docs/` at the repo root — they are contributor documentation, not
something a `pip install` should carry into `site-packages`:

- [`docs/provider-guide.md`](../../docs/provider-guide.md) — writing a data provider, end to end
- [`docs/file-processing.md`](../../docs/file-processing.md) — the file-parsing pipeline
- [`docs/apple-health.md`](../../docs/apple-health.md) — Apple Health / CDA import

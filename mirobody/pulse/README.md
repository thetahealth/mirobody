# `mirobody.pulse` — ① Collect

Pulse is the health data integration engine of Mirobody. It ingests data from wearables and health platforms (Garmin, Whoop, Apple Health, PostgreSQL, etc.) via a **Platform-Provider** plugin architecture, normalizes everything into `StandardPulseData`, and persists it to the database.

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
  → StandardHealthService.process()    (StandardPulseData → series_data table)
  → AggregateIndicator pipeline        (series → daily summaries)
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
| **Providers** | `providers/` | ~6.4k | The live provider platform — plugin discovery, OAuth, pull scheduling | `providers/platform/platform.py` |
| **Apple** | `apple/` | ~1.2k | Apple Health platform + CDA processing | `apple/platform.py` |
| **File Parser** | `file_parser/` | ~8.5k | Files as a data source: upload, parse PDF/CSV/Excel/Office/image/text/genetic | `file_parser/file_upload_manager.py` |
| *— what happens to it —* | | | | |
| **Ingest** | `ingest/` | ~1.1k | `StandardPulseData` → DB write. Every source above converges here | `ingest/services/upload_health.py` |
| **Standardize** | `standardize/` | ~4.8k | What a value means: indicator catalogue, units, value ranges, fhir_id | `standardize/indicators_info.py` |
| **Aggregate** | `aggregate/` | ~4.9k | Series data → daily summaries; derived indicators | `aggregate/service.py` |
| *— what they all stand on —* | | | | |
| **Core** | `core/` | ~2.5k | The plugin framework runtime: provider contract types, scheduler, DB base classes, push, distributed lock | `core/constants.py`, `core/models.py` |

Read top to bottom and the table is the data flow: three source shapes, one
convergence point, then meaning and rollups. The directory listing cannot show
that ordering — `aggregate/` sorts before `providers/` — which is why it is
spelled out here.

`ingest/` was called `data_upload/` until it was renamed for saying the
opposite of what it does: it holds `StandardPulseRecord` and
`StandardHealthService`, the normalized-record core, while the directory that
actually handles file *uploads* is `file_parser/`. The old name reliably sent
readers to the wrong place.

Two subsystems are gone rather than moved: `insight/` (a recipe + LLM engine
writing rows nothing displayed) and `monitor/` (ingestion counters read only by
the admin API). An insight is what the agent produces when you ask it about your
own data, over the same series; a second scheduled path to the same answer was a
duplicate with its own tables and prompts.

There is no `router/` here any more — the HTTP endpoints moved to
`mirobody/server/routers/`, where importing the agent layer is legal.

## Key Files by Task

### Adding a new provider
- `providers/platform/base.py` — `BasePullProvider` (inherit from this)
- `providers/mirobody_garmin_connect/` — full OAuth reference (OAuth1)
- `providers/mirobody_whoop/` — OAuth2 reference
- `providers/__init__.py` — add import here after creating provider

### Adding a new health indicator
- `standardize/indicators_info.py` — `StandardIndicator` enum + `IndicatorInfo` dataclass
- `standardize/units.py` — unit conversion definitions
- `apple/models.py` — `FlutterHealthTypeEnum` mapping (if from Apple Health)

### Modifying API endpoints
**Not in this package.** The HTTP layer moved to `mirobody/server/routers/`;
`pulse/` has no FastAPI routes, which is what keeps ① Collect importable without
a web framework.
- `mirobody/server/routers/public_router.py` — main user-facing API
- `mirobody/server/routers/file_router.py` — file upload endpoints

### Data processing pipeline
- `ingest/models/requests.py` — `StandardPulseData`, `StandardPulseRecord`, `StandardPulseMetaInfo`
- `ingest/services/upload_health.py` — `StandardHealthService.process_standard_data()`
- `ingest/repositories/health_data.py` — DB queries for health data

### Aggregate indicators
- `aggregate/service.py` — aggregation orchestrator
- `aggregate/rule_generator.py` — rule generation
- `aggregate/task.py` — background task scheduling

## Framework Protection

These files form the framework skeleton. Modifying them affects ALL providers and platforms.

| Risk | File | What It Does |
|------|------|-------------|
| :red_circle: | `base.py` | `Provider` / `Platform` ABCs — contract for all plugins |
| :red_circle: | `manager.py` | `PlatformManager` singleton — orchestrates all platforms |
| :red_circle: | `core/constants.py` | Shared enums (`LinkType`, `ProviderStatus`) — used everywhere |
| :red_circle: | `core/scheduler.py` | Global pull scheduler — timing affects all providers |
| :red_circle: | `setup.py` | Platform registration sequence — startup order matters |
| :yellow_circle: | `providers/platform/platform.py` | `ProviderPlatform` — provider loading and registration |
| :yellow_circle: | `providers/platform/base.py` | `BasePullProvider` — shared provider logic |

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
    from ..standardize.fhir_mapping import get_fhir_id  # wrong path won't be caught at startup
    return get_fhir_id(indicator)

# GOOD: top-level import, fails fast at startup if path is wrong
from ...standardize.fhir_mapping import get_fhir_id

def process():
    return get_fhir_id(indicator)
```

**Incident**: TH-126 introduced `from ..core.fhir_mapping` (wrong: resolved to `ingest/core/`; the module was `core/fhir_mapping.py` then, `standardize/fhir_mapping.py` now) as a lazy import inside `_prepare_summary_record()`. The bug was never caught because tests only exercised the SERIES path, not SUMMARY. A top-level import would have failed immediately at startup.

### Sleep data uses 18:00-18:00 time window
Sleep data uses previous-day 18:00 to current-day 18:00, NOT 00:00-24:00. This affects `data_begin` calculation in SQL. See `aggregate/` for implementation details.

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
from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.standardize.indicators_info import StandardIndicator
from mirobody.pulse.ingest.models.requests import (
    StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord,
)
from mirobody.pulse.providers.platform.base import BasePullProvider
from mirobody.pulse.providers.platform.normalize import DataFormatter, TimeUtils
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
- [`docs/aggregation-tests.md`](../../docs/aggregation-tests.md) — the aggregation test suite

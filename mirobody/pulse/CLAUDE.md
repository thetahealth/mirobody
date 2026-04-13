# CLAUDE.md — Pulse Module

Pulse is the health data integration engine of Mirobody. It ingests data from wearables and health platforms (Garmin, Whoop, Apple Health, PostgreSQL, etc.) via a **Platform-Provider** plugin architecture, normalizes everything into `StandardPulseData`, and persists it to the database.

## Architecture

### Platform-Provider Pattern

```
PlatformManager (singleton)
  ├── ThetaPlatform        — direct device integrations (Garmin, Whoop, Renpho, etc.)
  │     └── BaseThetaProvider subclasses (one per device)
  ├── AppleHealthPlatform  — Apple Health data import + CDA documents
  └── (future platforms)
```

**Data flow (Theta path)**:
```
Vendor API → Provider.pull_from_vendor_api()
  → Provider.save_raw_data_to_db()     (raw JSON → health_data_<name>)
  → Provider.format_data()             (raw → StandardPulseData)
  → StandardHealthService.process()    (StandardPulseData → series_data table)
  → AggregateIndicator pipeline        (series → daily summaries)
```

All platforms converge at `StandardPulseData` — the universal exchange format defined in `data_upload/models/requests.py`.

### Plugin System (Current: file-scan autoload)

Theta providers are discovered at startup by `ThetaPlatform._load_providers_from_directory()`:
1. Glob `theta/mirobody_*/provider_*.py`
2. Import module, find class matching `Theta*Provider(BaseThetaProvider)`
3. Call `create_provider(config)` — returns instance or `None` (graceful skip)
4. Register with `ThetaPlatform.register_provider()` which also schedules pull tasks

> **Planned**: migrate to `__init_subclass__` registry (see todo: `init-subclass-provider-loading`).

## Subsystem Map

| Subsystem | Directory | Responsibility | Entry Point |
|-----------|-----------|---------------|-------------|
| **Core** | `core/` | Constants, models, auth, DB, scheduler, indicators, aggregation | `core/constants.py`, `core/models.py` |
| **Data Upload** | `data_upload/` | `StandardPulseData` → DB write pipeline | `data_upload/services/upload_health.py` |
| **File Parser** | `file_parser/` | File upload via WebSocket, parse PDF/CSV/Excel/audio/image/genetic | `file_parser/file_upload_manager.py` |
| **Router** | `router/` | FastAPI endpoints (public API, manage, file, food, user, OAuth callbacks) | `router/public_router.py` |
| **Theta** | `theta/` | Theta platform + all Theta providers | `theta/platform/platform.py` |
| **Apple** | `apple/` | Apple Health platform + CDA processing | `apple/platform.py` |

## Key Files by Task

### Adding a new Theta Provider
- `theta/platform/base.py` — `BaseThetaProvider` (inherit from this)
- `theta/mirobody_pgsql/` — simplest reference implementation
- `theta/mirobody_garmin_connect/` — full OAuth reference (OAuth1)
- `theta/mirobody_whoop/` — OAuth2 reference
- `theta/__init__.py` — add import here after creating provider

### Adding a new health indicator
- `core/indicators_info.py` — `StandardIndicator` enum + `IndicatorInfo` dataclass
- `core/units.py` — unit conversion definitions
- `apple/models.py` — `FlutterHealthTypeEnum` mapping (if from Apple Health)

### Modifying API endpoints
- `router/public_router.py` — main user-facing API (~1100 lines, see section index at top)
- `router/manage_router.py` — admin/management endpoints
- `router/file_router.py` — file upload endpoints
- `router/food_router.py` — food recognition endpoints

### Data processing pipeline
- `data_upload/models/requests.py` — `StandardPulseData`, `StandardPulseRecord`, `StandardPulseMetaInfo`
- `data_upload/services/upload_health.py` — `StandardHealthService.process_standard_data()`
- `data_upload/repositories/health_data.py` — DB queries for health data

### Aggregate indicators
- `core/aggregate_indicator/service.py` — aggregation orchestrator
- `core/aggregate_indicator/rule_generator.py` — rule generation
- `core/aggregate_indicator/task.py` — background task scheduling

## Framework Protection

These files form the framework skeleton. Modifying them affects ALL providers and platforms.

| Risk | File | What It Does |
|------|------|-------------|
| :red_circle: | `base.py` | `Provider` / `Platform` ABCs — contract for all plugins |
| :red_circle: | `manager.py` | `PlatformManager` singleton — orchestrates all platforms |
| :red_circle: | `core/constants.py` | Shared enums (`LinkType`, `ProviderStatus`) — used everywhere |
| :red_circle: | `core/scheduler.py` | Global pull scheduler — timing affects all providers |
| :red_circle: | `setup.py` | Platform registration sequence — startup order matters |
| :yellow_circle: | `theta/platform/platform.py` | `ThetaPlatform` — provider loading and registration |
| :yellow_circle: | `theta/platform/base.py` | `BaseThetaProvider` — shared Theta provider logic |

## Coding Constraints

### All content must use English
Comments, docstrings, API messages, error messages, variable names, exception messages — all English.

### Token verification (CRITICAL SECURITY)
STRICTLY FORBIDDEN: creating mock token verification, returning fixed user IDs, bypassing real verification, duplicating verification functions.

```python
# ONLY correct import:
from ...utils.utils_auth import verify_token, verify_token_optional

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
2. Optional heavy dependency that may not be installed (e.g. `pandas`)

```python
# BAD: lazy import hides path errors
def process():
    from ..core.fhir_mapping import get_fhir_id  # wrong path won't be caught at startup
    return get_fhir_id(indicator)

# GOOD: top-level import, fails fast at startup if path is wrong
from ...core.fhir_mapping import get_fhir_id

def process():
    return get_fhir_id(indicator)
```

**Incident**: TH-126 introduced `from ..core.fhir_mapping` (wrong: resolves to `data_upload/core/`) as a lazy import inside `_prepare_summary_record()`. The bug was never caught because tests only exercised the SERIES path, not SUMMARY. A top-level import would have failed immediately at startup.

### Sleep data uses 18:00-18:00 time window
Sleep data uses previous-day 18:00 to current-day 18:00, NOT 00:00-24:00. This affects `data_begin` calculation in SQL. See `core/aggregate_indicator/` for implementation details.

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

### Import timing — use dynamic getters for runtime-initialized objects
```python
# BAD:  from ...utils.utils_redis import redis_client  # may be None at import time
# GOOD: def get_redis_client():
#            from ...utils.utils_redis import redis_client
#            return redis_client
```

### Type annotations
```python
# BAD:  def func(items=List[str]):     # uses type object as default!
# GOOD: def func(items: Optional[List[str]] = None):
```

## Commands

```bash
# Start service
sh run.sh
# or: ENV=test-inlocal bash deploy.sh

# Health check
curl http://localhost:18060/api/health

# Provider loading check
docker restart data_service
docker logs data_service | grep "Loaded provider"

# Trigger theta provider pull
curl -X POST "http://localhost:18060/api/v1/manage/theta/pull/trigger" \
  -H "Content-Type: application/json" \
  -d '{"provider_slug": "theta_renpho", "force": true}'
```

## Common Patterns

### Standard imports for a Theta Provider
```python
from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.indicators_info import StandardIndicator
from mirobody.pulse.data_upload.models.requests import (
    StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord,
)
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.theta.platform.utils import ThetaDataFormatter, ThetaTimeUtils
from mirobody.utils.config import safe_read_cfg, global_config
```

### Provider factory method pattern
```python
@classmethod
def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaXxxProvider']:
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
| Router endpoints not responding | Router not imported in `__init__.py` | Add import to `router/__init__.py` |

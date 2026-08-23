# Apple Health Platform

Processes Apple Health export data, statistics batches, and CDA (Clinical
Document Architecture) documents.

> An earlier revision of this file described an event-driven architecture —
> `event_providers/`, `registry.py`, `HeartRateEventProvider`, per-
> `HKQuantityTypeIdentifier*` handlers — that does not exist in this codebase
> and never shipped from it. Extending along that guide produced an immediate
> `ImportError`. What follows describes the files that are actually here;
> [docs/apple-health.md](../../../docs/apple-health.md) is the long-form
> companion and agrees with this layout.

## What is actually here

```
mirobody/pulse/apple/
├── platform.py               # AppleHealthPlatform — registers both providers,
│                             #   post_data() drives format + store
├── provider.py               # AppleHealthProvider (health records)
│                             # CDAProvider (CDA documents; slug "cda")
├── models.py                 # FlutterHealthTypeEnum, AppleHealthRecord,
│                             #   MetaInfo, AppleHealthRequest, and
│                             #   FLUTTER_TO_RECORD_TYPE_MAPPING (type → indicator)
├── statistics_service.py     # /apple/statistics batches → summary records
└── services/
    └── database_service.py   # provider-link rows for apple_health / cda
```

The HTTP surface lives in `mirobody/server/routers/apple_router.py`:

| Endpoint | Handled by |
| --- | --- |
| `POST /apple/health` | `AppleHealthProvider.format_data` via `platform.post_data("apple_health", …)` |
| `POST /apple/statistics` | `statistics_service.process_apple_health_statistics` |
| `POST /apple/cda` | `CDAProvider.format_data` via `platform.post_data("cda", …)` |

All three accept optional gzip bodies (`Content-Encoding: gzip`); the router
caps decompressed size, because a compressed body is attacker-shaped input.

## Features

- **No authentication dance**: data arrives over the API under the caller's
  JWT — no OAuth link step. Both providers report `LinkType.NONE`.
- **Batch processing**: a single upload can carry many record types; inserts
  are batched (1000 records per batch).

## Request format (`POST /apple/health`)

```json
{
    "request_id": "unique_request_id",
    "metaInfo": {
        "timezone": "Asia/Shanghai"
    },
    "healthData": [
        {
            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "type": "HEART_RATE",
            "dateFrom": 1705284600000,
            "dateTo": 1705284600000,
            "value": {"numericValue": 72},
            "unitSymbol": "bpm",
            "sourceId": "com.apple.health",
            "timezone": "Asia/Shanghai"
        }
    ]
}
```

- Pydantic-validated; `uuid` and `type` are required, `type` must be a
  `FlutterHealthTypeEnum` value; invalid data returns 400.
- `type` maps to a standard indicator through
  `FLUTTER_TO_RECORD_TYPE_MAPPING` in `models.py` — 50+ types across vital
  signs, activity, body measurements, sleep and nutrition; sleep stages map
  to the dedicated `StandardIndicator` sleep types.

## Data flow

1. The router parses (and, if needed, size-capped-decompresses) the body.
2. `platform_manager.get_platform("apple").post_data(provider_slug, …)` looks
   up the provider — both `apple_health` and `cda` are registered in
   `AppleHealthPlatform._register_built_in_providers` (the CDA registration
   was once missing, which made `/apple/cda` permanently answer
   `{"success": false}`; `test_provider_registration.py` pins it now).
3. The provider's `format_data` returns `StandardPulseData`.
4. `VitalHealthService.process_standard_data` stores the records; unit
   conversion happens on that shared ingest path
   (see [`../standardize/README.md`](../standardize/README.md)).

## Extending to a new data type

There is no handler registry to extend. To accept a new Apple Health type:

1. Add the member to `FlutterHealthTypeEnum` in `models.py`.
2. Map it in `FLUTTER_TO_RECORD_TYPE_MAPPING` to a `StandardIndicator`
   (add the indicator to the catalogue first if it is new — see
   `../standardize/indicators_info.py`).
3. If the value shape is unusual, teach
   `AppleHealthProvider._extract_value` about it in `provider.py`.

## Important notes

- Timestamps arrive as millisecond epochs plus an explicit timezone;
  everything is converted to standard `StandardPulseData` on the way in.
- ZoneInfo objects are cached per timezone string — the hot path avoids
  re-parsing on every record.

# Apple Health Platform

Receives Apple Health data a client app already holds: HealthKit records,
pre-aggregated statistics, and clinical (CDA/FHIR) documents. HealthKit has no
server API, so nothing here pulls; [docs/apple-health.md](../../../../docs/apple-health.md)
is the long-form companion, with the request format and the export importer.

## What is here

```
mirobody/collect/providers/apple/
├── platform.py               # AppleHealthPlatform: registers both providers,
│                             #   post_data() formats, stores, then starts an
│                             #   incremental aggregation
├── provider.py               # AppleHealthProvider (health records, slug "apple_health")
│                             # CDAProvider (clinical documents, slug "cda")
├── models.py                 # AppleHealthRequest, AppleHealthRecord, MetaInfo,
│                             #   and the statistics request models
├── statistics_service.py     # /apple/statistics batches -> summary records
└── services/
    └── database_service.py   # the LLM-access flag on an apple_health or cda link
```

The HTTP surface lives in `mirobody/server/routers/apple_router.py`:

| Endpoint | Handled by |
| --- | --- |
| `POST /apple/health` | `AppleHealthProvider.format_data` via `platform.post_data("apple_health", …)` |
| `POST /apple/statistics` | `statistics_service.process_apple_health_statistics` |
| `POST /apple/cda` | `CDAProvider.format_data` via `platform.post_data("cda", …)` |

All three accept gzip bodies (`Content-Encoding: gzip`); the router caps the
decompressed size, because a compressed body is attacker-shaped input. Data
arrives under the caller's token, so there is no link step: both providers
report `LinkType.NONE`.

## Request format (`POST /apple/health`)

```json
{
    "request_id": "unique_request_id",
    "metaInfo": {"timezone": "Asia/Shanghai"},
    "healthData": [
        {
            "type": "HKQuantityTypeIdentifierHeartRate",
            "startDate": 1705284600000,
            "endDate": 1705284600000,
            "value": 72,
            "unit": "count/min",
            "sourceName": "Apple Watch",
            "sourceId": "com.apple.health"
        }
    ]
}
```

`type` is a HealthKit identifier and `unit` is the record's own, as Apple
writes them. The body is validated whole as `AppleHealthRequest`; a record
that does not fit `AppleHealthRecord` fails the request with a 400.

## Data flow

1. The router decompresses and validates the body.
2. `platform_manager.get_platform("apple").post_data(provider_slug, …)` looks
   up the provider. Both `apple_health` and `cda` are registered in
   `AppleHealthPlatform._register_built_in_providers`; the CDA registration
   was once missing, and `/apple/cda` then answered `{"success": false}` to
   every request.
3. `AppleHealthProvider.format_data` decodes each record with
   `mirobody.kernel.decoders.apple`, the table `mirobody import apple` reads
   too, so both front doors agree on every type, unit and sleep stage. A
   record of a type the table does not carry is dropped and counted.
4. `StandardHealthService.process_standard_data` stores the records.

`CDAProvider` stores a document's medications as medication plans
(`meds.MedicationPlan`), never as readings.

## Adding a data type

Add the HealthKit identifier to `QUANTITY` (or `CATEGORY`) in
`mirobody/kernel/decoders/apple.py`, pointing at a catalogue metric, and a
case to `mirobody/kernel/decoders/samples/apple/` worked out by hand. Nothing
in this directory changes.

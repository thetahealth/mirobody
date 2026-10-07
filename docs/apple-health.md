# Apple Health: export and CDA import

Two front doors, one vocabulary. Both decode through
[`mirobody/kernel/decoders/apple.py`](../mirobody/kernel/decoders/apple.py), so a
type name means the same thing whichever way the data arrives.

| | What it reads | Needs |
| --- | --- | --- |
| `mirobody import apple export.zip` | the archive the iOS Health app writes (Summary → your picture → Export All Health Data) | nothing: no key, no database, no extra |
| `POST /apple/health` | JSON pushed by a client app | a running server and a token |

## The vocabulary is HealthKit's own

**`type` is a HealthKit identifier**: `HKQuantityTypeIdentifierHeartRate`, not
`HEART_RATE`. That is what `export.xml` carries, what HealthKit names a type on
the device, and what this endpoint now accepts.

Earlier releases took a second vocabulary here, a `FlutterHealthTypeEnum` of
names like `HEART_RATE`, and mapped it onto the same catalogue with a 74-row
table. It described one client that no longer exists, and two tables for one
mapping is how they drift. If you are moving a client off it: send Apple's
identifier, `startDate`/`endDate` instead of `dateFrom`/`dateTo`, a plain
`value` instead of `{"numericValue": n}`, and `unit` instead of `unitSymbol`.

## POST /apple/health

Accepts gzip (`Content-Encoding: gzip`).

```json
{
    "request_id": "unique_request_id",
    "metaInfo": {
        "timezone": "Asia/Shanghai",
        "taskId": "optional, marks records uploaded in one batch"
    },
    "healthData": [
        {
            "type": "HKQuantityTypeIdentifierHeartRate",
            "startDate": 1705284600000,
            "endDate": 1705284600000,
            "value": 72,
            "unit": "count/min",
            "sourceName": "Apple Watch",

            "uuid": "550e8400-e29b-41d4-a716-446655440000",
            "sourceId": "com.apple.health",
            "timezone": "Asia/Shanghai",
            "sourcePlatform": "iOS",
            "sourceDeviceId": "device123",
            "recordingMethod": "automatic",
            "createdAt": 1705284600000
        }
    ]
}
```

Only the first block is read. `startDate`/`endDate` take epoch milliseconds or
Apple's own date string (`2026-06-01 08:00:00 +0800`); `endDate` defaults to
`startDate`. A record with no parsable start decodes to nothing, never to a
guessed time.

**`unit` is per record, not per type.** Apple writes it that way (it is CDATA
in their DTD and follows the device's region), so one upload can carry `mg/dL`
and `mmol/L` for the same type. Every value is converted by reading its own
unit. Send what Apple gave you and do not pre-convert.

**Percent-typed quantities are fractions.** `HKUnit.percent()` ranges 0.0 to
1.0, so an oxygen saturation of 98% is `0.98`.

### Category records

A category record's `value` is the `HKCategoryValue*` name as a string, kept
verbatim:

```json
{"type": "HKCategoryTypeIdentifierSleepAnalysis",
 "value": "HKCategoryValueSleepAnalysisAsleepDeep",
 "startDate": 1736895600000, "endDate": 1736901000000}
```

A sleep record carries no number: its value is the stage and its measurement is
the span, so the duration is `endDate - startDate` in milliseconds. The four
stages that are time asleep (Deep, Core, REM, Unspecified) each also land as a
`sleepAnalysis_Asleep(Total)` record. `InBed` and `Awake` do not.

### Blood pressure

Apple writes a cuff reading as two separate records, and so should you. They
are recognised as one measurement when they share a `startDate` and a
`sourceName`. A `HKCorrelationTypeIdentifierBloodPressure` record carrying
`systolic` and `diastolic` is also accepted.

## What the endpoint accepts

47 identifiers: 35 quantity types, 10 category types, sleep analysis and the
blood pressure correlation. The tables in
[`mirobody/kernel/decoders/apple.py`](../mirobody/kernel/decoders/apple.py) are
the only authoritative list, and they are one command away, so nothing here can
drift into being a second wrong copy:

```bash
python -c "from mirobody.kernel.decoders import apple as a; \
           print(len(a.DATA_TYPES)); [print(t) for t in sorted(a.DATA_TYPES)]"
```

Covered: vital signs, activity and fitness, body measurements, nutrition, sleep
stages, UV exposure, and ten reproductive-health category types.

**Not covered, deliberately**: body water, bone mass, muscle and visceral fat,
body age, protein percentage. Those are not HealthKit identifiers. They came
from a body-scale vendor's own API, so nothing in an Apple export can produce
them, and they belong to whichever scale integration reads that vendor.

An identifier the table does not carry is dropped, not rejected: the shape was
right, this build just does not carry that type. The upload succeeds, and the
server log counts the records dropped.

A record that does not fit the model, such as one without a `type` or with a
`value` that is an object (the `{"numericValue": n}` of the vocabulary this
endpoint dropped), fails the whole request with a 400 and nothing in it is
stored. A record with no parsable `startDate`, one that sends `dateFrom`
instead, fits the model, decodes to nothing and is counted as dropped.

## Adding a data type

One table, one file. Add the identifier to `QUANTITY` (or `CATEGORY`) in
[`mirobody/kernel/decoders/apple.py`](../mirobody/kernel/decoders/apple.py),
pointing at a catalogue metric. If that metric does not exist yet, add it to
`StandardIndicator` in
[`mirobody/translate/indicators_info.py`](../mirobody/translate/indicators_info.py)
with its canonical unit, and see that package's
[README](../mirobody/translate/README.md).

Add a case to `mirobody/kernel/decoders/samples/apple/` in the same change: the
expected numbers there are worked by hand, never read back from the decoder,
which is what makes them evidence.

## Response

```json
{"success": true, "data": {"request_id": "unique_request_id"},
 "message": "Apple Health data processed successfully"}
```

```json
{"success": false, "message": "Error message"}
```

## Notes

- **Duplicates** are the client's problem: deduplicate before sending.
- **Volume**: large uploads are fine, and gzip is worth using.
- **Time**: send the offset. A record's own `timezone` wins over `metaInfo`.

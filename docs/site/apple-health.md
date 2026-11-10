# Apple Health

Import an export from the iOS Health app, or send HealthKit records from an
authorized client app. Apple Health does not provide a server-side OAuth
connection like the device cloud services.

## Import an export

In Health, open your profile and choose **Export All Health Data**.
The base Python package can read the archive without a model or database:

```bash
mirobody import apple export.zip
```

To store it in a person's record, use an application installation with its
server configuration:

```bash
mirobody import apple export.zip --into <user-id>
```

Re-importing the same archive does not add another copy. When export data and
phone pushes overlap, daily aggregation selects a source rather than adding
both streams together.

Each reading counts on the day the phone showed: Apple writes every time
with its offset, and the import reads the day in that offset. An archive
imported by an earlier release, which read every day in UTC unless `--tz`
was given, is corrected by importing it again.

## Client integration

Authenticated clients can send HealthKit JSON to `POST /apple/health`.
Use Apple's identifiers, such as `HKQuantityTypeIdentifierHeartRate`, and
send each record's original unit with `startDate` and `endDate`.

- Dates accept epoch milliseconds or Apple's date strings with an offset.
- Percentage quantities use fractions: 98% oxygen saturation is `0.98`.
- Category values use the corresponding `HKCategoryValue*` string.
- Clients should deduplicate live pushes before sending them.

CDA documents use the application’s CDA import path.
Supported types, request examples and validation behavior are in the
[Apple Health reference](../apple-health.md).

# Gate Tests — format_data()

Acceptance tests for `Provider.format_data()`. Feeds raw input data and verifies the output `StandardPulseData` matches expectations.

## Usage

```bash
# Run all tests
python -m pytest test_format_data.py -v

# Update snapshots (after format_data output changes)
python -m pytest test_format_data.py --update-snapshots -v
```

## Structure

```
gate_tests/
├── format_tester.py          # Core test framework
├── test_format_data.py       # pytest entry (parametrized)
├── conftest.py               # --update-snapshots option
├── pytest.ini
└── fixtures/
    ├── apple_health/         # Apple Health (3 fixtures)
    ├── theta_garmin/         # Garmin Connect (8 fixtures)
    └── theta_whoop/          # Whoop (5 fixtures)
```

## Fixture Format

```json
{
  "test_id": "platform_datatype_001",
  "provider_class": "mirobody.pulse.xxx.ProviderClass",
  "platform": "theta | apple",
  "mock_context": { "_get_user_timezone": "America/Los_Angeles" },
  "input": { ... },
  "expected": {
    "success": true,
    "health_data_count": 15,
    "required_indicators": ["heartRates", "dailySteps"],
    "snapshot": { ... }
  }
}
```

## Validation

1. **Rule assertions** — exact `health_data_count`, all `required_indicators` present
2. **Snapshot comparison** — full output matched field-by-field against `expected.snapshot` (skips dynamic fields like requestId, timestamp)

## Adding a New Fixture

1. Create a JSON file under `fixtures/<platform>/` with input and mock_context
2. Set `expected.snapshot` to `null`
3. Run `--update-snapshots` to auto-fill the snapshot
4. Fill in `health_data_count` and `required_indicators`
5. Run normally to confirm all tests pass

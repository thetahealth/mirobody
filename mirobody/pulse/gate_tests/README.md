# Gate Tests — format_data()

Acceptance tests for `Provider.format_data()`. Feeds raw input data and verifies the output `StandardPulseData` matches expectations.

## Why this is a directory, and why it is not in a top-level `tests/`

Every other test in this repo is a `test_*.py` sitting next to the code it
covers, and `pyproject.toml` says so explicitly — there is no top-level
`tests/` directory. The README used to document one anyway, and following it
produced "0 tests collected".

These do not fit that shape: they need 21 recorded JSON fixtures and a
377-line runner. So they get a directory — but one that still lives *next to
the code it covers*, inside `pulse/`, alongside the providers whose
`format_data()` it gates. The convention is satisfied; only the file count
differs.

Moving them to a top-level `tests/` would trade that for a second, competing
test convention and re-create the directory the packaging config was written
to avoid. They are already excluded from the wheel
(`[tool.setuptools.packages.find] exclude`), so they cost a consumer nothing.

They also carry no separate `pytest.ini` any more. That file set
`asyncio_mode` (already set in `pyproject.toml`) and disabled two plugins, and
its real effect was to become the *configfile* whenever these tests were run
directly — shifting pytest's rootdir into this folder so the root
configuration, including the registered markers, silently did not apply. The
same tests behaved differently depending on how you invoked them.

## Usage

Run from the repository root, like every other test here:

```bash
# Run all gate tests
pytest mirobody/pulse/gate_tests

# Update snapshots (only after a DELIBERATE change to format_data output)
pytest mirobody/pulse/gate_tests --update-snapshots
```

They also run as part of the whole suite (`pytest`), which is the usual way.

The paths above are repo-root-relative on purpose: the previous form
(`python -m pytest test_format_data.py`) only worked if you had already cd'd
into this directory, and printed a bare "no tests ran" if you had not.

## Structure

```
gate_tests/
├── format_tester.py          # Core test framework
├── test_format_data.py       # pytest entry (parametrized)
├── conftest.py               # --update-snapshots option
└── fixtures/
    ├── apple_health/         # Apple Health (3 fixtures)
    ├── theta_garmin/         # Garmin Connect (8 fixtures)
    ├── theta_oura/           # Oura (5 fixtures)
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

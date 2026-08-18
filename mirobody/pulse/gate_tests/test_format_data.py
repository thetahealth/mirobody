"""Snapshot acceptance tests for `Provider.format_data()`.

The contract every data source converges on: raw vendor payload in,
`StandardPulseData` out. 21 recorded fixtures across Apple Health, Garmin,
Oura and Whoop, checked against expectation rules plus a stored snapshot.

Run: `pytest mirobody/pulse/gate_tests`, or with `--update-snapshots` after a
deliberate change to format_data output. (The fixtures path is
`gate_tests/fixtures/`; this docstring used to say `tests/fixtures/`, a
top-level directory that has never existed in this repo.)
"""

import pytest
from pathlib import Path

from .format_tester import FormatTestRunner

runner = FormatTestRunner(fixtures_dir=Path(__file__).parent / "fixtures")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", runner.cases, ids=runner.case_ids)
async def test_format_data(case, request):
    if request.config.getoption("--update-snapshots"):
        result = await runner.run_and_update_snapshot(case)
        assert result.passed, result.error_report
    else:
        result = await runner.run(case)
        assert result.passed, result.error_report

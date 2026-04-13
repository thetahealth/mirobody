"""
Parametrized tests for Provider.format_data()

Uses FormatTestRunner to load fixtures from tests/fixtures/ and validate
each provider's format_data output against expected rules + snapshots.
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

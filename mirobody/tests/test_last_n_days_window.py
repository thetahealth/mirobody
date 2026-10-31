"""'The last N days' is N calendar days ending today, both ends counted.

`start` and `end` of `query_health_indicators` are inclusive local dates. A
model that wrote the last 60 days as today minus 60 read 61 and said so in
its answer ("61 of the last 61 days"). The schema now carries the conversion
as a worked example; these tests hold that example to the window
`resolve_window` actually builds, so the sentence the model copies from and
the arithmetic the server does cannot drift apart.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta

import pytest

from mirobody.kernel import query

DAY_MS = 86_400_000
_EXAMPLE = re.compile(
    r"on (\d{4}-\d{2}-\d{2}) the last (\d+) days are start=(\d{4}-\d{2}-\d{2}) "
    r"\((\d{4}-\d{2}-\d{2}) would read (\d+) days\)"
)


def _example() -> tuple[str, int, str, str, int]:
    text = query.TOOL_SCHEMA["properties"]["start"]["description"]  # type: ignore[index]
    match = _EXAMPLE.search(text)
    assert match, "the start description lost its worked 'last N days' example"
    today, n, start, early, early_n = match.groups()
    return today, int(n), start, early, int(early_n)


def _days(start: str, end: str) -> float:
    """Days the window between two inclusive dates spans. UTC, so every day
    is 24 hours and the count is exact."""
    window = query.resolve_window("UTC", start=start, end=end, hours=24, now=datetime(2026, 10, 31, tzinfo=UTC))
    return (window.end_ms - window.start_ms) / DAY_MS


def test_the_worked_example_reads_exactly_n_days():
    today, n, start, _, _ = _example()
    assert _days(start, today) == n


def test_the_start_it_warns_against_reads_one_day_more():
    today, n, _, early, early_n = _example()
    assert early_n == n + 1
    assert _days(early, today) == early_n


@pytest.mark.parametrize("n", [1, 7, 14, 30, 60])
def test_n_minus_one_days_before_today_is_n_days(n):
    """The rule the example illustrates, for the windows people ask about."""
    today = date(2022, 1, 16)
    start = today - timedelta(days=n - 1)
    assert _days(start.isoformat(), today.isoformat()) == n

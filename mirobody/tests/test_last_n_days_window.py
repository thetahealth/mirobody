"""'The last N days' is N calendar days ending today, both ends counted.

`start` and `end` of `query_health_indicators` are inclusive local dates.
Asked about the last 60 days, DeepSeek wrote today minus 60 and read 61 days
("61 of the last 61 days"), and a local 2B model read about a month. So the
server does the arithmetic: `days=N` becomes the N local dates ending today,
in the person's time zone. A model that still writes `start` itself has a
worked example in the schema; these tests hold both the example and `days`
to the window `resolve_window` actually builds.
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


# --- days: the server counts -------------------------------------------------------


@pytest.mark.parametrize("value", [7, 7.0, "7"])
def test_days_parses_from_what_a_model_sends(value):
    assert query.parse_request({"indicators": ["Steps"], "days": value}).days == 7


@pytest.mark.parametrize("value", [0, -3, 7.5, "a week", True, query.DAYS_MAX + 1])
def test_days_that_is_not_a_whole_number_in_range_is_refused(value):
    problems = query.validate_request({"indicators": ["Steps"], "days": value})
    assert [p.parameter for p in problems] == ["days"]


def test_days_with_a_start_is_refused_and_with_an_end_is_not():
    """A start would contradict days; an end says where the N days stop."""
    problems = query.validate_request({"indicators": ["Steps"], "days": 7, "start": "2026-01-01"})
    assert [p.parameter for p in problems] == ["days+start"]
    assert query.validate_request({"indicators": ["Steps"], "days": 7, "end": "2026-01-10"}) == ()


def test_days_ends_on_the_persons_today_not_the_servers():
    """20:00 UTC on the 16th is already the 17th in Shanghai."""
    request = query.parse_request({"indicators": ["Steps"], "days": 7})
    resolved = query.resolve_days(request, "Asia/Shanghai", datetime(2022, 1, 16, 20, tzinfo=UTC))
    assert (resolved.start, resolved.end, resolved.days) == ("2022-01-11", "2022-01-17", None)


def test_days_with_an_end_counts_back_from_it():
    request = query.parse_request({"indicators": ["Steps"], "days": 7, "end": "2025-03-09"})
    resolved = query.resolve_days(request, "UTC", datetime(2026, 10, 31, tzinfo=UTC))
    assert (resolved.start, resolved.end) == ("2025-03-03", "2025-03-09")


@pytest.mark.parametrize("n", [1, 7, 14, 30, 60])
def test_days_resolves_to_a_window_of_n_days(n):
    request = query.resolve_days(query.parse_request({"indicators": ["Steps"], "days": n}), "UTC",
                                 datetime(2022, 1, 16, 21, tzinfo=UTC))
    assert _days(request.start, request.end) == n


def test_a_request_without_days_is_left_as_it_was():
    request = query.parse_request({"indicators": ["Steps"], "start": "2022-01-01"})
    assert query.resolve_days(request, "UTC", datetime(2022, 1, 16, tzinfo=UTC)) is request


async def test_the_service_reads_and_reports_the_n_days():
    """The store gets the N days and the answer names them, so the model
    can say which dates it read."""
    pytest.importorskip("dotenv")
    from unittest.mock import Mock

    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService

    hq = Mock()
    hq.tz.return_value = "Asia/Shanghai"
    hq.on_read.return_value = None
    hq.stats.return_value = [{"indicator": "Steps", "count": 7, "days": 7, "unit": "{#}"}]
    service = HealthIndicatorsService(hq, now=lambda: datetime(2022, 1, 16, 20, tzinfo=UTC), outside_note=False)
    envelope = await service.envelope({"user_id": "1"}, indicators=["Steps"], view="stats", days=7)
    window = hq.stats.call_args.args[2]
    assert (window.end_ms - window.start_ms) == 7 * DAY_MS
    assert envelope.meta.window == ("2022-01-11", "2022-01-17")

"""'The last N days' is N calendar days ending today, both ends counted.

`start` and `end` of `query_health_indicators` and `query_medications` are
inclusive local dates. A model that wrote the last 60 days as today minus 60
read 61 and said so in its answer ("61 of the last 61 days"), and the dose
log's own default, "the last 30 days", was built the same way and read 31.
Both schemas now quote one worked example (`query.LAST_N_DAYS_EXAMPLE`);
these tests hold that example to the window `resolve_window` actually builds,
and the dose log's default to the same arithmetic, so the sentence the model
copies from and the windows the server reads cannot drift apart.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from mirobody.kernel import meds, query, tools

DAY_MS = 86_400_000
_EXAMPLE = re.compile(
    r"on (\d{4}-\d{2}-\d{2}) the last (\d+) days are start=(\d{4}-\d{2}-\d{2}) "
    r"\((\d{4}-\d{2}-\d{2}) would read (\d+) days\)"
)
TODAY = date(2026, 6, 10)


def _example() -> tuple[str, int, str, str, int]:
    match = _EXAMPLE.search(query.LAST_N_DAYS_EXAMPLE)
    assert match, "LAST_N_DAYS_EXAMPLE lost its worked 'last N days' example"
    today, n, start, early, early_n = match.groups()
    return today, int(n), start, early, int(early_n)


def _days(start: str, end: str) -> float:
    """Days the window between two inclusive dates spans. UTC, so every day
    is 24 hours and the count is exact."""
    window = query.resolve_window("UTC", start=start, end=end, hours=24, now=datetime(2026, 10, 31, tzinfo=UTC))
    return (window.end_ms - window.start_ms) / DAY_MS


@pytest.mark.parametrize("schema", [query.TOOL_SCHEMA, meds.TOOL_SCHEMA], ids=[query.TOOL_NAME, meds.TOOL_NAME])
def test_both_dated_tools_publish_the_example(schema):
    assert query.LAST_N_DAYS_EXAMPLE in schema["properties"]["start"]["description"]


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


@pytest.mark.parametrize(
    ("args", "window"),
    [
        # No dates: the published default, the last 30 days, today the 30th.
        ({}, (date(2026, 5, 12), TODAY)),
        # Only `end`: the same 30 days, ending there.
        ({"end": "2026-05-31"}, (date(2026, 5, 2), date(2026, 5, 31))),
        # The example's rule, written by the model, reads as written.
        ({"start": "2026-06-04", "end": "2026-06-10"}, (date(2026, 6, 4), TODAY)),
    ],
    ids=["default", "end-only", "explicit"],
)
async def test_the_dose_log_reads_the_days_it_says(args, window):
    medications_service = pytest.importorskip("mirobody.agent.tools.medications_service")
    asked: list[tuple[date, date]] = []
    service = medications_service.MedicationsService(
        store=SimpleNamespace(list=lambda _subject: []),
        dose_log=SimpleNamespace(list=lambda _subject, w: asked.append(w) or []),
        tz=lambda _subject: "UTC", now=lambda: datetime(2026, 6, 10, 12, tzinfo=UTC),
    )

    env = await service.envelope({"user_id": "synthetic-subject"}, view=meds.VIEW_LOG, **args)

    assert env.status == tools.STATUS_OK
    assert asked == [window]
    assert env.meta.window == (window[0].isoformat(), window[1].isoformat())
    if "start" not in args:
        assert f"the last {meds.LOG_DEFAULT_DAYS} days" in meds.TOOL_SCHEMA["properties"]["start"]["description"]
        assert _days(*env.meta.window) == meds.LOG_DEFAULT_DAYS

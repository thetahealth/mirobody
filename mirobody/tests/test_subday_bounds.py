"""`query_health_indicators`'s `start`/`end` can reach into a single day.

`view="minute"` keeps the newest `query.BUCKET_CAP` (92) points per
indicator, `view="raw"` the newest `query.ROW_CAP` (50): a wearable sampling
about once a minute has ~1,400 points a day, so the early hours of a busy
day were unreachable either way. `start`/`end` only ever took a whole
calendar date, and the cut notice's own advice, `end=<the day before>`,
skipped the rest of the SAME day rather than reaching it (#303).

`start`/`end` now also accept a local date-time (`YYYY-MM-DDTHH:MM`) when
`view` is `raw` (the schema's own default), `minute` or `hour`
(`query.SUBDAY_VIEWS`); every other view keeps dates only, because day and
coarser, `stats` and `trend` already answer from the elected DAY authority
or the whole window, so a time there would parse but narrow nothing while
looking like it did.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import pytest

from mirobody.kernel import meds, query

# The cut note lives in the agent's tool layer, which reads the config and so
# needs python-dotenv: absent from CI's minimal install (`.[test]` only), where
# the schema and window tests above it must still run.
_service = None


def _agent_helpers():
    global _service
    if _service is None:
        pytest.importorskip("dotenv")
        from mirobody.agent.tools import health_indicators_service as _service
    return _service

_PATTERN = re.compile(query.TOOL_SCHEMA["properties"]["start"]["pattern"])


# --- the published schema accepts/rejects -----------------------------------------------


@pytest.mark.parametrize("text", ["2026-03-10", "2026-03-10T21:59", "2026-03-10T00:00"])
def test_the_schema_pattern_accepts_a_date_or_a_minute_precision_datetime(text):
    assert _PATTERN.fullmatch(text)


@pytest.mark.parametrize("text", ["2026-03-10T21", "2026-03-10 21:59", "2026-03-10T21:59:00", "21:59", "not-a-date"])
def test_the_schema_pattern_rejects_anything_else(text):
    assert not _PATTERN.fullmatch(text)


# --- reject_dates / validate_request: view decides whether a time is allowed ------------


@pytest.mark.parametrize("view", query.SUBDAY_VIEWS)
def test_a_minute_precision_datetime_is_accepted_with_a_subday_view(view):
    assert query.validate_request({"view": view, "start": "2026-03-10T06:00", "end": "2026-03-10T21:59"}) == ()


@pytest.mark.parametrize("view", [v for v in query.VIEWS if v not in query.SUBDAY_VIEWS])
def test_a_minute_precision_datetime_is_refused_outside_a_subday_view(view):
    problems = query.validate_request({"view": view, "end": "2026-03-10T21:59"})
    assert problems and problems[0].parameter == "end"
    assert "YYYY-MM-DD" in problems[0].reason


def test_a_minute_precision_datetime_is_accepted_with_no_view_named_since_raw_is_the_default():
    # `view` defaults to "raw" (`TOOL_SCHEMA`), itself one of `SUBDAY_VIEWS`:
    # a model that relies on the default rather than writing `view="raw"`
    # must not lose the date-time option because of it (`default_view`).
    assert query.validate_request({"end": "2026-03-10T21:59"}) == ()
    assert query.parse_request({"end": "2026-03-10T21:59"}).view == "raw"


@pytest.mark.parametrize("bad", ["2026-02-30T06:00", "2026-03-10T24:00", "2026-03-10T06:60"])
def test_an_invalid_calendar_datetime_is_refused_even_with_a_subday_view(bad):
    problems = query.validate_request({"view": "minute", "start": bad})
    assert problems and problems[0].parameter == "start"


def test_medications_keeps_dates_only_regardless_of_its_own_view_values():
    # `meds.TOOL_SCHEMA` shares `query.reject_dates` but never passes
    # `subday_views`: its own views ("plan"/"log"/"history") are not
    # minute/hour either way, but the default must stay strict even if they
    # collided, because a dose has no sub-day grammar to narrow into.
    problems = meds.validate_query({"view": "log", "start": "2026-03-10T06:00"})
    assert problems and problems[0].parameter == "start"


def test_reject_dates_default_is_unchanged_date_only():
    assert query.reject_dates({"start": "2026-03-10"}) == []
    assert query.reject_dates({"start": "2026-03-10T06:00"})


# --- window resolution: the datetime end is inclusive through that minute --------------


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def test_a_datetime_end_is_inclusive_through_its_minute_like_a_date_end_is_through_its_day():
    now = datetime(2026, 3, 11, tzinfo=UTC)
    window = query.resolve_window("UTC", start="2026-03-10T06:00", end="2026-03-10T21:59", hours=24, now=now)
    assert window.start_ms == _ms(datetime(2026, 3, 10, 6, 0, tzinfo=UTC))
    # Inclusive through 21:59 means the right edge is 22:00, exactly as a
    # date end's right edge is the NEXT day's midnight.
    assert window.end_ms == _ms(datetime(2026, 3, 10, 22, 0, tzinfo=UTC))


def test_a_reading_exactly_on_the_minute_boundary_is_included_the_next_is_not():
    now = datetime(2026, 3, 11, tzinfo=UTC)
    window = query.resolve_window("UTC", start="2026-03-10", end="2026-03-10T21:59", hours=24, now=now)
    on_boundary = _ms(datetime(2026, 3, 10, 21, 59, 0, tzinfo=UTC))
    past_boundary = _ms(datetime(2026, 3, 10, 22, 0, 0, tzinfo=UTC))
    assert window.start_ms <= on_boundary < window.end_ms
    assert not (window.start_ms <= past_boundary < window.end_ms)


def test_a_plain_date_bound_is_unaffected():
    now = datetime(2026, 3, 11, tzinfo=UTC)
    window = query.resolve_window("UTC", start="2026-03-10", end="2026-03-10", hours=24, now=now)
    assert window.start_ms == _ms(datetime(2026, 3, 10, tzinfo=UTC))
    assert window.end_ms == _ms(datetime(2026, 3, 11, tzinfo=UTC))


# --- the cut note: a sub-day view points at a minute, not the day before ---------------


def _minute_rows(indicator: str, day: str, periods: list[str], total: int) -> list[dict]:
    return [{"indicator": indicator, "period": f"{day} {p}", "total": total} for p in periods]


def test_the_cut_note_for_minute_points_at_one_minute_before_the_first_point_shown():
    # The issue's own repro: 92 of ~1,400 minute points, the newest 22:00-23:31.
    periods = [f"{hh:02d}:{mm:02d}" for hh in (22, 23) for mm in range(60)][:92]
    rows = _minute_rows("hr", "2026-03-10", periods, total=1400)
    note = _agent_helpers()._cut_note("minute", rows, 92)
    assert "end=2026-03-10T21:59" in note
    assert "end=2026-03-09" not in note
    assert "view=hour" in note


def test_the_cut_note_for_hour_points_at_the_last_minute_of_the_preceding_hour():
    # An hour bucket's own period is its top of hour; one bucket back would
    # be a whole hour short of the true edge (see `_minute_before`).
    periods = [f"{hh:02d}:00" for hh in range(10, 20)]
    rows = _minute_rows("hr", "2026-03-10", periods, total=40)
    note = _agent_helpers()._cut_note("hour", rows, 40)
    assert "end=2026-03-10T09:59" in note
    assert "end=2026-03-09" not in note
    assert "view=day" in note


def test_the_cut_note_for_day_still_points_at_the_day_before():
    rows = [{"indicator": "steps", "period": f"2026-06-{d:02d}", "total": 120} for d in range(1, 10)]
    note = _agent_helpers()._cut_note("day", rows, 92)
    assert "end=2026-05-31" in note
    assert "T" not in note.split("end=")[-1][:10]


def _raw_rows(indicator: str, times: list[str], total: int) -> list[dict]:
    return [{"indicator": indicator, "time": t, "total": total} for t in times]


def test_the_raw_cut_note_points_at_one_minute_before_the_oldest_reading_shown():
    # A day with far more than ROW_CAP (50) readings: the newest 50 are
    # shown, the oldest of them at 23:01:30 - its own MINUTE, not the day
    # before, is what the next call must exclude to keep walking backward.
    times = [f"2026-03-10 23:{mm:02d}:30.000000" for mm in range(1, 51)]
    rows = _raw_rows("hr", times, total=1400)
    note = _agent_helpers()._raw_cut_note(rows, 50)
    assert "end=2026-03-10T23:00" in note
    assert "end=2026-03-09" not in note
    assert "view=day" in note and "view=stats" in note


def test_the_raw_cut_note_crosses_into_the_previous_day_at_midnight():
    times = [f"2026-03-10 00:{mm:02d}:00" for mm in range(50)]
    rows = _raw_rows("hr", times, total=1400)
    note = _agent_helpers()._raw_cut_note(rows, 50)
    assert "end=2026-03-09T23:59" in note


@pytest.mark.parametrize(("period", "expected"), [
    ("2026-03-10 22:00", "2026-03-10T21:59"),
    ("2026-01-01 00:00", "2025-12-31T23:59"),
])
def test_minute_before_crosses_hour_and_day_boundaries(period, expected):
    assert _agent_helpers()._minute_before(period) == expected


def test_minute_before_is_empty_for_an_unparseable_period():
    assert _agent_helpers()._minute_before("not-a-period") == ""

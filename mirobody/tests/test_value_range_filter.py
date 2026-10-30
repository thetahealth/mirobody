"""A counting question has to be expressible, and its count has to be right.

"How many days did I sleep less than an hour of deep sleep" is a band plus a
count. The tool returned the whole series and left both to the model; asked
that PHIA question, a local 2B answered 30.75, a duration, not a number of
days. `value_min` / `value_max` with `view="stats"` answer it with
`count_in_range` and `days_in_range`. The band is compared on the basis the
row's min/max/avg use, refused when it is not a finite number, said when a
view does not count, and withheld for a series stored in pieces. The SQL is
exercised by the Postgres suite (`test_postgres_regressions.py`).
"""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from mirobody.kernel import query


def test_a_band_parses_into_the_request():
    request = query.parse_request(
        {"indicators": ["dailyDeepSleep"], "view": "stats", "value_min": 1000, "value_max": 5000})
    assert (request.value_min, request.value_max) == (1000.0, 5000.0)
    assert request.banded and not request.band_unapplied


def test_one_sided_bounds_are_allowed_and_absent_stays_absent():
    lower = query.parse_request({"indicators": ["dailyDeepSleep"], "value_min": "900"})
    assert lower.value_min == 900.0 and lower.value_max is None
    upper = query.parse_request({"indicators": ["dailyDeepSleep"], "value_max": 6})
    assert upper.value_max == 6.0 and upper.value_min is None
    bare = query.parse_request({"indicators": ["dailyDeepSleep"]})
    assert bare.value_min is None and bare.value_max is None and not bare.banded


@pytest.mark.parametrize("bad", ["abc", "nan", "inf", float("nan"), True])
def test_a_bound_that_is_not_a_finite_number_is_refused(bad):
    """Ignored, a bad bound counted every reading as in the band."""
    problems = query.validate_request({"indicators": ["dailyDeepSleep"], "view": "stats", "value_min": bad})
    assert [p.parameter for p in problems] == ["value_min"]


def test_a_band_upside_down_is_refused():
    problems = query.validate_request({"indicators": ["dailyDeepSleep"], "view": "stats",
                                       "value_min": 10, "value_max": 5})
    assert [p.parameter for p in problems] == ["value_min"]


def test_the_schema_says_what_each_count_means():
    properties = query.TOOL_SCHEMA["properties"]
    assert properties["value_min"]["type"] == properties["value_max"]["type"] == "number"
    text = properties["value_min"]["description"]
    assert "days_in_range" in text and "count_in_range" in text and "view=stats" in text


def _stats_row(**over):
    row = {"indicator": "Deep sleep duration", "code": "93831-6", "count": 7, "numeric_count": 7, "days": 7,
           "count_in_range": 4, "days_in_range": 4, "min": 1_800_000.0, "max": 5_400_000.0,
           "avg": 3_600_000.0, "unit": "ms"}
    row.update(over)
    return row


def test_the_band_counts_reach_the_model_only_when_a_band_was_asked():
    """The stats table is a fixed column list, and a column it does not
    list never reaches the model; without a band the counts are None, and a
    None cell is not rendered."""
    from mirobody.agent.tools import _render
    from mirobody.kernel import tools

    def text(row):
        return _render.render_compact(tools.Envelope(tools.STATUS_OK, data=[row],
                                                     meta=tools.Meta(view="stats", row_count=1)))

    banded = text(_stats_row())
    assert "days=7" in banded and "count_in_range=4" in banded and "days_in_range=4" in banded
    plain = text(_stats_row(count_in_range=None, days_in_range=None))
    assert "days=7" in plain and "in_range" not in plain


def _service(rows):
    pytest.importorskip("dotenv")
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService

    hq = Mock()
    hq.tz.return_value = "UTC"
    hq.on_read.return_value = None
    hq.stats.return_value = rows
    hq.buckets.return_value = rows
    return hq, HealthIndicatorsService(hq, outside_note=False)


async def test_a_band_is_passed_to_the_store_and_said_in_the_answer():
    hq, service = _service([_stats_row()])
    envelope = await service.envelope({"user_id": "1"}, indicators=["dailyDeepSleep"], view="stats",
                                      value_max=3_600_000)
    assert hq.stats.call_args.kwargs == {"value_min": None, "value_max": 3_600_000.0}
    assert any("at or below 3600000," in a and "days_in_range" in a for a in envelope.assumptions)


async def test_no_band_leaves_the_store_call_as_it_was():
    """A store written before the band existed still answers stats."""
    hq, service = _service([_stats_row(count_in_range=None, days_in_range=None)])
    await service.envelope({"user_id": "1"}, indicators=["dailyDeepSleep"], view="stats")
    assert hq.stats.call_args.kwargs == {}


async def test_a_series_stored_in_pieces_gets_no_band_count():
    steps = _stats_row(indicator="Steps", code="55423-8", unit="{#}", count_in_range=0, days_in_range=0)
    _, service = _service([steps])
    envelope = await service.envelope({"user_id": "1"}, indicators=["steps"], view="stats", value_min=10000)
    [row] = envelope.data
    assert row["count_in_range"] is None and row["days_in_range"] is None
    assert any("no band was counted for Steps" in a and "dailySteps" in a for a in envelope.assumptions)


async def test_a_band_on_a_view_that_does_not_count_is_said_not_dropped():
    day = {"indicator": "Deep sleep duration", "series": "s", "period": "2026-01-02", "avg": 1.0, "n": 1,
           "unit": "ms", "total": 1}
    _, service = _service([day])
    envelope = await service.envelope({"user_id": "1"}, indicators=["dailyDeepSleep"], view="day", value_min=5)
    assert any(a.startswith("value_min/value_max were not applied: view=day") for a in envelope.assumptions)




def test_stats_publishes_a_median_and_a_spread():
    """`stats` answered with min/max/avg/sum, and 393 of PHIA's 4,000 objective
    questions ask for a median (281 for a standard deviation). Asked for the
    median deep-sleep percentage, a local 2B answered with one day's value —
    it had no statistic to read, only rows to count by hand.
    """
    from mirobody.collect import query as collect_query

    source = collect_query._STATS_CTE  # noqa: F841  (documented here, asserted below)
    sql = str(collect_query._STAT_MEDIAN) + str(collect_query._STAT_STDDEV)
    assert "PERCENTILE_CONT(0.5)" in sql
    assert "STDDEV_SAMP" in sql


def test_the_median_respects_the_unit_guard():
    """A median across mg/dL and mmol/L is not a median of anything."""
    from mirobody.collect import query as collect_query

    for macro in (collect_query._STAT_MEDIAN, collect_query._STAT_STDDEV):
        assert "COUNT(DISTINCT unit_ucum) <= 1" in str(macro)

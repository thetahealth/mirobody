"""A sleep score is a figure about one sleep, not a span.

Oura's and WHOOP's sleep efficiency, performance and consistency (percent) and
their count of disturbances are each one number the vendor computed for one
sleep, and the decoders file it with that sleep's start..end. Under
`duration_union` the catalogue summarised them by the minutes their spans
cover: WHOOP's 91% efficiency became 480, the night's length in minutes, and
a count with no span summarised to nothing. These pin that each summarises to
the vendor's own figure, that a re-scored sleep replaces the earlier score
instead of being averaged with it, and that only a duration is summarised by
its spans.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from mirobody import units
from mirobody.kernel import metrics, series
from mirobody.kernel.decoders import oura, whoop

WHOOP_TZ = "America/New_York"
OURA_TZ = "Asia/Tokyo"

WHOOP_NIGHT = {
    "score_state": "SCORED", "nap": False,
    "start": "2026-01-02T04:00:00.000Z", "end": "2026-01-02T12:00:00.000Z",
    "score": {"sleep_efficiency_percentage": 91, "sleep_performance_percentage": 88,
              "sleep_consistency_percentage": 76, "stage_summary": {"disturbance_count": 12}},
}

OURA_NIGHT = {
    "type": "long_sleep", "day": "2026-01-02",
    "bedtime_start": "2026-01-01T23:30:00+09:00", "bedtime_end": "2026-01-02T07:00:00+09:00",
    "efficiency": 93, "restless_periods": 3,
}


def _summarise(system: str, facts: list[series.Fact], tz: str) -> series.Projection | None:
    """A day of one metric's facts, under the shape the catalogue gives it."""
    m = metrics.mapping_for(system, facts[0].metric_key)
    day = series.local_date(facts[0].effective_start_ms, tz, m.metric.window)
    return series.aggregate(m.aggregation_policy, facts, day=day, tz=tz, window=m.metric.window, unit=m.unit_ucum)


@pytest.mark.parametrize("system, facts, tz, expected", [
    ("whoop", whoop.decode("sleep", WHOOP_NIGHT, WHOOP_TZ), WHOOP_TZ,
     {"sleepEfficiency": 91, "sleepPerformance": 88, "sleepConsistency": 76, "sleepDisturbances": 12}),
    ("oura", oura.decode("sleep", OURA_NIGHT, OURA_TZ), OURA_TZ,
     {"sleepEfficiency": 93, "sleepDisturbances": 3}),
])
def test_a_sleep_score_summarises_to_the_vendors_figure(system, facts, tz, expected):
    for name, value in expected.items():
        mine = [f for f in facts if f.metric_key == name]
        assert len(mine) == 1 and mine[0].is_interval, name
        projected = _summarise(system, mine, tz)
        assert projected is not None, name
        assert (projected.value, projected.unit) == (value, metrics.METRICS[name].unit_ucum), name


def test_a_rescored_sleep_replaces_the_earlier_score():
    # WHOOP re-scores a sleep after it is first synced; both revisions carry
    # the same span, and the later one is the vendor's answer, not half of it.
    later = [f for f in whoop.decode("sleep", WHOOP_NIGHT, WHOOP_TZ, ingested_at_ms=2)
             if f.metric_key == "sleepEfficiency"]
    earlier = [replace(f, value_num=89.0, ingested_at_ms=1) for f in later]
    assert _summarise("whoop", earlier + later, WHOOP_TZ).value == 91
    assert _summarise("whoop", later + earlier, WHOOP_TZ).value == 91


def test_a_score_with_no_span_is_still_summarised():
    point = series.Fact("sleepDisturbances", 4.0, 1_767_340_800_000, unit="{#}")
    projected = _summarise("open_wearables", [point], "UTC")
    assert projected is not None and projected.value == 4


def test_only_a_duration_is_summarised_by_its_spans():
    # `duration_union` returns minutes covered whatever the values are, so a
    # row under it whose unit is not a time reports the length of its spans.
    wrong = sorted(m.member for m in metrics.ROWS
                   if m.aggregation_policy == metrics.POLICY_DURATION_UNION
                   and not units.convertible(m.unit_ucum, "min"))
    assert wrong == []

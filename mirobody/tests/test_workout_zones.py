"""A workout's time in a heart-rate zone is a count of minutes, not a span.

WHOOP reports how long a workout spent in each zone; the decoder files each
intensity with the workout's start..end. The catalogue gave those rows a
sleep stage's shape, the union of the spans, so zones of 20, 10 and 1
minutes in a 60 minute workout all summarised to 60. They are summed now,
like the same workout's calories and distance.
"""

from __future__ import annotations

from datetime import date

import pytest

from mirobody.kernel import metrics, series
from mirobody.kernel.decoders import whoop

TZ = "Asia/Shanghai"
MIN = 60_000
ZONES = ("workoutDurationLow", "workoutDurationMedium", "workoutDurationHigh")


def _workout(start: str, end: str, **zones: int) -> dict:
    return {"score_state": "SCORED", "start": start, "end": end,
            "score": {"zone_durations": {k: v * MIN for k, v in zones.items()}}}


def _day_of(facts, name: str) -> float | None:
    m = metrics.mapping_for("whoop", name)
    picked = [f for f in facts if f.metric_key == name]
    p = series.aggregate(m.aggregation_policy, picked, day=date(2026, 1, 2), tz=TZ, window=m.metric.window,
                         unit=m.unit_ucum)
    return p.value if p else None


def test_each_zone_summarises_to_its_own_minutes():
    hour = _workout("2026-01-02T00:00:00Z", "2026-01-02T01:00:00Z", zone_zero_milli=5, zone_one_milli=15,
                    zone_two_milli=4, zone_three_milli=6, zone_four_milli=1)
    facts = whoop.decode("workout", hour, TZ)
    assert [_day_of(facts, name) for name in ZONES] == [pytest.approx(20), pytest.approx(10), pytest.approx(1)]
    # The span is still the workout's: the minutes were counted inside it.
    for f in facts:
        if f.metric_key in ZONES:
            assert f.end_ms - f.effective_start_ms == 60 * MIN


def test_two_workouts_on_one_day_add_up():
    morning = _workout("2026-01-02T00:00:00Z", "2026-01-02T01:00:00Z", zone_one_milli=20)
    evening = _workout("2026-01-02T08:00:00Z", "2026-01-02T08:30:00Z", zone_one_milli=15)
    facts = whoop.decode("workout", morning, TZ) + whoop.decode("workout", evening, TZ)
    assert _day_of(facts, "workoutDurationLow") == pytest.approx(35)


@pytest.mark.parametrize("name", ZONES)
def test_a_zone_has_the_shape_of_the_workouts_other_totals(name):
    zone, calories = metrics.METRICS[name], metrics.METRICS["activeCalories"]
    assert (zone.state_class, zone.aggregation_policy) == (calories.state_class, calories.aggregation_policy)

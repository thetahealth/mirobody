"""A night's totals are the night's figures, not spans.

Oura and WHOOP report deep, light and REM sleep, awake time, latency and the
sleeping heart and breathing rates as totals over a whole sleep. Filed under
the interval metrics with the sleep's start..end, a duration union read every
stage as the whole night: Oura's 90 minutes of deep sleep became 480. These
pin where each total goes, on which day, and that a nap or a deleted sleep
does not replace the night's totals or its scores.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from mirobody.kernel import metrics, series
from mirobody.kernel.decoders import oura, whoop

TZ = "Asia/Shanghai"

OURA_NIGHT = {
    "type": "long_sleep", "day": "2026-01-02",
    "bedtime_start": "2026-01-01T23:00:00+08:00", "bedtime_end": "2026-01-02T07:00:00+08:00",
    "deep_sleep_duration": 5400, "light_sleep_duration": 14400, "rem_sleep_duration": 5400,
    "awake_time": 3600, "total_sleep_duration": 25200, "time_in_bed": 28800, "latency": 600,
    "average_heart_rate": 54.5, "lowest_heart_rate": 47, "average_breath": 14.2,
}

WHOOP_NIGHT = {
    "score_state": "SCORED", "nap": False,
    "start": "2026-01-01T15:00:00.000Z", "end": "2026-01-01T23:00:00.000Z",
    "score": {"respiratory_rate": 15.1, "stage_summary": {
        "total_in_bed_time_milli": 28_800_000, "total_awake_time_milli": 1_800_000,
        "total_light_sleep_time_milli": 14_400_000, "total_slow_wave_sleep_time_milli": 5_400_000,
        "total_rem_sleep_time_milli": 6_000_000}},
}


def _local_day(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, ZoneInfo(TZ)).date()


def _by_metric(facts):
    return {f.metric_key: f for f in facts}


def test_an_oura_night_keeps_its_totals_on_its_own_day():
    facts = _by_metric(oura.decode("sleep", OURA_NIGHT, TZ))
    expected = {
        "dailyDeepSleep": 5_400_000, "dailyLightSleep": 14_400_000, "dailyRemSleep": 5_400_000,
        "dailyAwakeTime": 3_600_000, "dailySleepLatency": 600_000, "dailyTotalSleepTime": 25_200_000,
        "dailySleepAvgHeartRate": 54.5, "dailySleepLowestHeartRate": 47, "dailySleepRespiratoryRate": 14.2,
    }
    for name, value in expected.items():
        m = metrics.METRICS[name]
        assert m.state_class == metrics.STATE_PROVIDER_DAILY, name
        # Oura's `day` ("Day that the sleep belongs to"), not the evening the night began.
        assert _local_day(facts[name].effective_start_ms) == date(2026, 1, 2), name
        projected = series.aggregate(m.aggregation_policy, [facts[name]], day=date(2026, 1, 2), tz=TZ,
                                     window=m.window, unit=m.unit_ucum)
        assert projected is not None and projected.value == pytest.approx(value), name
    # Time in bed is the bedtime span itself, so the union of that span is right.
    assert facts["sleepAnalysis_InBed"].effective_start_ms < facts["dailyDeepSleep"].effective_start_ms


def test_an_oura_nap_or_rejected_sleep_does_not_replace_the_night():
    nap = dict(OURA_NIGHT, type="sleep", bedtime_start="2026-01-02T14:00:00+08:00",
               bedtime_end="2026-01-02T14:40:00+08:00", deep_sleep_duration=600)
    assert not {f.metric_key for f in oura.decode("sleep", nap, TZ)} & {
        oura.MAPPING["sleep"][path][0] for path in oura.NIGHTLY}
    for kind in ("deleted", "rest"):
        assert oura.decode("sleep", dict(OURA_NIGHT, type=kind), TZ) == []


def test_a_whoop_night_is_filed_on_the_day_it_ends_and_a_nap_is_not():
    facts = _by_metric(whoop.decode("sleep", WHOOP_NIGHT, TZ))
    for name, value in {"dailyDeepSleep": 5_400_000, "dailyLightSleep": 14_400_000, "dailyRemSleep": 6_000_000,
                        "dailyAwakeTime": 1_800_000, "dailySleepRespiratoryRate": 15.1}.items():
        assert metrics.METRICS[name].state_class == metrics.STATE_PROVIDER_DAILY, name
        assert facts[name].value_num == pytest.approx(value), name
        assert _local_day(facts[name].effective_start_ms) == date(2026, 1, 2), name
    nap = {f.metric_key for f in whoop.decode("sleep", dict(WHOOP_NIGHT, nap=True), TZ)}
    assert nap.isdisjoint(whoop.MAPPING["sleep"][path][0] for path in whoop.NIGHTLY)


def _scores(facts) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for f in facts:
        if f.metric_key in ("sleepEfficiency", "sleepPerformance", "sleepConsistency", "sleepDisturbances"):
            out.setdefault(f.metric_key, []).append(f.value_num)
    return out


def test_a_nap_does_not_replace_the_nights_scores():
    """Both vendors score a nap under the night's fields, and an afternoon nap
    falls in the night's 18:00 day: night 91 % and nap 80 % read 80 as the
    day's last and 85.5 as its average, and 12 disturbances became 13."""
    stages = dict(WHOOP_NIGHT["score"]["stage_summary"], disturbance_count=12)
    night = dict(WHOOP_NIGHT, score=dict(WHOOP_NIGHT["score"], stage_summary=stages, sleep_efficiency_percentage=91,
                                          sleep_performance_percentage=88, sleep_consistency_percentage=76))
    nap = dict(night, nap=True, start="2026-01-02T06:00:00.000Z", end="2026-01-02T06:40:00.000Z",
               score=dict(night["score"], stage_summary=dict(stages, disturbance_count=1),
                          sleep_efficiency_percentage=80, sleep_performance_percentage=70,
                          sleep_consistency_percentage=60))
    filed = _scores(whoop.decode("sleep", night, TZ) + whoop.decode("sleep", nap, TZ))
    assert filed == {"sleepEfficiency": [91], "sleepPerformance": [88], "sleepConsistency": [76],
                     "sleepDisturbances": [12]}

    oura_night = dict(OURA_NIGHT, efficiency=93, restless_periods=3)
    naps = [dict(oura_night, type=kind, bedtime_start="2026-01-02T14:00:00+08:00",
                 bedtime_end="2026-01-02T14:40:00+08:00", efficiency=85, restless_periods=1)
            for kind in ("sleep", "late_nap")]
    facts = [f for doc in (oura_night, *naps) for f in oura.decode("sleep", doc, TZ)]
    assert _scores(facts) == {"sleepEfficiency": [93], "sleepDisturbances": [3]}
    # The night's score keeps its bedtime span; only the totals move to `day`.
    spans = {f.metric_key: (f.effective_start_ms, f.effective_end_ms) for f in oura.decode("sleep", oura_night, TZ)}
    assert spans["sleepEfficiency"] == spans["sleepAnalysis_InBed"]


def test_a_whoop_cycle_heart_rate_is_the_days_figure():
    cycle = {"score_state": "SCORED", "start": "2026-01-01T15:00:00.000Z", "end": "2026-01-02T15:30:00.000Z",
             "score": {"average_heart_rate": 68, "max_heart_rate": 171}}
    assert {f.metric_key for f in whoop.decode("cycle", cycle, TZ)} == {"dailyHeartRateAvg", "dailyHeartRateMax"}


def test_a_workout_distance_is_an_exercise_distance_for_any_sport():
    ride = oura.decode("workout", {"activity": "cycling", "start_datetime": "2026-01-02T08:00:00+08:00",
                                   "end_datetime": "2026-01-02T09:00:00+08:00", "distance": 20000}, TZ)
    swim = whoop.decode("workout", {"score_state": "SCORED", "start": "2026-01-02T00:00:00Z",
                                    "end": "2026-01-02T01:00:00Z", "sport_name": "swimming",
                                    "score": {"distance_meter": 1500}}, TZ)
    assert [f.metric_key for f in ride] == ["exerciseDistances"]
    assert [f.metric_key for f in swim] == ["exerciseDistances"]

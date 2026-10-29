"""WHOOP API v2 records → facts. Pure; field paths are WHOOP's own.

The record types are ``sleep``, ``cycle``, ``workout``, ``recovery`` (WHOOP's
own singular names) and ``body``, the body measurement. WHOOP's profile
carries nothing measurable and is not a type here. Scored types are only
decoded once WHOOP has scored them (``score_state == "SCORED"``); a pending
record decodes to nothing rather than to zeros.

Two conversions here fix numbers a previous decoder got wrong by a large
factor and a downstream repository had to correct on its own: kilojoules
are DIVIDED by 4.184 to get kilocalories, and zone durations in
milliseconds are DIVIDED by 60,000 to get minutes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mirobody.kernel.series import Fact
from ._common import dig, fact, local_day_window, number, parse_ts_smart

_KJ_PER_KCAL = 4.184
_MS_PER_MIN = 60_000


def _same(x: float) -> float:
    return x


def kj_to_kcal(x: float) -> float:
    return x / _KJ_PER_KCAL


def ms_to_min(x: float) -> float:
    return x / _MS_PER_MIN


Entry = tuple[str, Callable[[float], float]]  # (metric, converter into the catalogue unit)

MAPPING: dict[str, dict[str, Entry]] = {
    "sleep": {
        "score.stage_summary.total_in_bed_time_milli": ("sleepAnalysis_InBed", _same),
        "score.stage_summary.total_awake_time_milli": ("dailyAwakeTime", _same),
        "score.stage_summary.total_light_sleep_time_milli": ("dailyLightSleep", _same),
        # Slow-wave sleep (SWS) is WHOOP's name for deep sleep (N3).
        "score.stage_summary.total_slow_wave_sleep_time_milli": ("dailyDeepSleep", _same),
        "score.stage_summary.total_rem_sleep_time_milli": ("dailyRemSleep", _same),
        "score.sleep_efficiency_percentage": ("sleepEfficiency", _same),
        "score.respiratory_rate": ("dailySleepRespiratoryRate", _same),
        "score.sleep_performance_percentage": ("sleepPerformance", _same),
        "score.sleep_consistency_percentage": ("sleepConsistency", _same),
        "score.stage_summary.disturbance_count": ("sleepDisturbances", _same),
    },
    "cycle": {
        # "during the cycle": one figure for the whole cycle, as Garmin's
        # dailies average and maximum are; as heart-rate readings they were
        # averaged in with the workouts' own.
        "score.average_heart_rate": ("dailyHeartRateAvg", _same),
        "score.max_heart_rate": ("dailyHeartRateMax", _same),
        # A cycle's energy is the whole day's, basal included; as active
        # calories it was summed with the day's workouts and counted twice.
        "score.kilojoule": ("dailyTotalCalories", kj_to_kcal),
        "score.strain": ("strain", _same),
    },
    "recovery": {
        "score.resting_heart_rate": ("restingHeartRates", _same),
        "score.hrv_rmssd_milli": ("hrvRMSSD", _same),
        "score.spo2_percentage": ("oxygenSaturations", _same),
        "score.recovery_score": ("recoveryScore", _same),
        "score.skin_temp_celsius": ("skinTemperature", _same),
    },
    "workout": {
        "score.average_heart_rate": ("heartRates", _same),
        "score.max_heart_rate": ("heartRateMax", _same),
        # Any sport: walkingRunningDistances is walking or running only.
        "score.distance_meter": ("exerciseDistances", _same),
        "score.kilojoule": ("activeCalories", kj_to_kcal),
        "score.altitude_gain_meter": ("altitudeGain", _same),
        "score.altitude_change_meter": ("altitudeChange", _same),
    },
    "body": {
        "height_meter": ("heights", _same),
        "weight_kilogram": ("bodyMasss", _same),
        "max_heart_rate": ("maxHeartRateProfile", _same),
    },
}

#: Heart-rate zones are summed in pairs into the catalogue's three workout
#: intensity durations. Each is minutes counted inside the workout, so it
#: keeps the workout's span as the workout's calories and distance do, and the
#: catalogue sums it: a union of that span read every zone as the whole
#: workout (20, 10 and 1 minutes of a 60 minute workout each became 60).
ZONES = (
    ("workoutDurationLow", ("zone_zero_milli", "zone_one_milli")),
    ("workoutDurationMedium", ("zone_two_milli", "zone_three_milli")),
    ("workoutDurationHigh", ("zone_four_milli", "zone_five_milli")),
)

SCORED = frozenset({"sleep", "cycle", "workout", "recovery"})
DATA_TYPES: tuple[str, ...] = tuple(MAPPING)

#: The sleep fields that are the night's figure: totals over the whole sleep,
#: not spans, so a duration union of the sleep's start..end read every stage
#: as the whole night. Filed on the local day the sleep ends, as a night is
#: on the day the person wakes, and never from a nap: a ``daily…`` summary
#: keeps one value a day, and a nap's would replace the night's.
NIGHTLY: frozenset[str] = frozenset({
    "score.stage_summary.total_awake_time_milli", "score.stage_summary.total_light_sleep_time_milli",
    "score.stage_summary.total_slow_wave_sleep_time_milli", "score.stage_summary.total_rem_sleep_time_milli",
    "score.respiratory_rate",
})

#: One sleep's scores, filed with the sleep's own span and never from a nap.
#: WHOOP scores a nap under the same four fields and a nap falls in the
#: night's 18:00 day, so a night of 91 % and a nap of 80 % averaged to 85.5 in
#: ``dailyAvgSleepEfficiency`` and the nap's 80 was the day's last score. The
#: catalogue has no nap score to file it under; ``napDuration`` is a duration.
SCORES: frozenset[str] = frozenset({
    "score.sleep_efficiency_percentage", "score.sleep_performance_percentage",
    "score.sleep_consistency_percentage", "score.stage_summary.disturbance_count",
})


def window_ms(data_type: str, item: dict, tz: str, pulled_at_ms: int) -> tuple[int, int]:
    """The instant or span a record describes, ``(0, 0)`` when it carries
    none. Sleep, cycle and workout carry start/end; a recovery is stamped when
    created; body measurements have no time of their own, they state what
    holds now, and are filed at the pull instant."""
    if data_type in ("sleep", "cycle", "workout"):
        start = parse_ts_smart(item.get("start"), tz)
        if not start and data_type == "sleep":
            start = parse_ts_smart(item.get("created_at"), tz)
        if not start:
            return 0, 0
        end = parse_ts_smart(item.get("end"), tz)
        return start, (end if end and end > start else start)
    if data_type == "recovery":
        point = parse_ts_smart(item.get("created_at"), tz)
        return point, point
    return pulled_at_ms, pulled_at_ms


#: Every catalogue metric this table can emit: derived from MAPPING, so it
#: cannot drift from what `decode` actually produces. `connect.Coverage` is
#: built from it (`decoders.coverage_of`).
METRICS: frozenset[str] = frozenset(
    metric for fields in MAPPING.values() for metric, _convert in fields.values()
)


def decode(
    data_type: str, item: dict, tz: str, *, pulled_at_ms: int = 0, source_record_id: str = "", ingested_at_ms: int = 0
) -> list[Fact]:
    mapping = MAPPING.get(data_type)
    if not mapping or not isinstance(item, dict):
        return []
    if data_type in SCORED and item.get("score_state") != "SCORED":
        return []
    start, end = window_ms(data_type, item, tz, pulled_at_ms)
    if not start:
        return []
    out: list[Fact] = []
    night = local_day_window(end, tz) if data_type == "sleep" and item.get("nap") is not True else None

    def emit(metric: str, value: float, span: tuple[int, int] | None = None) -> None:
        s, e = span or (start, end)
        out.append(fact(metric, value, s, e, source_record_id=source_record_id, ingested_at_ms=ingested_at_ms))

    if data_type == "workout":
        zones: Any = dig(item, "score.zone_durations")
        if isinstance(zones, dict):
            for metric, keys in ZONES:
                total = sum((number(zones.get(k)) or 0.0) for k in keys)
                if total > 0:
                    emit(metric, ms_to_min(total))
    for path, (metric, conv) in mapping.items():
        if data_type == "sleep" and night is None and (path in NIGHTLY or path in SCORES):
            continue
        v = number(dig(item, path))
        if v is None:
            continue
        emit(metric, float(conv(v)), night if data_type == "sleep" and path in NIGHTLY else None)
    return out

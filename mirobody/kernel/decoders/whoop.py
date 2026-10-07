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
from ._common import dig, fact, number, parse_ts_smart

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
        "score.stage_summary.total_awake_time_milli": ("sleepAnalysis_Awake", _same),
        "score.stage_summary.total_light_sleep_time_milli": ("sleepAnalysis_Asleep(Core)", _same),
        "score.stage_summary.total_slow_wave_sleep_time_milli": ("sleepAnalysis_Asleep(Deep)", _same),
        "score.stage_summary.total_rem_sleep_time_milli": ("sleepAnalysis_Asleep(REM)", _same),
        "score.sleep_efficiency_percentage": ("sleepEfficiency", _same),
        "score.respiratory_rate": ("respiratoryRates", _same),
        "score.sleep_performance_percentage": ("sleepPerformance", _same),
        "score.sleep_consistency_percentage": ("sleepConsistency", _same),
        "score.stage_summary.disturbance_count": ("sleepDisturbances", _same),
    },
    "cycle": {
        "score.average_heart_rate": ("heartRates", _same),
        "score.max_heart_rate": ("heartRateMax", _same),
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
        "score.distance_meter": ("walkingRunningDistances", _same),
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
#: intensity durations; per-zone facts would double-count the workout.
ZONES = (
    ("workoutDurationLow", ("zone_zero_milli", "zone_one_milli")),
    ("workoutDurationMedium", ("zone_two_milli", "zone_three_milli")),
    ("workoutDurationHigh", ("zone_four_milli", "zone_five_milli")),
)

SCORED = frozenset({"sleep", "cycle", "workout", "recovery"})
DATA_TYPES: tuple[str, ...] = tuple(MAPPING)


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

    def emit(metric: str, value: float) -> None:
        out.append(fact(metric, value, start, end, source_record_id=source_record_id, ingested_at_ms=ingested_at_ms))

    if data_type == "workout":
        zones: Any = dig(item, "score.zone_durations")
        if isinstance(zones, dict):
            for metric, keys in ZONES:
                total = sum((number(zones.get(k)) or 0.0) for k in keys)
                if total > 0:
                    emit(metric, ms_to_min(total))
    for path, (metric, conv) in mapping.items():
        v = number(dig(item, path))
        if v is None:
            continue
        emit(metric, float(conv(v)))
    return out

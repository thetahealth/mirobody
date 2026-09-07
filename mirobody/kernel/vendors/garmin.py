"""Garmin Health API payloads → facts. Pure; field names are Garmin's own.

One table drives it: for each summary type the field that carries the
record's time, the scalar fields (with the unit conversion each needs),
the time-series fields (per-sample offsets from the record's start), and the
derived values a summary implies but does not state. The table is the same
shape two repositories had converged on independently; this copy takes the
corrections each had made and the other had not:

- ``bodyComps`` field names are the API's (``weightInGrams``,
  ``measurementTimeInSeconds``) — an earlier table invented names no
  payload has and every scale reading decoded to nothing.
- ``pulseOx`` monitoring mode (``timeOffsetSpo2Values``) is a series, not
  only the on-demand ``singleReadingSpO2``.
- ``activities`` carry their window (start, start + duration), so a workout
  is a session, not a point.
- ``sleepLevelsMap`` stages become interval facts under the catalogue's
  sleep-stage names, so a night can be unioned rather than summed.

Everything is emitted in the catalogue's standard unit for that metric.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..series import Fact
from ._common import MS, epoch_to_ms, fact, number, parse_ts_smart

_S_TO_MIN = 1 / 60
_S_TO_MS = MS
_G_TO_KG = 1 / 1000


def _same(x: float) -> float:
    return x


Field = tuple[str, Callable[[float], float]]  # (metric, converter into the catalogue unit)

CONFIG: dict[str, dict[str, Any]] = {
    "dailies": {
        "time": "calendarDate",
        "fields": {
            "steps": ("dailySteps", _same),
            "distanceInMeters": ("dailyDistance", _same),
            "activeKilocalories": ("dailyCaloriesActive", _same),
            "bmrKilocalories": ("dailyCaloriesBasal", _same),
            "floorsClimbed": ("dailyFloors", _same),
            "activeTimeInSeconds": ("exerciseMinutes", lambda x: x * _S_TO_MIN),
            "moderateIntensityDurationInSeconds": ("dailyActivityIntensityHigh", lambda x: x * _S_TO_MIN),
            "vigorousIntensityDurationInSeconds": ("dailyActivityIntensityMedium", lambda x: x * _S_TO_MIN),
            "minHeartRateInBeatsPerMinute": ("dailyHeartRateMin", _same),
            "maxHeartRateInBeatsPerMinute": ("dailyHeartRateMax", _same),
            "averageHeartRateInBeatsPerMinute": ("dailyAvgHeartRate", _same),
            "restingHeartRateInBeatsPerMinute": ("dailyRestingHeartRates", _same),
        },
        "series": {
            "timeOffsetHeartRateSamples": (
                "heartRates",
                "list",
                "heartRateInBeatsPerMinute",
                "timestampOffsetInSeconds",
                False,
            ),
        },
    },
    "sleeps": {
        "time": "calendarDate",
        "fields": {
            # durationInSeconds is not a plain field: the derived block below
            # turns it into total sleep, time in bed and efficiency.
            "awakeDurationInSeconds": ("dailyAwakeTime", lambda x: x * _S_TO_MS),
            "deepSleepDurationInSeconds": ("dailyDeepSleep", lambda x: x * _S_TO_MS),
            "lightSleepDurationInSeconds": ("dailyLightSleep", lambda x: x * _S_TO_MS),
            "remSleepInSeconds": ("dailyRemSleep", lambda x: x * _S_TO_MS),
        },
    },
    "hrv": {
        "time": "calendarDate",
        "series": {"hrvValues": ("hrvDatas", "dict", None, None, False)},
    },
    "respiration": {
        "time": "startTimeInSeconds",
        "series": {"timeOffsetEpochToBreaths": ("respiratoryRates", "dict", None, None, False)},
    },
    "stress": {
        "time": "calendarDate",
        "fields": {"overallStressLevel": ("stressLevel", _same)},
    },
    "bodyComps": {
        "time": "measurementTimeInSeconds",
        "fields": {
            "weightInGrams": ("bodyMasss", lambda x: x * _G_TO_KG),
            "bodyMassIndex": ("bmis", _same),
            "bodyFatInPercent": ("bodyFatPercentages", _same),
            "bodyWaterInPercent": ("bodyWater", _same),
            "boneMassInGrams": ("bodyBone", lambda x: x * _G_TO_KG),
            # muscleMassInGrams is a mass; the catalogue's muscle metric is a
            # percentage. Mapping grams to a percent is the silent wrong
            # number this table exists to prevent.
        },
        "panel": "body_composition",
    },
    "userMetrics": {
        "time": "calendarDate",
        "fields": {"vo2Max": ("vo2Maxs", _same)},
    },
    "pulseOx": {
        "time": "startTimeInSeconds",
        "fields": {"singleReadingSpO2": ("oxygenSaturations", _same)},
        "series": {"timeOffsetSpo2Values": ("oxygenSaturations", "dict", None, None, True)},
    },
    "bloodPressures": {
        "time": "startTimeInSeconds",
        "fields": {
            "systolicPressure": ("systolicPressures", _same),
            "diastolicPressure": ("diastolicPressures", _same),
        },
        "panel": "blood_pressure",
    },
    "skinTemp": {
        "time": "calendarDate",
        "fields": {"nightlyValue": ("skinTemperature", _same)},
    },
    "activities": {
        "time": "startTimeInSeconds",
        "fields": {
            "averageHeartRateInBeatsPerMinute": ("heartRates", _same),
            "maxHeartRateInBeatsPerMinute": ("heartRateMax", _same),
            "calories": ("activeCalories", _same),
            "bmrCalories": ("basalCalories", _same),
            "steps": ("steps", _same),
            "distanceInMeters": ("walkingRunningDistances", _same),
            "durationInSeconds": ("workoutDuration", lambda x: x * _S_TO_MIN),
            "elevationGainInMeters": ("altitudeGain", _same),
            "averageSpeedInMetersPerSecond": ("speeds", _same),
            "activityTrainingLoad": ("trainingLoad", _same),
        },
    },
}

DATA_TYPES: tuple[str, ...] = tuple(CONFIG)

#: Garmin's ``sleepLevelsMap`` keys → the catalogue's sleep-stage names.
SLEEP_STAGES = {
    "deep": "sleepAnalysis_Asleep(Deep)",
    "light": "sleepAnalysis_Asleep(Core)",
    "rem": "sleepAnalysis_Asleep(REM)",
    "awake": "sleepAnalysis_Awake",
}


def record_time_ms(item: dict, data_type: str, tz: str) -> int:
    """The record's anchor instant: a ``calendarDate`` is the user's local
    midnight; an epoch is an epoch. ``0`` when the payload has neither."""
    key = CONFIG[data_type]["time"]
    raw = item.get(key)
    if raw in (None, ""):
        return 0
    if key == "calendarDate":
        return parse_ts_smart(str(raw), tz)
    return epoch_to_ms(raw)


#: Metrics `decode` emits from the tables above but that no table LISTS: the
#: three derived from a sleep record's own arithmetic, and the daily total
#: that is active plus basal. Named here rather than discovered, because
#: `connect.Coverage` claiming a metric this decoder cannot produce — or
#: missing one it can — is exactly the kind of documentation drift the
#: generated matrix exists to prevent.
DERIVED_METRICS: frozenset[str] = frozenset(
    {"dailyTotalCalories", "dailySleepDuration", "dailyTotalSleepTime", "dailySleepEfficiency"}
)

#: Every catalogue metric this decoder can emit, from its own tables.
METRICS: frozenset[str] = (
    frozenset(metric for cfg in CONFIG.values() for metric, _c in cfg.get("fields", {}).values())
    | frozenset(entry[0] for cfg in CONFIG.values() for entry in cfg.get("series", {}).values())
    | frozenset(SLEEP_STAGES.values())
    | DERIVED_METRICS
)


def decode(
    data_type: str, item: dict, tz: str, *, pulled_at_ms: int = 0, source_record_id: str = "", ingested_at_ms: int = 0
) -> list[Fact]:
    """Facts from one Garmin summary object of ``data_type``. Unknown types
    and records without a time decode to nothing — never to a guessed time.
    ``pulled_at_ms`` is accepted for signature parity; Garmin records carry
    their own time."""
    cfg = CONFIG.get(data_type)
    if not cfg or not isinstance(item, dict):
        return []
    base = record_time_ms(item, data_type, tz)
    if not base:
        return []
    panel = cfg.get("panel", "")
    out: list[Fact] = []

    def emit(metric: str, value: float, start: int, end: int = 0) -> None:
        out.append(
            fact(
                metric,
                value,
                start,
                end,
                source_record_id=source_record_id,
                ingested_at_ms=ingested_at_ms,
                panel_id=(f"{panel}:{base}" if panel else ""),
            )
        )

    duration_s = number(item.get("durationInSeconds")) if data_type == "activities" else None
    for field_name, (metric, conv) in cfg.get("fields", {}).items():
        raw = number(item.get(field_name))
        if raw is None:
            continue
        value = float(conv(raw))
        if data_type == "activities" and duration_s:
            emit(metric, value, base, base + int(duration_s * MS))
        else:
            emit(metric, value, base)

    for key, (metric, shape, value_field, offset_field, drop_negative) in cfg.get("series", {}).items():
        samples = item.get(key)
        if not samples:
            continue
        if shape == "list" and isinstance(samples, list):
            for s in samples:
                if not isinstance(s, dict):
                    continue
                v = number(s.get(value_field))
                off = number(s.get(offset_field)) or 0.0
                if v is None or (drop_negative and v < 0):
                    continue
                emit(metric, v, base + int(off * MS))
        elif shape == "dict" and isinstance(samples, dict):
            for off_text, raw in samples.items():
                v = number(raw)
                off = number(off_text)
                if v is None or off is None or (drop_negative and v < 0):
                    continue
                emit(metric, v, base + int(off * MS))

    if data_type == "dailies":
        active, bmr = number(item.get("activeKilocalories")), number(item.get("bmrKilocalories"))
        if active is not None and bmr is not None:
            emit("dailyTotalCalories", active + bmr, base)
    elif data_type == "sleeps":
        _sleep_derived(item, base, emit)
        _sleep_stages(item, emit)
    return out


def _sleep_derived(item: dict, base: int, emit) -> None:
    asleep = number(item.get("durationInSeconds"))
    if asleep is None:
        return
    awake = number(item.get("awakeDurationInSeconds")) or 0.0
    in_bed = asleep + awake
    emit("dailySleepDuration", in_bed * _S_TO_MS, base)
    emit("dailyTotalSleepTime", asleep * _S_TO_MS, base)
    if in_bed > 0:
        emit("dailySleepEfficiency", round(asleep / in_bed * 100, 4), base)


def _sleep_stages(item: dict, emit) -> None:
    levels = item.get("sleepLevelsMap")
    if not isinstance(levels, dict):
        return
    for stage, spans in levels.items():
        metric = SLEEP_STAGES.get(stage)
        if not metric or not isinstance(spans, list):
            continue
        for span in spans:
            if not isinstance(span, dict):
                continue
            s, e = epoch_to_ms(span.get("startTimeInSeconds")), epoch_to_ms(span.get("endTimeInSeconds"))
            if s and e > s:
                emit(metric, float(e - s), s, e)

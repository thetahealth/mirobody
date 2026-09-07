"""Oura API v2 documents → facts. Pure; field paths are Oura's own.

Each collection has a *time strategy*: ``day`` documents (daily_activity,
daily_sleep, …) describe one local day and are filed on that day's whole
window; ``timestamp`` documents (sleep sessions, workouts, heart rate)
carry their own instant or span; ``personal_info`` has no time at all and
is filed at the pull instant's local midnight, so repeated syncs collapse to
one reading per day.

Oura reports durations in seconds; the catalogue's sleep metrics are in
milliseconds and its activity durations in minutes. The conversion happens
here, so a fact leaves this module already in the unit its metric expects.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from ..series import Fact, zone
from ._common import MS, dig, fact, local_day_window, number, parse_ts_smart

STRATEGY: dict[str, str] = {
    "personal_info": "pull_day",
    "sleep": "timestamp",
    "daily_sleep": "day",
    "daily_activity": "day",
    "daily_readiness": "day",
    "heartrate": "timestamp",
    "daily_spo2": "day",
    "daily_stress": "day",
    "vo2_max": "day",
    "daily_cardiovascular_age": "day",
    "workout": "timestamp",
    "session": "timestamp",
    "sleep_time": "day",
}
DATA_TYPES: tuple[str, ...] = tuple(STRATEGY)


def _same(x: float) -> float:
    return x


def s_to_ms(x: float) -> float:
    return x * MS


def s_to_min(x: float) -> float:
    return round(x / 60, 1)


Entry = tuple[str, Callable[[float], float]]  # (metric, converter into the catalogue unit)

MAPPING: dict[str, dict[str, Entry]] = {
    "sleep": {
        "total_sleep_duration": ("dailyTotalSleepTime", s_to_ms),
        "time_in_bed": ("sleepAnalysis_InBed", s_to_ms),
        "awake_time": ("sleepAnalysis_Awake", s_to_ms),
        "deep_sleep_duration": ("sleepAnalysis_Asleep(Deep)", s_to_ms),
        "light_sleep_duration": ("sleepAnalysis_Asleep(Core)", s_to_ms),
        "rem_sleep_duration": ("sleepAnalysis_Asleep(REM)", s_to_ms),
        "efficiency": ("sleepEfficiency", _same),
        "latency": ("sleepLatency", _same),
        "average_heart_rate": ("dailyHeartRateAvg", _same),
        "lowest_heart_rate": ("dailyHeartRateMin", _same),
        "average_hrv": ("hrvRMSSD", _same),
        "average_breath": ("respiratoryRates", _same),
        "restless_periods": ("sleepDisturbances", _same),
        "temperature_delta": ("temperatureDelta", _same),
    },
    "daily_sleep": {"score": ("sleepOverallScore", _same)},
    "daily_activity": {
        "steps": ("dailySteps", _same),
        "active_calories": ("dailyCaloriesActive", _same),
        "total_calories": ("dailyTotalCalories", _same),
        "equivalent_walking_distance": ("dailyDistance", _same),
        "high_activity_time": ("dailyActivityIntensityHigh", s_to_min),
        "medium_activity_time": ("dailyActivityIntensityMedium", s_to_min),
        "low_activity_time": ("dailyActivityIntensityLow", s_to_min),
        "sedentary_time": ("sedentaryTime", s_to_min),
        "resting_time": ("restingTime", s_to_min),
        "score": ("dailyActivityScore", _same),
    },
    "daily_readiness": {
        "score": ("recoveryScore", _same),
        "temperature_deviation": ("temperatureDelta", _same),
    },
    "heartrate": {"bpm": ("heartRates", _same)},
    "daily_spo2": {"spo2_percentage.average": ("oxygenSaturations", _same)},
    "daily_stress": {
        "stress_high": ("stressHighDuration", s_to_min),
        "recovery_high": ("recoveryHighDuration", s_to_min),
    },
    "vo2_max": {"vo2_max": ("vo2Maxs", _same)},
    "daily_cardiovascular_age": {"vascular_age": ("bodyAge", _same)},
    "workout": {
        "calories": ("activeCalories", _same),
        "distance": ("walkingRunningDistances", _same),
    },
    "personal_info": {
        "weight": ("bodyMasss", _same),
        "height": ("heights", _same),
    },
}


def record_time_ms(data_type: str, item: dict, tz: str, pulled_at_ms: int) -> int:
    strategy = STRATEGY.get(data_type, "timestamp")
    if strategy == "pull_day":
        if not pulled_at_ms:
            return 0
        d = datetime.fromtimestamp(pulled_at_ms / MS, zone(tz))
        return int(d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * MS)
    if strategy == "day":
        return parse_ts_smart(str(item.get("day") or ""), tz)
    return parse_ts_smart(
        str(item.get("timestamp") or item.get("bedtime_start") or item.get("start_datetime") or ""), tz
    )


#: Every catalogue metric this table can emit — derived from MAPPING, so it
#: cannot drift from what `decode` actually produces. `connect.Coverage` is
#: built from it (`vendors.coverage_of`).
METRICS: frozenset[str] = frozenset(
    metric for fields in MAPPING.values() for metric, _convert in fields.values()
)


def decode(
    data_type: str, item: dict, tz: str, *, pulled_at_ms: int = 0, source_record_id: str = "", ingested_at_ms: int = 0
) -> list[Fact]:
    mapping = MAPPING.get(data_type)
    if not mapping or not isinstance(item, dict):
        return []
    ts = record_time_ms(data_type, item, tz, pulled_at_ms)
    if not ts:
        return []
    start, end = ts, ts
    if STRATEGY[data_type] == "day":
        start, end = local_day_window(ts, tz)
    elif data_type == "sleep":
        bed_start = parse_ts_smart(str(item.get("bedtime_start") or ""), tz)
        bed_end = parse_ts_smart(str(item.get("bedtime_end") or ""), tz)
        if bed_start:
            start = bed_start
        end = bed_end if bed_end and bed_end > start else start
    elif data_type == "workout":
        s = parse_ts_smart(str(item.get("start_datetime") or ""), tz)
        e = parse_ts_smart(str(item.get("end_datetime") or ""), tz)
        if s:
            start = s
        end = e if e and e > start else start

    out: list[Fact] = []
    for path, (metric, conv) in mapping.items():
        v = number(dig(item, path))
        if v is None:
            continue
        out.append(
            fact(metric, float(conv(v)), start, end, source_record_id=source_record_id, ingested_at_ms=ingested_at_ms)
        )
    return out

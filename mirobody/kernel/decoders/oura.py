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

A sleep period's stage durations, latency and its average and lowest heart
rate and breathing rate are totals Oura computed over the whole period, not
spans: filed under the interval metrics (``sleepAnalysis_*``) and given the
bedtime span, a duration union read every stage as the whole night (a 90
minute deep sleep became 480 minutes). They are the night's own figures, so
they go to the ``daily…`` summaries and, like Garmin's ``calendarDate``, on
the day Oura files the night under.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from mirobody.kernel.series import Fact, zone
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
}


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
        "awake_time": ("dailyAwakeTime", s_to_ms),
        "deep_sleep_duration": ("dailyDeepSleep", s_to_ms),
        "light_sleep_duration": ("dailyLightSleep", s_to_ms),
        "rem_sleep_duration": ("dailyRemSleep", s_to_ms),
        "efficiency": ("sleepEfficiency", _same),
        "latency": ("dailySleepLatency", s_to_ms),
        # "during sleep" in Oura's own definition: the day's average or
        # minimum heart rate is a different number, usually a higher one.
        "average_heart_rate": ("dailySleepAvgHeartRate", _same),
        "lowest_heart_rate": ("dailySleepLowestHeartRate", _same),
        "average_hrv": ("hrvRMSSD", _same),
        "average_breath": ("dailySleepRespiratoryRate", _same),
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
        # Any activity, cycling included: walkingRunningDistances is
        # walking or running only (112427-0).
        "distance": ("exerciseDistances", _same),
    },
    "personal_info": {
        "weight": ("bodyMasss", _same),
        "height": ("heights", _same),
    },
}


DATA_TYPES: tuple[str, ...] = tuple(MAPPING)

#: The sleep fields that are the night's figure: filed on the period's
#: ``day``, and only from the main sleep. A ``daily…`` summary keeps one value
#: a day (the last revision), so a nap's deep sleep filed beside the night's
#: would replace it.
NIGHTLY: frozenset[str] = frozenset({
    "total_sleep_duration", "awake_time", "deep_sleep_duration", "light_sleep_duration",
    "rem_sleep_duration", "latency", "average_heart_rate", "lowest_heart_rate", "average_breath",
})

#: One sleep's scores, filed with the bedtime span and only from the main
#: sleep. Oura scores a nap under the same fields and a nap falls in the
#: night's 18:00 day, so its efficiency was averaged into
#: ``dailyAvgSleepEfficiency`` and was the day's last score; the catalogue has
#: no nap score to file it under (``napDuration`` is a duration).
SCORES: frozenset[str] = frozenset({"efficiency", "restless_periods"})

#: Oura's ``PublicSleepType``: "long_sleep" is the sleep "long enough (>3h) to
#: automatically contribute to daily scores"; "deleted" is a sleep the user
#: deleted and "rest" one the user rejected as falsely detected, so neither
#: is a reading.
MAIN_SLEEP = "long_sleep"
NOT_SLEEP: frozenset[str] = frozenset({"deleted", "rest"})


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
    ts = record_time_ms(data_type, item, tz, pulled_at_ms)
    if not ts:
        return []
    start, end = ts, ts
    night: tuple[int, int] | None = None
    if STRATEGY[data_type] == "day":
        start, end = local_day_window(ts, tz)
    elif data_type == "sleep":
        kind = str(item.get("type") or "")
        if kind in NOT_SLEEP:
            return []
        bed_start = parse_ts_smart(str(item.get("bedtime_start") or ""), tz)
        bed_end = parse_ts_smart(str(item.get("bedtime_end") or ""), tz)
        if bed_start:
            start = bed_start
        end = bed_end if bed_end and bed_end > start else start
        # A period with no type cannot be told from the night; it is read as one.
        if kind in ("", MAIN_SLEEP):
            day = parse_ts_smart(str(item.get("day") or ""), tz) or end
            night = local_day_window(day, tz)
    elif data_type == "workout":
        s = parse_ts_smart(str(item.get("start_datetime") or ""), tz)
        e = parse_ts_smart(str(item.get("end_datetime") or ""), tz)
        if s:
            start = s
        end = e if e and e > start else start

    out: list[Fact] = []
    for path, (metric, conv) in mapping.items():
        span = (start, end)
        if data_type == "sleep" and (path in NIGHTLY or path in SCORES):
            if night is None:
                continue
            if path in NIGHTLY:
                span = night
        v = number(dig(item, path))
        if v is None:
            continue
        out.append(
            fact(metric, float(conv(v)), *span, source_record_id=source_record_id, ingested_at_ms=ingested_at_ms)
        )
    return out

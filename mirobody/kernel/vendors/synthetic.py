"""Plausible vendor payloads for demos and tests, from a seed.

``synthesize("garmin", seed=1, start=date(2026, 6, 1), end=date(2026, 6, 7),
tz="Asia/Shanghai")`` yields ``(data_type, item)`` pairs shaped like the
vendor's public API documents, so a demo database, a decoder test or a
consumer's own pipeline can be fed without any real person's data. Values
are drawn from adult reference ranges with day-to-day drift; nothing here
is a measurement. Deterministic for a given seed. Pure; stdlib only.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import date, datetime, timedelta

from ..series import day_bounds_ms, zone

MS = 1000


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / MS, zone("UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _days(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def synthesize(vendor: str, *, seed: int, start: date, end: date, tz: str = "UTC") -> list[tuple[str, dict]]:
    """Sample payloads for ``vendor`` covering ``start..end`` (inclusive)."""
    rng = random.Random(f"{vendor}:{seed}")
    fn = {"garmin": _garmin, "whoop": _whoop, "oura": _oura}.get(vendor)
    if fn is None:
        raise ValueError(f"no synthesizer for vendor {vendor!r}")
    out: list[tuple[str, dict]] = []
    for d in _days(start, end):
        out.extend(fn(rng, d, tz))
    return out


def _garmin(rng: random.Random, d: date, tz: str) -> list[tuple[str, dict]]:
    day_start_ms, _ = day_bounds_ms(d, tz)
    day_start_s = day_start_ms // MS
    steps = int(rng.gauss(8000, 2500))
    rhr = int(rng.gauss(58, 5))
    hr_samples = [
        {"timestampOffsetInSeconds": off, "heartRateInBeatsPerMinute": max(45, int(rng.gauss(rhr + 15, 12)))}
        for off in range(0, 86400, 900)
    ]
    sleep_start = day_start_s - 3600 * 1  # 23:00 the evening before
    asleep = int(rng.gauss(7 * 3600, 2400))
    awake = int(abs(rng.gauss(1500, 600)))
    deep = int(asleep * rng.uniform(0.15, 0.25))
    rem = int(asleep * rng.uniform(0.18, 0.25))
    light = asleep - deep - rem
    return [
        (
            "dailies",
            {
                "summaryId": f"x-d-{d.isoformat()}",
                "calendarDate": d.isoformat(),
                "startTimeInSeconds": day_start_s,
                "steps": max(0, steps),
                "distanceInMeters": round(max(0, steps) * 0.72, 1),
                "activeKilocalories": int(rng.gauss(450, 120)),
                "bmrKilocalories": int(rng.gauss(1500, 100)),
                "floorsClimbed": int(abs(rng.gauss(10, 5))),
                "activeTimeInSeconds": int(abs(rng.gauss(3600, 900))),
                "minHeartRateInBeatsPerMinute": rhr - rng.randint(3, 8),
                "maxHeartRateInBeatsPerMinute": int(rng.gauss(140, 15)),
                "averageHeartRateInBeatsPerMinute": rhr + rng.randint(8, 16),
                "restingHeartRateInBeatsPerMinute": rhr,
                "timeOffsetHeartRateSamples": hr_samples,
            },
        ),
        (
            "sleeps",
            {
                "summaryId": f"x-s-{d.isoformat()}",
                "calendarDate": d.isoformat(),
                "startTimeInSeconds": sleep_start,
                "durationInSeconds": asleep,
                "awakeDurationInSeconds": awake,
                "deepSleepDurationInSeconds": deep,
                "lightSleepDurationInSeconds": light,
                "remSleepInSeconds": rem,
                "sleepLevelsMap": {
                    "deep": [{"startTimeInSeconds": sleep_start, "endTimeInSeconds": sleep_start + deep}],
                    "light": [
                        {"startTimeInSeconds": sleep_start + deep, "endTimeInSeconds": sleep_start + deep + light}
                    ],
                    "rem": [
                        {"startTimeInSeconds": sleep_start + deep + light, "endTimeInSeconds": sleep_start + asleep}
                    ],
                    "awake": [
                        {"startTimeInSeconds": sleep_start + asleep, "endTimeInSeconds": sleep_start + asleep + awake}
                    ],
                },
            },
        ),
    ]


def _whoop(rng: random.Random, d: date, tz: str) -> list[tuple[str, dict]]:
    day_start_ms, day_end_ms = day_bounds_ms(d, tz)
    kj = rng.gauss(8400, 1200)
    return [
        (
            "cycle",
            {
                "id": int(d.strftime("%Y%m%d")),
                "user_id": 1,
                "start": _iso(day_start_ms),
                "end": _iso(day_end_ms),
                "score_state": "SCORED",
                "score": {
                    "strain": round(rng.uniform(6, 16), 1),
                    "kilojoule": round(kj, 1),
                    "average_heart_rate": int(rng.gauss(68, 6)),
                    "max_heart_rate": int(rng.gauss(150, 15)),
                },
            },
        ),
        (
            "recovery",
            {
                "cycle_id": int(d.strftime("%Y%m%d")),
                "created_at": _iso(day_start_ms + 7 * 3600 * MS),
                "score_state": "SCORED",
                "score": {
                    "recovery_score": int(rng.uniform(30, 95)),
                    "resting_heart_rate": int(rng.gauss(55, 5)),
                    "hrv_rmssd_milli": round(rng.gauss(50, 15), 1),
                    "spo2_percentage": round(rng.uniform(95, 99), 1),
                    "skin_temp_celsius": round(rng.gauss(33.5, 0.4), 1),
                },
            },
        ),
    ]


def _oura(rng: random.Random, d: date, tz: str) -> list[tuple[str, dict]]:
    day_start_ms, _ = day_bounds_ms(d, tz)
    steps = max(0, int(rng.gauss(9000, 2500)))
    bed_start = datetime.fromtimestamp((day_start_ms - 30 * 60 * MS) / MS, zone(tz))
    total = int(rng.gauss(7 * 3600, 2400))
    bed_end = bed_start + timedelta(seconds=total + 1800)
    return [
        (
            "daily_activity",
            {
                "id": f"a-{d.isoformat()}",
                "day": d.isoformat(),
                "score": int(rng.uniform(60, 95)),
                "steps": steps,
                "active_calories": int(rng.gauss(480, 120)),
                "total_calories": int(rng.gauss(2200, 200)),
                "equivalent_walking_distance": int(steps * 0.75),
                "high_activity_time": int(abs(rng.gauss(600, 300))),
                "medium_activity_time": int(abs(rng.gauss(1800, 600))),
                "low_activity_time": int(abs(rng.gauss(7200, 1800))),
                "sedentary_time": int(rng.gauss(30000, 4000)),
                "resting_time": int(rng.gauss(28800, 2400)),
            },
        ),
        (
            "sleep",
            {
                "id": f"s-{d.isoformat()}",
                "day": d.isoformat(),
                "bedtime_start": bed_start.isoformat(),
                "bedtime_end": bed_end.isoformat(),
                "type": "long_sleep",
                "total_sleep_duration": total,
                "time_in_bed": total + 1800,
                "awake_time": 1800,
                "deep_sleep_duration": int(total * 0.2),
                "light_sleep_duration": int(total * 0.6),
                "rem_sleep_duration": total - int(total * 0.2) - int(total * 0.6),
                "efficiency": int(rng.uniform(85, 96)),
                "latency": int(abs(rng.gauss(600, 300))),
                "average_heart_rate": round(rng.gauss(56, 5), 1),
                "lowest_heart_rate": int(rng.gauss(50, 4)),
                "average_hrv": int(rng.gauss(45, 12)),
                "average_breath": round(rng.gauss(14.5, 1.2), 1),
                "restless_periods": int(abs(rng.gauss(3, 2))),
                "temperature_delta": round(rng.gauss(0, 0.2), 2),
            },
        ),
    ]


__all__ = ["synthesize"]

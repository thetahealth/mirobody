"""
R01: Multi-Signal Deterioration

Detects when >=2 indicators simultaneously deviate from baseline
in the "unhealthy" direction for >=3 consecutive days.
"""

from datetime import date, timedelta
from typing import Dict, List, Tuple

from ..models import (
    DailyValues,
    IndicatorDeviation,
    InsightDetection,
    Severity,
    UserProfile,
)

# Which direction is "bad" for each category
BAD_DIRECTION = {
    "heartRate": "up",
    "sleepDeep": "down",
    "hrv": "down",
    "bloodGlucose": "up",
    "stress": "up",
    "spo2": "down",
    "steps": "down",
    "bpSystolic": "up",
}

MIN_SIGNALS = 2
MIN_CONSECUTIVE_DAYS = 3
SIGMA_THRESHOLD = 2.0


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    """Detect multi-signal deterioration.

    Args:
        profile: UserProfile with baselines
        daily_values: Dict of indicator_name -> [(date, value), ...]

    Returns:
        InsightDetection (triggered=True if >=2 signals deviate for >=3 days)
    """
    target = profile.target_date

    # Build per-day deviation map: day -> list of deviations
    day_deviations: Dict[date, List[IndicatorDeviation]] = {}

    for category, baseline in profile.baselines.items():
        if category not in BAD_DIRECTION:
            continue
        if baseline.std <= 0:
            continue

        bad_dir = BAD_DIRECTION[category]
        values = daily_values.get(baseline.indicator_name, [])
        value_map = {d: v for d, v in values}

        for day_offset in range(MIN_CONSECUTIVE_DAYS + 7):  # check recent days
            check_date = target - timedelta(days=day_offset)
            val = value_map.get(check_date)
            if val is None:
                continue

            sigma = (val - baseline.mean) / baseline.std
            direction = "up" if val > baseline.mean else "down"

            if direction == bad_dir and abs(sigma) >= SIGMA_THRESHOLD:
                if check_date not in day_deviations:
                    day_deviations[check_date] = []
                day_deviations[check_date].append(IndicatorDeviation(
                    category=category,
                    indicator_name=baseline.indicator_name,
                    direction=direction,
                    current_value=round(val, 2),
                    baseline_mean=baseline.mean,
                    baseline_std=baseline.std,
                    sigma_deviation=round(abs(sigma), 2),
                    consecutive_days=0,  # filled below
                ))

    # Find consecutive runs of days with >= MIN_SIGNALS deviations
    best_run_days = 0
    best_run_deviations: List[IndicatorDeviation] = []
    current_run = 0

    for day_offset in range(30):
        check_date = target - timedelta(days=day_offset)
        devs = day_deviations.get(check_date, [])

        if len(devs) >= MIN_SIGNALS:
            current_run += 1
            if current_run > best_run_days:
                best_run_days = current_run
                best_run_deviations = devs
        else:
            current_run = 0

    if best_run_days < MIN_CONSECUTIVE_DAYS:
        return InsightDetection(triggered=False)

    # Determine severity
    max_sigma = max(d.sigma_deviation for d in best_run_deviations) if best_run_deviations else 0
    if max_sigma >= 4.0 or best_run_days >= 7:
        severity = Severity.SEVERE
    elif max_sigma >= 3.0 or best_run_days >= 5:
        severity = Severity.MODERATE
    else:
        severity = Severity.MILD

    # Update consecutive_days
    for dev in best_run_deviations:
        dev.consecutive_days = best_run_days

    # Build observation text
    details = "; ".join(
        f"{d.category} {d.direction} {d.sigma_deviation}σ "
        f"(基线{d.baseline_mean:.1f}, 当前{d.current_value:.1f})"
        for d in best_run_deviations
    )
    observation = f"过去{best_run_days}天，观察到{len(best_run_deviations)}项指标同时偏离基线：{details}"

    return InsightDetection(
        triggered=True,
        severity=severity,
        observation_text=observation,
        deviations=best_run_deviations,
        metadata={"consecutive_days": best_run_days, "num_signals": len(best_run_deviations)},
    )

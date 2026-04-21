"""
R02: Single Indicator Sustained Anomaly

Detects when a single indicator deviates >2.5σ from baseline for >=5 consecutive days.
"""

from datetime import date, timedelta
from typing import List

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

SIGMA_THRESHOLD = 2.5
MIN_CONSECUTIVE_DAYS = 5
MAX_STABLE_DAYS = 14  # If sustained > 14 days and not worsening, stop triggering

# Body composition indicators change slowly; require larger absolute deviation
SLOW_CHANGE_CATEGORIES = {"weight", "bodyFat", "bmi", "bodyMass", "leanBodyMass", "visceralFat"}
SLOW_CHANGE_MIN_ABSOLUTE = {
    "weight": 3.0,       # kg
    "bodyFat": 3.0,      # percentage points
    "bmi": 1.5,          # kg/m²
    "bodyMass": 3.0,     # kg
    "leanBodyMass": 2.0, # kg
    "visceralFat": 2.0,  # level
}
SLOW_CHANGE_SIGMA_THRESHOLD = 5.0  # higher bar for body composition


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    target = profile.target_date
    best_deviation = None
    best_run = 0

    for category, baseline in profile.baselines.items():
        if baseline.std <= 0:
            continue

        # Body composition: use higher sigma threshold
        is_slow = category in SLOW_CHANGE_CATEGORIES
        sigma_thresh = SLOW_CHANGE_SIGMA_THRESHOLD if is_slow else SIGMA_THRESHOLD

        values = daily_values.get(baseline.indicator_name, [])
        value_map = {d: v for d, v in values}

        # Count consecutive days of deviation ending at target_date
        run = 0
        last_sigma = 0.0
        last_val = 0.0
        sigmas = []  # track sigma trend
        for day_offset in range(30):
            check_date = target - timedelta(days=day_offset)
            val = value_map.get(check_date)
            if val is None:
                break

            sigma = abs(val - baseline.mean) / baseline.std
            if sigma >= sigma_thresh:
                run += 1
                last_sigma = sigma
                last_val = val
                sigmas.append(sigma)
            else:
                break

        # Skip if sustained too long AND not worsening (chronic stable state)
        if run > MAX_STABLE_DAYS and len(sigmas) >= 7:
            recent_avg = sum(sigmas[:7]) / 7       # last 7 days
            earlier_avg = sum(sigmas[7:14]) / min(len(sigmas[7:14]), 7) if len(sigmas) > 7 else recent_avg
            if recent_avg <= earlier_avg * 1.1:     # not worsening (< 10% increase)
                continue  # chronic stable state, don't re-trigger

        if run >= MIN_CONSECUTIVE_DAYS and run > best_run:
            best_run = run
            direction = "up" if last_val > baseline.mean else "down"
            best_deviation = IndicatorDeviation(
                category=category,
                indicator_name=baseline.indicator_name,
                direction=direction,
                current_value=round(last_val, 2),
                baseline_mean=baseline.mean,
                baseline_std=baseline.std,
                sigma_deviation=round(last_sigma, 2),
                consecutive_days=run,
            )

    if not best_deviation:
        return InsightDetection(triggered=False)

    # Body composition: require minimum absolute change from baseline
    if best_deviation.category in SLOW_CHANGE_CATEGORIES:
        min_abs = SLOW_CHANGE_MIN_ABSOLUTE.get(best_deviation.category, 2.0)
        abs_change = abs(best_deviation.current_value - best_deviation.baseline_mean)
        if abs_change < min_abs:
            return InsightDetection(triggered=False)

    if best_deviation.sigma_deviation >= 4.0 or best_run >= 10:
        severity = Severity.SEVERE
    elif best_deviation.sigma_deviation >= 3.0 or best_run >= 7:
        severity = Severity.MODERATE
    else:
        severity = Severity.MILD

    direction_cn = "高" if best_deviation.direction == "up" else "低"
    observation = (
        f"你的{best_deviation.category}连续{best_run}天偏{direction_cn}"
        f"（基线{best_deviation.baseline_mean:.1f}，当前{best_deviation.current_value:.1f}，"
        f"偏离{best_deviation.sigma_deviation}σ）"
    )

    return InsightDetection(
        triggered=True,
        severity=severity,
        observation_text=observation,
        deviations=[best_deviation],
        metadata={"consecutive_days": best_run},
    )

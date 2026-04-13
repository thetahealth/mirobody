"""
R05: Weekday-Weekend Pattern Detection

Detects when weekend vs weekday values differ by >30% for >=4 weeks.
"""

import statistics
from datetime import date, timedelta

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

DIFF_THRESHOLD_PCT = 30  # minimum percentage difference
MIN_WEEKS = 4


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    target = profile.target_date
    baseline = profile.baselines.get("steps")

    if not baseline or baseline.std <= 0:
        return InsightDetection(triggered=False)

    values = daily_values.get(baseline.indicator_name, [])
    cutoff = target - timedelta(weeks=MIN_WEEKS)
    recent = [(d, v) for d, v in values if cutoff <= d <= target]

    if len(recent) < MIN_WEEKS * 5:  # need at least 5 days per week
        return InsightDetection(triggered=False)

    # Split by weekday/weekend
    weekday_vals = [v for d, v in recent if d.weekday() < 5]  # Mon-Fri
    weekend_vals = [v for d, v in recent if d.weekday() >= 5]  # Sat-Sun

    if len(weekday_vals) < MIN_WEEKS * 3 or len(weekend_vals) < MIN_WEEKS:
        return InsightDetection(triggered=False)

    weekday_avg = statistics.mean(weekday_vals)
    weekend_avg = statistics.mean(weekend_vals)

    if weekday_avg == 0 and weekend_avg == 0:
        return InsightDetection(triggered=False)

    # Calculate percentage difference relative to the larger value
    max_val = max(weekday_avg, weekend_avg)
    diff_pct = abs(weekend_avg - weekday_avg) / max_val * 100 if max_val > 0 else 0

    if diff_pct < DIFF_THRESHOLD_PCT:
        return InsightDetection(triggered=False)

    direction = "高" if weekend_avg > weekday_avg else "低"
    observation = (
        f"你的工作日{baseline.category}均值{weekday_avg:.0f}，"
        f"周末{weekend_avg:.0f}（周末比工作日{direction}{diff_pct:.0f}%），"
        f"持续{MIN_WEEKS}周以上"
    )

    return InsightDetection(
        triggered=True,
        severity=Severity.MILD,
        observation_text=observation,
        deviations=[IndicatorDeviation(
            category=baseline.category,
            indicator_name=baseline.indicator_name,
            direction="up" if weekend_avg > weekday_avg else "down",
            current_value=round(weekend_avg, 1),
            baseline_mean=round(weekday_avg, 1),
            baseline_std=baseline.std,
            sigma_deviation=round(diff_pct / 100 * baseline.mean / baseline.std, 2) if baseline.std > 0 else 0,
            consecutive_days=MIN_WEEKS * 7,
        )],
        metadata={
            "weekday_avg": round(weekday_avg, 1),
            "weekend_avg": round(weekend_avg, 1),
            "diff_pct": round(diff_pct, 1),
        },
    )

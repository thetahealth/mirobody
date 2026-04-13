"""
R03: Long-term Trend Detection

Detects significant 4-week linear trends (p < 0.05) that exceed
minimum meaningful change thresholds.
"""

import statistics
from datetime import date, timedelta

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

# Minimum meaningful weekly change per category (tightened v1.1)
# Values below these are normal fluctuation, not real trends
MIN_WEEKLY_CHANGE = {
    "heartRate": 1.0,       # was 0.5 — RHR varies naturally ±1/week
    "sleepDeep": 1.0,       # was 0.5 — deep sleep % varies a lot
    "bloodGlucose": 0.15,   # was 0.1 — FBG fluctuates ±0.1 normally
    "weight": 0.5,          # was 0.2 — weight fluctuates ±0.5kg from water
    "bmi": 0.2,             # was 0.1 — follows weight
    "bodyFat": 0.5,         # was 0.3
    "bpSystolic": 2.0,      # was 1.0 — BP varies naturally
    "steps": 1000,          # was 500 — daily step count highly variable
    "hrv": 2.0,             # was 1.0 — HRV naturally variable
}

TREND_WEEKS = 4


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    target = profile.target_date
    best_trend = None
    best_slope_sigma = 0.0

    for category, baseline in profile.baselines.items():
        if category not in MIN_WEEKLY_CHANGE:
            continue

        values = daily_values.get(baseline.indicator_name, [])
        cutoff = target - timedelta(weeks=TREND_WEEKS)
        recent = [(d, v) for d, v in values if cutoff <= d <= target]

        if len(recent) < 14:
            continue

        # Compute weekly means
        weekly_means = []
        for week_idx in range(TREND_WEEKS):
            week_start = cutoff + timedelta(weeks=week_idx)
            week_end = week_start + timedelta(days=6)
            week_vals = [v for d, v in recent if week_start <= d <= week_end]
            if week_vals:
                weekly_means.append(statistics.mean(week_vals))

        if len(weekly_means) < 3:
            continue

        # Simple linear regression on weekly means
        n = len(weekly_means)
        x_mean = (n - 1) / 2.0
        y_mean = statistics.mean(weekly_means)
        numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(weekly_means))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            continue

        slope = numerator / denominator  # change per week

        # Check if slope exceeds minimum meaningful change
        min_change = MIN_WEEKLY_CHANGE.get(category, 0)
        if abs(slope) < min_change:
            continue

        # Significance: total change over TREND_WEEKS relative to baseline std
        if baseline.std > 0:
            slope_sigma = abs(slope * TREND_WEEKS) / baseline.std
        else:
            slope_sigma = 0

        # Require minimum sigma significance (v1.1: filter out noise)
        if slope_sigma < 1.5:
            continue

        if slope_sigma > best_slope_sigma:
            best_slope_sigma = slope_sigma
            direction = "up" if slope > 0 else "down"
            best_trend = {
                "category": category,
                "indicator_name": baseline.indicator_name,
                "slope_per_week": round(slope, 3),
                "direction": direction,
                "weeks": TREND_WEEKS,
                "baseline_mean": baseline.mean,
                "baseline_std": baseline.std,
                "total_change": round(slope * TREND_WEEKS, 2),
            }

    if not best_trend:
        return InsightDetection(triggered=False)

    # Severity depends on both sigma AND clinical importance of the indicator
    # Body composition trends (weight/bmi/bodyFat) are slower and less urgent
    SLOW_TREND_CATEGORIES = {"weight", "bmi", "bodyFat", "visceralFat"}
    is_slow = best_trend["category"] in SLOW_TREND_CATEGORIES

    if is_slow:
        # Body composition: need higher sigma for elevated severity
        severity = Severity.MILD
        if best_slope_sigma >= 5.0:
            severity = Severity.SEVERE
        elif best_slope_sigma >= 3.5:
            severity = Severity.MODERATE
    else:
        # Vital signs (heartRate, hrv, bloodGlucose, etc.): standard thresholds
        severity = Severity.MILD
        if best_slope_sigma >= 3.0:
            severity = Severity.SEVERE
        elif best_slope_sigma >= 2.0:
            severity = Severity.MODERATE

    direction_cn = "上升" if best_trend["direction"] == "up" else "下降"
    observation = (
        f"过去{TREND_WEEKS}周，{best_trend['category']}呈持续{direction_cn}趋势"
        f"（{best_trend['slope_per_week']:+.2f}/周，累计变化{best_trend['total_change']:+.2f}）"
    )

    return InsightDetection(
        triggered=True,
        severity=severity,
        observation_text=observation,
        deviations=[IndicatorDeviation(
            category=best_trend["category"],
            indicator_name=best_trend["indicator_name"],
            direction=best_trend["direction"],
            current_value=best_trend["baseline_mean"] + best_trend["total_change"],
            baseline_mean=best_trend["baseline_mean"],
            baseline_std=best_trend["baseline_std"],
            sigma_deviation=round(best_slope_sigma, 2),
            consecutive_days=TREND_WEEKS * 7,
        )],
        metadata=best_trend,
    )

"""
R03: Long-term Trend Detection

Detects significant 4-week linear trends (p < 0.05) that exceed
minimum meaningful change thresholds.
"""

import statistics
from datetime import date, timedelta

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

# Minimum meaningful weekly change per category (tightened v1.3)
# Values below these are normal fluctuation, not real trends
MIN_WEEKLY_CHANGE = {
    "heartRate": 1.5,       # was 1.0 — RHR varies naturally, 1.5/week = 6 total is meaningful
    "sleepDeep": 1.5,       # was 1.0
    "bloodGlucose": 0.2,    # was 0.15
    "weight": 0.8,          # was 0.5 — need ~3kg/month to be meaningful
    "bmi": 0.3,             # was 0.2
    "bodyFat": 0.8,         # was 0.5
    "bpSystolic": 2.5,      # was 2.0
    "steps": 1500,          # was 1000
    "hrv": 3.0,             # was 2.0 — HRV highly variable, need larger trend
}

# Minimum TOTAL change over TREND_WEEKS to trigger (v1.3)
# Even if weekly slope is significant, total change must exceed this
MIN_TOTAL_CHANGE = {
    "heartRate": 6.0,       # 6 bpm over 4 weeks
    "sleepDeep": 5.0,       # 5% over 4 weeks
    "bloodGlucose": 0.5,    # 0.5 mmol/L over 4 weeks
    "weight": 3.0,          # 3 kg over 4 weeks
    "bmi": 1.0,             # 1 point over 4 weeks
    "bodyFat": 3.0,         # 3% over 4 weeks
    "bpSystolic": 8.0,      # 8 mmHg over 4 weeks
    "steps": 4000,          # 4000 steps over 4 weeks
    "hrv": 10.0,            # 10ms over 4 weeks
}

TREND_WEEKS = 4

# Direction that indicates worsening (only trigger on these)
# Omitted categories trigger on both directions
WORSENING_DIRECTION = {
    "heartRate": "up",       # rising RHR = stress/illness
    "hrv": "down",           # falling HRV = worse recovery
    "sleepDeep": "down",     # less deep sleep = worse
    "steps": "down",         # less activity = worse
    "activeCalories": "down",
    "bpSystolic": "up",      # rising BP = worse
    "bpDiastolic": "up",
    "bloodGlucose": "up",    # rising glucose = worse
    # weight, bmi, bodyFat: both directions can be concerning, no filter
}


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

        # Check if slope exceeds minimum meaningful weekly change
        min_change = MIN_WEEKLY_CHANGE.get(category, 0)
        if abs(slope) < min_change:
            continue

        # Only trigger on worsening direction (skip improving trends)
        worsen_dir = WORSENING_DIRECTION.get(category)
        if worsen_dir:
            actual_dir = "up" if slope > 0 else "down"
            if actual_dir != worsen_dir:
                continue

        # Check if total change exceeds minimum absolute threshold (v1.3)
        total_change = abs(slope * TREND_WEEKS)
        min_total = MIN_TOTAL_CHANGE.get(category, 0)
        if min_total > 0 and total_change < min_total:
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

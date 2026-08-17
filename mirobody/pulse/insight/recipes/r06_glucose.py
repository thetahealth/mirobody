"""
R06: Glucose Control Fluctuation

Detects sustained elevation in fasting blood glucose (>1.5σ for >=7 days)
or TAR (time above range) elevation.
"""

from datetime import date, timedelta

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

SIGMA_THRESHOLD = 1.5
MIN_CONSECUTIVE_DAYS = 7


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    target = profile.target_date
    baseline = profile.baselines.get("bloodGlucose")

    if not baseline or baseline.std <= 0:
        return InsightDetection(triggered=False)

    values = daily_values.get(baseline.indicator_name, [])
    value_map = {d: v for d, v in values}

    # Count consecutive days of elevated glucose ending at target_date
    run = 0
    total = 0.0
    count = 0
    for day_offset in range(30):
        d = target - timedelta(days=day_offset)
        val = value_map.get(d)
        if val is None:
            break

        sigma = (val - baseline.mean) / baseline.std
        if sigma >= SIGMA_THRESHOLD:
            run += 1
            total += val
            count += 1
        else:
            break

    if run < MIN_CONSECUTIVE_DAYS:
        return InsightDetection(triggered=False)

    recent_avg = total / count if count > 0 else 0
    sigma_avg = (recent_avg - baseline.mean) / baseline.std if baseline.std > 0 else 0

    if sigma_avg >= 3.0:
        severity = Severity.SEVERE
    elif sigma_avg >= 2.0:
        severity = Severity.MODERATE
    else:
        severity = Severity.MILD

    direction_cn = "高" if recent_avg > baseline.mean else "低"
    observation = (
        f"近期空腹血糖持续偏{direction_cn}"
        f"（基线{baseline.mean:.2f}，近{run}天均值{recent_avg:.2f}，"
        f"偏离{sigma_avg:.1f}σ）"
    )

    return InsightDetection(
        triggered=True,
        severity=severity,
        observation_text=observation,
        deviations=[IndicatorDeviation(
            category="bloodGlucose",
            indicator_name=baseline.indicator_name,
            direction="up" if recent_avg > baseline.mean else "down",
            current_value=round(recent_avg, 3),
            baseline_mean=baseline.mean,
            baseline_std=baseline.std,
            sigma_deviation=round(abs(sigma_avg), 2),
            consecutive_days=run,
        )],
        metadata={"consecutive_days": run, "recent_avg": round(recent_avg, 3)},
    )

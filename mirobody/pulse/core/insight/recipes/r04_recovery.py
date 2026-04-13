"""
R04: Recovery Trend Detection

Detects when a previously elevated indicator is returning toward baseline
for >=5 consecutive days.
"""

from datetime import date, timedelta

from ..models import DailyValues, IndicatorDeviation, InsightDetection, Severity, UserProfile

MIN_RECOVERY_DAYS = 5


def detect(profile: UserProfile, daily_values: DailyValues) -> InsightDetection:
    target = profile.target_date
    baseline = profile.baselines.get("heartRate")

    if not baseline or baseline.std <= 0:
        return InsightDetection(triggered=False)

    values = daily_values.get(baseline.indicator_name, [])
    value_map = {d: v for d, v in values}

    # Find peak (max deviation in last 60 days)
    peak_sigma = 0.0
    peak_val = 0.0
    peak_date = None
    for day_offset in range(60):
        d = target - timedelta(days=day_offset)
        val = value_map.get(d)
        if val is None:
            continue
        sigma = abs(val - baseline.mean) / baseline.std
        if sigma > peak_sigma:
            peak_sigma = sigma
            peak_val = val
            peak_date = d

    # Need a significant peak (>= 2σ)
    if peak_sigma < 2.0 or peak_date is None:
        return InsightDetection(triggered=False)

    # Check if values are declining from peak toward baseline
    # Walk FORWARD from peak to target, counting consecutive days where sigma decreases
    recovery_days = 0
    prev_sigma = None
    current_val = None

    d = peak_date
    while d <= target:
        val = value_map.get(d)
        if val is not None:
            current_sigma = abs(val - baseline.mean) / baseline.std
            if prev_sigma is not None:
                if current_sigma < prev_sigma:
                    recovery_days += 1
                else:
                    recovery_days = 0
            prev_sigma = current_sigma
            current_val = val
        d += timedelta(days=1)

    if recovery_days < MIN_RECOVERY_DAYS or current_val is None:
        return InsightDetection(triggered=False)

    current_sigma = abs(current_val - baseline.mean) / baseline.std

    # Only report if still somewhat elevated but clearly declining
    # Or if just recovered (current_sigma < 1.0 but had a big peak)
    if current_sigma < 0.5 and peak_sigma < 3.0:
        return InsightDetection(triggered=False)

    observation = (
        f"好消息：{baseline.category}从峰值{peak_val:.1f}降至{current_val:.1f}，"
        f"已连续{recovery_days}天回落（基线{baseline.mean:.1f}）"
    )

    return InsightDetection(
        triggered=True,
        severity=Severity.MILD,
        observation_text=observation,
        deviations=[IndicatorDeviation(
            category=baseline.category,
            indicator_name=baseline.indicator_name,
            direction="down",
            current_value=round(current_val, 2),
            baseline_mean=baseline.mean,
            baseline_std=baseline.std,
            sigma_deviation=round(current_sigma, 2),
            consecutive_days=recovery_days,
        )],
        metadata={
            "peak_value": round(peak_val, 1),
            "peak_date": str(peak_date),
            "recovery_days": recovery_days,
        },
    )

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
    # Use sliding window comparison: recent 5 days avg sigma vs earlier 5 days avg sigma
    all_sigmas = []  # (date, sigma, value) from peak to target
    d = peak_date
    while d <= target:
        val = value_map.get(d)
        if val is not None:
            sigma = abs(val - baseline.mean) / baseline.std
            all_sigmas.append((d, sigma, val))
        d += timedelta(days=1)

    if len(all_sigmas) < MIN_RECOVERY_DAYS * 2:
        return InsightDetection(triggered=False)

    # Compare recent window vs earlier window
    window = MIN_RECOVERY_DAYS
    recent_sigmas = [s for _, s, _ in all_sigmas[-window:]]
    earlier_sigmas = [s for _, s, _ in all_sigmas[-(window * 2):-window]]

    if not recent_sigmas or not earlier_sigmas:
        return InsightDetection(triggered=False)

    recent_avg = sum(recent_sigmas) / len(recent_sigmas)
    earlier_avg = sum(earlier_sigmas) / len(earlier_sigmas)
    current_val = all_sigmas[-1][2]
    current_sigma = all_sigmas[-1][1]

    # Recovery = recent window clearly lower than earlier window (>20% reduction)
    if recent_avg >= earlier_avg * 0.8:
        return InsightDetection(triggered=False)

    recovery_days = len(all_sigmas) - all_sigmas.index(max(all_sigmas, key=lambda x: x[1]))

    # Minimum absolute drop from peak (v1.3: filter trivial recoveries like 2bpm)
    absolute_drop = abs(peak_val - current_val)
    if absolute_drop < baseline.std * 2:  # must drop at least 2σ in absolute terms
        return InsightDetection(triggered=False)

    # Only report if had a meaningful peak and still somewhat elevated
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

"""
Baseline Engine

Computes personal baselines for each user's indicator categories.
Uses EWMA (Exponentially Weighted Moving Average) with freeze logic.

Input:  user_id + target_date + raw daily values from th_series_data
Output: UserProfile (baselines + tags + densities + available_categories)
"""

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

from .indicator_aliases import resolve_all
from .models import (
    BaselineResult,
    DensityLevel,
    DailyValues,
    IndicatorDensity,
    UserProfile,
)


# =============================================================================
# Tag inference rules
# =============================================================================

TAG_RULES = [
    # (tag_name, category, condition_fn)
    # condition_fn receives (mean, std, data_days) and returns bool
    ("obesity", "bmi", lambda mean, std, days: mean > 28 and days >= 7),
    ("pre_diabetes", "bloodGlucose", lambda mean, std, days: mean > 5.6 and days >= 7),
    ("athlete", "heartRate", lambda mean, std, days: mean < 50 and days >= 14),
    ("cardiac_risk", "heartRate", lambda mean, std, days: mean > 90 and days >= 14),
    ("hypertension", "bpSystolic", lambda mean, std, days: mean > 130 and days >= 7),
]


# =============================================================================
# Baseline Engine
# =============================================================================

class BaselineEngine:
    """Compute personal baselines using EWMA + freeze logic.

    Usage:
        engine = BaselineEngine()
        profile = engine.compute(user_id, target_date, user_indicators, daily_values)
    """

    def __init__(
        self,
        alpha: float = 0.1,
        baseline_lookback_days: int = 90,
        density_lookback_days: int = 14,
        freeze_sigma: float = 2.0,
        freeze_min_days: int = 3,
        unfreeze_sigma: float = 1.0,
        unfreeze_min_days: int = 7,
    ):
        self.alpha = alpha
        self.baseline_lookback_days = baseline_lookback_days
        self.density_lookback_days = density_lookback_days
        self.freeze_sigma = freeze_sigma
        self.freeze_min_days = freeze_min_days
        self.unfreeze_sigma = unfreeze_sigma
        self.unfreeze_min_days = unfreeze_min_days

    def compute(
        self,
        user_id: str,
        target_date: date,
        user_indicators: Set[str],
        daily_values: DailyValues,
    ) -> UserProfile:
        """Compute complete user profile.

        Args:
            user_id: User ID
            target_date: The date we're computing for
            user_indicators: Set of all indicator names the user has
            daily_values: Raw daily values keyed by actual indicator name

        Returns:
            UserProfile with baselines, tags, densities, available_categories
        """
        # Resolve indicator categories
        category_map = resolve_all(user_indicators)

        baselines: Dict[str, BaselineResult] = {}
        densities: Dict[str, IndicatorDensity] = {}
        available_categories: List[str] = []

        for category, indicator_name in category_map.items():
            values = daily_values.get(indicator_name, [])
            if not values:
                continue

            # Compute baseline
            baseline = self._compute_baseline(category, indicator_name, values, target_date)
            if baseline and baseline.data_days >= 7:
                baselines[category] = baseline
                available_categories.append(category)

            # Compute density
            density = self._compute_density(category, indicator_name, values, target_date)
            densities[category] = density

        # Infer tags
        tags = self._infer_tags(baselines)

        profile = UserProfile(
            user_id=user_id,
            target_date=target_date,
            tags=tags,
            baselines=baselines,
            densities=densities,
            available_categories=available_categories,
        )

        logging.info(
            f"[BaselineEngine] user={user_id} date={target_date} "
            f"categories={len(available_categories)} tags={tags}"
        )
        return profile

    def _compute_baseline(
        self,
        category: str,
        indicator_name: str,
        values: List[Tuple[date, float]],
        target_date: date,
    ) -> Optional[BaselineResult]:
        """Compute EWMA baseline with freeze logic for a single indicator.

        Freeze: if value deviates > freeze_sigma for >= freeze_min_days consecutive,
                freeze baseline (stop updating).
        Unfreeze: if value returns within unfreeze_sigma for >= unfreeze_min_days,
                  unfreeze and resume updating.
        """
        cutoff = target_date - timedelta(days=self.baseline_lookback_days)
        relevant = [(d, v) for d, v in values if cutoff <= d <= target_date]

        if len(relevant) < 3:
            return None

        # Use the EARLIEST 1/3 of data as "normal period" for initial mean/std estimate.
        # This avoids contamination from recent anomaly periods.
        import statistics
        vals = [v for _, v in relevant]
        normal_count = max(len(vals) // 3, 7)
        normal_vals = vals[:normal_count]

        if len(normal_vals) < 2:
            return None

        initial_mean = statistics.mean(normal_vals)
        initial_std = statistics.stdev(normal_vals) if len(normal_vals) >= 2 else 0.0

        # EWMA computation with freeze logic
        baseline = initial_mean
        current_std = initial_std if initial_std > 0 else 1.0
        frozen = False
        frozen_since = None
        consecutive_deviated = 0
        consecutive_normal = 0

        for d, v in relevant:
            deviation = abs(v - baseline)

            if not frozen:
                if current_std > 0 and deviation > self.freeze_sigma * current_std:
                    consecutive_deviated += 1
                    consecutive_normal = 0
                    if consecutive_deviated >= self.freeze_min_days:
                        frozen = True
                        frozen_since = d
                        logging.debug(
                            f"[Baseline] Frozen {category} for user at {d}: "
                            f"value={v}, baseline={baseline:.1f}, std={current_std:.1f}"
                        )
                else:
                    consecutive_deviated = 0
                    consecutive_normal += 1
                    # Update EWMA
                    baseline = self.alpha * v + (1 - self.alpha) * baseline
            else:
                # Frozen: check if we should unfreeze
                if current_std > 0 and deviation <= self.unfreeze_sigma * current_std:
                    consecutive_normal += 1
                    if consecutive_normal >= self.unfreeze_min_days:
                        frozen = False
                        frozen_since = None
                        consecutive_deviated = 0
                        # Resume updating
                        baseline = self.alpha * v + (1 - self.alpha) * baseline
                        logging.debug(f"[Baseline] Unfrozen {category} at {d}")
                else:
                    consecutive_normal = 0

        return BaselineResult(
            category=category,
            indicator_name=indicator_name,
            mean=round(baseline, 4),
            std=round(current_std, 4),
            frozen=frozen,
            frozen_since=frozen_since,
            data_days=len(relevant),
        )

    def _compute_density(
        self,
        category: str,
        indicator_name: str,
        values: List[Tuple[date, float]],
        target_date: date,
    ) -> IndicatorDensity:
        """Compute data density for a single indicator over the last 14 days."""
        cutoff = target_date - timedelta(days=self.density_lookback_days)
        recent = [(d, v) for d, v in values if cutoff <= d <= target_date]
        days_with_data = len(set(d for d, _ in recent))

        if days_with_data >= 12:
            level = DensityLevel.CONTINUOUS
        elif days_with_data >= 7:
            level = DensityLevel.INTERMITTENT
        elif days_with_data > 0:
            level = DensityLevel.SPARSE
        else:
            level = DensityLevel.NONE

        last_date = max((d for d, _ in recent), default=None) if recent else None

        return IndicatorDensity(
            category=category,
            indicator_name=indicator_name,
            days_with_data=days_with_data,
            level=level,
            last_data_date=last_date,
        )

    def _infer_tags(self, baselines: Dict[str, BaselineResult]) -> List[str]:
        """Infer user tags from baseline values."""
        tags = []
        for tag_name, category, condition_fn in TAG_RULES:
            baseline = baselines.get(category)
            if baseline and condition_fn(baseline.mean, baseline.std, baseline.data_days):
                tags.append(tag_name)
        return tags

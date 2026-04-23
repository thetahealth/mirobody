"""
User Data Profile Service (TH-181 W3.4)

Real-time user-level data profile from th_series_data.
No pre-computed table — single-user queries run in ~17ms.

Three capabilities:
1. Indicator coverage: what indicators does the user have, from which sources
2. Data density: how many days of data per indicator (continuous/intermittent/sparse)
3. Analyzability: how many indicator pairs have enough overlap for correlation analysis
"""

import logging
from datetime import date, timedelta
from itertools import combinations
from typing import Any, Dict, List, Optional

from ....utils import execute_query
from ..indicators_info import StandardIndicator, HealthDataType


# Category mapping for display grouping
INDICATOR_CATEGORIES = {
    "cardiovascular": ["heartRates", "restingHeartRates", "walkingHeartRates",
                        "hrvDatas", "hrvRMSSD", "oxygenSaturations",
                        "systolicPressures", "diastolicPressures",
                        "respiratoryRates", "vo2Maxs"],
    "sleep": ["sleepAnalysis_Asleep(Total)", "sleepAnalysis_Asleep(Deep)",
              "sleepAnalysis_Asleep(REM)", "sleepAnalysis_Asleep(Core)",
              "sleepAnalysis_Awake", "sleepAnalysis_InBed",
              "sleepEfficiency", "sleepDisturbances"],
    "activity": ["steps", "walkingRunningDistances", "exerciseMinutes",
                 "floors", "activeCalories", "basalCalories",
                 "walkingSpeeds", "cyclingDistances"],
    "body": ["bodyMasss", "bmis", "bodyFatPercentages", "heights",
             "bodyTemperatures", "wristTemperatures", "waters"],
    "metabolic": ["bloodGlucoses"],
}

# Reverse map: indicator name → category
_INDICATOR_TO_CATEGORY = {}
for cat, indicators in INDICATOR_CATEGORIES.items():
    for ind in indicators:
        _INDICATOR_TO_CATEGORY[ind] = cat


def _classify_indicator(indicator: str) -> str:
    """Classify an indicator into a domain category."""
    if indicator.startswith("derived"):
        return "derived"

    # Try to extract base indicator name from aggregated/daily_stats naming
    base = _extract_base_indicator(indicator)
    return _INDICATOR_TO_CATEGORY.get(base, "other")


def _extract_base_indicator(indicator: str) -> str:
    """Extract the base indicator name from various naming conventions."""
    # daily_stats_heartRatesMax → heartRates
    if indicator.startswith("daily_stats_"):
        rest = indicator[len("daily_stats_"):]
        for suffix in ["Sum", "Avg", "Max", "Min", "Last"]:
            if rest.endswith(suffix):
                return rest[:-len(suffix)]
        return rest

    # dailyAvgHeartRates.apple_health → heartRates
    base = indicator.split(".")[0]
    for prefix in ["dailyTotal", "dailyAvg", "dailyMax", "dailyMin",
                    "dailyLast", "dailyStddev", "dailyTimeOfMax", "dailyTimeOfMin",
                    "dailyTir70180", "dailyPctBelow70", "dailyPctAbove180",
                    "dailyHypoEventCount", "dailyHypoEventTimes",
                    "dailyMorningHrJump", "dailyNighttimeRestingHr",
                    "dailySleepOnsetLatency"]:
        if base.startswith(prefix):
            rest = base[len(prefix):]
            # Restore camelCase: first char lowercase
            if rest:
                return rest[0].lower() + rest[1:]
            return rest

    return base


class UserProfileService:
    """Real-time user data profile from th_series_data."""

    async def get_user_profile(
        self, user_id: str, days: int = 14, as_of: str = None
    ) -> Dict[str, Any]:
        """
        Generate complete user data profile.

        Args:
            user_id: Target user ID
            days: Lookback window for density calculation

        Returns:
            Dict with indicators, density, and analyzability assessment.
        """
        # Query th_series_data for this user
        raw_indicators = await self._query_user_indicators(user_id, days, as_of)

        # Build indicator list with classification
        indicators = []
        for row in raw_indicators:
            ind_name = row["indicator"]
            base = _extract_base_indicator(ind_name)
            category = _classify_indicator(ind_name)
            density = self._classify_density(row["days_with_data"], days)

            indicators.append({
                "indicator": ind_name,
                "base_indicator": base,
                "category": category,
                "sources": row["sources"],
                "days_with_data": row["days_with_data"],
                "total_records": row["total_records"],
                "latest_date": row["latest_date"].isoformat() if row["latest_date"] else None,
                "days_since_last": int(row["days_since_last"]) if row["days_since_last"] is not None else None,
                "min_val": float(row["min_val"]) if row["min_val"] is not None else None,
                "max_val": float(row["max_val"]) if row["max_val"] is not None else None,
                "mean_val": float(row["mean_val"]) if row["mean_val"] is not None else None,
                "density": density,
            })

        # Summary by category
        category_summary = {}
        for ind in indicators:
            cat = ind["category"]
            if cat not in category_summary:
                category_summary[cat] = {"count": 0, "continuous": 0, "intermittent": 0, "sparse": 0}
            category_summary[cat]["count"] += 1
            category_summary[cat][ind["density"]] += 1

        # Density summary
        density_counts = {"continuous": 0, "intermittent": 0, "sparse": 0}
        for ind in indicators:
            density_counts[ind["density"]] += 1

        # Analyzability: count indicator pairs with enough overlap
        # min_overlap = 85% of lookback window (e.g., 12 for 14-day window)
        min_overlap = max(int(days * 0.85), 7)
        analyzable = self._assess_analyzability(indicators, days, min_overlap)

        return {
            "user_id": user_id,
            "lookback_days": days,
            "summary": {
                "total_indicators": len(indicators),
                "by_density": density_counts,
                "by_category": category_summary,
                "analyzable_pairs": analyzable["total_pairs"],
                "sufficient_pairs": analyzable["sufficient_pairs"],
            },
            "indicators": sorted(indicators, key=lambda x: (-x["days_with_data"], x["indicator"])),
            "analyzability": analyzable,
        }

    async def _query_user_indicators(self, user_id: str, days: int, as_of: str = None) -> List[Dict]:
        """Query th_series_data for user's indicator stats."""
        # as_of: reference date (default: today). Useful for viewing historical data.
        if as_of:
            date_filter = "start_time::date <= CAST(:as_of AS date) AND start_time::date > CAST(:as_of AS date) - CAST(:days AS integer)"
            days_since = "(CAST(:as_of AS date) - MAX(start_time::date))"
        else:
            date_filter = "start_time >= NOW() - CAST(:days AS integer) * INTERVAL '1 day'"
            days_since = "(CURRENT_DATE - MAX(start_time::date))"

        query = f"""
            SELECT indicator,
                   ARRAY_AGG(DISTINCT source) as sources,
                   COUNT(DISTINCT start_time::date) as days_with_data,
                   COUNT(*) as total_records,
                   MAX(start_time::date) as latest_date,
                   {days_since} as days_since_last,
                   ROUND(MIN(value::numeric)::numeric, 2) as min_val,
                   ROUND(MAX(value::numeric)::numeric, 2) as max_val,
                   ROUND(AVG(value::numeric)::numeric, 2) as mean_val
            FROM th_series_data
            WHERE user_id = :user_id
              AND deleted = 0
              AND {date_filter}
              AND value ~ '^-?[0-9]+\\.?[0-9]*$'
            GROUP BY indicator
            ORDER BY days_with_data DESC, total_records DESC
        """
        params = {"user_id": user_id, "days": days}
        if as_of:
            params["as_of"] = as_of
        return await execute_query(query, params)

    @staticmethod
    def _classify_density(days_with_data: int, lookback: int) -> str:
        """Classify data density based on coverage ratio."""
        threshold_continuous = max(int(lookback * 0.85), 1)
        threshold_intermittent = max(int(lookback * 0.5), 1)
        if days_with_data >= threshold_continuous:
            return "continuous"
        elif days_with_data >= threshold_intermittent:
            return "intermittent"
        else:
            return "sparse"

    @staticmethod
    def _assess_analyzability(
        indicators: List[Dict], days: int, min_overlap: int = 14
    ) -> Dict[str, Any]:
        """
        Assess how many indicator pairs have enough data for correlation analysis.

        Only considers indicators with density >= intermittent.
        """
        # Filter to analyzable indicators (at least intermittent density)
        analyzable_inds = [
            ind for ind in indicators
            if ind["density"] in ("continuous", "intermittent")
        ]

        total_pairs = 0
        sufficient_pairs = 0
        sample_pairs = []

        if len(analyzable_inds) >= 2:
            for a, b in combinations(analyzable_inds, 2):
                # Skip same-base pairs (e.g., dailyAvg vs daily_stats of same indicator)
                if a["base_indicator"] == b["base_indicator"]:
                    continue
                # Skip same-category derived pairs
                if a["category"] == "derived" and b["category"] == "derived":
                    continue

                total_pairs += 1
                # Approximate overlap: min of both days_with_data
                approx_overlap = min(a["days_with_data"], b["days_with_data"])
                sufficient = approx_overlap >= min_overlap

                if sufficient:
                    sufficient_pairs += 1
                    if len(sample_pairs) < 10:
                        sample_pairs.append({
                            "indicator_a": a["indicator"],
                            "indicator_b": b["indicator"],
                            "approx_overlap_days": approx_overlap,
                        })

        return {
            "total_pairs": total_pairs,
            "sufficient_pairs": sufficient_pairs,
            "min_overlap_required": min_overlap,
            "analyzable_indicators": len(analyzable_inds),
            "sample_pairs": sample_pairs,
        }

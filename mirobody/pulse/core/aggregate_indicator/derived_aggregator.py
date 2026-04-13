"""
Derived Aggregator (TH-174 W2.2)

Computes derived indicators from existing daily summaries in th_series_data.
Independent from SQLAggregator — reads th_series_data, computes, writes back.

Data source priority:
  1. holywell stage2 output (daily_stats_*, already source-resolved)
  2. SQLAggregator output (daily{Method}{Indicator}.{source}, pick by source priority)
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from ....utils import execute_query
from .database_service import AggregateDatabaseService


class DerivedRule:
    """Definition of a derived indicator calculation rule."""

    def __init__(
        self,
        name: str,
        output_indicator: str,
        output_unit: str,
        input_indicators: List[str],
        compute: Callable[[List[float]], Optional[float]],
        description: str = "",
    ):
        self.name = name
        self.output_indicator = output_indicator
        self.output_unit = output_unit
        self.input_indicators = input_indicators
        self.compute = compute
        self.description = description


def _safe_divide_pct(numerator: float, denominator: float) -> Optional[float]:
    """Divide and multiply by 100, with strict validation."""
    if denominator is None or denominator <= 0:
        return None
    if numerator is None or numerator < 0:
        return None
    result = numerator / denominator * 100
    if result > 100:
        logging.debug(f"[DerivedAggregator] Ratio exceeded 100%: {numerator}/{denominator} = {result:.1f}%")
        result = min(result, 100.0)
    return round(result, 2)


def _safe_subtract(a: float, b: float) -> Optional[float]:
    """Subtract with validation. Both inputs must be positive."""
    if a is None or b is None or a < 0 or b < 0:
        return None
    return round(a - b, 2)


def _safe_divide(numerator: float, denominator: float) -> Optional[float]:
    """Divide without percentage multiplication."""
    if denominator is None or denominator <= 0:
        return None
    if numerator is None or numerator < 0:
        return None
    return round(numerator / denominator, 4)


# ---------------------------------------------------------------------------
# Holywell alias mapping: standard name → holywell daily_stats_* name
# holywell stage2 uses: daily_stats_{indicator}{Method}
# SQLAggregator uses:   daily{Method}{Indicator}
# ---------------------------------------------------------------------------
HOLYWELL_ALIASES: Dict[str, str] = {
    # Sleep
    "dailyTotalSleepAnalysis_Asleep(Total)": "daily_stats_sleepAnalysis_Asleep(Total)Sum",
    "dailyTotalSleepAnalysis_InBed": "daily_stats_sleepAnalysis_InBedSum",
    "dailyTotalSleepAnalysis_Asleep(Deep)": "daily_stats_sleepAnalysis_Asleep(Deep)Sum",
    "dailyTotalSleepAnalysis_Asleep(REM)": "daily_stats_sleepAnalysis_Asleep(REM)Sum",
    "dailyTotalSleepAnalysis_Asleep(Core)": "daily_stats_sleepAnalysis_Asleep(Core)Sum",
    "dailyTotalSleepAnalysis_Awake": "daily_stats_sleepAnalysis_AwakeSum",
    # Heart rate
    "dailyMaxHeartRates": "daily_stats_heartRatesMax",
    "dailyMinHeartRates": "daily_stats_heartRatesMin",
    "dailyAvgHeartRates": "daily_stats_heartRatesAvg",
    "dailyAvgRestingHeartRates": "daily_stats_restingHeartRatesAvg",
    "dailyAvgWalkingHeartRates": "daily_stats_walkingHeartRatesAvg",
    # Activity
    "dailyTotalSteps": "daily_stats_stepsSum",
    "dailyTotalWalkingRunningDistances": "daily_stats_walkingRunningDistancesSum",
    "dailyTotalExerciseMinutes": "daily_stats_exerciseMinutesSum",
    # HRV
    "dailyAvgHrvDatas": "daily_stats_hrvDatasAvg",
    # Respiratory
    "dailyAvgRespiratoryRates": "daily_stats_respiratoryRatesAvg",
    # Oxygen
    "dailyAvgOxygenSaturations": "daily_stats_oxygenSaturationsAvg",
    "dailyMinOxygenSaturations": "daily_stats_oxygenSaturationsMin",
}


# Phase 1: Hard-coded derived rules (standard naming)
DERIVED_RULES: List[DerivedRule] = [
    DerivedRule(
        name="sleep_efficiency",
        output_indicator="derivedSleepEfficiency",
        output_unit="%",
        input_indicators=[
            "dailyTotalSleepAnalysis_Asleep(Total)",
            "dailyTotalSleepAnalysis_InBed",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[1]),
        description="Sleep efficiency = total sleep / time in bed * 100",
    ),
    DerivedRule(
        name="deep_sleep_ratio",
        output_indicator="derivedDeepSleepRatio",
        output_unit="%",
        input_indicators=[
            "dailyTotalSleepAnalysis_Asleep(Deep)",
            "dailyTotalSleepAnalysis_Asleep(Total)",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[1]),
        description="Deep sleep ratio = deep sleep / total sleep * 100",
    ),
    DerivedRule(
        name="rem_sleep_ratio",
        output_indicator="derivedRemSleepRatio",
        output_unit="%",
        input_indicators=[
            "dailyTotalSleepAnalysis_Asleep(REM)",
            "dailyTotalSleepAnalysis_Asleep(Total)",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[1]),
        description="REM sleep ratio = REM sleep / total sleep * 100",
    ),

    # --- Sleep extended ---
    DerivedRule(
        name="light_sleep_ratio",
        output_indicator="derivedLightSleepRatio",
        output_unit="%",
        input_indicators=[
            "dailyTotalSleepAnalysis_Asleep(Core)",
            "dailyTotalSleepAnalysis_Asleep(Total)",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[1]),
        description="Light sleep ratio = core(light) sleep / total sleep * 100",
    ),
    DerivedRule(
        name="awake_ratio",
        output_indicator="derivedAwakeRatio",
        output_unit="%",
        input_indicators=[
            "dailyTotalSleepAnalysis_Awake",
            "dailyTotalSleepAnalysis_Asleep(Total)",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[0] + vals[1]) if vals[0] + vals[1] > 0 else None,
        description="Awake ratio = awake / (awake + total sleep) * 100",
    ),

    # --- Cardiovascular ---
    DerivedRule(
        name="hr_range",
        output_indicator="derivedHrRange",
        output_unit="bpm",
        input_indicators=[
            "dailyMaxHeartRates",
            "dailyMinHeartRates",
        ],
        compute=lambda vals: _safe_subtract(vals[0], vals[1]) if vals[0] > vals[1] else None,
        description="Heart rate range = max HR - min HR",
    ),
    DerivedRule(
        name="hr_reserve",
        output_indicator="derivedHrReserve",
        output_unit="bpm",
        input_indicators=[
            "dailyMaxHeartRates",
            "dailyAvgRestingHeartRates",
        ],
        compute=lambda vals: _safe_subtract(vals[0], vals[1]) if vals[0] > vals[1] else None,
        description="Heart rate reserve = max HR - resting HR",
    ),
    DerivedRule(
        name="walking_hr_elevation",
        output_indicator="derivedWalkingHrElevation",
        output_unit="bpm",
        input_indicators=[
            "dailyAvgWalkingHeartRates",
            "dailyAvgRestingHeartRates",
        ],
        compute=lambda vals: _safe_subtract(vals[0], vals[1]) if vals[0] > vals[1] else None,
        description="Walking HR elevation = walking HR - resting HR",
    ),

    # --- Activity ---
    DerivedRule(
        name="step_efficiency",
        output_indicator="derivedStepEfficiency",
        output_unit="m/step",
        input_indicators=[
            "dailyTotalWalkingRunningDistances",
            "dailyTotalSteps",
        ],
        compute=lambda vals: _safe_divide(vals[0], vals[1]),
        description="Step efficiency = distance / steps (stride length proxy)",
    ),
    DerivedRule(
        name="activity_minutes_ratio",
        output_indicator="derivedActivityMinutesRatio",
        output_unit="%",
        input_indicators=[
            "dailyTotalExerciseMinutes",
        ],
        compute=lambda vals: round(vals[0] / 1440 * 100, 2) if vals[0] is not None and vals[0] >= 0 else None,
        description="Activity ratio = exercise minutes / 1440 * 100",
    ),

    # --- Metabolic (W2.6) ---
    DerivedRule(
        name="blood_glucose_cv",
        output_indicator="derivedBloodGlucoseCV",
        output_unit="%",
        input_indicators=[
            "dailyStddevBloodGlucoses",
            "dailyAvgBloodGlucoses",
        ],
        compute=lambda vals: _safe_divide_pct(vals[0], vals[1]),
        description="Glucose CV = stddev / mean * 100. <36% = stable, >36% = high variability",
    ),
]


class DerivedAggregator:
    """
    Computes derived indicators from th_series_data daily summaries.

    Source priority:
      1. holywell stage2 (daily_stats_*, already source-resolved, priority=0)
      2. SQLAggregator (daily{Method}{Indicator}.{source}, priority from th_data_source_priority)
    Uses DISTINCT ON + priority ordering to pick the best value per user-day-indicator.
    """

    def __init__(self):
        self.db_service = AggregateDatabaseService()
        self.rules = DERIVED_RULES

    async def process(self, lookback_days: int = 7) -> Dict[str, Any]:
        """
        Scan recent data and compute all derived indicators.

        Args:
            lookback_days: How many days back to scan for input data

        Returns:
            Dict with processing statistics
        """
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        total_computed = 0
        total_skipped = 0
        results_by_rule: Dict[str, int] = {}

        for rule in self.rules:
            computed, skipped = await self._process_rule(rule, cutoff)
            total_computed += computed
            total_skipped += skipped
            results_by_rule[rule.name] = computed

        logging.info(
            f"[DerivedAggregator] Done: {total_computed} derived values computed, "
            f"{total_skipped} skipped (invalid inputs)"
        )

        return {
            "total_computed": total_computed,
            "total_skipped": total_skipped,
            "by_rule": results_by_rule,
            "lookback_days": lookback_days,
        }

    async def _process_rule(self, rule: DerivedRule, cutoff: datetime) -> Tuple[int, int]:
        """
        Process a single derived rule across all user-days.

        For each input indicator, queries BOTH holywell (daily_stats_*) and
        SQLAggregator (daily{Method}*.{source}) data, picks best by priority.
        """
        n_inputs = len(rule.input_indicators)

        # Build UNION ALL for each input: holywell alias (priority=0) + SQLAggregator (source priority)
        union_parts = []
        params: Dict[str, Any] = {"cutoff": cutoff, "n_inputs": n_inputs}

        for i, inp in enumerate(rule.input_indicators):
            alias = HOLYWELL_ALIASES.get(inp)
            param_std = f"std_{i}"
            params[param_std] = inp

            # SQLAggregator: match exact or with .source suffix
            union_parts.append(f"""
                SELECT user_id, start_time::date AS day,
                       :{param_std} AS base_indicator,
                       value::numeric AS num_value,
                       COALESCE(get_source_priority(source), 999) AS priority
                FROM th_series_data
                WHERE (indicator = :{param_std} OR indicator LIKE :{param_std} || '.%')
                  AND deleted = 0
                  AND start_time >= :cutoff
                  AND value ~ '^-?[0-9]+\\.?[0-9]*$'
            """)

            # Holywell alias: exact match, priority=0 (highest)
            if alias:
                param_hw = f"hw_{i}"
                params[param_hw] = alias
                union_parts.append(f"""
                    SELECT user_id, start_time::date AS day,
                           :{param_std} AS base_indicator,
                           value::numeric AS num_value,
                           0 AS priority
                    FROM th_series_data
                    WHERE indicator = :{param_hw}
                      AND deleted = 0
                      AND start_time >= :cutoff
                      AND value ~ '^-?[0-9]+\\.?[0-9]*$'
                """)

        union_sql = " UNION ALL ".join(union_parts)

        query = f"""
            WITH all_candidates AS (
                {union_sql}
            ),
            resolved AS (
                SELECT DISTINCT ON (user_id, day, base_indicator)
                       user_id, day, base_indicator, num_value
                FROM all_candidates
                ORDER BY user_id, day, base_indicator, priority ASC, num_value DESC
            )
            SELECT user_id, day,
                   ARRAY_AGG(base_indicator ORDER BY base_indicator) AS indicators,
                   ARRAY_AGG(num_value::text ORDER BY base_indicator) AS values
            FROM resolved
            GROUP BY user_id, day
            HAVING COUNT(DISTINCT base_indicator) = :n_inputs
        """

        try:
            rows = await execute_query(query, params)
        except Exception as e:
            logging.error(f"[DerivedAggregator] Query failed for {rule.name}: {e}")
            return 0, 0

        computed = 0
        skipped = 0
        records_to_save = []

        for row in rows:
            user_id = row["user_id"]
            day = row["day"]
            indicators = row["indicators"]
            raw_values = row["values"]

            # Map input indicators to their values in the correct order
            ind_val_map = {}
            for ind, val in zip(indicators, raw_values):
                try:
                    ind_val_map[ind] = float(val)
                except (ValueError, TypeError):
                    ind_val_map[ind] = None

            # Get values in rule's input order
            ordered_values = []
            valid = True
            for inp in rule.input_indicators:
                v = ind_val_map.get(inp)
                if v is None or v < 0:
                    valid = False
                    break
                ordered_values.append(v)

            if not valid:
                skipped += 1
                continue

            # Compute derived value
            try:
                result = rule.compute(ordered_values)
            except Exception as e:
                logging.warning(f"[DerivedAggregator] Compute error for {rule.name}, user={user_id}, day={day}: {e}")
                skipped += 1
                continue

            if result is None:
                skipped += 1
                continue

            # Build record for UPSERT
            start_time = datetime.combine(day, datetime.min.time())
            end_time = start_time + timedelta(days=1)

            records_to_save.append({
                "user_id": user_id,
                "indicator": rule.output_indicator,
                "value": str(round(result, 2)),
                "start_time": start_time,
                "end_time": end_time,
                "source": "derived",
                "task_id": "derived_aggregator",
                "comment": f"Derived: {rule.description}, unit={rule.output_unit}",
                "source_table": "",
                "source_table_id": "",
                "indicator_id": "",
                "fhir_id": None,
            })
            computed += 1

        # Batch save
        if records_to_save:
            try:
                await self.db_service.batch_save_summary_data(records_to_save)
                logging.info(
                    f"[DerivedAggregator] Rule '{rule.name}': {computed} computed, {skipped} skipped"
                )
            except Exception as e:
                logging.error(f"[DerivedAggregator] Save failed for {rule.name}: {e}")
                return 0, skipped

        return computed, skipped

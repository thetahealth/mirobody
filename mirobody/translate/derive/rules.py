"""Derived indicators: quantities computed from the stored daily summaries.

Reads `v_observation`, computes, and writes the results back through the same
writer as an aggregation pass (`AggregateDatabaseService`). Each input of a
rule is the day's published value of that indicator, as every reader takes
it: the row election chose (`th_day_authority`), or the newest row of a day
election has not decided. A legacy `daily_stats_*` row, source-resolved when
it was written, is taken before the newer spelling of the same day.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from collections.abc import Callable

from mirobody.kernel.ops import is_driver_exception
from mirobody.translate import AggregateDatabaseService
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class DerivedRule:
    """Definition of a derived indicator calculation rule."""

    def __init__(
        self,
        name: str,
        output_indicator: str,
        output_unit: str,
        input_indicators: list[str],
        compute: Callable[[list[float]], float | None],
        description: str = "",
    ):
        self.name = name
        self.output_indicator = output_indicator
        self.output_unit = output_unit
        self.input_indicators = input_indicators
        self.compute = compute
        self.description = description


def _safe_divide_pct(numerator: float, denominator: float) -> float | None:
    """Divide and multiply by 100, with strict validation."""
    if denominator is None or denominator <= 0:
        return None
    if numerator is None or numerator < 0:
        return None
    return round(min(numerator / denominator * 100, 100.0), 2)


def _safe_subtract(a: float, b: float) -> float | None:
    """Subtract with validation. Both inputs must be positive."""
    if a is None or b is None or a < 0 or b < 0:
        return None
    return round(a - b, 2)


def _safe_divide(numerator: float, denominator: float) -> float | None:
    """Divide without percentage multiplication."""
    if denominator is None or denominator <= 0:
        return None
    if numerator is None or numerator < 0:
        return None
    return round(numerator / denominator, 4)


# Legacy alias mapping: current name -> the `daily_stats_*` spelling.
# The record still holds rows from an older scheme that named things
# `daily_stats_{indicator}{Method}` where SQLAggregator now writes
# `daily{Method}{Indicator}`. Nothing produces the old spelling, but those rows
# are real data and a derived rule ignoring them loses years of history, so
# every lookup checks both. These values are DATA, not naming preference: do
# not "modernise" them.
LEGACY_DAILY_STATS_ALIASES: dict[str, str] = {
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
DERIVED_RULES: list[DerivedRule] = [
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

    # --- Metabolic ---
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
    """Computes every rule of `DERIVED_RULES` for each person-day whose inputs
    all have a published value, and writes the results as observations."""

    def __init__(self):
        self.db_service = AggregateDatabaseService()
        self.rules = DERIVED_RULES

    async def process(self, lookback_days: int = 7) -> dict[str, Any]:
        """Compute every rule over the last `lookback_days` of summaries.
        Returns the counts: computed and skipped in total and per rule."""
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        total_computed = 0
        total_skipped = 0
        results_by_rule: dict[str, int] = {}

        for rule in self.rules:
            computed, skipped = await self._process_rule(rule, cutoff)
            total_computed += computed
            total_skipped += skipped
            results_by_rule[rule.name] = computed

        logger.info("derived values computed: computed=%d skipped=%d", total_computed, total_skipped)
        return {
            "total_computed": total_computed,
            "total_skipped": total_skipped,
            "by_rule": results_by_rule,
            "lookback_days": lookback_days,
        }

    async def _process_rule(self, rule: DerivedRule, cutoff: datetime) -> tuple[int, int]:
        """One rule across every person-day since `cutoff`: `(computed, skipped)`."""
        union_parts = []
        params: dict[str, Any] = {"cutoff": cutoff, "n_inputs": len(rule.input_indicators)}
        for i, inp in enumerate(rule.input_indicators):
            params[f"std_{i}"] = inp
            # The aggregator's own name, and the same name with a `.<source>` suffix.
            union_parts.append(_CANDIDATES.format(base=f":std_{i}", legacy="false", name=f"split_part(name_text, '.', 1) = :std_{i}"))
            alias = LEGACY_DAILY_STATS_ALIASES.get(inp)
            if alias:
                params[f"legacy_{i}"] = alias
                union_parts.append(_CANDIDATES.format(base=f":std_{i}", legacy="true", name=f"name_text = :legacy_{i}"))

        try:
            rows = await execute_query(_RESOLVE.format(candidates=" UNION ALL ".join(union_parts)), params) or []
        except Exception as e:
            logger.error("derived rule query failed: rule=%s error_type=%s", rule.name, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return 0, 0

        computed = 0
        skipped = 0
        records_to_save = []

        for row in rows:
            by_input: dict[str, float | None] = {}
            for ind, val in zip(row["indicators"], row["values"], strict=True):
                try:
                    by_input[ind] = float(val)
                except (ValueError, TypeError):
                    by_input[ind] = None

            ordered_values = [by_input.get(inp) for inp in rule.input_indicators]
            if any(v is None or v < 0 for v in ordered_values):
                skipped += 1
                continue

            try:
                result = rule.compute(ordered_values)
            except (ArithmeticError, TypeError, ValueError):
                skipped += 1
                continue
            if result is None:
                skipped += 1
                continue

            start_time = datetime.combine(row["day"], datetime.min.time())
            records_to_save.append({
                "user_id": row["user_id"],
                "indicator": rule.output_indicator,
                "value": str(round(result, 2)),
                "unit": rule.output_unit,
                "timezone": row["tz"],
                "start_time": start_time,
                "end_time": start_time + timedelta(days=1),
                "source": "derived",
                "task_id": "derived_aggregator",
                "source_table": "",
                "source_table_id": "",
            })
            computed += 1

        if records_to_save and not await self.db_service.batch_save_summary_data(records_to_save):
            logger.error("derived rule results not written: rule=%s records=%d", rule.name, len(records_to_save))
            return 0, skipped
        if records_to_save:
            logger.info("derived rule done: rule=%s computed=%d skipped=%d", rule.name, computed, skipped)
        return computed, skipped


# One input's candidates: `{name}` selects the rows, `{legacy}` says whether
# they are the older `daily_stats_*` spelling.
_CANDIDATES = """
    SELECT user_id, local_date AS day, {base} AS base_indicator, value_num AS num_value, tz,
           {legacy} AS legacy, elected, observed_start, id
      FROM v_observation
     WHERE {name} AND observed_start >= :cutoff AND value_num IS NOT NULL
"""

# The day's published value of each input (see the module docstring), then the
# person-days that have every input.
_RESOLVE = """
WITH all_candidates AS ({candidates}),
resolved AS (
    SELECT DISTINCT ON (user_id, day, base_indicator)
           user_id, day, base_indicator, num_value, tz
      FROM all_candidates
     ORDER BY user_id, day, base_indicator, legacy DESC, elected DESC, observed_start DESC, id DESC
)
SELECT user_id, day,
       ARRAY_AGG(base_indicator ORDER BY base_indicator) AS indicators,
       ARRAY_AGG(num_value::text ORDER BY base_indicator) AS values,
       (ARRAY_AGG(tz ORDER BY base_indicator))[1] AS tz
  FROM resolved
 GROUP BY user_id, day
HAVING COUNT(*) = :n_inputs
"""

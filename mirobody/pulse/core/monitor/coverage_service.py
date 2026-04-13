"""
Aggregation Coverage Service (TH-175 W2.8)

Generates coverage report: which indicators have data, aggregation config,
aggregated output, and derived output. Identifies gaps.

All DB reads use indicator_daily_profile (pre-computed report table only).
"""

import logging
from typing import Any, Dict, List

from ....utils import execute_query
from ..indicators_info import StandardIndicator, HealthDataType
from ..aggregate_indicator.rule_generator import generate_rules_from_indicators
from ..aggregate_indicator.derived_aggregator import DERIVED_RULES, HOLYWELL_ALIASES


class CoverageService:
    """Generates aggregation coverage reports from pre-computed report tables only."""

    async def get_coverage_report(self, lookback_days: int = 30) -> Dict[str, Any]:
        """Generate full coverage report from indicator_daily_profile."""
        # 1. Config from code
        series_config = self._get_series_config()
        agg_rules = generate_rules_from_indicators()

        # 2. Single query: all indicator stats from report table
        all_data = await self._query_all_from_report_table(lookback_days)

        # 3. Classify by indicator name pattern
        source_data = {}
        agg_data = {}
        derived_data = []
        daily_stats_data = {}

        for ind, info in all_data.items():
            if ind.startswith("derived"):
                derived_data.append({"indicator": ind, **info})
            elif ind.startswith("daily_stats_"):
                daily_stats_data[ind] = info
            elif ind.startswith("daily"):
                agg_data[ind] = info
            else:
                source_data[ind] = info

        # 4. Build coverage matrix (sorted: configured first)
        matrix = self._build_coverage_matrix(
            series_config, agg_rules, source_data, agg_data
        )

        # 5. Derived dependency check
        derived_status = self._check_derived_dependencies(agg_data, daily_stats_data)

        # 6. Identify gaps
        gaps = self._identify_gaps(matrix)

        # 7. Summary
        configured_count = sum(1 for m in matrix if m["has_config"])
        has_data_count = sum(1 for m in matrix if m["has_source_data"])
        has_output_count = sum(1 for m in matrix if m["has_agg_output"])
        uncovered_with_data = sum(
            1 for m in matrix if m["has_source_data"] and not m["has_config"]
        )

        return {
            "summary": {
                "total_series_indicators": len(matrix),
                "with_aggregation_config": configured_count,
                "with_source_data": has_data_count,
                "with_agg_output": has_output_count,
                "uncovered_with_data": uncovered_with_data,
                "derived_rules_total": len(DERIVED_RULES),
                "derived_rules_satisfied": sum(
                    1 for d in derived_status if d["all_inputs_available"]
                ),
                "derived_indicators_produced": len(derived_data),
                "config_coverage_rate": f"{configured_count}/{len(matrix)}",
                "output_coverage_rate": f"{has_output_count}/{configured_count}" if configured_count else "0/0",
                "lookback_days": lookback_days,
            },
            "coverage_matrix": matrix,
            "gaps": gaps,
            "derived_status": derived_status,
            "derived_output": sorted(derived_data, key=lambda x: x["row_count"], reverse=True),
        }

    def _get_series_config(self) -> Dict[str, List[str]]:
        """Extract SERIES indicators and their aggregation_methods from StandardIndicator."""
        config = {}
        for indicator_enum in StandardIndicator:
            info = indicator_enum.value
            if info.data_type != HealthDataType.SERIES:
                continue
            methods = getattr(info, "aggregation_methods", None)
            config[info.name] = methods or []
        return config

    async def _query_all_from_report_table(self, lookback_days: int) -> Dict[str, Dict]:
        """
        Single query to get ALL indicator stats from indicator_daily_profile.

        This table now includes:
        - Raw source indicators (from series_data)
        - Aggregated indicators (from th_series_data, task_id='aggregate_indicator')
        - Derived indicators (from th_series_data, task_id='derived_aggregator')
        - daily_stats_* indicators (from th_series_data, apple_health_statistics)

        ~20ms query on pre-computed table.
        """
        query = """
            SELECT indicator,
                   SUM(record_count) as row_count,
                   COUNT(DISTINCT source) as source_count,
                   SUM(filtered_count) as filtered_count
            FROM indicator_daily_profile
            WHERE stat_date >= NOW()::date - CAST(:days AS integer)
            GROUP BY indicator
            ORDER BY row_count DESC
        """
        rows = await execute_query(query, {"days": lookback_days})
        return {
            row["indicator"]: {
                "row_count": row["row_count"] or 0,
                "user_count": row["source_count"] or 0,
                "filtered_count": row["filtered_count"] or 0,
            }
            for row in rows
        }

    async def get_derived_trend(self, indicator: str, days: int = 30) -> Dict[str, Any]:
        """Get daily production trend for a derived indicator from report table."""
        query = """
            SELECT stat_date,
                   SUM(record_count) as records,
                   COUNT(DISTINCT source) as sources
            FROM indicator_daily_profile
            WHERE indicator = :indicator
              AND stat_date >= NOW()::date - CAST(:days AS integer)
            GROUP BY stat_date
            ORDER BY stat_date
        """
        rows = await execute_query(query, {"indicator": indicator, "days": days})
        return {
            "indicator": indicator,
            "days": days,
            "trend": [
                {
                    "date": row["stat_date"].isoformat() if hasattr(row["stat_date"], "isoformat") else str(row["stat_date"]),
                    "records": row["records"] or 0,
                    "sources": row["sources"] or 0,
                }
                for row in rows
            ],
        }

    def _build_coverage_matrix(
        self,
        series_config: Dict[str, List[str]],
        agg_rules,
        source_data: Dict[str, Dict],
        agg_data: Dict[str, Dict],
    ) -> List[Dict]:
        """Build per-indicator coverage matrix, sorted: configured first, then by source rows."""
        source_to_targets = {}
        for rule in agg_rules:
            source_to_targets.setdefault(rule.source_indicator, []).append(
                rule.target_indicator
            )

        matrix = []
        for indicator, methods in sorted(series_config.items()):
            source_info = source_data.get(indicator, {})
            has_source = bool(source_info)

            targets = source_to_targets.get(indicator, [])
            agg_output = {}
            for target in targets:
                for agg_ind, agg_info in agg_data.items():
                    base_name = agg_ind.split(".")[0]
                    if base_name == target:
                        agg_output[agg_ind] = agg_info

            matrix.append({
                "indicator": indicator,
                "has_config": bool(methods),
                "aggregation_methods": methods,
                "has_source_data": has_source,
                "source_rows": source_info.get("row_count", 0),
                "source_users": source_info.get("user_count", 0),
                "has_agg_output": bool(agg_output),
                "agg_targets": list(agg_output.keys()),
                "agg_total_rows": sum(v["row_count"] for v in agg_output.values()),
                "agg_total_users": max(
                    (v["user_count"] for v in agg_output.values()), default=0
                ),
            })

        # Sort: configured first, then by source data volume
        matrix.sort(key=lambda m: (not m["has_config"], -m["source_rows"]))
        return matrix

    def _check_derived_dependencies(
        self, agg_data: Dict[str, Dict], daily_stats_data: Dict[str, Dict]
    ) -> List[Dict]:
        """Check if each DerivedRule has all input indicators available."""
        available_bases = set()
        for agg_ind in agg_data:
            base = agg_ind.split(".")[0]
            available_bases.add(base)
        for ds_ind in daily_stats_data:
            available_bases.add(ds_ind)

        # Build reverse alias map: standard name → holywell name
        # So we can check both naming conventions
        reverse_aliases = {v: k for k, v in HOLYWELL_ALIASES.items()}

        results = []
        for rule in DERIVED_RULES:
            inputs_status = []
            all_available = True
            for inp in rule.input_indicators:
                # Check standard name (direct or with .source suffix)
                found = inp in available_bases
                # Also check via holywell alias
                if not found:
                    alias = HOLYWELL_ALIASES.get(inp)
                    if alias and alias in available_bases:
                        found = True
                if not found:
                    all_available = False
                inputs_status.append({
                    "indicator": inp,
                    "available": found,
                })

            results.append({
                "rule_name": rule.name,
                "output_indicator": rule.output_indicator,
                "all_inputs_available": all_available,
                "inputs": inputs_status,
            })

        return results

    def _identify_gaps(self, matrix: List[Dict]) -> Dict[str, List[Dict]]:
        """Identify coverage gaps from the matrix."""
        uncovered = [
            {
                "indicator": m["indicator"],
                "source_rows": m["source_rows"],
                "source_users": m["source_users"],
            }
            for m in matrix
            if m["has_source_data"] and not m["has_config"]
        ]
        uncovered.sort(key=lambda x: x["source_rows"], reverse=True)

        no_data = [
            {
                "indicator": m["indicator"],
                "aggregation_methods": m["aggregation_methods"],
            }
            for m in matrix
            if m["has_config"] and not m["has_source_data"]
        ]

        no_output = [
            {
                "indicator": m["indicator"],
                "aggregation_methods": m["aggregation_methods"],
                "source_rows": m["source_rows"],
            }
            for m in matrix
            if m["has_config"] and m["has_source_data"] and not m["has_agg_output"]
        ]

        return {
            "uncovered_with_data": uncovered,
            "configured_no_data": no_data,
            "configured_no_output": no_output,
        }

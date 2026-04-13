"""
Data Quality Service (W3.2)

Provides data health metrics for the Pulse data quality monitoring API.
Analyzes a single date's data (daily report model) for fast performance.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...utils import execute_query

# Threshold for statistical anomaly detection
SUSPICIOUS_MULTIPLIER = 10  # value > mean + N*stddev → suspicious
CROSS_SOURCE_RATIO_THRESHOLD = 100  # max ratio between sources → unit_issue

# Only analyze device platform sources
SOURCE_FILTER = "(source LIKE 'apple_health%%' OR source LIKE 'vital.%%' OR source LIKE 'theta.%%')"


class DataQualityService:
    """Analyzes data quality across series_data and th_series_data."""

    async def get_series_data_quality(
        self,
        date: Optional[str] = None,
        indicator: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get value distribution and anomaly analysis for series_data.
        Analyzes a single date for fast performance (~2s).

        Args:
            date: Date to analyze (YYYY-MM-DD). Default = yesterday.
            indicator: Specific indicator (optional, default = all for that date)
        """
        if not date:
            date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        indicator_filter = "AND indicator = :indicator" if indicator else ""
        params = {"date_start": date, "date_end": date}
        if indicator:
            params["indicator"] = indicator

        query = f"""
            SELECT indicator, source,
                COUNT(*) as record_count,
                MIN(value::numeric) as min_val,
                MAX(value::numeric) as max_val,
                ROUND(AVG(value::numeric)::numeric, 4) as mean_val,
                ROUND(STDDEV(value::numeric)::numeric, 4) as stddev_val,
                COUNT(*) FILTER (WHERE task_id = 'filtered_out_of_range') as filtered_count
            FROM series_data
            WHERE time >= :date_start::date
              AND time < :date_end::date + INTERVAL '1 day'
              AND {SOURCE_FILTER}
              {indicator_filter}
            GROUP BY indicator, source
            ORDER BY record_count DESC
        """

        try:
            rows = await execute_query(query, params)
        except Exception as e:
            logging.error(f"[DataQuality] series_data query failed: {e}")
            return {"error": str(e)}

        # Build profiles and detect anomalies
        profiles = []
        indicator_sources = {}

        for row in rows:
            ind = row["indicator"]
            src = row["source"]
            count = row["record_count"]
            min_v = float(row["min_val"]) if row["min_val"] is not None else None
            max_v = float(row["max_val"]) if row["max_val"] is not None else None
            mean_v = float(row["mean_val"]) if row["mean_val"] is not None else None
            std_v = float(row["stddev_val"]) if row["stddev_val"] is not None else None
            filtered = row["filtered_count"] or 0

            # Statistical anomaly detection (stddev-based)
            issues = []
            if mean_v is not None and std_v is not None and std_v > 0:
                if max_v is not None and max_v > mean_v + SUSPICIOUS_MULTIPLIER * std_v:
                    issues.append({
                        "type": "suspicious_max",
                        "detail": f"max={max_v} exceeds mean+{SUSPICIOUS_MULTIPLIER}σ ({mean_v + SUSPICIOUS_MULTIPLIER * std_v:.1f})"
                    })
                if min_v is not None and min_v < mean_v - SUSPICIOUS_MULTIPLIER * std_v:
                    issues.append({
                        "type": "suspicious_min",
                        "detail": f"min={min_v} below mean-{SUSPICIOUS_MULTIPLIER}σ ({mean_v - SUSPICIOUS_MULTIPLIER * std_v:.1f})"
                    })
            if min_v is not None and min_v < 0:
                issues.append({"type": "negative_value", "detail": f"min={min_v}"})

            profile = {
                "indicator": ind,
                "source": src,
                "count": count,
                "min": min_v,
                "max": max_v,
                "mean": mean_v,
                "stddev": std_v,
                "filtered_count": filtered,
                "exclusion_rate": round(filtered / count * 100, 2) if count > 0 else 0,
                "health": "ok" if not issues else ("warning" if len(issues) == 1 else "critical"),
                "issues": issues,
            }
            profiles.append(profile)

            if ind not in indicator_sources:
                indicator_sources[ind] = []
            indicator_sources[ind].append(profile)

        # Cross-source comparison
        cross_source_issues = []
        for ind, sources in indicator_sources.items():
            if len(sources) < 2:
                continue
            max_vals = [(s["source"], s["max"]) for s in sources if s["max"] is not None and s["max"] > 0]
            if len(max_vals) < 2:
                continue
            max_vals.sort(key=lambda x: x[1])
            lowest_src, lowest_max = max_vals[0]
            highest_src, highest_max = max_vals[-1]
            if lowest_max > 0 and highest_max / lowest_max > CROSS_SOURCE_RATIO_THRESHOLD:
                cross_source_issues.append({
                    "indicator": ind,
                    "type": "cross_source_max_ratio",
                    "ratio": round(highest_max / lowest_max, 1),
                    "high_source": highest_src,
                    "high_max": highest_max,
                    "low_source": lowest_src,
                    "low_max": lowest_max,
                    "likely_cause": "unit_conversion_bug",
                })

        total_records = sum(p["count"] for p in profiles)
        total_filtered = sum(p["filtered_count"] for p in profiles)
        indicators_with_issues = len(set(p["indicator"] for p in profiles if p["issues"]))

        return {
            "table": "series_data",
            "date": date,
            "summary": {
                "total_records": total_records,
                "total_filtered": total_filtered,
                "overall_exclusion_rate": round(total_filtered / total_records * 100, 4) if total_records > 0 else 0,
                "indicators_analyzed": len(indicator_sources),
                "indicators_with_issues": indicators_with_issues,
                "cross_source_issues": len(cross_source_issues),
            },
            "profiles": profiles,
            "cross_source_issues": cross_source_issues,
        }

    async def get_th_series_data_quality(self, date: Optional[str] = None) -> Dict[str, Any]:
        """Misclassification detection + exclusion rate for th_series_data."""
        if not date:
            date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        query = """
            SELECT indicator, COUNT(DISTINCT user_id) as affected_users,
                   MAX(day_count) as max_per_day
            FROM (
                SELECT indicator, user_id, start_time::date as day, COUNT(*) as day_count
                FROM th_series_data
                WHERE start_time >= :date_start::date AND start_time < :date_end::date + INTERVAL '1 day'
                  AND deleted = 0
                GROUP BY indicator, user_id, start_time::date
                HAVING COUNT(*) > 10
            ) sub
            GROUP BY indicator
            ORDER BY max_per_day DESC
            LIMIT 20
        """
        params = {"date_start": date, "date_end": date}

        try:
            rows = await execute_query(query, params)
        except Exception as e:
            logging.error(f"[DataQuality] th_series_data query failed: {e}")
            return {"error": str(e)}

        misclassified = [{
            "indicator": r["indicator"],
            "affected_users": r["affected_users"],
            "max_records_per_day": r["max_per_day"],
            "likely_cause": "time_series_in_summary_table",
        } for r in rows]

        # Exclusion rate
        try:
            ex_rows = await execute_query("""
                SELECT COUNT(*) as total,
                       COUNT(*) FILTER (WHERE task_id = 'filtered_out_of_range') as filtered
                FROM th_series_data
                WHERE start_time >= :date_start::date AND start_time < :date_end::date + INTERVAL '1 day'
            """, params)
            total = ex_rows[0]["total"] if ex_rows else 0
            filtered = ex_rows[0]["filtered"] if ex_rows else 0
        except Exception:
            total, filtered = 0, 0

        return {
            "table": "th_series_data",
            "date": date,
            "summary": {
                "total_records": total,
                "total_filtered": filtered,
                "exclusion_rate": round(filtered / total * 100, 4) if total > 0 else 0,
                "misclassified_indicators": len(misclassified),
            },
            "misclassified": misclassified,
        }

    async def get_name_consistency(self) -> Dict[str, Any]:
        """Check indicator name variants (case-insensitive duplicates)."""
        query = """
            SELECT LOWER(indicator) as lower_name,
                   ARRAY_AGG(DISTINCT indicator) as variants,
                   COUNT(DISTINCT indicator) as variant_count,
                   SUM(cnt) as total_records
            FROM (
                SELECT indicator, COUNT(*) as cnt FROM series_data GROUP BY indicator
            ) sub
            GROUP BY LOWER(indicator)
            HAVING COUNT(DISTINCT indicator) > 1
            ORDER BY total_records DESC
        """
        try:
            rows = await execute_query(query)
        except Exception as e:
            logging.error(f"[DataQuality] name consistency query failed: {e}")
            return {"error": str(e)}

        return {
            "variant_groups": len(rows),
            "details": [{
                "canonical": r["lower_name"],
                "variants": r["variants"],
                "variant_count": r["variant_count"],
                "total_records": r["total_records"],
            } for r in rows],
        }

    async def get_timestamp_quality(self) -> Dict[str, Any]:
        """Check timestamp anomalies (epoch 0, future times)."""
        query = """
            SELECT
                COUNT(*) FILTER (WHERE time < '2010-01-01') as epoch_zero,
                COUNT(*) FILTER (WHERE time > NOW() + INTERVAL '48 hours') as future_time
            FROM series_data
            WHERE time < '2010-01-01' OR time > NOW() + INTERVAL '48 hours'
        """
        try:
            rows = await execute_query(query)
            row = rows[0] if rows else {}
        except Exception as e:
            logging.error(f"[DataQuality] timestamp query failed: {e}")
            return {"error": str(e)}

        return {
            "epoch_zero_count": row.get("epoch_zero", 0),
            "future_time_count": row.get("future_time", 0),
        }

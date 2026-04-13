"""
Monitor Query Service (TH-141)

Read-only service for report tables. Powers W3.1 and W3.2 API endpoints.
All queries target pre-computed tables for <100ms response.
"""

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from ....utils import execute_query

# Anomaly detection thresholds (same as collector_service.py)
CROSS_SOURCE_RATIO_THRESHOLD = 100


class MonitorQueryService:
    """Read service for platform_hourly_profile and indicator_daily_profile."""

    # ------------------------------------------------------------------
    # W3.1: Ingestion monitoring
    # ------------------------------------------------------------------

    async def get_ingestion_stats(
        self,
        period_hours: int = 24,
        platform: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query platform_hourly_profile for ingestion monitoring (W3.1).

        Args:
            period_hours: Lookback window in hours (default 24)
            platform: Filter by platform (optional)

        Returns:
            Dict with by_platform summary and hourly_trend
        """
        cutoff = datetime.utcnow() - timedelta(hours=period_hours)

        platform_filter = "AND platform = :platform" if platform else ""
        params: Dict[str, Any] = {"cutoff": cutoff}
        if platform:
            params["platform"] = platform

        # By-platform aggregation
        summary_query = f"""
            SELECT platform,
                   SUM(records_ingested)  AS total_records,
                   SUM(unique_users)      AS total_users,
                   SUM(unique_indicators) AS total_indicators,
                   SUM(filtered_count)    AS total_filtered,
                   COUNT(DISTINCT source) AS source_count
            FROM platform_hourly_profile
            WHERE stat_hour >= :cutoff
              {platform_filter}
            GROUP BY platform
            ORDER BY total_records DESC
        """

        # Hourly trend
        trend_query = f"""
            SELECT stat_hour,
                   platform,
                   SUM(records_ingested) AS records,
                   SUM(filtered_count)   AS filtered
            FROM platform_hourly_profile
            WHERE stat_hour >= :cutoff
              {platform_filter}
            GROUP BY stat_hour, platform
            ORDER BY stat_hour
        """

        try:
            summary_rows = await execute_query(summary_query, params)
            trend_rows = await execute_query(trend_query, params)
        except Exception as e:
            logging.error(f"[MonitorQuery] ingestion query failed: {e}")
            return {"error": str(e)}

        by_platform = []
        grand_total = 0
        grand_filtered = 0
        for row in summary_rows:
            total = row["total_records"] or 0
            filtered = row["total_filtered"] or 0
            grand_total += total
            grand_filtered += filtered
            by_platform.append({
                "platform": row["platform"],
                "total_records": total,
                "total_users": row["total_users"] or 0,
                "total_indicators": row["total_indicators"] or 0,
                "total_filtered": filtered,
                "exclusion_rate": round(filtered / total * 100, 2) if total > 0 else 0,
                "source_count": row["source_count"] or 0,
            })

        hourly_trend = []
        for row in trend_rows:
            hourly_trend.append({
                "hour": row["stat_hour"].isoformat() if hasattr(row["stat_hour"], "isoformat") else str(row["stat_hour"]),
                "platform": row["platform"],
                "records": row["records"] or 0,
                "filtered": row["filtered"] or 0,
            })

        return {
            "period_hours": period_hours,
            "summary": {
                "total_records": grand_total,
                "total_filtered": grand_filtered,
                "overall_exclusion_rate": round(grand_filtered / grand_total * 100, 4) if grand_total > 0 else 0,
                "platforms": len(by_platform),
            },
            "by_platform": by_platform,
            "hourly_trend": hourly_trend,
        }

    # ------------------------------------------------------------------
    # W3.2: Data quality (from indicator_daily_profile)
    # ------------------------------------------------------------------

    async def get_data_quality(
        self,
        target_date: Optional[str] = None,
        indicator: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Query indicator_daily_profile for data quality monitoring (W3.2).

        Args:
            target_date: Date to analyze (YYYY-MM-DD). Default = yesterday.
            indicator: Filter by indicator name (optional)

        Returns:
            Dict with profiles, cross-source issues, and summary
        """
        if not target_date:
            target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        indicator_filter = "AND indicator = :indicator" if indicator else ""
        params: Dict[str, Any] = {"stat_date": target_date}
        if indicator:
            params["indicator"] = indicator

        query = f"""
            SELECT indicator, source, record_count, non_numeric_count,
                   filtered_count, min_val, max_val, mean_val, stddev_val,
                   p1, p5, p95, p99, issues, health
            FROM indicator_daily_profile
            WHERE stat_date = CAST(:stat_date AS date)
              {indicator_filter}
            ORDER BY record_count DESC
        """

        try:
            rows = await execute_query(query, params)
        except Exception as e:
            logging.error(f"[MonitorQuery] data quality query failed: {e}")
            return {"error": str(e)}

        profiles = []
        indicator_sources: Dict[str, list] = {}

        for row in rows:
            ind = row["indicator"]
            count = row["record_count"] or 0
            filtered = row["filtered_count"] or 0

            # Parse issues from jsonb
            raw_issues = row["issues"]
            if isinstance(raw_issues, str):
                issues = json.loads(raw_issues)
            elif isinstance(raw_issues, list):
                issues = raw_issues
            else:
                issues = []

            profile = {
                "indicator": ind,
                "source": row["source"],
                "count": count,
                "non_numeric_count": row["non_numeric_count"] or 0,
                "min": row["min_val"],
                "max": row["max_val"],
                "mean": row["mean_val"],
                "stddev": row["stddev_val"],
                "p1": row["p1"],
                "p5": row["p5"],
                "p95": row["p95"],
                "p99": row["p99"],
                "filtered_count": filtered,
                "exclusion_rate": round(filtered / count * 100, 2) if count > 0 else 0,
                "health": row["health"],
                "issues": issues,
            }
            profiles.append(profile)

            if ind not in indicator_sources:
                indicator_sources[ind] = []
            indicator_sources[ind].append(profile)

        # Cross-source comparison
        cross_source_issues = self._detect_cross_source_issues(indicator_sources)

        total_records = sum(p["count"] for p in profiles)
        total_filtered = sum(p["filtered_count"] for p in profiles)
        indicators_with_issues = len(set(p["indicator"] for p in profiles if p["issues"]))

        return {
            "table": "indicator_daily_profile",
            "date": target_date,
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

    @staticmethod
    def _detect_cross_source_issues(indicator_sources: Dict[str, list]) -> List[Dict]:
        """Cross-source max-value ratio check."""
        issues = []
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
                issues.append({
                    "indicator": ind,
                    "type": "cross_source_max_ratio",
                    "ratio": round(highest_max / lowest_max, 1),
                    "high_source": highest_src,
                    "high_max": highest_max,
                    "low_source": lowest_src,
                    "low_max": lowest_max,
                    "likely_cause": "unit_conversion_bug",
                })
        return issues

    # ------------------------------------------------------------------
    # W3.8: Trends and alerts
    # ------------------------------------------------------------------

    async def get_trends_and_alerts(self) -> Dict[str, Any]:
        """
        Compute week-over-week trends and check alert rules (W3.8).

        Returns:
            Dict with trend percentages and active alerts
        """
        now = datetime.utcnow()

        # Week-over-week: this week (last 7d) vs previous week (7-14d ago)
        this_week_start = now - timedelta(days=7)
        prev_week_start = now - timedelta(days=14)
        prev_week_end = now - timedelta(days=7)

        wow_query = """
            SELECT
                COALESCE(SUM(CASE WHEN stat_hour >= :this_start THEN records_ingested END), 0) AS this_week,
                COALESCE(SUM(CASE WHEN stat_hour >= :prev_start AND stat_hour < :prev_end THEN records_ingested END), 0) AS prev_week,
                COALESCE(SUM(CASE WHEN stat_hour >= :this_start THEN filtered_count END), 0) AS this_filtered,
                COALESCE(SUM(CASE WHEN stat_hour >= :prev_start AND stat_hour < :prev_end THEN filtered_count END), 0) AS prev_filtered,
                COALESCE(SUM(CASE WHEN stat_hour >= :this_start THEN unique_users END), 0) AS this_users,
                COALESCE(SUM(CASE WHEN stat_hour >= :prev_start AND stat_hour < :prev_end THEN unique_users END), 0) AS prev_users
            FROM platform_hourly_profile
            WHERE stat_hour >= :prev_start
        """
        params = {
            "this_start": this_week_start,
            "prev_start": prev_week_start,
            "prev_end": prev_week_end,
        }

        try:
            rows = await execute_query(wow_query, params)
        except Exception as e:
            logging.error(f"[MonitorQuery] trend query failed: {e}")
            return {"error": str(e)}

        row = rows[0] if rows else {}
        this_w = row.get("this_week", 0) or 0
        prev_w = row.get("prev_week", 0) or 0
        this_f = row.get("this_filtered", 0) or 0
        prev_f = row.get("prev_filtered", 0) or 0
        this_u = row.get("this_users", 0) or 0
        prev_u = row.get("prev_users", 0) or 0

        def pct_change(cur: int, prev: int) -> Optional[float]:
            if prev == 0:
                return None
            return round((cur - prev) / prev * 100, 1)

        this_rate = round(this_f / this_w * 100, 4) if this_w > 0 else 0
        prev_rate = round(prev_f / prev_w * 100, 4) if prev_w > 0 else 0

        trend = {
            "ingestion_wow_pct": pct_change(this_w, prev_w),
            "ingestion_this_week": this_w,
            "ingestion_prev_week": prev_w,
            "filter_rate_this_week": this_rate,
            "filter_rate_prev_week": prev_rate,
            "users_wow_pct": pct_change(this_u, prev_u),
        }

        # Alert rules
        alerts: List[Dict[str, str]] = []

        # 1. Provider silent: any platform with 0 records in last 24h but had data in prev 24h
        silent_query = """
            WITH last_24h AS (
                SELECT platform, SUM(records_ingested) AS records
                FROM platform_hourly_profile
                WHERE stat_hour >= :cutoff_24h
                GROUP BY platform
            ),
            prev_24h AS (
                SELECT platform, SUM(records_ingested) AS records
                FROM platform_hourly_profile
                WHERE stat_hour >= :cutoff_48h AND stat_hour < :cutoff_24h
                GROUP BY platform
            )
            SELECT p.platform, p.records AS prev_records
            FROM prev_24h p
            LEFT JOIN last_24h l ON p.platform = l.platform
            WHERE (l.records IS NULL OR l.records = 0) AND p.records > 0
        """
        try:
            silent_rows = await execute_query(silent_query, {
                "cutoff_24h": now - timedelta(hours=24),
                "cutoff_48h": now - timedelta(hours=48),
            })
            for sr in silent_rows:
                alerts.append({
                    "severity": "warning",
                    "type": "provider_silent",
                    "message": f"Platform {sr['platform']} had {sr['prev_records']} records yesterday but 0 in last 24h",
                })
        except Exception as e:
            logging.error(f"[MonitorQuery] silent check failed: {e}")

        # 2. Filter rate spike: >5% exclusion rate in last 24h
        if this_rate > 5:
            alerts.append({
                "severity": "warning",
                "type": "filter_rate_spike",
                "message": f"Exclusion rate {this_rate}% exceeds 5% threshold",
            })

        # 3. Aggregation stuck: check from scheduler
        try:
            from ..aggregate_indicator.startup import get_aggregate_task_full_status
            agg = await get_aggregate_task_full_status()
            last_run = agg.get("last_run")
            interval = agg.get("execution_interval_hours", 0.1)
            if last_run:
                from datetime import datetime as dt
                last_dt = dt.fromisoformat(last_run) if isinstance(last_run, str) else last_run
                stuck_threshold = timedelta(hours=interval * 3)
                if datetime.now() - last_dt > stuck_threshold:
                    alerts.append({
                        "severity": "critical",
                        "type": "aggregation_stuck",
                        "message": f"Aggregation pipeline last ran at {last_run}, exceeded 3x interval ({interval}h)",
                    })
        except Exception as e:
            logging.error(f"[MonitorQuery] aggregation stuck check failed: {e}")

        return {
            "trend": trend,
            "alerts": alerts,
            "alert_count": len(alerts),
        }

    # ------------------------------------------------------------------
    # Source-level monitoring
    # ------------------------------------------------------------------

    async def get_source_status(self) -> Dict[str, Any]:
        """
        Per-source health status: last active time, daily average,
        and whether the source appears to have stopped sending data.
        """
        now = datetime.utcnow()

        query = """
            SELECT source, platform,
                   MAX(stat_hour) AS last_active,
                   SUM(records_ingested) AS total_7d,
                   SUM(unique_users) AS total_users_7d,
                   SUM(unique_indicators) AS total_indicators_7d,
                   SUM(filtered_count) AS total_filtered_7d,
                   COUNT(DISTINCT stat_hour::date) AS active_days,
                   ROUND(SUM(records_ingested)::numeric / GREATEST(COUNT(DISTINCT stat_hour::date), 1), 0) AS avg_daily
            FROM platform_hourly_profile
            WHERE stat_hour >= :cutoff
            GROUP BY source, platform
            ORDER BY total_7d DESC
        """
        try:
            rows = await execute_query(query, {"cutoff": now - timedelta(days=7)})
        except Exception as e:
            logging.error(f"[MonitorQuery] source status query failed: {e}")
            return {"error": str(e)}

        sources = []
        for row in rows:
            last_active = row["last_active"]
            hours_since = None
            status = "active"

            if last_active:
                if hasattr(last_active, "timestamp"):
                    hours_since = round((now - last_active).total_seconds() / 3600, 1)
                else:
                    try:
                        la = datetime.fromisoformat(str(last_active))
                        hours_since = round((now - la).total_seconds() / 3600, 1)
                    except Exception:
                        pass

            if hours_since is not None:
                if hours_since > 48:
                    status = "stopped"
                elif hours_since > 24:
                    status = "warning"

            total = row["total_7d"] or 0
            filtered = row["total_filtered_7d"] or 0

            sources.append({
                "source": row["source"],
                "platform": row["platform"],
                "status": status,
                "last_active": last_active.isoformat() if hasattr(last_active, "isoformat") else str(last_active),
                "hours_since_last": hours_since,
                "total_records_7d": total,
                "avg_daily": int(row["avg_daily"] or 0),
                "total_users_7d": row["total_users_7d"] or 0,
                "total_indicators_7d": row["total_indicators_7d"] or 0,
                "total_filtered_7d": filtered,
                "exclusion_rate_7d": round(filtered / total * 100, 2) if total > 0 else 0,
                "active_days": row["active_days"] or 0,
            })

        active = sum(1 for s in sources if s["status"] == "active")
        warning = sum(1 for s in sources if s["status"] == "warning")
        stopped = sum(1 for s in sources if s["status"] == "stopped")

        return {
            "summary": {
                "total_sources": len(sources),
                "active": active,
                "warning": warning,
                "stopped": stopped,
            },
            "sources": sources,
        }

    async def get_source_detail(self, source: str, days: int = 7) -> Dict[str, Any]:
        """
        Detailed view for a single source: hourly trend + indicator breakdown.
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(days=days)

        # 1. Hourly trend for this source
        trend_query = """
            SELECT stat_hour, records_ingested, unique_users, filtered_count
            FROM platform_hourly_profile
            WHERE source = :source AND stat_hour >= :cutoff
            ORDER BY stat_hour
        """

        # 2. Indicator breakdown from indicator_daily_profile
        indicator_query = """
            SELECT indicator, SUM(record_count) AS total_records,
                   SUM(non_numeric_count) AS non_numeric,
                   SUM(filtered_count) AS filtered,
                   ROUND(AVG(mean_val)::numeric, 2) AS avg_mean,
                   MIN(min_val) AS overall_min,
                   MAX(max_val) AS overall_max
            FROM indicator_daily_profile
            WHERE source = :source AND stat_date >= CAST(:cutoff_date AS date)
            GROUP BY indicator
            ORDER BY total_records DESC
        """

        try:
            trend_rows = await execute_query(trend_query, {"source": source, "cutoff": cutoff})
            indicator_rows = await execute_query(indicator_query, {
                "source": source,
                "cutoff_date": (now - timedelta(days=days)).strftime("%Y-%m-%d"),
            })
        except Exception as e:
            logging.error(f"[MonitorQuery] source detail query failed: {e}")
            return {"error": str(e)}

        hourly_trend = []
        for row in trend_rows:
            h = row["stat_hour"]
            hourly_trend.append({
                "hour": h.isoformat() if hasattr(h, "isoformat") else str(h),
                "records": row["records_ingested"] or 0,
                "users": row["unique_users"] or 0,
                "filtered": row["filtered_count"] or 0,
            })

        indicators = []
        total_all = sum((r["total_records"] or 0) for r in indicator_rows)
        for row in indicator_rows:
            total = row["total_records"] or 0
            indicators.append({
                "indicator": row["indicator"],
                "total_records": total,
                "pct": round(total / total_all * 100, 1) if total_all > 0 else 0,
                "non_numeric": row["non_numeric"] or 0,
                "filtered": row["filtered"] or 0,
                "avg_mean": float(row["avg_mean"]) if row["avg_mean"] is not None else None,
                "overall_min": float(row["overall_min"]) if row["overall_min"] is not None else None,
                "overall_max": float(row["overall_max"]) if row["overall_max"] is not None else None,
            })

        return {
            "source": source,
            "days": days,
            "hourly_trend": hourly_trend,
            "indicators": indicators,
            "indicator_count": len(indicators),
        }

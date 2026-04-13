"""
Monitor Collector Service (TH-141)

Pre-computes report tables from series_data for fast API reads:
- platform_hourly_profile: hourly ingestion stats by platform/source
- indicator_daily_profile: daily value distribution per indicator/source
"""

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from ..database import BaseDatabaseService
from .platform_mapping import resolve_platform

# Anomaly detection thresholds (aligned with data_quality_service.py)
SUSPICIOUS_MULTIPLIER = 10
CROSS_SOURCE_RATIO_THRESHOLD = 100


class MonitorCollectorService(BaseDatabaseService):
    """Collects aggregated stats from series_data into report tables."""

    # ------------------------------------------------------------------
    # Hourly: platform_hourly_profile
    # ------------------------------------------------------------------

    async def collect_hourly_stats(self, target_hour: datetime) -> Dict[str, Any]:
        """
        Aggregate series_data for a single hour into platform_hourly_profile.

        Args:
            target_hour: The hour to collect (truncated to hour boundary)

        Returns:
            Dict with rows_upserted count
        """
        hour_start = target_hour.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)

        query = """
            SELECT source,
                   COUNT(*)                                              AS records_ingested,
                   COUNT(DISTINCT user_id)                               AS unique_users,
                   COUNT(DISTINCT indicator)                             AS unique_indicators,
                   COUNT(*) FILTER (WHERE task_id = 'filtered_out_of_range') AS filtered_count
            FROM series_data
            WHERE time >= :hour_start AND time < :hour_end
            GROUP BY source
        """
        params = {"hour_start": hour_start, "hour_end": hour_end}

        rows = await self.execute_query_with_session_params(
            query, params, session_params=["SET LOCAL enable_seqscan = off"]
        )

        upserted = 0
        for row in rows:
            src = row["source"] or ""
            platform = resolve_platform(src)

            upsert_sql = """
                INSERT INTO platform_hourly_profile
                    (stat_hour, platform, source, records_ingested, unique_users,
                     unique_indicators, filtered_count, updated_at)
                VALUES
                    (:stat_hour, :platform, :source, :records_ingested, :unique_users,
                     :unique_indicators, :filtered_count, now())
                ON CONFLICT (stat_hour, platform, source) DO UPDATE SET
                    records_ingested  = EXCLUDED.records_ingested,
                    unique_users      = EXCLUDED.unique_users,
                    unique_indicators = EXCLUDED.unique_indicators,
                    filtered_count    = EXCLUDED.filtered_count,
                    updated_at        = now()
            """
            upsert_params = {
                "stat_hour": hour_start,
                "platform": platform,
                "source": src,
                "records_ingested": row["records_ingested"],
                "unique_users": row["unique_users"],
                "unique_indicators": row["unique_indicators"],
                "filtered_count": row["filtered_count"],
            }
            await self.execute_query(upsert_sql, upsert_params)
            upserted += 1

        logging.info(
            f"[MonitorCollector] hourly stats collected: hour={hour_start.isoformat()}, "
            f"sources={len(rows)}, upserted={upserted}"
        )
        return {"hour": hour_start.isoformat(), "rows_upserted": upserted}

    async def collect_changed_hourly_stats(
        self, lookback_hours: int = 2, max_days: int = 7
    ) -> Dict[str, Any]:
        """
        Find hours with recently modified data and recalculate their stats.

        Uses update_time to detect changes, then recalculates the affected
        hour slots based on the data's original time field.

        Args:
            lookback_hours: How far back to check update_time (default 2h)
            max_days: Only recalculate hour slots within this many recent days (default 7d)

        Returns:
            Dict with hours_recalculated count
        """
        cutoff_update = datetime.utcnow() - timedelta(hours=lookback_hours)
        cutoff_time = datetime.utcnow() - timedelta(days=max_days)

        # Find distinct hour slots that have been modified recently
        query = """
            SELECT DISTINCT date_trunc('hour', time) AS hour_slot
            FROM series_data
            WHERE update_time >= :cutoff_update
              AND time >= :cutoff_time
            ORDER BY hour_slot
        """
        rows = await self.execute_query_with_session_params(
            query,
            {"cutoff_update": cutoff_update, "cutoff_time": cutoff_time},
            session_params=["SET LOCAL enable_seqscan = off"],
        )

        hour_slots = [row["hour_slot"] for row in rows]

        if not hour_slots:
            logging.info("[MonitorCollector] No changed hour slots found")
            return {"hours_recalculated": 0}

        logging.info(
            f"[MonitorCollector] Found {len(hour_slots)} changed hour slots "
            f"(lookback={lookback_hours}h, max_days={max_days}d)"
        )

        total_upserted = 0
        for hour_slot in hour_slots:
            result = await self.collect_hourly_stats(hour_slot)
            total_upserted += result.get("rows_upserted", 0)

        logging.info(
            f"[MonitorCollector] Changed hourly stats done: "
            f"{len(hour_slots)} hours, {total_upserted} rows upserted"
        )
        return {"hours_recalculated": len(hour_slots), "rows_upserted": total_upserted}

    # ------------------------------------------------------------------
    # Daily: indicator_daily_profile
    # ------------------------------------------------------------------

    async def collect_daily_profile(self, target_date: date) -> Dict[str, Any]:
        """
        Aggregate series_data for a single day into indicator_daily_profile.

        Computes value distribution (min/max/mean/stddev/percentiles),
        non-numeric count, filtered count, and anomaly detection per indicator×source.

        Args:
            target_date: The date to profile

        Returns:
            Dict with rows_upserted count
        """
        date_str = target_date.isoformat()

        # SQL 1: numeric value stats with percentiles (clean data only)
        # Use CAST() instead of :: to avoid SQLAlchemy param-parsing conflicts
        # Exclude filtered records so stats reflect clean data only.
        # When W1.1 marks dirty data, these stats will automatically be clean.
        stats_query = """
            SELECT indicator, source,
                   COUNT(*)                                    AS record_count,
                   MIN(CAST(value AS numeric))                 AS min_val,
                   MAX(CAST(value AS numeric))                 AS max_val,
                   ROUND(CAST(AVG(CAST(value AS numeric)) AS numeric), 4) AS mean_val,
                   ROUND(CAST(STDDEV(CAST(value AS numeric)) AS numeric), 4) AS stddev_val,
                   PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p1,
                   PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p5,
                   PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p25,
                   PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS median_val,
                   PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p75,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p95,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY CAST(value AS numeric)) AS p99
            FROM series_data
            WHERE time >= CAST(:date_start AS date)
              AND time < CAST(:date_start AS date) + INTERVAL '1 day'
              AND value ~ '^-?[0-9]+\\.?[0-9]*$'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
            GROUP BY indicator, source
        """

        # SQL 2: non-numeric counts
        non_numeric_query = """
            SELECT indicator, source,
                   COUNT(*) AS non_numeric_count
            FROM series_data
            WHERE time >= CAST(:date_start AS date)
              AND time < CAST(:date_start AS date) + INTERVAL '1 day'
              AND NOT (value ~ '^-?[0-9]+\\.?[0-9]*$')
            GROUP BY indicator, source
        """

        # SQL 3: filtered counts
        filtered_query = """
            SELECT indicator, source,
                   COUNT(*) AS filtered_count
            FROM series_data
            WHERE time >= CAST(:date_start AS date)
              AND time < CAST(:date_start AS date) + INTERVAL '1 day'
              AND task_id = 'filtered_out_of_range'
            GROUP BY indicator, source
        """

        params = {"date_start": date_str}

        stats_rows = await self.execute_query_with_session_params(
            stats_query, params, session_params=["SET LOCAL enable_seqscan = off"]
        )
        non_numeric_rows = await self.execute_query_with_session_params(
            non_numeric_query, params, session_params=["SET LOCAL enable_seqscan = off"]
        )
        filtered_rows = await self.execute_query_with_session_params(
            filtered_query, params, session_params=["SET LOCAL enable_seqscan = off"]
        )

        # Index lookup helpers
        stats_map: Dict[str, Dict] = {}
        for r in stats_rows:
            key = f"{r['indicator']}|{r['source'] or ''}"
            stats_map[key] = r

        nn_map: Dict[str, int] = {}
        for r in non_numeric_rows:
            nn_map[f"{r['indicator']}|{r['source'] or ''}"] = r["non_numeric_count"]

        fl_map: Dict[str, int] = {}
        for r in filtered_rows:
            fl_map[f"{r['indicator']}|{r['source'] or ''}"] = r["filtered_count"]

        # Collect all indicator×source keys (union of numeric + non-numeric + filtered)
        all_keys = set(stats_map.keys()) | set(nn_map.keys()) | set(fl_map.keys())

        upsert_sql = """
            INSERT INTO indicator_daily_profile
                (stat_date, indicator, source, record_count, non_numeric_count,
                 filtered_count, min_val, max_val, mean_val, stddev_val,
                 p1, p5, p25, median_val, p75, p95, p99, issues, health, updated_at)
            VALUES
                (:stat_date, :indicator, :source, :record_count, :non_numeric_count,
                 :filtered_count, :min_val, :max_val, :mean_val, :stddev_val,
                 :p1, :p5, :p25, :median_val, :p75, :p95, :p99, :issues, :health, now())
            ON CONFLICT (stat_date, indicator, source) DO UPDATE SET
                record_count      = EXCLUDED.record_count,
                non_numeric_count = EXCLUDED.non_numeric_count,
                filtered_count    = EXCLUDED.filtered_count,
                min_val           = EXCLUDED.min_val,
                max_val           = EXCLUDED.max_val,
                mean_val          = EXCLUDED.mean_val,
                stddev_val        = EXCLUDED.stddev_val,
                p1                = EXCLUDED.p1,
                p5                = EXCLUDED.p5,
                p25               = EXCLUDED.p25,
                median_val        = EXCLUDED.median_val,
                p75               = EXCLUDED.p75,
                p95               = EXCLUDED.p95,
                p99               = EXCLUDED.p99,
                issues            = EXCLUDED.issues,
                health            = EXCLUDED.health,
                updated_at        = now()
        """

        upserted = 0
        for key in all_keys:
            ind, src = key.split("|", 1)
            row = stats_map.get(key)

            if row:
                min_v = _to_float(row["min_val"])
                max_v = _to_float(row["max_val"])
                mean_v = _to_float(row["mean_val"])
                std_v = _to_float(row["stddev_val"])
                record_count = row["record_count"]
                p1 = _to_float(row["p1"])
                p5 = _to_float(row["p5"])
                p25 = _to_float(row["p25"])
                median_v = _to_float(row["median_val"])
                p75 = _to_float(row["p75"])
                p95 = _to_float(row["p95"])
                p99 = _to_float(row["p99"])
            else:
                # Non-numeric only or filtered-only — no stats
                min_v = max_v = mean_v = std_v = None
                record_count = 0
                p1 = p5 = p25 = median_v = p75 = p95 = p99 = None

            issues = _detect_anomalies(min_v, max_v, mean_v, std_v)
            health = "ok"
            if len(issues) == 1:
                health = "warning"
            elif len(issues) > 1:
                health = "critical"

            upsert_params = {
                "stat_date": target_date,
                "indicator": ind,
                "source": src,
                "record_count": record_count,
                "non_numeric_count": nn_map.get(key, 0),
                "filtered_count": fl_map.get(key, 0),
                "min_val": min_v,
                "max_val": max_v,
                "mean_val": mean_v,
                "stddev_val": std_v,
                "p1": p1,
                "p5": p5,
                "p25": p25,
                "median_val": median_v,
                "p75": p75,
                "p95": p95,
                "p99": p99,
                "issues": json.dumps(issues),
                "health": health,
            }
            await self.execute_query(upsert_sql, upsert_params)
            upserted += 1

        # ------------------------------------------------------------------
        # Phase 2: Collect aggregated/derived/daily_stats from th_series_data
        # These indicators live in th_series_data, not series_data.
        # Source = task_id value for identification.
        # ------------------------------------------------------------------
        th_stats_query = """
            SELECT indicator, source,
                   COUNT(*)                                    AS record_count,
                   MIN(CAST(value AS numeric))                 AS min_val,
                   MAX(CAST(value AS numeric))                 AS max_val,
                   ROUND(CAST(AVG(CAST(value AS numeric)) AS numeric), 4) AS mean_val,
                   ROUND(CAST(STDDEV(CAST(value AS numeric)) AS numeric), 4) AS stddev_val
            FROM th_series_data
            WHERE start_time >= CAST(:date_start AS date)
              AND start_time < CAST(:date_start AS date) + INTERVAL '1 day'
              AND deleted = 0
              AND (task_id IN ('aggregate_indicator', 'derived_aggregator')
                   OR task_id IS NULL)
              AND value ~ '^-?[0-9]+\\.?[0-9]*$'
            GROUP BY indicator, source
        """
        th_rows = await self.execute_query_with_session_params(
            th_stats_query, params, session_params=[]
        )

        for r in th_rows:
            ind = r["indicator"] or ""
            src = r["source"] or ""
            upsert_params = {
                "stat_date": target_date,
                "indicator": ind,
                "source": src,
                "record_count": r["record_count"],
                "non_numeric_count": 0,
                "filtered_count": 0,
                "min_val": _to_float(r["min_val"]),
                "max_val": _to_float(r["max_val"]),
                "mean_val": _to_float(r["mean_val"]),
                "stddev_val": _to_float(r["stddev_val"]),
                "p1": None,
                "p5": None,
                "p25": None,
                "median_val": None,
                "p75": None,
                "p95": None,
                "p99": None,
                "issues": "[]",
                "health": "ok",
            }
            await self.execute_query(upsert_sql, upsert_params)
            upserted += 1

        logging.info(
            f"[MonitorCollector] daily profile collected: date={date_str}, "
            f"series={len(all_keys)}, th_series={len(th_rows)}, upserted={upserted}"
        )
        return {"date": date_str, "rows_upserted": upserted}

    async def collect_changed_daily_profiles(
        self, lookback_hours: int = 12, max_days: int = 30
    ) -> Dict[str, Any]:
        """
        Find days with recently modified data and recalculate their daily profiles.

        Args:
            lookback_hours: How far back to check update_time (default 12h)
            max_days: Only recalculate days within this many recent days (default 30d)

        Returns:
            Dict with days_recalculated count
        """
        cutoff_update = datetime.utcnow() - timedelta(hours=lookback_hours)
        cutoff_time = date.today() - timedelta(days=max_days)

        query = """
            SELECT DISTINCT time::date AS day_slot
            FROM series_data
            WHERE update_time >= :cutoff_update
              AND time::date >= :cutoff_time
            ORDER BY day_slot
        """
        rows = await self.execute_query_with_session_params(
            query,
            {"cutoff_update": cutoff_update, "cutoff_time": str(cutoff_time)},
            session_params=["SET LOCAL enable_seqscan = off"],
        )

        day_slots = [row["day_slot"] for row in rows]

        if not day_slots:
            logging.info("[MonitorCollector] No changed day slots found")
            return {"days_recalculated": 0}

        logging.info(
            f"[MonitorCollector] Found {len(day_slots)} changed day slots "
            f"(lookback={lookback_hours}h, max_days={max_days}d)"
        )

        total_upserted = 0
        for day_slot in day_slots:
            result = await self.collect_daily_profile(day_slot)
            total_upserted += result.get("rows_upserted", 0)

        logging.info(
            f"[MonitorCollector] Changed daily profiles done: "
            f"{len(day_slots)} days, {total_upserted} rows upserted"
        )
        return {"days_recalculated": len(day_slots), "rows_upserted": total_upserted}

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    async def backfill_hourly(self, days: int = 7) -> Dict[str, Any]:
        """Backfill platform_hourly_profile for the past N days."""
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        total = 0
        for offset_hours in range(days * 24):
            target = now - timedelta(hours=offset_hours + 1)
            result = await self.collect_hourly_stats(target)
            total += result.get("rows_upserted", 0)

        logging.info(f"[MonitorCollector] hourly backfill done: {days} days, {total} rows")
        return {"days": days, "total_rows_upserted": total}

    async def backfill_daily(self, days: int = 30) -> Dict[str, Any]:
        """Backfill indicator_daily_profile for the past N days."""
        today = date.today()
        total = 0
        for offset in range(1, days + 1):
            target = today - timedelta(days=offset)
            result = await self.collect_daily_profile(target)
            total += result.get("rows_upserted", 0)

        logging.info(f"[MonitorCollector] daily backfill done: {days} days, {total} rows")
        return {"days": days, "total_rows_upserted": total}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _to_float(val) -> float | None:
    """Safely convert a DB value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _detect_anomalies(
    min_v: float | None,
    max_v: float | None,
    mean_v: float | None,
    std_v: float | None,
) -> List[Dict[str, str]]:
    """
    Run statistical anomaly checks on a single indicator×source profile.

    Only flags extreme statistical outliers (mean ± 10σ).
    Domain-specific valid ranges are handled by W1.1 (filtered_out_of_range).
    Negative values are NOT flagged here as some indicators are naturally negative
    (e.g. temperatureDelta, weight change).
    """
    issues: List[Dict[str, str]] = []

    if mean_v is not None and std_v is not None and std_v > 0:
        upper = mean_v + SUSPICIOUS_MULTIPLIER * std_v
        lower = mean_v - SUSPICIOUS_MULTIPLIER * std_v
        if max_v is not None and max_v > upper:
            issues.append({
                "type": "suspicious_max",
                "detail": f"max={max_v} exceeds mean+{SUSPICIOUS_MULTIPLIER}σ ({upper:.1f})",
            })
        if min_v is not None and min_v < lower:
            issues.append({
                "type": "suspicious_min",
                "detail": f"min={min_v} below mean-{SUSPICIOUS_MULTIPLIER}σ ({lower:.1f})",
            })

    return issues

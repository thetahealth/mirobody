"""
Apple Health Statistics Service (TH-154)

Converts client-submitted pre-aggregated statistics into summary records
and writes them to th_series_data via AggregateDatabaseService.
"""

import logging
import time

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .models import (
    AppleHealthStatistic,
    AppleHealthStatisticsRequest,
    FLUTTER_TO_RECORD_TYPE_MAPPING,
    FlutterHealthTypeEnum,
)
from ..core.aggregate_indicator.naming import build_indicator_name
from ..core.aggregate_indicator.database_service import AggregateDatabaseService

# Mapping from statistics payload fields to aggregation method keys
STAT_FIELD_TO_METHOD = {
    "sum": "total",
    "average": "avg",
    "minimum": "min",
    "maximum": "max",
    "mostRecent": "last",
}


def _resolve_source_indicator(health_type: str) -> Optional[str]:
    """
    Resolve Flutter health type string to our source indicator name.

    Args:
        health_type: e.g. "STEPS", "HEART_RATE"

    Returns:
        Source indicator name (e.g. "steps", "heartRates") or None if unmapped
    """
    try:
        enum_val = FlutterHealthTypeEnum(health_type)
    except ValueError:
        return None
    return FLUTTER_TO_RECORD_TYPE_MAPPING.get(enum_val)


def _statistics_to_summary_records(
    statistics: List[AppleHealthStatistic],
    user_id: str,
    default_timezone: str,
) -> List[Dict[str, Any]]:
    """
    Convert statistics list into summary records for th_series_data UPSERT.

    For each statistic, iterates over non-null aggregation fields (sum, average, etc.),
    maps them to the corresponding indicator name via build_indicator_name(),
    and produces one summary record per non-null field.

    Args:
        statistics: List of AppleHealthStatistic from the request
        user_id: Authenticated user ID (from token)
        default_timezone: Fallback timezone from metaInfo

    Returns:
        List of dicts ready for AggregateDatabaseService.batch_save_summary_data()
    """
    records = []

    for stat in statistics:
        source_indicator = _resolve_source_indicator(stat.type)
        if not source_indicator:
            logging.warning(f"Unmapped health type in statistics: {stat.type}")
            continue

        tz = stat.timezone or default_timezone

        # Convert epoch ms to user's local time (naive) for th_series_data storage.
        # th_series_data stores start_time/end_time as "timestamp without time zone"
        # representing user's local time. Same approach as upload_health.py:380-384.
        start_time_utc = datetime.fromtimestamp(stat.dateFrom / 1000, tz=timezone.utc)
        end_time_utc = datetime.fromtimestamp(stat.dateTo / 1000, tz=timezone.utc)
        if tz == "UTC":
            start_time = start_time_utc.replace(tzinfo=None)
            end_time = end_time_utc.replace(tzinfo=None)
        else:
            try:
                user_tz = ZoneInfo(tz)
                start_time = start_time_utc.astimezone(user_tz).replace(tzinfo=None)
                end_time = end_time_utc.astimezone(user_tz).replace(tzinfo=None)
            except Exception:
                logging.warning(f"Invalid timezone {tz!r}, falling back to UTC")
                start_time = start_time_utc.replace(tzinfo=None)
                end_time = end_time_utc.replace(tzinfo=None)

        for field_name, method in STAT_FIELD_TO_METHOD.items():
            value = getattr(stat, field_name, None)
            if value is None:
                continue

            try:
                indicator_name = build_indicator_name(stat.grouping, method, source_indicator)
            except ValueError as e:
                logging.warning(f"Failed to build indicator name: {e}")
                continue

            records.append({
                "user_id": user_id,
                "indicator": indicator_name,
                "value": str(value),
                "start_time": start_time,
                "end_time": end_time,
                "source": "apple_health",
                "task_id": "apple_health_statistics",
                "comment": f"grouping={stat.grouping}, tz={tz}, unit={stat.unitSymbol or stat.unit or ''}",
                "source_table": "",
                "source_table_id": "",
                "indicator_id": "",
                "fhir_id": None,
            })

    return records


async def process_apple_health_statistics(
    request: AppleHealthStatisticsRequest,
    user_id: str,
) -> int:
    """
    Process an Apple Health statistics request.

    Args:
        request: Validated AppleHealthStatisticsRequest
        user_id: Authenticated user ID (from token)

    Returns:
        Number of accepted (saved) summary records

    Raises:
        Exception: On database errors
    """
    t1 = time.time()
    default_tz = request.metaInfo.timezone

    records = _statistics_to_summary_records(request.statistics, user_id, default_tz)
    t2 = time.time()

    logging.info(
        f"Statistics mapping: {len(request.statistics)} stats -> {len(records)} summary records, "
        f"user={user_id}, time={((t2 - t1) * 1e3):.1f}ms"
    )

    if not records:
        return 0

    db_service = AggregateDatabaseService()
    success = await db_service.batch_save_summary_data(records)
    t3 = time.time()

    logging.info(
        f"Statistics save: {len(records)} records, success={success}, "
        f"user={user_id}, time={((t3 - t2) * 1e3):.1f}ms"
    )

    return len(records) if success else 0

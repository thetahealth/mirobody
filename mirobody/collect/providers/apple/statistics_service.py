"""
Apple Health Statistics Service (TH-154)

Converts client-submitted pre-aggregated statistics into summary records
and writes them as day-grained observations via AggregateDatabaseService.
"""

import logging
import time

from datetime import datetime, UTC
from typing import Any
from zoneinfo import ZoneInfo

from .models import AppleHealthStatistic, AppleHealthStatisticsRequest
from mirobody.kernel.decoders import apple as apple_decoder
from mirobody.translate import build_indicator_name
from mirobody.translate import AggregateDatabaseService

logger = logging.getLogger(__name__)

# Mapping from statistics payload fields to aggregation method keys
STAT_FIELD_TO_METHOD = {
    "sum": "total",
    "average": "avg",
    "minimum": "min",
    "maximum": "max",
    "mostRecent": "last",
}


def _resolve_source_indicator(health_type: str) -> str | None:
    """HealthKit identifier to catalogue metric, off the decoder's own tables.

    Args:
        health_type: e.g. "HKQuantityTypeIdentifierStepCount"

    Returns:
        Source indicator name (e.g. "steps", "heartRates") or None if unmapped
    """
    return apple_decoder.QUANTITY.get(health_type) or apple_decoder.CATEGORY.get(health_type)


def _statistics_to_summary_records(
    statistics: list[AppleHealthStatistic],
    user_id: str,
    default_timezone: str,
) -> list[dict[str, Any]]:
    """
    Convert statistics list into summary records for the observation writer.

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
            logger.warning(f"Unmapped health type in statistics: {stat.type}")
            continue

        tz = stat.timezone or default_timezone

        # Convert epoch ms to the user's local wall clock (naive): the summary
        # record shape carries local times, and the writer places them in `tz`.
        start_time_utc = datetime.fromtimestamp(stat.dateFrom / 1000, tz=UTC)
        end_time_utc = datetime.fromtimestamp(stat.dateTo / 1000, tz=UTC)
        if tz == "UTC":
            start_time = start_time_utc.replace(tzinfo=None)
            end_time = end_time_utc.replace(tzinfo=None)
        else:
            try:
                user_tz = ZoneInfo(tz)
                start_time = start_time_utc.astimezone(user_tz).replace(tzinfo=None)
                end_time = end_time_utc.astimezone(user_tz).replace(tzinfo=None)
            except Exception:
                logger.warning(f"Invalid timezone {tz!r}, falling back to UTC")
                start_time = start_time_utc.replace(tzinfo=None)
                end_time = end_time_utc.replace(tzinfo=None)

        for field_name, method in STAT_FIELD_TO_METHOD.items():
            value = getattr(stat, field_name, None)
            if value is None:
                continue

            try:
                indicator_name = build_indicator_name(stat.grouping, method, source_indicator)
            except ValueError as e:
                logger.warning(f"Failed to build indicator name: {e}")
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
                "unit": stat.unitSymbol or stat.unit or "",
                "timezone": tz,
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

    logger.info(
        f"Statistics mapping: {len(request.statistics)} stats -> {len(records)} summary records, "
        f"user={user_id}, time={((t2 - t1) * 1e3):.1f}ms"
    )

    if not records:
        return 0

    db_service = AggregateDatabaseService()
    success = await db_service.batch_save_summary_data(records)
    t3 = time.time()

    logger.info(
        f"Statistics save: {len(records)} records, success={success}, "
        f"user={user_id}, time={((t3 - t2) * 1e3):.1f}ms"
    )

    return len(records) if success else 0

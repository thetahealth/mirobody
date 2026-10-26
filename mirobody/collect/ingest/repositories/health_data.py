"""SQL for `series_data`, the device point buffer the upload path writes.

Kept apart from `services/` so the service reads as the decisions it makes and
not as the queries it runs. Observations are written by
`collect/observations.py` alone.
"""

import logging

from datetime import datetime
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.standardize import build_apple_priority_case
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

# Rows per executemany.
_BATCH_SIZE = 10000

_CURRENT_PRIORITY = build_apple_priority_case("series_data.source", "series_data.source_id")
_INCOMING_PRIORITY = build_apple_priority_case("EXCLUDED.source", "EXCLUDED.source_id")

# The buffer's key does not include source_id. For an export's same-instant
# device collision, keep the same valid winner the daily aggregator would
# elect; a repeated reading from that device still applies its correction.
_UPSERT_POINTS = f"""
    INSERT INTO series_data (user_id, indicator, source, time, value, timezone, task_id, source_id, create_time, update_time)
    VALUES (:user_id, :indicator, :source, :time, :value, :timezone, :task_id, :source_id, now(), now())
    ON CONFLICT (user_id, indicator, source, time)
    DO UPDATE
    SET
      value = EXCLUDED.value,
      timezone = EXCLUDED.timezone,
      task_id = EXCLUDED.task_id,
      source_id = EXCLUDED.source_id,
      update_time = now()
    WHERE (
      series_data.value IS DISTINCT FROM EXCLUDED.value
      OR series_data.task_id IS DISTINCT FROM EXCLUDED.task_id
      OR (series_data.source = 'apple_health_export' AND series_data.source_id IS DISTINCT FROM EXCLUDED.source_id)
    ) AND (
      series_data.source IS DISTINCT FROM 'apple_health_export'
      OR series_data.source_id IS NOT DISTINCT FROM EXCLUDED.source_id
      OR (
        EXCLUDED.task_id IS NOT DISTINCT FROM 'filtered_out_of_range',
        ({_INCOMING_PRIORITY}), EXCLUDED.source_id IS NULL, COALESCE(EXCLUDED.source_id, '')
      ) <= (
        series_data.task_id IS NOT DISTINCT FROM 'filtered_out_of_range',
        ({_CURRENT_PRIORITY}), series_data.source_id IS NULL, COALESCE(series_data.source_id, '')
      )
    )
"""


class HealthDataRepository:
    """Health data repository"""

    async def save_health_records(
        self,
        records: list[dict[str, Any]],
    ) -> bool:
        """Upsert device points into `series_data`, `_BATCH_SIZE` per statement.

        Each record carries `user_id`, `indicator`, `value`, `start_time` (the
        point's naive UTC instant), `source`, `timezone`, and optionally
        `task_id` and `source_id`. A point already stored with the same value
        and task id is left as it is, unless an export elects a better device
        under the same key. Returns whether every batch was written.
        """
        if not records:
            return True
        try:
            for i in range(0, len(records), _BATCH_SIZE):
                batch = [
                    {
                        "user_id": str(record["user_id"]),
                        "indicator": record["indicator"],
                        "source": record["source"],
                        "time": record["start_time"],
                        "value": record["value"],
                        "timezone": record["timezone"],
                        "task_id": record.get("task_id"),
                        "source_id": record.get("source_id"),
                    }
                    for record in records[i : i + _BATCH_SIZE]
                ]
                await execute_query(_UPSERT_POINTS, batch)
            logger.info("series points saved: records=%d", len(records))
            return True

        except Exception as e:
            logger.error("series points save failed: records=%d error_type=%s", len(records), type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False

    async def sweep_series_data_repair(
        self,
        user_id: str,
        sources: list[str],
        indicators: set[str],
        window_from: datetime,
        window_to: datetime,
        repair_task_id: str,
    ) -> int:
        """The repair sweep on `series_data`: delete the points of `sources`
        and `indicators` inside `[window_from, window_to]` (naive UTC) that the
        repair batch did not re-confirm. The table has no deleted flag, so this
        is a hard delete; the caller makes sure the batch is not empty.

        A repair can arrive split across several batches sharing one task id.
        `task_id IS DISTINCT FROM :repair_task_id` keeps every point an earlier
        batch of the same repair wrote, so batch N never sweeps batch N-1.
        Returns how many points were deleted.
        """
        if not sources or not indicators:
            return 0

        # Out-of-range points are not exempt: the window is authoritative, and a
        # stale filtered point the repair did not re-send goes like any other.
        query = """
            DELETE FROM series_data
            WHERE user_id = :user_id
              AND source = ANY(:sources)
              AND indicator = ANY(:indicators)
              AND time >= :window_from
              AND time <= :window_to
              AND (task_id IS DISTINCT FROM :repair_task_id)
        """
        params = {
            "user_id": str(user_id),
            "sources": list(sources),
            "indicators": list(indicators),
            "window_from": window_from,
            "window_to": window_to,
            "repair_task_id": repair_task_id,
        }
        result = await execute_query(query, params)
        deleted_count = int(result.get("record_count") or 0) if isinstance(result, dict) else 0
        logger.info("repair sweep of series points: user_id=%s deleted=%d indicators=%d",
                    user_id, deleted_count, len(indicators))
        return deleted_count


# Create singleton instance
health_data_repository = HealthDataRepository()

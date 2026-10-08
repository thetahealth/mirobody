"""The sweep of a data-repair batch.

When a phone re-sends a window it has corrected, its batch carries
`metaInfo.taskId = "repair-<uuid>"`, and the upload stamps that id on every
row it writes (the "mark"). This module is the "sweep": after the save, it
removes what the batch did NOT re-confirm inside the window the caller names,
so a re-sync fixes rows that should not exist (a night of sleep stages stored
twice) instead of only overwriting values.

The window is `[windowFrom, windowTo]` in epoch ms, from metaInfo. If either
bound is missing, or they are out of order, nothing is swept: the batch is
still written, and nothing is deleted.

What is swept:
  - `series_data`, the device point buffer (sleep stages among it): deleted,
    then the window is re-aggregated so the daily observations follow.
    `series_data.time` is naive UTC.
  - observations the batch wrote directly (summary indicators): retracted,
    never deleted (`observations.retract_unconfirmed`); the read view hides a
    retracted row and the retraction says which repair made it.

Only Apple Health sources are swept (`apple.cda` is a document, not a stream),
and never a row of the repair's own task id, so a repair that arrives in
several batches does not sweep its own earlier batches.
"""

import logging

from datetime import UTC, datetime, timedelta
from typing import Any

from mirobody.collect import observations
from mirobody.collect.ingest.repositories.health_data import HealthDataRepository
from mirobody.kernel.ops import is_driver_exception
from mirobody.translate import AggregateIndicatorService
from mirobody.translate import HealthDataType, get_indicators_in_same_categories

logger = logging.getLogger(__name__)

# Prefix that marks an upload batch as a data-repair re-sync (contract with iOS).
REPAIR_TASK_ID_PREFIX = "repair-"

# Apple sources eligible for repair sweep (already lower-cased upstream).
# apple.cda (clinical documents) is intentionally excluded: different semantics.
APPLE_REPAIR_SOURCES = {"apple_health", "apple_health_watch"}

# Pad applied to the aggregation window so the sleep 18:00-18:00 boundary is fully
# re-derived (sleep aggregation buckets a night under the following calendar day).
_AGGREGATE_WINDOW_PAD = timedelta(days=1)


def detect_repair_task_id(records: list[dict[str, Any]]) -> str | None:
    """The batch's `repair-<uuid>` task id, or None when it is not a repair:
    the first row whose task id carries the prefix."""
    for record in records:
        task_id = record.get("task_id")
        if task_id and task_id.startswith(REPAIR_TASK_ID_PREFIX):
            return task_id
    return None


def _apple_sources(records: list[dict[str, Any]]) -> list[str]:
    """Apple sources present in the batch, restricted to the repair-eligible set."""
    present = {r.get("source") for r in records if r.get("source")}
    return sorted(present & APPLE_REPAIR_SOURCES)


def _instant(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


class RepairReconciler:
    """Orchestrates the mark-and-sweep reconcile for a single repair batch."""

    def __init__(self, repository: HealthDataRepository | None = None):
        self.repository = repository or HealthDataRepository()

    async def reconcile(
        self,
        user_id: str,
        summary_records: list[dict[str, Any]],
        series_records: list[dict[str, Any]],
        window_from_ms: int | None = None,
        window_to_ms: int | None = None,
    ) -> dict[str, Any]:
        """Run the sweep if this is a repair batch with a complete window.

        `summary_records` were written as observations, `series_records` to
        `series_data`; both are the upload's prepared rows. Returns what was
        done, for the log. Never raises into the upload path.
        """
        all_records = (summary_records or []) + (series_records or [])

        repair_task_id = detect_repair_task_id(all_records)
        if not repair_task_id:
            return {"status": "not_repair"}

        if window_from_ms is None or window_to_ms is None or window_from_ms > window_to_ms:
            logger.info("repair sweep skipped, incomplete window: user_id=%s task_id=%s", user_id, repair_task_id)
            return {"status": "repair_no_window", "repair_task_id": repair_task_id}

        result: dict[str, Any] = {
            "status": "success",
            "repair_task_id": repair_task_id,
            "window_from_ms": window_from_ms,
            "window_to_ms": window_to_ms,
            "series_deleted": 0,
            "observations_retracted": 0,
            "reaggregated": False,
        }
        start, end = _instant(window_from_ms), _instant(window_to_ms)

        try:
            if series_records:
                sources = _apple_sources(series_records)
                present = {r.get("indicator") for r in series_records if r.get("indicator")}
                family = get_indicators_in_same_categories(
                    present,
                    data_types={HealthDataType.SERIES, HealthDataType.MIX},
                )
                if sources and family:
                    # `series_data.time` is naive UTC.
                    naive_start, naive_end = start.replace(tzinfo=None), end.replace(tzinfo=None)
                    result["series_deleted"] = await self.repository.sweep_series_data_repair(
                        user_id=user_id,
                        sources=sources,
                        indicators=family,
                        window_from=naive_start,
                        window_to=naive_end,
                        repair_task_id=repair_task_id,
                    )
                    result["reaggregated"] = await self._reaggregate(user_id, naive_start, naive_end)

            if summary_records:
                sources = _apple_sources(summary_records)
                present = {r.get("indicator") for r in summary_records if r.get("indicator")}
                family = get_indicators_in_same_categories(
                    present,
                    data_types={HealthDataType.SUMMARY, HealthDataType.MIX},
                )
                if sources and family:
                    result["observations_retracted"] = await observations.retract_unconfirmed(
                        str(user_id),
                        vendors=sources,
                        names=sorted(family),
                        start=start,
                        end=end,
                        task_id=repair_task_id,
                    )

            series_deleted_count, retracted_count = result["series_deleted"], result["observations_retracted"]
            reaggregate_ok = result["reaggregated"]
            logger.info(
                "repair sweep done: user_id=%s task_id=%s series_deleted=%d observations_retracted=%d reaggregated=%s",
                user_id, repair_task_id, series_deleted_count, retracted_count, reaggregate_ok,
            )
            return result

        except Exception as e:
            logger.error("repair sweep failed: user_id=%s task_id=%s error_type=%s", user_id, repair_task_id,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return {"status": "error", "repair_task_id": repair_task_id, "error_type": type(e).__name__}

    async def _reaggregate(self, user_id: str, window_from: datetime, window_to: datetime) -> bool:
        """Recalculate the repaired window so the daily observations derived
        from `series_data` (sleep totals among them) reflect the sweep.

        `window_from`/`window_to` are naive UTC, padded by a day on each side
        and widened to whole days so the sleep window (18:00 to 18:00) is
        covered."""
        start = (window_from - _AGGREGATE_WINDOW_PAD).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end = (window_to + _AGGREGATE_WINDOW_PAD).replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        agg_result = await AggregateIndicatorService().recalculate_date_range(
            start_date=start, end_date=end, user_id=user_id
        )
        status = agg_result.get("status")
        created = agg_result.get("summaries_created", 0)
        # A recalculation that fails inside the aggregator can still report
        # status=success with nothing written; zero summaries is not a refresh.
        if status == "success" and created == 0:
            logger.warning("repair re-aggregation wrote no summaries: user_id=%s", user_id)
        else:
            logger.info("repair re-aggregation: user_id=%s status=%s summaries_created=%s", user_id, status, created)
        return status == "success" and created > 0

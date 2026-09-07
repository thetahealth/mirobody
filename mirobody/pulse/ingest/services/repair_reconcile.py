"""
Data-repair mark-and-sweep reconcile.

Backend slice of the OpenSpec change `user-data-repair` (capability
`apple-health-repair-reconcile`). When iOS uploads a repair batch
(`metaInfo.taskId = "repair-<uuid>"`), the existing upsert/save stamps that taskId on
every re-confirmed row (the "mark"). This module performs the "sweep": after the save,
it removes rows the repair batch did NOT re-confirm WITHIN a caller-supplied window, so
a re-sync of a date range fixes structural corruption (e.g. TH-449 sleep-stage
duplication), not just overwrites values.

Window contract (epoch ms, from metaInfo): the sweep deletes ONLY within
[windowFrom, windowTo]. If either bound is missing/invalid, the sweep is SKIPPED — the
batch is still upserted, but nothing is deleted (keep the original, safe behavior).

Sweep targets:
  - series_data (SERIES/MIX, raw sleep stages): PHYSICAL delete, then re-aggregate so
    derived th_series_data values refresh. series_data.time is naive UTC.
  - th_series_data (directly-upserted SUMMARY/MIX): reversible SOFT delete (deleted=1).
    th_series_data.start_time is naive LOCAL (user timezone), so the window is converted
    to local time for this table.

Safety rails: only for a non-empty repair batch; window must be complete; Apple sources
only (apple.cda excluded); th_series_data removal is reversible + audited; rows of the
CURRENT repair task_id are never touched (multi-batch safe).
"""

import logging

from datetime import datetime, timedelta, UTC
from typing import Any
from zoneinfo import ZoneInfo

from ..repositories.health_data import HealthDataRepository
from ...aggregate.service import AggregateIndicatorService
from ...standardize.indicators_info import (
    HealthDataType,
    get_indicators_in_same_categories,
)

logger = logging.getLogger(__name__)

# Prefix that marks an upload batch as a data-repair re-sync (contract with iOS).
REPAIR_TASK_ID_PREFIX = "repair-"

# Apple sources eligible for repair sweep (already lower-cased upstream).
# apple.cda (clinical documents) is intentionally excluded — different semantics.
APPLE_REPAIR_SOURCES = {"apple_health", "apple_health_watch"}

# Pad applied to the aggregation window so the sleep 18:00-18:00 boundary is fully
# re-derived (sleep aggregation buckets a night under the following calendar day).
_AGGREGATE_WINDOW_PAD = timedelta(days=1)


def detect_repair_task_id(records: list[dict[str, Any]]) -> str | None:
    """Return the `repair-<uuid>` taskId if this batch is a repair batch, else None.

    All rows in a repair batch share the metaInfo taskId, except value-filtered rows
    (`filtered_out_of_range`). We pick the first row whose task_id has the repair prefix.
    """
    for record in records:
        task_id = record.get("task_id")
        if task_id and task_id.startswith(REPAIR_TASK_ID_PREFIX):
            return task_id
    return None


def _apple_sources(records: list[dict[str, Any]]) -> list[str]:
    """Apple sources present in the batch, restricted to the repair-eligible set."""
    present = {r.get("source") for r in records if r.get("source")}
    return sorted(present & APPLE_REPAIR_SOURCES)


def _ms_to_naive_utc(ms: int) -> datetime:
    """epoch ms -> naive UTC datetime (matches series_data.time storage)."""
    return datetime.fromtimestamp(ms / 1000, tz=UTC).replace(tzinfo=None)


def _ms_to_naive_local(ms: int, tz_name: str) -> datetime:
    """epoch ms -> naive local datetime (matches th_series_data.start_time storage)."""
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    try:
        return dt.astimezone(ZoneInfo(tz_name)).replace(tzinfo=None)
    except Exception:
        logger.warning(f"[RepairReconcile] bad timezone {tz_name!r}, using UTC for window")
        return dt.replace(tzinfo=None)


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
        user_timezone: str = "UTC",
    ) -> dict[str, Any]:
        """Run the reconcile if this is a non-empty repair batch with a complete window.

        Args:
            user_id: the uploading user.
            summary_records: prepared th_series_data rows (SUMMARY/MIX).
            series_records: prepared series_data rows (SERIES/MIX).
            window_from_ms / window_to_ms: repair window bounds (epoch ms) from metaInfo.
                BOTH are required; if either is missing/invalid the sweep is skipped.
            user_timezone: user's tz, used to convert the window for th_series_data.

        Returns:
            Dict summarizing what was done. Never raises into the upload path.
        """
        all_records = (summary_records or []) + (series_records or [])

        repair_task_id = detect_repair_task_id(all_records)
        if not repair_task_id:
            return {"status": "not_repair"}

        # Safety rail: never sweep on an empty re-upload (would wipe good data).
        if not all_records:
            logger.info(f"[RepairReconcile] empty repair batch, skip sweep: user={user_id}")
            return {"status": "empty_batch", "repair_task_id": repair_task_id}

        # Window completeness guard: BOTH bounds required and ordered. If incomplete,
        # keep the original behavior (upsert only, NO delete) — explicit per requirement.
        if (
            window_from_ms is None
            or window_to_ms is None
            or window_from_ms > window_to_ms
        ):
            logger.info(
                f"[RepairReconcile] repair batch {repair_task_id} has incomplete window "
                f"(from={window_from_ms}, to={window_to_ms}); skip sweep (upsert only)."
            )
            return {"status": "repair_no_window", "repair_task_id": repair_task_id}

        result: dict[str, Any] = {
            "status": "success",
            "repair_task_id": repair_task_id,
            "window_from_ms": window_from_ms,
            "window_to_ms": window_to_ms,
            "series_deleted": 0,
            "th_series_soft_deleted": 0,
            "reaggregated": False,
        }

        try:
            # --- 1. series_data physical sweep (raw layer, incl. sleep stages) ---
            # series_data.time is naive UTC -> convert the window to naive UTC.
            series_from = _ms_to_naive_utc(window_from_ms)
            series_to = _ms_to_naive_utc(window_to_ms)
            if series_records:
                sources = _apple_sources(series_records)
                present = {r.get("indicator") for r in series_records if r.get("indicator")}
                family = get_indicators_in_same_categories(
                    present,
                    data_types={HealthDataType.SERIES, HealthDataType.MIX},
                )
                if sources and family:
                    result["series_deleted"] = await self.repository.sweep_series_data_repair(
                        user_id=user_id,
                        sources=sources,
                        indicators=family,
                        window_from=series_from,
                        window_to=series_to,
                        repair_task_id=repair_task_id,
                    )
                    # --- 2. re-aggregate the window so th_series_data refreshes ---
                    result["reaggregated"] = await self._reaggregate(
                        user_id, series_from, series_to
                    )

            # --- 3. th_series_data soft sweep (directly-upserted SUMMARY/MIX) ---
            # th_series_data.start_time is naive LOCAL -> convert window to user tz.
            if summary_records:
                sources = _apple_sources(summary_records)
                present = {r.get("indicator") for r in summary_records if r.get("indicator")}
                family = get_indicators_in_same_categories(
                    present,
                    data_types={HealthDataType.SUMMARY, HealthDataType.MIX},
                )
                if sources and family:
                    th_from = _ms_to_naive_local(window_from_ms, user_timezone)
                    th_to = _ms_to_naive_local(window_to_ms, user_timezone)
                    result["th_series_soft_deleted"] = await self.repository.sweep_th_series_data_repair(
                        user_id=user_id,
                        sources=sources,
                        indicators=family,
                        window_from=th_from,
                        window_to=th_to,
                        repair_task_id=repair_task_id,
                    )

            logger.info(f"[RepairReconcile] done: user={user_id}, {result}")
            return result

        except Exception as e:
            logger.error(f"[RepairReconcile] failed: user={user_id}, error={e}", stack_info=True)
            return {"status": "error", "repair_task_id": repair_task_id, "error": str(e)}

    async def _reaggregate(self, user_id: str, window_from: datetime, window_to: datetime) -> bool:
        """Recalculate aggregations for the repaired window so derived th_series_data
        values (e.g. dailyTotalSleepAnalysis*) reflect the cleaned series_data.

        window_from/window_to are naive UTC (series_data.time basis). They are padded by
        one day on each side and floored/ceiled to whole days so the sleep 18:00-18:00
        boundary is fully covered.
        """
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
        # Honest reporting: recalculate_date_range can swallow an internal aggregator
        # error and still return status=success with 0 summaries (e.g. the TH-424
        # AmbiguousParameter bug in _get_tasks_for_user_date_range). Treat "success but
        # nothing produced" as a re-aggregation that did NOT actually refresh derived
        # values, and surface it loudly so the repair isn't reported as fully done.
        ok = status == "success" and created > 0
        if status == "success" and created == 0:
            logger.warning(
                f"[RepairReconcile] re-aggregate produced 0 summaries for user={user_id} "
                f"[{start}, {end}] — derived th_series_data NOT refreshed. The series_data "
                f"sweep still applied; aggregates will refresh on the next successful "
                f"aggregation. NOTE: depends on TH-424 (recalculate-range AmbiguousParameter)."
            )
        else:
            logger.info(
                f"[RepairReconcile] re-aggregate user={user_id} [{start}, {end}]: "
                f"status={status}, summaries_created={created}"
            )
        return ok

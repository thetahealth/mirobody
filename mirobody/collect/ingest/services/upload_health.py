"""`StandardPulseData` to rows, the one path every device source finishes on.

Per record: name the indicator, convert its value to the indicator's standard
unit, and check it against the indicator's plausible range
(`indicator_valid_rules`). Then, by the indicator's kind:

- a summary indicator (a vendor's own daily figure) is written as an
  observation (`observations.ingest_legacy`). A value outside its range is
  not written; the records refused are counted in the log as
  `observations.REJECT_OUT_OF_RANGE`.
- a series indicator (one point of a stream) is upserted into `series_data`,
  which the aggregation reads. A value outside its range is kept there under
  `task_id = "filtered_out_of_range"`, which the aggregation skips: a point
  we refuse to believe is still evidence the device produced it.

A repair batch then sweeps what it did not re-confirm (`repair_reconcile`).
"""

import logging
import time

from datetime import UTC, datetime
from typing import Any

from .repair_reconcile import RepairReconciler
from mirobody.collect import observations
from mirobody.collect.ingest.models.requests import StandardPulseData, StandardPulseRecord
from mirobody.collect.ingest.repositories.health_data import HealthDataRepository, health_data_repository
from mirobody.kernel.ops import is_driver_exception
from mirobody.translate import (
    ValueRangeValidator,
    convert_to_standard,
    get_indicator_by_str,
    is_series_indicator,
    is_summary_indicator,
    normalize_indicator_name,
)

logger = logging.getLogger(__name__)

#: The `task_id` a series point outside its plausible range is stored under.
FILTERED_OUT_OF_RANGE = "filtered_out_of_range"


def _to_standard_unit(indicator: str, value: float | str, unit: str) -> tuple[float | str, str]:
    """`(value, unit)` in the indicator's standard unit; as given when the
    value is not a number, the indicator is not in the catalogue or the unit
    does not convert."""
    std = get_indicator_by_str(indicator)
    if std is None:
        return value, unit
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value, unit
    return convert_to_standard(std, number, unit)


def _instant(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


class StandardHealthService:
    """Writes a `StandardPulseData` batch: summaries as observations, series
    points into `series_data`."""

    def __init__(self, repository: HealthDataRepository | None = None):
        self.repository = repository or health_data_repository

    async def process_standard_data(self, standard_data: StandardPulseData, current_user: str) -> bool:
        """Write one batch for `current_user`, the authenticated person.
        Returns whether both writes succeeded; a refused record does not make
        the batch fail."""
        user_id = current_user
        health_data = standard_data.healthData
        try:
            started = time.monotonic()
            summary_records, series_records = await self._classify_and_prepare_records(health_data, user_id)

            summary_success, summary_count = await self._batch_save_summary_records(summary_records)
            series_success, series_count = await self._batch_save_series_records(series_records)

            meta = standard_data.metaInfo
            await RepairReconciler().reconcile(
                user_id=user_id,
                summary_records=summary_records,
                series_records=series_records,
                window_from_ms=getattr(meta, "windowFrom", None),
                window_to_ms=getattr(meta, "windowTo", None),
            )

            logger.info(
                "pulse batch written: user_id=%s records=%d summaries=%d series=%d elapsed_ms=%d",
                user_id, len(health_data), summary_count, series_count, (time.monotonic() - started) * 1000,
            )
            return summary_success and series_success

        except Exception as e:
            logger.error("pulse batch failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False

    async def _classify_and_prepare_records(
        self, health_data: list[StandardPulseRecord], user_id: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """`(summary_records, series_records)` from the batch's records. A
        summary value outside its range is left out of both and counted."""
        summary_records: list[dict[str, Any]] = []
        series_records: list[dict[str, Any]] = []
        out_of_range = unreadable = 0

        user_timezone = await observations.user_tz(user_id)
        ranges = await observations.value_ranges()

        for record in health_data:
            try:
                common = self._prepare_common_record_data(record, user_id, user_timezone, ranges)
            except (OverflowError, OSError, ValueError):
                # A timestamp no datetime can hold: that record, not the batch.
                unreadable += 1
                continue
            indicator = common["indicator"]
            if is_summary_indicator(indicator):
                if common["task_id"] == FILTERED_OUT_OF_RANGE:
                    out_of_range += 1
                else:
                    summary_records.append(self._prepare_summary_record(common))
            if is_series_indicator(indicator):
                series_records.append(self._prepare_series_record(common))

        logger.info("pulse records classified: user_id=%s summaries=%d series=%d unreadable=%d rejected=%d reason=%s",
                    user_id, len(summary_records), len(series_records), unreadable, out_of_range,
                    observations.REJECT_OUT_OF_RANGE)
        return summary_records, series_records

    def _prepare_common_record_data(
        self, record: StandardPulseRecord, user_id: str, user_timezone: str, ranges: ValueRangeValidator
    ) -> dict[str, Any]:
        indicator = normalize_indicator_name(record.type)
        source = record.source or "UNKNOWN"
        timezone_info = record.timezone
        if timezone_info == "UTC":
            timezone_info = user_timezone
        value, unit = _to_standard_unit(indicator, record.value, record.unit or "")
        task_id = record.task_id or ""
        if not ranges.validate(indicator, value).is_valid:
            task_id = FILTERED_OUT_OF_RANGE
        return {
            "user_id": user_id,
            "indicator": indicator,
            "source": source.lower(),
            "value": str(value),
            "unit": unit,
            "timezone": timezone_info,
            "source_id": record.source_id or "",
            "task_id": task_id,
            "at": _instant(record.timestamp),
            # A record without a span is a point: both ends are its timestamp.
            "start": _instant(record.timestamp if record.startTime is None else record.startTime),
            "end": _instant(record.timestamp if record.endTime is None else record.endTime),
        }

    def _prepare_summary_record(self, common: dict[str, Any]) -> dict[str, Any]:
        """The row `observations.legacy_draft` reads. The times are aware
        instants: the writer places them in `timezone` itself, so no second
        zone parser converts them on the way."""
        return {
            **common,
            "start_time": common["start"],
            "end_time": common["end"],
            "source_table": "",
            "source_table_id": common["source_id"],
        }

    def _prepare_series_record(self, common: dict[str, Any]) -> dict[str, Any]:
        """The `series_data` row: the point's naive UTC instant."""
        return {
            "user_id": common["user_id"],
            "indicator": common["indicator"],
            "value": common["value"],
            "start_time": common["at"].replace(tzinfo=None),
            "source": common["source"],
            "timezone": common["timezone"],
            "source_id": common["source_id"],
            "task_id": common["task_id"],
        }

    async def _batch_save_summary_records(self, summary_records: list[dict[str, Any]]) -> tuple[bool, int]:
        """Write the summaries as observations: `(success, inserted)`."""
        if not summary_records:
            return True, 0
        try:
            # A device sync re-sends the truth: a changed value amends the
            # row it replaces; an equal one is skipped.
            inserted = await observations.ingest_legacy_rows(summary_records, on_conflict=observations.ON_CONFLICT_AMEND)
            return True, inserted
        except Exception as e:
            logger.error("pulse summaries not written: records=%d error_type=%s", len(summary_records),
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return False, 0

    async def _batch_save_series_records(self, series_records: list[dict[str, Any]]) -> tuple[bool, int]:
        """Upsert the points into `series_data`: `(success, records)`."""
        if not series_records:
            return True, 0
        success = await self.repository.save_health_records(series_records)
        return success, len(series_records) if success else 0

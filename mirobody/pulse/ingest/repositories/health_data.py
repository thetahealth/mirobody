"""
Health data repository
"""

import logging

from datetime import datetime
from typing import Any, Dict, List, Set

from ....utils import execute_query



class HealthDataRepository:
    """Health data repository"""

    async def save_health_records(
        self,
        records: list[Dict[str, Any]],
    ) -> bool:
        """
        Save health data records to database, supports single or batch processing, 1000 records per batch

        Args:
            records: Health data record list, each record contains the following fields:
                - indicator: Indicator name
                - value: Value
                - start_time: Start time (used as time field)
                - source: Data source
                - timezone: Timezone

        Returns:
            bool: Whether save succeeded
        """
        try:
            if not records:
                logging.info("No records to save")
                return True

            # Batch insert SQL
            query_batch = """
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
                WHERE series_data.value IS DISTINCT FROM EXCLUDED.value
                   OR series_data.task_id IS DISTINCT FROM EXCLUDED.task_id
            """

            # Process in batches, 1000 records per batch
            batch_size = 10000
            total_records = len(records)
            successfully_processed = 0

            # Collect user ID and time range for subsequent analysis (simplified: only process first user)
            first_user_id = None
            min_time = None
            max_time = None

            for i in range(0, total_records, batch_size):
                batch_records = records[i : i + batch_size]

                # Prepare batch parameters
                batch_params = []
                for record in batch_records:
                    params = {
                        "user_id": str(record["user_id"]),
                        "indicator": record["indicator"],
                        "source": record["source"],
                        "time": record["start_time"],  # Use start_time as time field
                        "value": record["value"],
                        "timezone": record["timezone"],
                        "task_id": record.get("task_id"),  # Add task_id parameter
                        "source_id": record.get("source_id"),  # Add source_id parameter
                    }
                    batch_params.append(params)

                    # Only record first user ID
                    if first_user_id is None:
                        first_user_id = str(record["user_id"])

                    # Directly calculate min/max of time range
                    record_time = record["start_time"]
                    if min_time is None or record_time < min_time:
                        min_time = record_time
                    if max_time is None or record_time > max_time:
                        max_time = record_time

                # Execute batch insert
                await execute_query(query_batch, batch_params)

                successfully_processed += len(batch_records)

                logging.info(f"Successfully processed batch {i // batch_size + 1}: {len(batch_records)} records")

            logging.info(f"Successfully saved {successfully_processed} health records in {(total_records + batch_size - 1) // batch_size} batches")
            return True

        except Exception as e:
            logging.error(f"Failed to save health records: {str(e)}, total_records={len(records)}", stack_info=True)
            return False

    async def sweep_series_data_repair(
        self,
        user_id: str,
        sources: List[str],
        indicators: Set[str],
        window_from: datetime,
        window_to: datetime,
        repair_task_id: str,
    ) -> int:
        """
        Mark-and-sweep for a data-repair batch on series_data (PHYSICAL delete).

        Deletes raw rows in (user_id, source in sources, indicator in indicators,
        time within [window_from, window_to]) whose task_id != repair_task_id, i.e.
        window rows the repair batch did NOT re-confirm (stale / structurally
        corrupt, e.g. TH-449 duplicate sleep-stage rows).

        series_data has no `deleted` column, so this is a hard delete, following the
        existing physical-delete precedent in file_processing_service. The caller
        MUST guarantee the repair batch is non-empty before calling (safety rail).

        Multi-batch guarantee: a single repair (`repair-<uuid>`) may arrive split
        across several upload batches sharing the SAME task_id. The
        `task_id IS DISTINCT FROM :repair_task_id` predicate means rows already written
        by an earlier batch of THIS repair (same task_id) are NEVER deleted — only rows
        the repair has not (re-)confirmed are swept. So batch N's sweep cannot wipe
        batch N-1's rows; multi-batch repairs converge correctly.

        Returns:
            int: number of rows deleted (best-effort; 0 if unavailable).
        """
        if not sources or not indicators:
            logging.info("[RepairReconcile] series_data sweep skipped: empty sources/indicators")
            return 0

        # NOTE: this DELETE intentionally does NOT exclude task_id='filtered_out_of_range'.
        # Unlike a read (which must hide filtered rows), the repair sweep makes the window
        # authoritative: any in-window apple row of the repaired family NOT re-confirmed by
        # this repair — including stale out-of-range rows — is removed. Rows of the current
        # repair are still protected by the task_id predicate below.
        query = """
            DELETE FROM series_data
            WHERE user_id = :user_id
              AND source = ANY(:sources)
              AND indicator = ANY(:indicators)
              AND time >= :window_from
              AND time <= :window_to
              -- keep rows of the CURRENT repair (incl. earlier batches of the same
              -- repair-<uuid>); only sweep rows this repair did not re-confirm
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
        try:
            result = await execute_query(query, params)
            # execute_query returns {"record_count": cur.rowcount} for DML with dict params
            deleted = result.get("record_count", 0) if isinstance(result, dict) else 0
            logging.info(
                f"[RepairReconcile] series_data swept (physical): user={user_id}, "
                f"deleted~={deleted}, indicators={len(indicators)}, sources={sources}, "
                f"window=[{window_from}, {window_to}], keep_task_id={repair_task_id}"
            )
            return deleted or 0
        except Exception as e:
            logging.error(f"[RepairReconcile] series_data sweep failed: {e}", stack_info=True)
            raise

    async def sweep_th_series_data_repair(
        self,
        user_id: str,
        sources: List[str],
        indicators: Set[str],
        window_from: datetime,
        window_to: datetime,
        repair_task_id: str,
    ) -> int:
        """
        Mark-and-sweep for a data-repair batch on th_series_data (SOFT delete).

        For directly-upserted SUMMARY/MIX indicators, soft-deletes (deleted=1) window
        rows in (user_id, source in sources, indicator in indicators,
        start_time within [window_from, window_to]) whose task_id != repair_task_id
        and deleted=0. Reversible + audited (update_time + task_id). Never hard-deletes.

        Derived aggregate rows (e.g. dailyTotalSleepAnalysis*) are NOT in this
        indicator set (the family is built from SUMMARY/MIX indicators present in the
        batch; runtime aggregate names are not StandardIndicator members), so this
        does not touch re-aggregated rows.

        Multi-batch guarantee: like the series_data sweep, the
        `task_id IS DISTINCT FROM :repair_task_id` predicate never soft-deletes rows
        written by an earlier batch of the same `repair-<uuid>`.

        Returns:
            int: number of rows soft-deleted (best-effort; 0 if unavailable).
        """
        if not sources or not indicators:
            return 0

        query = """
            UPDATE th_series_data
            SET deleted = 1, update_time = CURRENT_TIMESTAMP
            WHERE user_id = :user_id
              AND source = ANY(:sources)
              AND indicator = ANY(:indicators)
              AND start_time >= :window_from
              AND start_time <= :window_to
              AND deleted = 0
              -- keep rows of the CURRENT repair (incl. earlier batches of the same
              -- repair-<uuid>); only soft-delete rows this repair did not re-confirm
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
        try:
            result = await execute_query(query, params)
            updated = result.get("record_count", 0) if isinstance(result, dict) else 0
            logging.info(
                f"[RepairReconcile] th_series_data swept (soft): user={user_id}, "
                f"soft_deleted~={updated}, indicators={len(indicators)}, "
                f"window=[{window_from}, {window_to}], keep_task_id={repair_task_id}"
            )
            return updated or 0
        except Exception as e:
            logging.error(f"[RepairReconcile] th_series_data sweep failed: {e}", stack_info=True)
            raise


# Create singleton instance
health_data_repository = HealthDataRepository()

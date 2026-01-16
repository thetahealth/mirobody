"""
Health data repository
"""

import logging

from typing import Any, Dict

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
                INSERT INTO theta_ai.series_data (user_id, indicator, source, time, value, timezone, task_id, source_id, create_time, update_time) 
                VALUES (:user_id, :indicator, :source, :time, :value, :timezone, :task_id, :source_id, now(), now())
                ON CONFLICT (user_id, indicator, source, time) 
                DO UPDATE 
                SET 
                  value = EXCLUDED.value, 
                  timezone = EXCLUDED.timezone,
                  task_id = EXCLUDED.task_id,
                  source_id = EXCLUDED.source_id,
                  update_time = now()
                WHERE theta_ai.series_data.value IS DISTINCT FROM EXCLUDED.value
                   OR theta_ai.series_data.task_id IS DISTINCT FROM EXCLUDED.task_id
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


# Create singleton instance
health_data_repository = HealthDataRepository()

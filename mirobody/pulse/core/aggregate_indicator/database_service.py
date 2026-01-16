"""
Database Service for Aggregate Indicator

Handles database operations for aggregate indicator:
- Batch saving summary data to th_series_data (UPSERT)
"""

import logging

from typing import Any, Dict, List

from ....utils import execute_query


class AggregateDatabaseService:
    """Database service for aggregate indicator operations"""

    def __init__(self):
        """Initialize database service"""

    async def batch_save_summary_data(
            self,
            summary_records: List[Dict[str, Any]],
            batch_size: int = 1000
    ) -> bool:
        """
        Batch save summary records to th_series_data using UPSERT
        
        Uses ON CONFLICT DO UPDATE for idempotency and performance.
        
        Args:
            summary_records: List of summary records to save
            batch_size: Number of records per batch
            
        Returns:
            True if successful, False otherwise
        """
        if not summary_records:
            logging.info("No summary records to save")
            return True

        try:
            # UPSERT query with ON CONFLICT
            query = """
            INSERT INTO theta_ai.th_series_data (
                user_id, indicator, value, start_time, end_time,
                source, task_id, comment, source_table, source_table_id, indicator_id,
                deleted
            ) VALUES (
                :user_id, :indicator, :value, :start_time, :end_time,
                :source, :task_id, :comment, :source_table, :source_table_id, :indicator_id,
                0
            )
            ON CONFLICT (user_id, indicator, start_time, end_time) 
            DO UPDATE SET
                value = EXCLUDED.value,
                comment = EXCLUDED.comment,
                source = EXCLUDED.source,
                task_id = EXCLUDED.task_id,
                update_time = CURRENT_TIMESTAMP
            """

            total_processed = 0

            # Process in batches
            for i in range(0, len(summary_records), batch_size):
                batch = summary_records[i:i + batch_size]

                await execute_query(query=query, params=batch)

                total_processed += len(batch)
                logging.info(
                    f"Saved batch {i // batch_size + 1}: {len(batch)} summary records "
                    f"(UPSERT to th_series_data)"
                )

            logging.info(
                f"Successfully saved {total_processed} summary records to th_series_data"
            )
            return True

        except Exception as e:
            logging.error(f"Error batch saving summary records: {e}")
            return False

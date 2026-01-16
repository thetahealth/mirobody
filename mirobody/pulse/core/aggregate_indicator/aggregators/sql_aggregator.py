"""
SQL Aggregator Implementation

Uses PostgreSQL native aggregation functions for maximum performance.
Handles all complex grouping and batching logic internally.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set

from .....utils import execute_query
from ..models import CalculationTask
from ..rule_generator import get_rules_by_source_indicator


class SQLAggregator:
    """
    SQL-based aggregator (default implementation)
    
    Handles all aggregation logic including:
    1. Trigger data querying and task generation
    2. Batch processing with intelligent grouping
    3. Time range processing with month splitting
    
    Uses PostgreSQL native aggregation functions for maximum performance.
    """

    def __init__(self):
        """
        Initialize SQL aggregator
        """
        self.MAX_TASKS_PER_SQL = 5000
        self.MAX_DAYS_PER_MONTH = 30

        self._supported_methods = {
            'avg', 'max', 'min', 'sum', 'total', 'count',
            'stddev', 'variance', 'last', 'first', 'median', 'p95'
        }

    async def get_trigger_tasks(self, since_timestamp: int) -> List[CalculationTask]:
        """
        Get trigger tasks based on time range
        
        Uses UNION to separate sleep data and normal data processing.
        Returns CalculationTask objects with data_begin (starting time point).
        
        Args:
            since_timestamp: Unix timestamp (seconds) to fetch updates after
            
        Returns:
            List of CalculationTask objects
        """
        try:
            since_time = datetime.fromtimestamp(since_timestamp)

            # Use UNION to separate sleep data and normal data
            query = """
            -- Sleep data query: data_begin_utc is 18:00 in user's local time, converted to UTC (naive)
            -- Example: America/Los_Angeles user, local 2025-10-01 18:00 -> UTC 2025-10-02 04:00
            -- Note: time field is stored as UTC timestamp, we explicitly specify 'UTC' first
            SELECT 
                user_id,
                indicator,
                timezone,
                (((((time AT TIME ZONE 'UTC') AT TIME ZONE timezone) - INTERVAL '18 hours')::date::text || ' 18:00:00')::timestamp AT TIME ZONE timezone) AT TIME ZONE 'UTC' AS data_begin_utc,
                MIN(update_time) as min_update_time,
                MAX(update_time) as max_update_time
            FROM theta_ai.series_data
            WHERE update_time > :since_time
              AND time >= NOW() - INTERVAL '3 months'
              AND LOWER(indicator) LIKE '%sleep%'
            GROUP BY user_id, indicator, timezone, data_begin_utc
            
            UNION ALL
            
            -- Normal data query: data_begin_utc is 00:00 in user's local time, converted to UTC (naive)
            -- Example: America/Los_Angeles user, local 2025-10-01 00:00 -> UTC 2025-10-01 08:00
            -- Note: time field is stored as UTC timestamp, we explicitly specify 'UTC' first
            SELECT 
                user_id,
                indicator,
                timezone,
                ((((time AT TIME ZONE 'UTC') AT TIME ZONE timezone)::date::text || ' 00:00:00')::timestamp AT TIME ZONE timezone) AT TIME ZONE 'UTC' AS data_begin_utc,
                MIN(update_time) as min_update_time,
                MAX(update_time) as max_update_time
            FROM theta_ai.series_data
            WHERE update_time > :since_time
              AND time >= NOW() - INTERVAL '3 months'
              AND LOWER(indicator) NOT LIKE '%sleep%'
            GROUP BY user_id, indicator, timezone, data_begin_utc
            
            ORDER BY min_update_time ASC
            """
            
            params = {
                "since_time": since_time,
            }

            result = await execute_query(query, params)

            logging.info(f"Fetched {len(result)} grouped series_data records since timestamp {since_timestamp} ({since_time.isoformat()})")

            # Convert to CalculationTask objects
            tasks = []

            for record in result:
                user_id = record.get('user_id')
                indicator = record.get('indicator')
                timezone = record.get('timezone')
                data_begin_utc = record.get('data_begin_utc')  # datetime type in UTC

                if not all([user_id, indicator, timezone, data_begin_utc]):
                    continue

                # Find rules for this indicator
                rules = get_rules_by_source_indicator(indicator)
                if not rules:
                    continue

                # Create tasks for each rule
                for rule in rules:
                    task = CalculationTask(
                        user_id=user_id,
                        source_indicator=indicator,
                        target_indicator=rule.target_indicator,
                        aggregation_type=rule.aggregation_type,
                        data_begin_utc=data_begin_utc,
                        timezone=timezone,
                        update_time=record.get('max_update_time')
                    )
                    tasks.append(task)

            return tasks

        except Exception as e:
            logging.error(f"Error fetching trigger tasks: {e}")
            return []

    async def calculate_batch_aggregations(self, tasks: List[CalculationTask]) -> List[Dict[str, Any]]:
        """
        Calculate aggregations for a batch of tasks
        
        This method handles all the complex grouping logic:
        1. Group tasks by data_begin
        2. For each data_begin, decide whether to use single SQL or split by indicator
        3. Execute aggregation and return summary records
        
        Args:
            tasks: List of CalculationTask objects
            
        Returns:
            List of summary record dicts ready for database insertion
        """
        if not tasks:
            return []

        all_summaries = []

        # Step 1: Group tasks by data_begin_utc first
        data_begin_groups = defaultdict(list)
        for task in tasks:
            data_begin_groups[task.data_begin_utc].append(task)

        # Step 2: Process each data_begin_utc group
        for data_begin_utc, data_begin_tasks in data_begin_groups.items():
            logging.info(f"Processing {len(data_begin_tasks)} tasks for data_begin_utc {data_begin_utc}")
            
            # Decide whether to use single SQL or split by indicator
            if len(data_begin_tasks) <= self.MAX_TASKS_PER_SQL:
                # Single SQL query for all users and indicators on this data_begin
                summaries = await self._process_data_begin_aggregations(data_begin_tasks)
                all_summaries.extend(summaries)
            else:
                # Split by indicator to avoid SQL complexity
                summaries = await self._process_data_begin_split_aggregations(data_begin_tasks)
                all_summaries.extend(summaries)

        logging.info(f"Generated {len(all_summaries)} summary records from {len(tasks)} tasks")
        return all_summaries

    async def calculate_time_range_aggregations(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Calculate aggregations for a single user over a time range
        
        This method handles historical data processing for a single user:
        1. If time range > 30 days, split by 30-day chunks and recurse
        2. If time range <= 30 days, execute direct aggregation for the entire range
        
        Args:
            start_date: Start date for aggregation
            end_date: End date for aggregation
            user_id: Single user ID to process
            
        Returns:
            List of summary record dicts ready for database insertion
        """
        # Calculate the number of days in the range
        days_diff = (end_date - start_date).days + 1
        
        if days_diff > self.MAX_DAYS_PER_MONTH:
            # Split into 30-day chunks and recurse
            return await self._process_30_day_chunks(start_date, end_date, user_id)
        else:
            # Process the entire range directly
            return await self._process_single_user_range(start_date, end_date, user_id)

    async def _process_30_day_chunks(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str
    ) -> List[Dict[str, Any]]:
        """Process time range by splitting into 30-day chunks"""
        
        all_summaries = []
        current_start = start_date
        
        while current_start <= end_date:
            # Calculate chunk end (30 days later or end_date, whichever is earlier)
            chunk_end = min(current_start + timedelta(days=self.MAX_DAYS_PER_MONTH - 1), end_date)
            
            logging.info(f"Processing 30-day chunk: {current_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')} for user {user_id}")
            
            # Recursively call calculate_time_range_aggregations for this chunk
            chunk_summaries = await self.calculate_time_range_aggregations(current_start, chunk_end, user_id)
            all_summaries.extend(chunk_summaries)
            
            # Move to next chunk
            current_start = chunk_end + timedelta(days=1)
        
        return all_summaries

    async def _process_single_user_range(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str
    ) -> List[Dict[str, Any]]:
        """Process a single user's data over a time range (≤30 days)"""
        
        # Get all tasks for this user and date range
        tasks = await self._get_tasks_for_user_date_range(start_date, end_date, user_id)
        
        if not tasks:
            logging.debug(f"No tasks found for user {user_id} from {start_date} to {end_date}")
            return []
        
        # Use calculate_batch_aggregations to handle the tasks
        # This will group by data_begin and process accordingly
        return await self.calculate_batch_aggregations(tasks)

    async def _get_tasks_for_user_date_range(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str
    ) -> List[CalculationTask]:
        """Get tasks for a specific user and date range"""
        
        try:
            # Build query to get all data for the user in the date range
            # Use UNION to separate sleep data and normal data
            # Convert to UTC for efficient index usage
            # Note: time field is stored as UTC timestamp, we explicitly specify 'UTC' first
            query = """
            -- Sleep data query: data_begin_utc is 18:00 in user's local time, converted to UTC (naive)
            SELECT 
                user_id,
                indicator,
                timezone,
                (((((time AT TIME ZONE 'UTC') AT TIME ZONE timezone) - INTERVAL '18 hours')::date::text || ' 18:00:00')::timestamp AT TIME ZONE timezone) AT TIME ZONE 'UTC' AS data_begin_utc,
                MIN(update_time) as min_update_time,
                MAX(update_time) as max_update_time
            FROM theta_ai.series_data
            WHERE user_id = :user_id
              AND time >= :start_date
              AND time <= :end_date
              AND LOWER(indicator) LIKE '%sleep%'
            GROUP BY user_id, indicator, timezone, data_begin_utc
            
            UNION ALL
            
            -- Normal data query: data_begin_utc is 00:00 in user's local time, converted to UTC (naive)
            SELECT 
                user_id,
                indicator,
                timezone,
                ((((time AT TIME ZONE 'UTC') AT TIME ZONE timezone)::date::text || ' 00:00:00')::timestamp AT TIME ZONE timezone) AT TIME ZONE 'UTC' AS data_begin_utc,
                MIN(update_time) as min_update_time,
                MAX(update_time) as max_update_time
            FROM theta_ai.series_data
            WHERE user_id = :user_id
              AND time >= :start_date
              AND time <= :end_date
              AND LOWER(indicator) NOT LIKE '%sleep%'
            GROUP BY user_id, indicator, timezone, data_begin_utc
            
            ORDER BY min_update_time ASC
            """
            
            params = {
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date,
            }
            
            result = await execute_query(query, params)
            
            logging.info(f"Fetched {len(result)} grouped series_data records for user {user_id} from {start_date.date()} to {end_date.date()}")
            
            # Convert to CalculationTask objects
            tasks = []
            
            for record in result:
                indicator = record.get('indicator')
                timezone = record.get('timezone')
                data_begin_utc = record.get('data_begin_utc')
                
                if not all([indicator, timezone, data_begin_utc]):
                    continue
                
                # Find rules for this indicator
                rules = get_rules_by_source_indicator(indicator)
                if not rules:
                    continue
                
                # Create tasks for each rule
                for rule in rules:
                    task = CalculationTask(
                        user_id=user_id,
                        source_indicator=indicator,
                        target_indicator=rule.target_indicator,
                        aggregation_type=rule.aggregation_type,
                        data_begin_utc=data_begin_utc,
                        timezone=timezone,
                        update_time=record.get('max_update_time')
                    )
                    tasks.append(task)
            
            return tasks
            
        except Exception as e:
            logging.error(f"Error fetching tasks for user {user_id} date range: {e}")
            return []

    async def _process_data_begin_aggregations(self, data_begin_tasks: List[CalculationTask]) -> List[Dict[str, Any]]:
        """
        Process all tasks for a specific data_begin using single SQL query
        
        Args:
            data_begin_tasks: List of CalculationTask objects for a specific data_begin
            
        Returns:
            List of summary record dicts
        """
        if not data_begin_tasks:
            return []

        # Get data_begin_utc from first task (all tasks have the same data_begin_utc)
        data_begin_utc = data_begin_tasks[0].data_begin_utc

        # Group tasks by user to process each user separately
        user_groups = defaultdict(list)
        for task in data_begin_tasks:
            user_groups[task.user_id].append(task)

        all_summaries = []

        # Process each user group separately
        for user_id, user_tasks in user_groups.items():
            # Get indicators and aggregation methods for this user
            indicators = list(set(task.source_indicator for task in user_tasks))
            aggregation_methods = set(task.aggregation_type for task in user_tasks)

            logging.debug(
                f"Single SQL processing: user {user_id}, {len(indicators)} indicators, "
                f"data_begin_utc: {data_begin_utc}"
            )

            # Single SQL query for this user and indicators
            agg_results = await self._execute_single_sql_aggregation(
                [user_id], indicators, data_begin_utc, aggregation_methods
            )

            if not agg_results:
                logging.debug(f"No aggregation results for user {user_id}, data_begin_utc {data_begin_utc}")
                continue

            # Convert results to summary records for this user
            summaries = self._convert_to_summary_records(agg_results, user_tasks, data_begin_utc)
            all_summaries.extend(summaries)

        return all_summaries

    async def _process_data_begin_split_aggregations(
            self,
            data_begin_tasks: List[CalculationTask]
    ) -> List[Dict[str, Any]]:
        """
        Process all tasks for a specific data_begin by splitting into indicator groups
        
        Args:
            data_begin_tasks: List of CalculationTask objects for a specific data_begin
            
        Returns:
            List of summary record dicts
        """
        if not data_begin_tasks:
            return []

        all_summaries = []

        # Get data_begin_utc from first task (all tasks have the same data_begin_utc)
        data_begin_utc = data_begin_tasks[0].data_begin_utc

        # Group by indicator first, then by user
        indicator_groups = defaultdict(list)
        for task in data_begin_tasks:
            indicator_groups[task.source_indicator].append(task)

        # Process each indicator group
        for indicator, indicator_tasks in indicator_groups.items():
            # Group tasks by user for this indicator
            user_groups = defaultdict(list)
            for task in indicator_tasks:
                user_groups[task.user_id].append(task)

            # Process each user separately for this indicator
            for user_id, user_tasks in user_groups.items():
                aggregation_methods = set(task.aggregation_type for task in user_tasks)

                logging.debug(
                    f"Split SQL processing: user {user_id}, indicator: {indicator}, "
                    f"data_begin_utc: {data_begin_utc}"
                )

                results = await self._execute_single_sql_aggregation(
                    [user_id], [indicator], data_begin_utc, aggregation_methods
                )

                if results:
                    summaries = self._convert_to_summary_records(results, user_tasks, data_begin_utc)
                    all_summaries.extend(summaries)

        return all_summaries

    async def _execute_single_sql_aggregation(
            self,
            user_ids: List[str],
            indicators: List[str],
            data_begin_utc: datetime,
            aggregation_methods: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Execute single SQL query for all users and indicators
        
        Args:
            user_ids: List of user IDs
            indicators: List of indicators
            data_begin_utc: Starting time point in UTC (allows direct comparison with time column)
            aggregation_methods: Set of aggregation methods to apply
            
        Returns:
            List of aggregation results
        """

        # Calculate time boundaries: data_begin_utc to data_begin_utc+24h
        # Both are in UTC, can directly compare with series_data.time (UTC) - uses index!
        # Remove timezone info if present (PostgreSQL may return timezone-aware datetime)
        if data_begin_utc.tzinfo is not None:
            day_start = data_begin_utc.replace(tzinfo=None)
        else:
            day_start = data_begin_utc
        day_end = day_start + timedelta(hours=24)

        # Build user filter - use ANY for both single and multiple users
        user_filter = "user_id = ANY(:user_ids)"
        params = {"user_ids": user_ids}

        # Build aggregation clauses
        agg_clauses = ["user_id", "indicator", "source"]

        if 'avg' in aggregation_methods:
            agg_clauses.append("ROUND(AVG(value::numeric), 2) as avg_value")
        if 'max' in aggregation_methods:
            agg_clauses.append("ROUND(MAX(value::numeric), 2) as max_value")
        if 'min' in aggregation_methods:
            agg_clauses.append("ROUND(MIN(value::numeric), 2) as min_value")
        if 'sum' in aggregation_methods or 'total' in aggregation_methods:
            agg_clauses.append("ROUND(SUM(value::numeric), 2) as sum_value")
        if 'count' in aggregation_methods:
            agg_clauses.append("COUNT(*) as count_value")
        if 'stddev' in aggregation_methods:
            agg_clauses.append("ROUND(STDDEV(value::numeric), 2) as stddev_value")
        if 'variance' in aggregation_methods:
            agg_clauses.append("ROUND(VARIANCE(value::numeric), 2) as variance_value")
        if 'last' in aggregation_methods:
            agg_clauses.append("(ARRAY_AGG(value::numeric ORDER BY time DESC))[1] as last_value")
        if 'first' in aggregation_methods:
            agg_clauses.append("(ARRAY_AGG(value::numeric ORDER BY time ASC))[1] as first_value")
        if 'median' in aggregation_methods:
            agg_clauses.append("PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value::numeric) as median_value")
        if 'p95' in aggregation_methods:
            agg_clauses.append("PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value::numeric) as p95_value")

        # Build query
        # IMPORTANT: Direct UTC time comparison - allows index usage!
        # day_start/day_end are UTC times, can directly compare with time column (UTC)
        query = f"""
        SELECT {', '.join(agg_clauses)}
        FROM theta_ai.series_data
        WHERE {user_filter}
          AND indicator = ANY(:indicators)
          AND time >= :day_start
          AND time < :day_end
        GROUP BY user_id, indicator, source
        ORDER BY user_id, indicator, source
        """

        # Execute query
        query_params = {
            "indicators": indicators,
            "day_start": day_start,
            "day_end": day_end,
        }
        query_params.update(params)

        return await execute_query(query, query_params)


    def _convert_to_summary_records(
            self,
            agg_results: List[Dict[str, Any]],
            tasks: List[CalculationTask],
            data_begin_utc: datetime
    ) -> List[Dict[str, Any]]:
        """
        Convert aggregation results to summary records for th_series_data
        
        Note: agg_results now include 'source' field due to GROUP BY indicator, source
        We append source as suffix to indicator name: dailyTotalSteps.apple_health
        
        Args:
            agg_results: Results from aggregator (one dict per indicator AND source)
            tasks: List of CalculationTask objects
            data_begin_utc: Starting time point in UTC
            
        Returns:
            List of summary record dicts ready for database insertion
        """
        summaries = []

        # Get timezone from first task (all tasks have the same timezone and data_begin_utc)
        if not tasks:
            return []
        
        timezone = tasks[0].timezone
        
        # Calculate time boundaries: data_begin_utc to data_begin_utc+24h (in UTC)
        # Remove timezone info if present (PostgreSQL may return timezone-aware datetime)
        if data_begin_utc.tzinfo is not None:
            day_start_utc = data_begin_utc.replace(tzinfo=None)
        else:
            day_start_utc = data_begin_utc
        day_end_utc = day_start_utc + timedelta(hours=24)
        
        # Convert UTC times to user's local time for th_series_data storage
        # This ensures start_time/end_time represent the user's local date (00:00-23:59:59)
        from zoneinfo import ZoneInfo
        import pytz
        
        # Use pytz for timezone conversion (more compatible)
        try:
            tz = pytz.timezone(timezone)
            # Convert UTC to local time
            day_start_local = pytz.utc.localize(day_start_utc).astimezone(tz).replace(tzinfo=None)
            day_end_local = pytz.utc.localize(day_end_utc).astimezone(tz).replace(tzinfo=None)
            
            # For th_series_data, we want to store the local date's 00:00-23:59:59
            # Regardless of whether it's sleep data (18:00-18:00) or normal data (00:00-24:00)
            local_date = day_start_local.date()
            day_start = datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0)
            day_end = datetime(local_date.year, local_date.month, local_date.day, 23, 59, 59)
            
            logging.debug(
                f"Timezone conversion: UTC [{day_start_utc} - {day_end_utc}] "
                f"-> Local {timezone} [{day_start_local} - {day_end_local}] "
                f"-> Stored [{day_start} - {day_end}]"
            )
        except Exception as e:
            logging.error(f"Error converting timezone {timezone}: {e}")
            # Fallback: use UTC times
            day_start = day_start_utc
            day_end = day_end_utc

        # Create mapping: (user_id, indicator, source) -> aggregation results
        results_by_user_indicator_source = {}
        for row in agg_results:
            user_id = row.get('user_id')
            indicator = row.get('indicator')
            source = row.get('source', '')
            key = (user_id, indicator, source)
            results_by_user_indicator_source[key] = row

        # Generate summary record for each task and each source
        for task in tasks:
            # Find results for this specific user and source indicator
            matching_results = [
                (source, indicator_results) for (user_id, indicator, source), indicator_results in results_by_user_indicator_source.items()
                if user_id == task.user_id and indicator == task.source_indicator
            ]

            if not matching_results:
                continue

            # Generate one summary per source
            for source, indicator_results in matching_results:
                # Map aggregation method to value key
                method = task.aggregation_type
                if method == 'total':
                    method = 'sum'  # total and sum map to sum_value

                value_key = f"{method}_value"
                value = indicator_results.get(value_key)

                if value is None:
                    continue

                # Build indicator name with source suffix
                # Format: dailyTotalSteps.apple_health
                source_suffix = source.replace('vital.', '') if source else 'unknown'
                target_indicator_with_source = f"{task.target_indicator}.{source_suffix}"

                summary = {
                    "user_id": task.user_id,
                    "indicator": target_indicator_with_source,
                    "value": str(value),
                    "start_time": day_start,
                    "end_time": day_end,
                    "source": source,
                    "task_id": "aggregate_indicator",
                    "comment": f"Aggregated from {task.source_indicator} using {task.aggregation_type}",
                    "source_table": "series_data",
                    "source_table_id": task.source_indicator,
                    "indicator_id": ""
                }

                summaries.append(summary)

        return summaries

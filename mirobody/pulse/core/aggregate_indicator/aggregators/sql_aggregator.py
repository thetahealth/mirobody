"""
SQL Aggregator Implementation

Uses PostgreSQL native aggregation functions for maximum performance.
Handles all complex grouping and batching logic internally.
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from .....utils import execute_query
from ..models import CalculationTask
from ..rule_generator import get_rules_by_source_indicator
from ...indicators_info import StandardIndicator, HealthDataType
from ...fhir_mapping import get_fhir_id


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
            'stddev', 'variance', 'last', 'first', 'median', 'p95',
            'time_of_max', 'time_of_min',
            'pct_below_70', 'pct_above_180', 'tir_70_180',
            'pct_above_140', 'tir_70_140',
            'hypo_event_count', 'hypo_event_times', 'hypo_event_details',
            'gmi_14d',
            # W2.7: complex derived methods
            'sleep_onset_latency', 'morning_hr_jump', 'nighttime_resting_hr',
        }
        # Regex patterns for parameterized threshold methods
        self._threshold_patterns = {
            'pct_below': re.compile(r'^pct_below_(\d+(?:\.\d+)?)$'),
            'pct_above': re.compile(r'^pct_above_(\d+(?:\.\d+)?)$'),
            'tir': re.compile(r'^tir_(\d+(?:\.\d+)?)_(\d+(?:\.\d+)?)$'),
        }
        # Methods that require separate CGM event detection query (not standard GROUP BY)
        self._cgm_event_methods = {'hypo_event_count', 'hypo_event_times', 'hypo_event_details'}
        # Methods that require 14-day rolling window on raw series_data
        self._cgm_gmi_methods = {'gmi_14d'}
        # W2.7: methods that require custom time-series queries on series_data
        self._custom_derived_methods = {
            'sleep_onset_latency', 'morning_hr_jump', 'nighttime_resting_hr',
        }

        # Build source indicator name → standard_unit lookup from StandardIndicator enum
        self._indicator_units = {}
        for ind_enum in StandardIndicator:
            info = ind_enum.value
            if info.name:
                self._indicator_units[info.name] = info.standard_unit

        # Methods whose output unit differs from source indicator's unit
        # Note: time units use "HHMM" instead of "HH:MM" to avoid colon being
        # misinterpreted as a key-value separator when parsing the comment field.
        self._method_unit_overrides = {
            'time_of_max': 'HHMM',
            'time_of_min': 'HHMM',
            'hypo_event_count': 'count',
            'hypo_event_times': 'HHMM[]',
            'hypo_event_details': 'JSON',
            'gmi_14d': '%',
            'count': 'count',
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
            FROM series_data
            WHERE update_time > :since_time
              AND time >= NOW() - INTERVAL '3 months'
              AND LOWER(indicator) LIKE '%sleep%'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
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
            FROM series_data
            WHERE update_time > :since_time
              AND time >= NOW() - INTERVAL '3 months'
              AND LOWER(indicator) NOT LIKE '%sleep%'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
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
            FROM series_data
            WHERE user_id = :user_id
              AND time >= :start_date
              AND time <= :end_date
              AND LOWER(indicator) LIKE '%sleep%'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
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
            FROM series_data
            WHERE user_id = :user_id
              AND time >= :start_date
              AND time <= :end_date
              AND LOWER(indicator) NOT LIKE '%sleep%'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
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
            # Split tasks into standard aggregation, CGM event detection, GMI, and custom derived
            _special = self._cgm_event_methods | self._cgm_gmi_methods | self._custom_derived_methods
            standard_tasks = [t for t in user_tasks if t.aggregation_type not in _special]
            event_tasks = [t for t in user_tasks if t.aggregation_type in self._cgm_event_methods]
            gmi_tasks = [t for t in user_tasks if t.aggregation_type in self._cgm_gmi_methods]
            custom_derived_tasks = [t for t in user_tasks if t.aggregation_type in self._custom_derived_methods]

            # Standard GROUP BY aggregation path
            if standard_tasks:
                indicators = list(set(task.source_indicator for task in standard_tasks))
                aggregation_methods = set(task.aggregation_type for task in standard_tasks)

                logging.debug(
                    f"Single SQL processing: user {user_id}, {len(indicators)} indicators, "
                    f"data_begin_utc: {data_begin_utc}"
                )

                agg_results = await self._execute_single_sql_aggregation(
                    [user_id], indicators, data_begin_utc, aggregation_methods
                )

                if agg_results:
                    summaries = self._convert_to_summary_records(agg_results, standard_tasks, data_begin_utc)
                    all_summaries.extend(summaries)
                else:
                    logging.debug(f"No aggregation results for user {user_id}, data_begin_utc {data_begin_utc}")

            # CGM event detection path (hypo_event_count, hypo_event_times)
            if event_tasks:
                event_summaries = await self._process_cgm_event_tasks(
                    user_id, event_tasks, data_begin_utc
                )
                all_summaries.extend(event_summaries)

            # GMI 14-day rolling window path
            if gmi_tasks:
                gmi_summaries = await self._process_gmi_tasks(
                    user_id, gmi_tasks, data_begin_utc
                )
                all_summaries.extend(gmi_summaries)

            # W2.7: Custom derived methods (sleep_onset_latency, morning_hr_jump, nighttime_resting_hr)
            if custom_derived_tasks:
                derived_summaries = await self._process_custom_derived_tasks(
                    user_id, custom_derived_tasks, data_begin_utc
                )
                all_summaries.extend(derived_summaries)

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
                # Split into standard and event tasks
                standard_tasks = [t for t in user_tasks if t.aggregation_type not in self._cgm_event_methods]
                event_tasks = [t for t in user_tasks if t.aggregation_type in self._cgm_event_methods]

                if standard_tasks:
                    aggregation_methods = set(task.aggregation_type for task in standard_tasks)
                    logging.debug(
                        f"Split SQL processing: user {user_id}, indicator: {indicator}, "
                        f"data_begin_utc: {data_begin_utc}"
                    )
                    results = await self._execute_single_sql_aggregation(
                        [user_id], [indicator], data_begin_utc, aggregation_methods
                    )
                    if results:
                        summaries = self._convert_to_summary_records(results, standard_tasks, data_begin_utc)
                        all_summaries.extend(summaries)

                if event_tasks:
                    event_summaries = await self._process_cgm_event_tasks(
                        user_id, event_tasks, data_begin_utc
                    )
                    all_summaries.extend(event_summaries)

        return all_summaries

    @staticmethod
    def _get_fhir_id(indicator: str):
        """Lookup fhir_id from FhirMapping cache. Returns None if not available."""
        return get_fhir_id(indicator)

    def _get_aggregation_unit(self, source_indicator: str, aggregation_type: str) -> str:
        """
        Determine the output unit for an aggregation result.

        Rules:
        - Methods with fixed output units (time_of_max → HH:MM, count → count) use overrides
        - Threshold methods (pct_below_X, pct_above_X, tir_X_Y) output "%"
        - All other methods (avg, max, min, sum, last, etc.) inherit source indicator's unit
        - Falls back to source indicator's unit if unknown

        Args:
            source_indicator: Source indicator name (e.g., "bloodGlucoses")
            aggregation_type: Aggregation method (e.g., "avg", "pct_below_70")

        Returns:
            Unit string (e.g., "mg/dL", "%", "HH:MM")
        """
        # Check fixed overrides first
        if aggregation_type in self._method_unit_overrides:
            return self._method_unit_overrides[aggregation_type]

        # Check threshold methods (pct_below_X, pct_above_X, tir_X_Y) → "%"
        if self._parse_threshold_method(aggregation_type) is not None:
            return '%'

        # Default: inherit source indicator's standard_unit
        return self._indicator_units.get(source_indicator, '')

    def _parse_threshold_method(self, method: str) -> Optional[Tuple[str, str, str]]:
        """
        Parse parameterized threshold method name into SQL clause.

        Supports:
            pct_below_{threshold}  → percentage of readings < threshold
            pct_above_{threshold}  → percentage of readings > threshold
            tir_{lower}_{upper}    → percentage of readings BETWEEN lower AND upper

        Args:
            method: Method name string (e.g., 'pct_below_70', 'tir_70_180')

        Returns:
            Tuple of (pattern_name, value_alias, sql_clause) or None if not a threshold method
        """
        m = self._threshold_patterns['pct_below'].match(method)
        if m:
            threshold = m.group(1)
            alias = f"{method}_value"
            clause = (
                f"ROUND(COUNT(CASE WHEN value::numeric < {threshold} THEN 1 END) "
                f"* 100.0 / NULLIF(COUNT(*), 0), 2) as {alias}"
            )
            return ('pct_below', alias, clause)

        m = self._threshold_patterns['pct_above'].match(method)
        if m:
            threshold = m.group(1)
            alias = f"{method}_value"
            clause = (
                f"ROUND(COUNT(CASE WHEN value::numeric > {threshold} THEN 1 END) "
                f"* 100.0 / NULLIF(COUNT(*), 0), 2) as {alias}"
            )
            return ('pct_above', alias, clause)

        m = self._threshold_patterns['tir'].match(method)
        if m:
            lower, upper = m.group(1), m.group(2)
            alias = f"{method}_value"
            clause = (
                f"ROUND(COUNT(CASE WHEN value::numeric BETWEEN {lower} AND {upper} THEN 1 END) "
                f"* 100.0 / NULLIF(COUNT(*), 0), 2) as {alias}"
            )
            return ('tir', alias, clause)

        return None

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

        # CGM blood glucose specific aggregations
        if 'time_of_max' in aggregation_methods:
            agg_clauses.append(
                "TO_CHAR((ARRAY_AGG(time ORDER BY value::numeric DESC))[1], 'HH24:MI') "
                "as time_of_max_value"
            )
        if 'time_of_min' in aggregation_methods:
            agg_clauses.append(
                "TO_CHAR((ARRAY_AGG(time ORDER BY value::numeric ASC))[1], 'HH24:MI') "
                "as time_of_min_value"
            )
        # Parameterized threshold aggregations: pct_below_X, pct_above_X, tir_X_Y
        for method in aggregation_methods:
            parsed = self._parse_threshold_method(method)
            if parsed is None:
                continue
            pattern, alias, clause = parsed
            agg_clauses.append(clause)

        # Build query
        # IMPORTANT: Direct UTC time comparison - allows index usage!
        # day_start/day_end are UTC times, can directly compare with time column (UTC)
        query = f"""
        SELECT {', '.join(agg_clauses)}
        FROM series_data
        WHERE {user_filter}
          AND indicator = ANY(:indicators)
          AND time >= :day_start
          AND time < :day_end
          AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
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

    async def _process_cgm_event_tasks(
            self,
            user_id: str,
            event_tasks: List[CalculationTask],
            data_begin_utc: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Process CGM event detection tasks (hypo_event_count, hypo_event_times)

        These require window functions (gap-and-islands algorithm) and cannot be
        computed in the standard GROUP BY query.
        """
        if not event_tasks:
            return []

        # All event tasks share the same source_indicator and data_begin_utc
        source_indicator = event_tasks[0].source_indicator

        # Calculate time boundaries
        if data_begin_utc.tzinfo is not None:
            day_start = data_begin_utc.replace(tzinfo=None)
        else:
            day_start = data_begin_utc
        day_end = day_start + timedelta(hours=24)

        # Get all sources for this user/indicator/day (to match per-source aggregation pattern)
        source_query = """
        SELECT DISTINCT source
        FROM series_data
        WHERE user_id = :user_id
          AND indicator = :indicator
          AND time >= :day_start
          AND time < :day_end
          AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        """
        sources = await execute_query(source_query, {
            "user_id": user_id,
            "indicator": source_indicator,
            "day_start": day_start,
            "day_end": day_end,
        })

        if not sources:
            return []

        all_summaries = []
        for source_row in sources:
            source = source_row.get('source', '')
            event_result = await self._execute_cgm_event_aggregation(
                user_id, source_indicator, source, day_start, day_end
            )

            if not event_result:
                continue

            # Build fake agg_results row to reuse _convert_to_summary_records
            fake_row = {
                'user_id': user_id,
                'indicator': source_indicator,
                'source': source,
                'hypo_event_count_value': event_result.get('event_count', 0),
                'hypo_event_times_value': event_result.get('event_times', '[]'),
                'hypo_event_details_value': event_result.get('event_details', '[]'),
            }
            summaries = self._convert_to_summary_records([fake_row], event_tasks, data_begin_utc)
            all_summaries.extend(summaries)

        return all_summaries

    async def _execute_cgm_event_aggregation(
            self,
            user_id: str,
            indicator: str,
            source: str,
            day_start: datetime,
            day_end: datetime,
    ) -> Dict[str, Any]:
        """
        Detect hypoglycemic episodes using gap-and-islands algorithm.

        Algorithm:
        1. Order all readings by time
        2. Cumulative sum assigns group IDs:
           - glucose >= 70 → increment (normal reading, ends low episode)
           - gap > 60 min → increment (data discontinuity, splits episodes)
           - glucose < 70 and gap <= 60 min → same group (continues low episode)
        3. Filter groups where ALL readings are <70 and duration >= 15 min
        4. Count = event count, collect start times = event times
        """
        query = """
        WITH ordered AS (
            SELECT
                time,
                value::numeric as glucose,
                LAG(time) OVER (ORDER BY time) as prev_time
            FROM series_data
            WHERE user_id = :user_id
              AND indicator = :indicator
              AND source = :source
              AND time >= :day_start
              AND time < :day_end
              AND value::numeric >= 20  -- Filter out sensor errors (0, near-zero values)
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        ),
        readings AS (
            SELECT
                time,
                glucose,
                SUM(CASE
                    WHEN glucose >= 70 THEN 1
                    WHEN prev_time IS NULL THEN 0
                    WHEN EXTRACT(EPOCH FROM time - prev_time) / 60.0 > 60 THEN 1
                    ELSE 0
                END) OVER (ORDER BY time) as grp
            FROM ordered
        ),
        events AS (
            SELECT
                grp,
                MIN(time) as event_start,
                MAX(time) as event_end,
                EXTRACT(EPOCH FROM MAX(time) - MIN(time)) / 60.0 as duration_min
            FROM readings
            WHERE glucose < 70
            GROUP BY grp
            HAVING EXTRACT(EPOCH FROM MAX(time) - MIN(time)) / 60.0 >= 15
        )
        SELECT
            COALESCE(COUNT(*), 0) as event_count,
            COALESCE(
                json_agg(TO_CHAR(event_start, 'HH24:MI') ORDER BY event_start),
                '[]'::json
            ) as event_times,
            COALESCE(
                json_agg(
                    json_build_object(
                        'start', TO_CHAR(event_start, 'HH24:MI'),
                        'end', TO_CHAR(event_end, 'HH24:MI'),
                        'duration_min', ROUND(duration_min::numeric, 0)
                    ) ORDER BY event_start
                ),
                '[]'::json
            ) as event_details
        FROM events
        """

        result = await execute_query(query, {
            "user_id": user_id,
            "indicator": indicator,
            "source": source,
            "day_start": day_start,
            "day_end": day_end,
        })

        if result and len(result) > 0:
            row = result[0]
            event_count = row.get('event_count', 0)
            event_times = row.get('event_times', '[]')
            event_details = row.get('event_details', '[]')
            # json_agg returns None when no rows, handle it
            if event_times is None:
                event_times = '[]'
            if event_details is None:
                event_details = '[]'
            return {"event_count": event_count, "event_times": event_times, "event_details": event_details}

        return {"event_count": 0, "event_times": "[]", "event_details": "[]"}

    # ------------------------------------------------------------------
    # GMI (Glucose Management Indicator) — 14-day rolling window
    # ------------------------------------------------------------------

    async def _process_gmi_tasks(
            self,
            user_id: str,
            gmi_tasks: List[CalculationTask],
            data_begin_utc: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Process GMI tasks using 14-day rolling window on raw series_data.

        GMI formula: GMI(%) = 3.31 + 0.02392 × mean_glucose(mg/dL)
        Requires ≥12 days of data with ≥70% sensor active time.
        """
        if not gmi_tasks:
            return []

        all_summaries = []
        timezone = gmi_tasks[0].timezone
        source_indicator = gmi_tasks[0].source_indicator

        # Get distinct sources for this user/indicator
        source_query = """
        SELECT DISTINCT source
        FROM series_data
        WHERE user_id = :user_id
          AND indicator = :indicator
          AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        """
        sources = await execute_query(source_query, {
            "user_id": user_id,
            "indicator": source_indicator,
        })

        if not sources:
            return all_summaries

        for source_row in sources:
            source = source_row.get('source', '')
            gmi_result = await self._execute_gmi_aggregation(
                user_id, source_indicator, source, data_begin_utc
            )

            if gmi_result is None:
                continue

            # Build fake_row for _convert_to_summary_records
            fake_row = {
                'user_id': user_id,
                'indicator': source_indicator,
                'source': source,
                'gmi_14d_value': gmi_result,
            }
            summaries = self._convert_to_summary_records([fake_row], gmi_tasks, data_begin_utc)
            all_summaries.extend(summaries)

        return all_summaries

    async def _execute_gmi_aggregation(
            self,
            user_id: str,
            indicator: str,
            source: str,
            data_begin_utc: datetime,
    ) -> Optional[float]:
        """
        Calculate GMI from raw CGM data over a 14-day window ending at data_begin_utc.

        Algorithm (per 2018 international consensus, Bergenstal et al.):
        1. Fetch all raw readings from series_data over past 14 days
        2. Filter sensor errors (value < 20)
        3. De-duplicate: remove readings < 30s apart (Libre scan overlap)
        4. Estimate sampling interval: round inter-reading gaps to nearest minute, take mode
        5. Compute coverage: (deduplicated_points × sampling_interval) / (14 × 24 × 60)
        6. If coverage < 70%, skip with warning
        7. GMI(%) = 3.31 + 0.02392 × mean_glucose(mg/dL)

        Returns:
            GMI value (float) or None if insufficient data/coverage
        """
        from collections import Counter

        day_end = data_begin_utc + timedelta(hours=24)
        day_start_14d = data_begin_utc - timedelta(days=13)

        # Fetch all raw readings sorted by time
        query = """
        SELECT time, value::numeric as glucose
        FROM series_data
        WHERE user_id = :user_id
          AND indicator = :indicator
          AND source = :source
          AND time >= :day_start_14d
          AND time < :day_end
          AND value::numeric >= 20
          AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        ORDER BY time
        """

        rows = await execute_query(query, {
            "user_id": user_id,
            "indicator": indicator,
            "source": source,
            "day_start_14d": day_start_14d,
            "day_end": day_end,
        })

        if not rows or len(rows) < 2:
            return None

        # Step 1: De-duplicate readings < 30s apart (Libre scan overlap)
        deduped = [rows[0]]
        for row in rows[1:]:
            gap = (row['time'] - deduped[-1]['time']).total_seconds()
            if gap >= 30:
                deduped.append(row)

        if len(deduped) < 2:
            return None

        # Step 2: Compute inter-reading intervals, round to nearest minute
        intervals_min = []
        for i in range(1, len(deduped)):
            diff_sec = (deduped[i]['time'] - deduped[i - 1]['time']).total_seconds()
            rounded_min = round(diff_sec / 60)
            if 0 < rounded_min <= 60:  # Ignore gaps > 60 min (sensor off / data break)
                intervals_min.append(rounded_min)

        if not intervals_min:
            return None

        # Step 3: Sampling interval = mode of rounded intervals
        interval_counts = Counter(intervals_min)
        sampling_interval_min = interval_counts.most_common(1)[0][0]

        # Step 4: Coverage = deduped_points × sampling_interval / total_window
        total_window_min = 14 * 24 * 60  # 20160 minutes
        estimated_active_min = len(deduped) * sampling_interval_min
        coverage = estimated_active_min / total_window_min

        if coverage < 0.70:
            logging.warning(
                f"[GMI] Insufficient sensor coverage for user {user_id}, source {source}: "
                f"{coverage:.1%} (need ≥70%). Points={len(deduped)}, "
                f"sampling_interval={sampling_interval_min}min, "
                f"active≈{estimated_active_min / 60:.0f}h / {total_window_min / 60:.0f}h. "
                f"Skipping GMI calculation."
            )
            return None

        # Step 5: Mean glucose from ALL de-duplicated readings
        mean_glucose = sum(float(r['glucose']) for r in deduped) / len(deduped)

        # Step 6: GMI formula
        # mg/dL: GMI(%) = 3.31 + 0.02392 × mean_glucose
        # mmol/L: GMI(%) = 3.31 + 0.431 × mean_glucose
        # series_data stores blood glucose in mg/dL (StandardIndicator.BLOOD_GLUCOSE.standard_unit)
        gmi = round(3.31 + 0.02392 * mean_glucose, 2)

        logging.info(
            f"[GMI] user={user_id}, source={source}: "
            f"mean={mean_glucose:.1f} mg/dL, GMI={gmi}%, "
            f"points={len(deduped)}, interval={sampling_interval_min}min, "
            f"coverage={coverage:.1%}"
        )

        return gmi

    # ------------------------------------------------------------------
    # W2.7: Custom derived methods (TH-177)
    # ------------------------------------------------------------------

    async def _process_custom_derived_tasks(
            self,
            user_id: str,
            tasks: List[CalculationTask],
            data_begin_utc: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Process W2.7 custom derived tasks that need raw series_data queries.

        Each method gets its own handler function, similar to CGM event detection.
        """
        if not tasks:
            return []

        if data_begin_utc.tzinfo is not None:
            day_start = data_begin_utc.replace(tzinfo=None)
        else:
            day_start = data_begin_utc
        day_end = day_start + timedelta(hours=24)

        summaries = []

        for task in tasks:
            try:
                if task.aggregation_type == 'sleep_onset_latency':
                    result = await self._execute_sleep_onset_latency(
                        user_id, day_start, day_end
                    )
                elif task.aggregation_type == 'morning_hr_jump':
                    result = await self._execute_morning_hr_jump(
                        user_id, day_start, day_end
                    )
                elif task.aggregation_type == 'nighttime_resting_hr':
                    result = await self._execute_nighttime_resting_hr(
                        user_id, day_start, day_end
                    )
                else:
                    continue

                if result is None:
                    continue

                # Build summary record
                value_str = str(round(result['value'], 2))
                source = result.get('source', 'derived')
                target_indicator = task.target_indicator
                unit = result.get('unit', '')
                comment = result.get('comment', '')

                fhir_id = get_fhir_id(target_indicator)
                fhir_info = f", fhir_id={fhir_id}" if fhir_id else ""

                summaries.append({
                    "user_id": user_id,
                    "indicator": f"{target_indicator}.{source}" if source != 'derived' else target_indicator,
                    "value": value_str,
                    "start_time": day_start,
                    "end_time": day_end,
                    "source": source,
                    "task_id": "aggregate_indicator",
                    "comment": f"Source/{source}/Unit/{unit}/Aggregated/{task.aggregation_type}{fhir_info}",
                    "source_table": "",
                    "source_table_id": "",
                    "indicator_id": "",
                    "fhir_id": fhir_id,
                })

            except Exception as e:
                logging.warning(
                    f"[SQLAggregator] Custom derived {task.aggregation_type} failed "
                    f"for user={user_id}, day={day_start}: {e}"
                )

        return summaries

    async def _execute_sleep_onset_latency(
            self, user_id: str, day_start: datetime, day_end: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Sleep Onset Latency = time from InBed start to first Asleep start.

        Uses 18:00-18:00 sleep window. Queries both sleepAnalysis_InBed and
        sleepAnalysis_Asleep(Total)/Asleep(Core) from series_data.
        Returns latency in minutes.
        """
        # Sleep window: previous day 18:00 to current day 18:00
        sleep_start = day_start - timedelta(hours=6)  # 18:00 previous day
        sleep_end = day_start + timedelta(hours=18)    # 18:00 current day

        query = """
        WITH inbed AS (
            SELECT MIN(time) as inbed_time, source
            FROM series_data
            WHERE user_id = :user_id
              AND indicator = 'sleepAnalysis_InBed'
              AND time >= :sleep_start
              AND time < :sleep_end
              AND value ~ '^[0-9]+\\.?[0-9]*$'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
            GROUP BY source
            LIMIT 1
        ),
        first_asleep AS (
            SELECT MIN(time) as asleep_time
            FROM series_data
            WHERE user_id = :user_id
              AND indicator IN ('sleepAnalysis_Asleep(Total)', 'sleepAnalysis_Asleep(Core)')
              AND time >= :sleep_start
              AND time < :sleep_end
              AND value ~ '^[0-9]+\\.?[0-9]*$'
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        )
        SELECT
            inbed.source,
            EXTRACT(EPOCH FROM (first_asleep.asleep_time - inbed.inbed_time)) / 60.0 as latency_minutes
        FROM inbed, first_asleep
        WHERE first_asleep.asleep_time > inbed.inbed_time
          AND EXTRACT(EPOCH FROM (first_asleep.asleep_time - inbed.inbed_time)) / 60.0 BETWEEN 0 AND 120
        """

        rows = await execute_query(query, {
            "user_id": user_id,
            "sleep_start": sleep_start,
            "sleep_end": sleep_end,
        })

        if rows and len(rows) > 0:
            row = rows[0]
            latency = float(row['latency_minutes'])
            return {
                "value": latency,
                "source": row.get('source', 'apple_health'),
                "unit": "min",
                "comment": f"Sleep onset latency: {latency:.1f} min",
            }

        return None

    async def _execute_morning_hr_jump(
            self, user_id: str, day_start: datetime, day_end: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Morning HR Jump = max HR in morning window (6:00-8:00 local) - avg HR in sleep window (1:00-5:00 local).

        Uses user's timezone from series_data. Returns jump in bpm.
        """
        query = """
        WITH user_tz AS (
            SELECT COALESCE(
                (SELECT timezone FROM series_data
                 WHERE user_id = :user_id AND indicator = 'heartRates'
                   AND time >= :day_start AND time < :day_end
                 LIMIT 1),
                'UTC'
            ) as tz
        ),
        hr_data AS (
            SELECT
                value::numeric as hr,
                EXTRACT(HOUR FROM (time AT TIME ZONE 'UTC') AT TIME ZONE (SELECT tz FROM user_tz)) as local_hour,
                source
            FROM series_data
            WHERE user_id = :user_id
              AND indicator = 'heartRates'
              AND time >= :day_start
              AND time < :day_end
              AND value ~ '^[0-9]+\\.?[0-9]*$'
              AND value::numeric BETWEEN 30 AND 220
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
        ),
        sleep_hr AS (
            SELECT AVG(hr) as avg_hr
            FROM hr_data
            WHERE local_hour BETWEEN 1 AND 4
        ),
        morning_hr AS (
            SELECT MAX(hr) as max_hr, MIN(source) as source
            FROM hr_data
            WHERE local_hour BETWEEN 6 AND 8
        )
        SELECT
            morning_hr.max_hr - sleep_hr.avg_hr as hr_jump,
            morning_hr.source,
            morning_hr.max_hr as morning_max,
            sleep_hr.avg_hr as sleep_avg
        FROM morning_hr, sleep_hr
        WHERE morning_hr.max_hr IS NOT NULL
          AND sleep_hr.avg_hr IS NOT NULL
          AND morning_hr.max_hr > sleep_hr.avg_hr
        """

        rows = await execute_query(query, {
            "user_id": user_id,
            "day_start": day_start,
            "day_end": day_end,
        })

        if rows and len(rows) > 0:
            row = rows[0]
            jump = float(row['hr_jump'])
            if jump > 0:
                return {
                    "value": jump,
                    "source": row.get('source', 'apple_health'),
                    "unit": "bpm",
                    "comment": f"Morning HR jump: morning_max={row['morning_max']:.0f}, sleep_avg={row['sleep_avg']:.0f}",
                }

        return None

    async def _execute_nighttime_resting_hr(
            self, user_id: str, day_start: datetime, day_end: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Nighttime Resting HR = 10th percentile of HR during 1:00-5:00 local time.

        More accurate than device-reported resting HR. Uses PERCENTILE_CONT.
        """
        query = """
        WITH user_tz AS (
            SELECT COALESCE(
                (SELECT timezone FROM series_data
                 WHERE user_id = :user_id AND indicator = 'heartRates'
                   AND time >= :day_start AND time < :day_end
                 LIMIT 1),
                'UTC'
            ) as tz
        ),
        night_hr AS (
            SELECT value::numeric as hr, MIN(source) OVER () as source
            FROM series_data
            WHERE user_id = :user_id
              AND indicator = 'heartRates'
              AND time >= :day_start
              AND time < :day_end
              AND value ~ '^[0-9]+\\.?[0-9]*$'
              AND value::numeric BETWEEN 30 AND 220
              AND (task_id IS NULL OR task_id != 'filtered_out_of_range')
              AND EXTRACT(HOUR FROM (time AT TIME ZONE 'UTC') AT TIME ZONE (SELECT tz FROM user_tz)) BETWEEN 1 AND 4
        )
        SELECT
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY hr) as p10_hr,
            AVG(hr) as avg_hr,
            MIN(hr) as min_hr,
            COUNT(*) as data_points,
            MIN(source) as source
        FROM night_hr
        HAVING COUNT(*) >= 5
        """

        rows = await execute_query(query, {
            "user_id": user_id,
            "day_start": day_start,
            "day_end": day_end,
        })

        if rows and len(rows) > 0:
            row = rows[0]
            p10 = row.get('p10_hr')
            if p10 is not None:
                return {
                    "value": float(p10),
                    "source": row.get('source', 'apple_health'),
                    "unit": "bpm",
                    "comment": f"Nighttime resting HR (p10): {float(p10):.0f}, avg={row['avg_hr']:.0f}, n={row['data_points']}",
                }

        return None

    @staticmethod
    def _convert_utc_time_to_local(utc_time_str: str, timezone_str: str, reference_date_utc: datetime) -> str:
        """
        Convert a UTC time string (HH:MM) to local time string.

        Args:
            utc_time_str: Time string in HH:MM format (UTC)
            timezone_str: User's timezone (e.g., 'Asia/Shanghai')
            reference_date_utc: The UTC date for context (to handle DST correctly)

        Returns:
            Time string in HH:MM format (local time)
        """
        try:
            import pytz
            hours, minutes = map(int, utc_time_str.split(':'))
            utc_dt = reference_date_utc.replace(hour=hours, minute=minutes, second=0, microsecond=0)
            tz = pytz.timezone(timezone_str)
            local_dt = pytz.utc.localize(utc_dt).astimezone(tz)
            return local_dt.strftime('%H:%M')
        except Exception as e:
            logging.warning(f"Failed to convert UTC time {utc_time_str} to {timezone_str}: {e}")
            return utc_time_str

    @staticmethod
    def _convert_utc_times_json_to_local(utc_times_json, timezone_str: str, reference_date_utc: datetime) -> str:
        """
        Convert a JSON array of hypo event objects with UTC times to local times.

        Supports both legacy format (["HH:MM", ...]) and new format
        ([{"start": "HH:MM", "end": "HH:MM", "duration_min": N}, ...]).

        Args:
            utc_times_json: JSON array (string or Python list)
            timezone_str: User's timezone
            reference_date_utc: Reference UTC date

        Returns:
            JSON string with times converted to local timezone
        """
        import json
        try:
            if isinstance(utc_times_json, str):
                times = json.loads(utc_times_json)
            elif isinstance(utc_times_json, list):
                times = utc_times_json
            else:
                return '[]'

            if not times:
                return '[]'

            # New format: list of event objects with start/end/duration_min
            if isinstance(times[0], dict):
                local_events = []
                for event in times:
                    local_event = {
                        'start': SQLAggregator._convert_utc_time_to_local(
                            event['start'], timezone_str, reference_date_utc),
                        'end': SQLAggregator._convert_utc_time_to_local(
                            event['end'], timezone_str, reference_date_utc),
                        'duration_min': event.get('duration_min', 0),
                    }
                    local_events.append(local_event)
                return json.dumps(local_events)

            # Legacy format: list of HH:MM strings
            local_times = [
                SQLAggregator._convert_utc_time_to_local(t, timezone_str, reference_date_utc)
                for t in times
            ]
            return json.dumps(local_times)
        except Exception as e:
            logging.warning(f"Failed to convert UTC times JSON to local: {e}")
            return '[]'

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

                # Convert UTC time strings to local timezone for time-based methods
                if method in ('time_of_max', 'time_of_min') and isinstance(value, str):
                    value = self._convert_utc_time_to_local(value, timezone, day_start_utc)
                elif method in ('hypo_event_times', 'hypo_event_details'):
                    value = self._convert_utc_times_json_to_local(value, timezone, day_start_utc)

                # Build indicator name with source suffix
                # Format: dailyTotalSteps.apple_health
                source_suffix = source.replace('vital.', '') if source else 'unknown'
                target_indicator_with_source = f"{task.target_indicator}.{source_suffix}"

                # Lookup fhir_id from cache (read-only, no DB call)
                fhir_id = self._get_fhir_id(target_indicator_with_source)

                summary = {
                    "user_id": task.user_id,
                    "indicator": target_indicator_with_source,
                    "value": str(value),
                    "start_time": day_start,
                    "end_time": day_end,
                    "source": source,
                    "task_id": "aggregate_indicator",
                    "comment": f"Source: {source}, Unit: {self._get_aggregation_unit(task.source_indicator, task.aggregation_type)}, Timezone: {timezone}, Aggregated: {task.source_indicator} via {task.aggregation_type}",
                    "source_table": "series_data",
                    "source_table_id": task.source_indicator,
                    "indicator_id": "",
                    "fhir_id": fhir_id,
                }

                summaries.append(summary)

        return summaries

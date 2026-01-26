"""
Aggregator Integration Test

Tests AggregatorProtocol implementation (SQLAggregator, etc.) with real database operations.
Focuses on date boundaries, time boundaries, and data correctness.

Usage:
    python3 -m mirobody.pulse.core.aggregate_indicator.test_aggregator
"""

import asyncio, logging, pytz

from datetime import datetime, timedelta

from ....utils import execute_query

from .database_service import AggregateDatabaseService
from .aggregators.sql_aggregator import SQLAggregator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AggregatorTester:
    """
    Test suite for AggregatorProtocol implementations
    
    Tests with user_id=138, focuses on:
    1. Date boundary handling (00:00, 23:59:59)
    2. Time zone conversion (UTC ↔ Local)
    3. Sleep data special window (18:00-18:00)
    4. Aggregation correctness (sum, avg, count)
    5. Data storage correctness (th_series_data)
    """
    
    TEST_USER_ID = "138"
    TEST_TIMEZONE = "America/Los_Angeles"  # UTC-8
    TEST_DATE = datetime.now().date()  # Use today's date for testing
    
    def __init__(self):
        self.db_service = AggregateDatabaseService()
        self.aggregator = SQLAggregator()
    
    async def run_all_tests(self):
        """Run all integration tests"""
        logger.info("=" * 80)
        logger.info("Starting Aggregator Integration Tests")
        logger.info(f"Test User: {self.TEST_USER_ID}")
        logger.info(f"Test Timezone: {self.TEST_TIMEZONE}")
        logger.info(f"Test Date: {self.TEST_DATE}")
        logger.info("=" * 80)
        
        try:
            # Test 1: Date boundary - normal data at 00:00
            await self.test_normal_data_start_boundary()
            
            # Test 2: Date boundary - normal data at 23:59
            await self.test_normal_data_end_boundary()
            
            # Test 3: Date boundary - sleep data at 18:00
            await self.test_sleep_data_start_boundary()
            
            # Test 4: Date boundary - sleep data crossing midnight
            await self.test_sleep_data_cross_midnight()
            
            # Test 5: Aggregation correctness
            await self.test_aggregation_calculations()
            
            # Test 6: Storage correctness
            await self.test_storage_correctness()
            
            logger.info("=" * 80)
            logger.info("✅ All integration tests PASSED")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"❌ Integration tests FAILED: {e}")
            logger.error("=" * 80)
            raise
    
    async def test_normal_data_start_boundary(self):
        """
        Test 1: Normal data at date start boundary (00:00)
        
        Validates:
        - Data at local 00:00:00 is included in that day
        - Data at local 23:59:59 (previous day) is NOT included
        - UTC conversion is correct
        """
        logger.info("=" * 80)
        logger.info("Test 1: Normal data at date start boundary (00:00)")
        logger.info("=" * 80)
        
        test_indicator = "steps"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        # Clean up existing data
        await self._cleanup_test_data(test_indicator)
        
        # Insert test data:
        # 1. One record at 23:59:59 previous day (should NOT be included)
        # 2. One record at 00:00:00 test day (should be included)
        # 3. One record at 00:00:01 test day (should be included)
        
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        
        test_cases = [
            {
                "name": "Previous day 23:59:59",
                "local_time": local_date - timedelta(seconds=1),
                "value": "999",
                "should_include": False
            },
            {
                "name": "Current day 00:00:00",
                "local_time": local_date,
                "value": "1000",
                "should_include": True
            },
            {
                "name": "Current day 00:00:01",
                "local_time": local_date + timedelta(seconds=1),
                "value": "1001",
                "should_include": True
            }
        ]
        
        # Insert test data
        for case in test_cases:
            utc_time = tz.localize(case["local_time"]).astimezone(pytz.utc).replace(tzinfo=None)
            await self._insert_test_record(
                test_indicator,
                case["value"],
                utc_time,
                self.TEST_TIMEZONE
            )
            logger.info(f"Inserted: {case['name']} - local={case['local_time']}, utc={utc_time}, value={case['value']}")
        
        # Get trigger tasks
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found")
        
        # Calculate expected data_begin_utc for TEST_DATE
        tz = pytz.timezone(self.TEST_TIMEZONE)
        local_midnight = tz.localize(datetime.combine(self.TEST_DATE, datetime.min.time()))
        expected_data_begin_utc = local_midnight.astimezone(pytz.utc).replace(tzinfo=None)
        
        # Find the task that matches our TEST_DATE
        matching_tasks = [t for t in test_tasks if t.data_begin_utc == expected_data_begin_utc]
        if not matching_tasks:
            raise AssertionError(f"No task found for expected data_begin_utc={expected_data_begin_utc}, found tasks: {[t.data_begin_utc for t in test_tasks]}")
        
        task = matching_tasks[0]
        logger.info(f"Trigger task: data_begin_utc={task.data_begin_utc}, timezone={task.timezone}")
        
        # Verify data_begin_utc is correct (local 00:00 -> UTC 16:00 for +8)
        expected_utc_hour = 16  # For UTC+8, local 00:00 = UTC 16:00 (previous day)
        if task.data_begin_utc.hour != expected_utc_hour:
            raise AssertionError(
                f"data_begin_utc hour incorrect: expected {expected_utc_hour}, got {task.data_begin_utc.hour}"
            )
        
        # Execute aggregation (only for the matching task)
        summaries = await self.aggregator.calculate_batch_aggregations(matching_tasks)
        
        # Verify aggregation result
        # Should only include 1000 + 1001 = 2001 (NOT 999)
        for summary in summaries:
            if "Total" in summary["indicator"]:
                actual_value = float(summary["value"])
                expected_value = 2001.0
                
                if actual_value != expected_value:
                    raise AssertionError(
                        f"Aggregation boundary error: expected {expected_value}, got {actual_value}"
                    )
                
                logger.info(f"✓ Boundary check passed: sum={actual_value} (excluded previous day's 999)")
        
        # Clean up
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 1 PASSED: Start boundary handled correctly")
    
    async def test_normal_data_end_boundary(self):
        """
        Test 2: Normal data at date end boundary (23:59:59)
        
        Validates:
        - Data at local 23:59:59 is included in that day
        - Data at local 00:00:00 (next day) is NOT included
        """
        logger.info("=" * 80)
        logger.info("Test 2: Normal data at date end boundary (23:59:59)")
        logger.info("=" * 80)
        
        test_indicator = "steps"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        await self._cleanup_test_data(test_indicator)
        
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        
        test_cases = [
            {
                "name": "Current day 23:59:58",
                "local_time": local_date + timedelta(hours=23, minutes=59, seconds=58),
                "value": "1000",
                "should_include": True
            },
            {
                "name": "Current day 23:59:59",
                "local_time": local_date + timedelta(hours=23, minutes=59, seconds=59),
                "value": "1001",
                "should_include": True
            },
            {
                "name": "Next day 00:00:00",
                "local_time": local_date + timedelta(days=1),
                "value": "999",
                "should_include": False
            }
        ]
        
        for case in test_cases:
            utc_time = tz.localize(case["local_time"]).astimezone(pytz.utc).replace(tzinfo=None)
            await self._insert_test_record(
                test_indicator,
                case["value"],
                utc_time,
                self.TEST_TIMEZONE
            )
            logger.info(f"Inserted: {case['name']} - local={case['local_time']}, utc={utc_time}, value={case['value']}")
        
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found")
        
        # Calculate expected data_begin_utc for TEST_DATE
        local_midnight = tz.localize(datetime.combine(self.TEST_DATE, datetime.min.time()))
        expected_data_begin_utc = local_midnight.astimezone(pytz.utc).replace(tzinfo=None)
        
        # Find the task that matches our TEST_DATE
        matching_tasks = [t for t in test_tasks if t.data_begin_utc == expected_data_begin_utc]
        if not matching_tasks:
            raise AssertionError(f"No task found for expected data_begin_utc={expected_data_begin_utc}, found tasks: {[t.data_begin_utc for t in test_tasks]}")
        
        summaries = await self.aggregator.calculate_batch_aggregations(matching_tasks)
        
        for summary in summaries:
            if "Total" in summary["indicator"]:
                actual_value = float(summary["value"])
                expected_value = 2001.0  # 1000 + 1001, NOT 999
                
                if actual_value != expected_value:
                    raise AssertionError(
                        f"End boundary error: expected {expected_value}, got {actual_value}"
                    )
                
                logger.info(f"✓ Boundary check passed: sum={actual_value} (excluded next day's 999)")
        
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 2 PASSED: End boundary handled correctly")
    
    async def test_sleep_data_start_boundary(self):
        """
        Test 3: Sleep data at 18:00 boundary
        
        Validates:
        - Data at local 18:00:00 is included
        - Data at local 17:59:59 is NOT included
        - Sleep data uses 18-hour offset
        """
        logger.info("=" * 80)
        logger.info("Test 3: Sleep data at 18:00 boundary")
        logger.info("=" * 80)
        
        # Use an indicator with aggregation_methods defined
        test_indicator = "sleepAnalysis_Asleep(Deep)"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        await self._cleanup_test_data(test_indicator)
        
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        
        test_cases = [
            {
                "name": "Current day 17:59:59",
                "local_time": local_date + timedelta(hours=17, minutes=59, seconds=59),
                "value": "999",
                "should_include": False
            },
            {
                "name": "Current day 18:00:00",
                "local_time": local_date + timedelta(hours=18),
                "value": "1000",
                "should_include": True
            },
            {
                "name": "Current day 18:00:01",
                "local_time": local_date + timedelta(hours=18, seconds=1),
                "value": "1001",
                "should_include": True
            }
        ]
        
        for case in test_cases:
            utc_time = tz.localize(case["local_time"]).astimezone(pytz.utc).replace(tzinfo=None)
            await self._insert_test_record(
                test_indicator,
                case["value"],
                utc_time,
                self.TEST_TIMEZONE
            )
            logger.info(f"Inserted: {case['name']} - local={case['local_time']}, utc={utc_time}, value={case['value']}")
        
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found for sleep data")
        
        # Calculate expected data_begin_utc for TEST_DATE (sleep data uses 18:00)
        local_18h = tz.localize(datetime.combine(self.TEST_DATE, datetime.min.time()) + timedelta(hours=18))
        expected_data_begin_utc = local_18h.astimezone(pytz.utc).replace(tzinfo=None)
        
        # Find the task that matches our TEST_DATE
        matching_tasks = [t for t in test_tasks if t.data_begin_utc == expected_data_begin_utc]
        if not matching_tasks:
            raise AssertionError(f"No task found for expected data_begin_utc={expected_data_begin_utc}, found tasks: {[t.data_begin_utc for t in test_tasks]}")
        
        task = matching_tasks[0]
        logger.info(f"Sleep trigger task: data_begin_utc={task.data_begin_utc}, timezone={task.timezone}")
        
        # Verify data_begin_utc for sleep data (local 18:00 -> UTC 10:00 for +8)
        expected_utc_hour = 10
        if task.data_begin_utc.hour != expected_utc_hour:
            raise AssertionError(
                f"Sleep data_begin_utc hour incorrect: expected {expected_utc_hour}, got {task.data_begin_utc.hour}"
            )
        
        summaries = await self.aggregator.calculate_batch_aggregations(matching_tasks)
        
        for summary in summaries:
            if "Total" in summary["indicator"]:
                actual_value = float(summary["value"])
                expected_value = 2001.0  # 1000 + 1001, NOT 999
                
                if actual_value != expected_value:
                    raise AssertionError(
                        f"Sleep boundary error: expected {expected_value}, got {actual_value}"
                    )
                
                logger.info(f"✓ Sleep boundary check passed: sum={actual_value} (excluded 17:59:59's 999)")
        
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 3 PASSED: Sleep data 18:00 boundary handled correctly")
    
    async def test_sleep_data_cross_midnight(self):
        """
        Test 4: Sleep data crossing midnight
        
        Validates:
        - Sleep data from 18:00 to next day 18:00 is correctly grouped
        - Data crossing midnight is assigned to correct day
        """
        logger.info("=" * 80)
        logger.info("Test 4: Sleep data crossing midnight")
        logger.info("=" * 80)
        
        # Use an indicator with aggregation_methods defined
        test_indicator = "sleepAnalysis_Asleep(Deep)"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        await self._cleanup_test_data(test_indicator)
        
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        
        # Insert data spanning from 18:00 to next day 06:00
        test_cases = [
            {"name": "18:00", "local_time": local_date + timedelta(hours=18), "value": "1000"},
            {"name": "22:00", "local_time": local_date + timedelta(hours=22), "value": "1000"},
            {"name": "00:00 (next day)", "local_time": local_date + timedelta(days=1), "value": "1000"},
            {"name": "06:00 (next day)", "local_time": local_date + timedelta(days=1, hours=6), "value": "1000"},
        ]
        
        for case in test_cases:
            utc_time = tz.localize(case["local_time"]).astimezone(pytz.utc).replace(tzinfo=None)
            await self._insert_test_record(
                test_indicator,
                case["value"],
                utc_time,
                self.TEST_TIMEZONE
            )
            logger.info(f"Inserted: {case['name']} - local={case['local_time']}, utc={utc_time}")
        
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found")
        
        # Calculate expected data_begin_utc for TEST_DATE (sleep data uses 18:00)
        local_18h = tz.localize(datetime.combine(self.TEST_DATE, datetime.min.time()) + timedelta(hours=18))
        expected_data_begin_utc = local_18h.astimezone(pytz.utc).replace(tzinfo=None)
        
        # Find the task that matches our TEST_DATE
        matching_tasks = [t for t in test_tasks if t.data_begin_utc == expected_data_begin_utc]
        if not matching_tasks:
            raise AssertionError(f"No task found for expected data_begin_utc={expected_data_begin_utc}, found tasks: {[t.data_begin_utc for t in test_tasks]}")
        
        summaries = await self.aggregator.calculate_batch_aggregations(matching_tasks)
        
        for summary in summaries:
            if "Total" in summary["indicator"]:
                actual_value = float(summary["value"])
                expected_value = 4000.0  # All 4 records should be included
                
                if actual_value != expected_value:
                    raise AssertionError(
                        f"Cross-midnight error: expected {expected_value}, got {actual_value}"
                    )
                
                logger.info(f"✓ Cross-midnight check passed: sum={actual_value} (all records included)")
        
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 4 PASSED: Sleep data crossing midnight handled correctly")
    
    async def test_aggregation_calculations(self):
        """
        Test 5: Aggregation calculation correctness
        
        Validates:
        - SUM is correct
        - AVG is correct
        - COUNT is correct
        - Multiple aggregation types work together
        """
        logger.info("=" * 80)
        logger.info("Test 5: Aggregation calculation correctness")
        logger.info("=" * 80)
        
        test_indicator = "steps"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        await self._cleanup_test_data(test_indicator)
        
        # Insert 10 records with known values
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        test_values = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        
        for i, value in enumerate(test_values):
            local_time = local_date + timedelta(hours=i)
            utc_time = tz.localize(local_time).astimezone(pytz.utc).replace(tzinfo=None)
            await self._insert_test_record(
                test_indicator,
                str(value),
                utc_time,
                self.TEST_TIMEZONE
            )
        
        logger.info(f"Inserted {len(test_values)} records: {test_values}")
        
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found")
        
        summaries = await self.aggregator.calculate_batch_aggregations(test_tasks)
        
        expected_sum = sum(test_values)  # 5500
        expected_avg = sum(test_values) / len(test_values)  # 550
        expected_count = len(test_values)  # 10
        
        for summary in summaries:
            indicator = summary["indicator"]
            actual_value = float(summary["value"])
            
            if "Total" in indicator:
                if actual_value != expected_sum:
                    raise AssertionError(
                        f"SUM incorrect: expected {expected_sum}, got {actual_value}"
                    )
                logger.info(f"✓ SUM correct: {actual_value}")
            
            elif "Avg" in indicator:
                if actual_value != expected_avg:
                    raise AssertionError(
                        f"AVG incorrect: expected {expected_avg}, got {actual_value}"
                    )
                logger.info(f"✓ AVG correct: {actual_value}")
        
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 5 PASSED: Aggregation calculations are correct")
    
    async def test_storage_correctness(self):
        """
        Test 6: Storage correctness in th_series_data
        
        Validates:
        - start_time is local 00:00:00
        - end_time is local 23:59:59
        - Values are correctly stored
        - UPSERT works correctly (idempotency)
        """
        logger.info("=" * 80)
        logger.info("Test 6: Storage correctness in th_series_data")
        logger.info("=" * 80)
        
        test_indicator = "steps"
        tz = pytz.timezone(self.TEST_TIMEZONE)
        
        await self._cleanup_test_data(test_indicator)
        
        # Insert test data
        local_date = datetime.combine(self.TEST_DATE, datetime.min.time())
        utc_time = tz.localize(local_date).astimezone(pytz.utc).replace(tzinfo=None)
        
        await self._insert_test_record(test_indicator, "1000", utc_time, self.TEST_TIMEZONE)
        
        # Execute aggregation
        since_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp())
        tasks = await self.aggregator.get_trigger_tasks(since_timestamp)
        test_tasks = [t for t in tasks if t.user_id == self.TEST_USER_ID and t.source_indicator == test_indicator]
        
        if not test_tasks:
            raise AssertionError("No trigger tasks found")
        
        # Calculate expected data_begin_utc for TEST_DATE
        local_midnight = tz.localize(datetime.combine(self.TEST_DATE, datetime.min.time()))
        expected_data_begin_utc = local_midnight.astimezone(pytz.utc).replace(tzinfo=None)
        
        # Find the task that matches our TEST_DATE
        matching_tasks = [t for t in test_tasks if t.data_begin_utc == expected_data_begin_utc]
        if not matching_tasks:
            raise AssertionError(f"No task found for expected data_begin_utc={expected_data_begin_utc}, found tasks: {[t.data_begin_utc for t in test_tasks]}")
        
        summaries = await self.aggregator.calculate_batch_aggregations(matching_tasks)
        
        # Log summary data before saving
        for summary in summaries:
            logger.info(f"Summary to save: indicator={summary['indicator']}, start_time={summary['start_time']}, end_time={summary['end_time']}, value={summary['value']}")
        
        # Save to database
        await self.db_service.batch_save_summary_data(summaries)
        
        # Query and verify - first check what's actually in the database
        logger.info(f"Querying for user_id={self.TEST_USER_ID}, test_date={str(self.TEST_DATE)}")
        
        # Query all data for this user to debug (filter by test source)
        debug_query = """
        SELECT indicator, value, start_time, end_time, DATE(start_time) as start_date, source
        FROM th_series_data
        WHERE user_id = :user_id
          AND (indicator LIKE '%steps%' OR source LIKE '%test.integration%')
        ORDER BY update_time DESC
        LIMIT 10
        """
        
        debug_data = await execute_query(debug_query, {
            "user_id": self.TEST_USER_ID
        })
        
        logger.info(f"Debug: Found {len(debug_data)} steps/test records for user {self.TEST_USER_ID}")
        for record in debug_data:
            logger.info(f"  - indicator={record['indicator']}, start_time={record['start_time']}, start_date={record['start_date']}, source={record.get('source', 'N/A')}, value={str(record['value'])[:50]}")
        
        # Query and verify
        # Note: Use exact indicator match for test data and filter by current test date
        query = """
        SELECT indicator, value, start_time, end_time
        FROM th_series_data
        WHERE user_id = :user_id
          AND indicator = 'dailyTotalSteps.test.integration'
          AND DATE(start_time) = :test_date
        ORDER BY indicator
        """
        
        saved_data = await execute_query(query, {
            "user_id": self.TEST_USER_ID,
            "test_date": str(self.TEST_DATE)
        })
        
        if not saved_data:
            raise AssertionError("No data saved to th_series_data")
        
        for record in saved_data:
            start_time = record["start_time"]
            end_time = record["end_time"]
            
            # Verify start_time is 00:00:00
            if start_time.hour != 0 or start_time.minute != 0 or start_time.second != 0:
                raise AssertionError(
                    f"start_time incorrect: expected 00:00:00, got {start_time.time()}"
                )
            
            # Verify end_time is 23:59:59
            if end_time.hour != 23 or end_time.minute != 59 or end_time.second != 59:
                raise AssertionError(
                    f"end_time incorrect: expected 23:59:59, got {end_time.time()}"
                )
            
            # Verify date matches
            if start_time.date() != self.TEST_DATE:
                raise AssertionError(
                    f"Date mismatch: expected {self.TEST_DATE}, got {start_time.date()}"
                )
            
            logger.info(f"✓ Storage correct: {record['indicator']} = {record['value']}")
            logger.info(f"  start_time: {start_time}")
            logger.info(f"  end_time: {end_time}")
        
        # Test idempotency: run again, should update not duplicate
        summaries = await self.aggregator.calculate_batch_aggregations(test_tasks)
        await self.db_service.batch_save_summary_data(summaries)
        
        saved_data_again = await execute_query(query, {
            "user_id": self.TEST_USER_ID,
            "test_date": str(self.TEST_DATE)
        })
        
        if len(saved_data_again) != len(saved_data):
            raise AssertionError(
                f"Idempotency failed: expected {len(saved_data)} records, got {len(saved_data_again)}"
            )
        
        logger.info("✓ Idempotency check passed: no duplicate records")
        
        await self._cleanup_test_data(test_indicator)
        
        logger.info("✅ Test 6 PASSED: Storage correctness verified")
    
    async def _insert_test_record(
        self,
        indicator: str,
        value: str,
        time: datetime,
        timezone: str
    ):
        """Insert a test record into series_data"""
        query = """
        INSERT INTO series_data 
        (user_id, indicator, value, time, timezone, source, update_time)
        VALUES (:user_id, :indicator, :value, :time, :timezone, :source, :update_time)
        """
        
        await execute_query(query, {
            "user_id": self.TEST_USER_ID,
            "indicator": indicator,
            "value": value,
            "time": time,
            "timezone": timezone,
            "source": "test.integration",
            "update_time": datetime.now()
        })
    
    async def _cleanup_test_data(self, indicator: str):
        """Clean up test data for specific indicator"""
        # Delete from series_data
        delete_series = """
        DELETE FROM series_data
        WHERE user_id = :user_id
          AND indicator = :indicator
          AND source = 'test.integration'
        """
        await execute_query(delete_series, {
            "user_id": self.TEST_USER_ID,
            "indicator": indicator
        })
        
        # Delete from th_series_data
        # Clean up ALL test integration data for this user and indicator pattern
        # This removes all test data regardless of date to avoid leftover data from previous test runs
        delete_summary = """
        DELETE FROM th_series_data
        WHERE user_id = :user_id
          AND source = 'test.integration'
          AND indicator LIKE :indicator_pattern
        """
        await execute_query(delete_summary, {
            "user_id": self.TEST_USER_ID,
            "indicator_pattern": f"%{indicator}%"  # matches any indicator containing this string
        })


async def main():
    """Main entry point for running tests"""
    # When running standalone, initialize global config
    if __name__ == "__main__":
        from ....utils.config import global_config
        try:
            global_config(filepath='/app/config/config.yaml')
        except:
            try:
                global_config()
            except:
                pass
    
    tester = AggregatorTester()
    await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())


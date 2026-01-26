# Aggregator Integration Test Documentation

## Overview

`test_aggregator.py` is a comprehensive integration test suite for verifying the correctness of `AggregatorProtocol` implementations (such as `SQLAggregator`).

## Test Characteristics

### Uses Real Data
- **Test user**: user_id = 138
- **Test timezone**: Asia/Shanghai (UTC+8)
- **Test date**: Current date (dynamic)

### Test Coverage

#### Test 1: Normal Data Start Boundary (00:00)
**Purpose**: Verify date start boundary handling

**Test Cases**:
```
Previous day 23:59:59 → value=999  (should be excluded)
Current day  00:00:00 → value=1000 (should be included)
Current day  00:00:01 → value=1001 (should be included)
```

**Verification Points**:
- `data_begin_utc` calculated correctly (local 00:00 → UTC 16:00)
- Aggregation result = 2001 (excludes 999)
- Boundary data correctly grouped

#### Test 2: Normal Data End Boundary (23:59:59)
**Purpose**: Verify date end boundary handling

**Test Cases**:
```
Current day  23:59:58 → value=1000 (should be included)
Current day  23:59:59 → value=1001 (should be included)
Next day     00:00:00 → value=999  (should be excluded)
```

**Verification Points**:
- Aggregation result = 2001 (excludes 999)
- Next day's data doesn't mix into current day

#### Test 3: Sleep Data 18:00 Boundary
**Purpose**: Verify sleep data special window

**Test Cases**:
```
Current day 17:59:59 → value=999  (should be excluded)
Current day 18:00:00 → value=1000 (should be included)
Current day 18:00:01 → value=1001 (should be included)
```

**Verification Points**:
- `data_begin_utc` calculated correctly (local 18:00 → UTC 10:00)
- 18-hour offset correctly applied
- Aggregation result = 2001 (excludes 999)

#### Test 4: Sleep Data Across Midnight
**Purpose**: Verify sleep data cross-date handling

**Test Cases**:
```
Current day 18:00 → value=1000
Current day 22:00 → value=1000
Next day    00:00 → value=1000
Next day    06:00 → value=1000
```

**Verification Points**:
- All data grouped into same day
- Aggregation result = 4000 (all included)
- Cross-midnight data correctly grouped

#### Test 5: Aggregation Calculation Correctness
**Purpose**: Verify aggregation function calculations

**Test Cases**:
```
Insert 10 records: [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
```

**Verification Points**:
- SUM = 5500
- AVG = 550
- COUNT = 10
- Multiple aggregation types correct simultaneously

#### Test 6: Storage Correctness
**Purpose**: Verify `th_series_data` storage

**Verification Points**:
- `start_time` = local date 00:00:00
- `end_time` = local date 23:59:59
- Values correctly stored
- UPSERT idempotency (repeated execution doesn't produce duplicate records)

## How to Run

### Method 1: Run Module Directly

```bash
# Run in container
docker exec a007-opensource-backend-1 python3 -m mirobody.pulse.core.aggregate_indicator.test_aggregator

# Or run on host machine
cd /Users/admin/go/a007-opensource
python3 -m mirobody.pulse.core.aggregate_indicator.test_aggregator
```

### Method 2: Run as Python Script

```bash
docker exec a007-opensource-backend-1 python3 /app/mirobody/pulse/core/aggregate_indicator/test_aggregator.py
```

## Test Flow

Each test follows this flow:

```
1. Clean up test data
   ↓
2. Insert designed test case data into series_data
   ↓
3. Call aggregator.get_trigger_tasks()
   ↓
4. Verify CalculationTask properties
   - data_begin_utc
   - timezone
   - source_indicator
   ↓
5. Call aggregator.calculate_batch_aggregations()
   ↓
6. Verify aggregation results
   - Are values correct
   - Are boundaries handled correctly
   ↓
7. Call db_service.batch_save_summary_data()
   ↓
8. Query th_series_data to verify storage
   - start_time / end_time
   - values
   - idempotency
   ↓
9. Clean up test data
```

## Expected Output

Example output on success:

```
================================================================================
Starting Aggregator Integration Tests
Test User: 138
Test Timezone: Asia/Shanghai
Test Date: 2025-10-30
================================================================================

================================================================================
Test 1: Normal data at date start boundary (00:00)
================================================================================
Inserted: Previous day 23:59:59 - local=2025-10-29 23:59:59, utc=2025-10-29 15:59:59, value=999
Inserted: Current day 00:00:00 - local=2025-10-30 00:00:00, utc=2025-10-29 16:00:00, value=1000
Inserted: Current day 00:00:01 - local=2025-10-30 00:00:01, utc=2025-10-29 16:00:01, value=1001
Trigger task: data_begin_utc=2025-10-29 16:00:00, timezone=Asia/Shanghai
✓ Boundary check passed: sum=2001.0 (excluded previous day's 999)
✅ Test 1 PASSED: Start boundary handled correctly

================================================================================
Test 2: Normal data at date end boundary (23:59:59)
================================================================================
...

================================================================================
✅ All integration tests PASSED
================================================================================
```

## Key Verification Points

### 1. SQL Timezone Conversion
- Whether PostgreSQL's `AT TIME ZONE` operation is correct
- Whether UTC ↔ local time conversion is consistent
- Whether timezone offset calculation is accurate

### 2. SQL Aggregation Queries
- Aggregation functions like `SUM`, `AVG`, `COUNT`
- `GROUP BY` grouping logic
- `WHERE` time range filtering

### 3. Date Boundary Handling
- 00:00:00 start boundary
- 23:59:59 end boundary
- Cross-date data grouping

### 4. Sleep Data Special Logic
- 18:00 start boundary
- 18-hour offset calculation
- Cross-midnight data attribution

### 5. Data Storage
- `th_series_data` time fields
- UPSERT idempotency
- Data integrity

## Troubleshooting

### Database Connection Failed
```
ERROR: password authentication failed
```
**Solution**: Ensure database configuration is correct and service is started

### Test Data Residue
```
ERROR: Aggregation boundary error
```
**Solution**: Manually clean up test data
```sql
DELETE FROM series_data 
WHERE user_id = '138' AND source = 'test.integration';

DELETE FROM th_series_data 
WHERE user_id = '138' AND DATE(start_time) = CURRENT_DATE;
```

### Timezone Configuration Error
```
ERROR: data_begin_utc hour incorrect
```
**Solution**: Check system timezone settings and PostgreSQL timezone configuration

## Extending Tests

To add new test cases:

1. Add new method in `AggregatorTester` class
2. Follow naming convention: `async def test_xxx(self):`
3. Call in `run_all_tests()`
4. Ensure test data cleanup

Example:

```python
async def test_new_scenario(self):
    """Test 7: New scenario description"""
    logger.info("Test 7: New scenario")
    
    # 1. Cleanup
    await self._cleanup_test_data("indicator_name")
    
    # 2. Insert test data
    # ...
    
    # 3. Execute and verify
    # ...
    
    # 4. Cleanup
    await self._cleanup_test_data("indicator_name")
    
    logger.info("✅ Test 7 PASSED")
```

## Design Advantages

1. **Implementation-agnostic**: Can be used to test any `AggregatorProtocol` implementation
2. **Boundary-focused**: Focuses on testing the most error-prone boundary cases
3. **End-to-end**: Complete flow from data insertion to storage verification
4. **Repeatable**: Automatic cleanup, can be executed repeatedly
5. **Real environment**: Uses real database and configuration

## Important Notes

1. **Uses real user**: Tests use user_id=138, please ensure this user exists
2. **Data cleanup**: Tests automatically clean up, but may leave residue on failure
3. **Timezone dependency**: Tests assume Asia/Shanghai timezone, modifications need synchronization
4. **Concurrent execution**: Not recommended to run multiple test instances concurrently
5. **Production environment**: Do NOT run tests in production environment!

## Related Documentation

- [Aggregate Indicator README](README.md)
- [Pulse Cursor Rules](../../cursorrules)
- [Testing Guide](../../../../../TESTING.md)





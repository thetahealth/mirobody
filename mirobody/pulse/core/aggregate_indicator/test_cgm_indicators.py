"""
CGM Indicator Unit Tests

Tests for CGM blood glucose aggregation indicators:
- Rule generation and naming
- Threshold method parsing
- Timezone conversion
- Event detection SQL (against real DB)

Usage:
    python3 -m mirobody.pulse.core.aggregate_indicator.test_cgm_indicators
"""

import asyncio
import json
import logging
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def test_rule_generation():
    """Test that rule_generator produces correct target indicator names for CGM methods"""
    from .rule_generator import generate_rules_from_indicators

    rules = generate_rules_from_indicators()
    bg_rules = [r for r in rules if r.source_indicator == 'bloodGlucoses']

    # Expected mapping: method → target indicator name
    expected = {
        'avg': 'dailyAvgBloodGlucoses',
        'max': 'dailyMaxBloodGlucoses',
        'min': 'dailyMinBloodGlucoses',
        'last': 'dailyLastBloodGlucoses',
        'time_of_max': 'dailyTimeOfMaxBloodGlucoses',
        'time_of_min': 'dailyTimeOfMinBloodGlucoses',
        'tir_70_180': 'dailyTir70180BloodGlucoses',
        'pct_below_70': 'dailyPctBelow70BloodGlucoses',
        'pct_above_180': 'dailyPctAbove180BloodGlucoses',
        'tir_70_140': 'dailyTir70140BloodGlucoses',
        'pct_above_140': 'dailyPctAbove140BloodGlucoses',
        'hypo_event_count': 'dailyHypoEventCountBloodGlucoses',
        'hypo_event_times': 'dailyHypoEventTimesBloodGlucoses',
        'hypo_event_details': 'dailyHypoEventDetailsBloodGlucoses',
        'gmi_14d': 'dailyGmi14dBloodGlucoses',
    }

    rule_map = {r.aggregation_type: r.target_indicator for r in bg_rules}

    for method, expected_name in expected.items():
        actual = rule_map.get(method)
        assert actual == expected_name, f"Rule name mismatch for '{method}': expected '{expected_name}', got '{actual}'"
        logger.info(f"  ✓ {method} → {actual}")

    logger.info(f"✅ Rule generation: {len(bg_rules)} bloodGlucoses rules generated correctly")


def test_parse_threshold_method():
    """Test parameterized threshold method parsing"""
    from .aggregators.sql_aggregator import SQLAggregator

    agg = SQLAggregator()

    # Test pct_below variants
    result = agg._parse_threshold_method('pct_below_70')
    assert result is not None, "pct_below_70 should match"
    assert result[0] == 'pct_below'
    assert 'value::numeric < 70' in result[2]
    logger.info(f"  ✓ pct_below_70 → SQL: ...{result[2][:50]}...")

    result = agg._parse_threshold_method('pct_below_60')
    assert result is not None, "pct_below_60 should match"
    assert 'value::numeric < 60' in result[2]
    logger.info(f"  ✓ pct_below_60 → SQL with threshold 60")

    # Test pct_above variants
    result = agg._parse_threshold_method('pct_above_180')
    assert result is not None
    assert 'value::numeric > 180' in result[2]
    logger.info(f"  ✓ pct_above_180 → SQL with threshold 180")

    # Test tir variant
    result = agg._parse_threshold_method('tir_70_180')
    assert result is not None
    assert result[0] == 'tir'
    assert 'BETWEEN 70 AND 180' in result[2]
    logger.info(f"  ✓ tir_70_180 → SQL with range [70, 180]")

    # Test 140 threshold variants
    result = agg._parse_threshold_method('pct_above_140')
    assert result is not None
    assert 'value::numeric > 140' in result[2]
    logger.info(f"  ✓ pct_above_140 → SQL with threshold 140")

    result = agg._parse_threshold_method('tir_70_140')
    assert result is not None
    assert 'BETWEEN 70 AND 140' in result[2]
    logger.info(f"  ✓ tir_70_140 → SQL with range [70, 140]")

    # Test non-threshold methods should return None
    for method in ['avg', 'max', 'min', 'time_of_max', 'hypo_event_count']:
        result = agg._parse_threshold_method(method)
        assert result is None, f"'{method}' should NOT be parsed as threshold"
    logger.info(f"  ✓ Non-threshold methods correctly return None")

    # Test decimal thresholds
    result = agg._parse_threshold_method('pct_below_3.9')
    assert result is not None
    assert 'value::numeric < 3.9' in result[2]
    logger.info(f"  ✓ pct_below_3.9 → decimal threshold supported")

    logger.info("✅ Threshold method parsing: all cases passed")


def test_timezone_conversion():
    """Test UTC time string to local time conversion"""
    from .aggregators.sql_aggregator import SQLAggregator

    # Test Asia/Shanghai (UTC+8)
    ref_date = datetime(2025, 12, 12, 16, 0, 0)  # data_begin_utc
    result = SQLAggregator._convert_utc_time_to_local('03:25', 'Asia/Shanghai', ref_date)
    assert result == '11:25', f"Expected 11:25, got {result}"
    logger.info(f"  ✓ UTC 03:25 + Asia/Shanghai → {result}")

    result = SQLAggregator._convert_utc_time_to_local('11:58', 'Asia/Shanghai', ref_date)
    assert result == '19:58', f"Expected 19:58, got {result}"
    logger.info(f"  ✓ UTC 11:58 + Asia/Shanghai → {result}")

    # Test America/Los_Angeles (UTC-8 in winter)
    ref_date_la = datetime(2025, 11, 28, 8, 0, 0)
    result = SQLAggregator._convert_utc_time_to_local('14:30', 'America/Los_Angeles', ref_date_la)
    assert result == '06:30', f"Expected 06:30, got {result}"
    logger.info(f"  ✓ UTC 14:30 + America/Los_Angeles → {result}")

    # Test JSON array conversion
    ref_date = datetime(2025, 12, 12, 16, 0, 0)
    result = SQLAggregator._convert_utc_times_json_to_local(
        '["17:37", "19:37", "09:40"]', 'Asia/Shanghai', ref_date
    )
    times = json.loads(result)
    assert times == ['01:37', '03:37', '17:40'], f"Expected ['01:37', '03:37', '17:40'], got {times}"
    logger.info(f"  ✓ JSON array conversion: {times}")

    # Test with Python list input
    result = SQLAggregator._convert_utc_times_json_to_local(
        ['09:40'], 'Asia/Shanghai', ref_date
    )
    times = json.loads(result)
    assert times == ['17:40'], f"Expected ['17:40'], got {times}"
    logger.info(f"  ✓ Python list input: {times}")

    # Test new object format (with start/end/duration_min)
    ref_date = datetime(2025, 12, 12, 16, 0, 0)
    obj_input = json.dumps([
        {"start": "17:37", "end": "18:07", "duration_min": 30},
        {"start": "09:40", "end": "10:10", "duration_min": 30},
    ])
    result = SQLAggregator._convert_utc_times_json_to_local(obj_input, 'Asia/Shanghai', ref_date)
    events = json.loads(result)
    assert len(events) == 2
    assert events[0]['start'] == '01:37', f"Expected start 01:37, got {events[0]['start']}"
    assert events[0]['end'] == '02:07', f"Expected end 02:07, got {events[0]['end']}"
    assert events[0]['duration_min'] == 30, f"Expected duration_min 30, got {events[0]['duration_min']}"
    assert events[1]['start'] == '17:40'
    assert events[1]['end'] == '18:10'
    logger.info(f"  ✓ Object format conversion: {events}")

    # Test empty/edge cases
    result = SQLAggregator._convert_utc_times_json_to_local('[]', 'Asia/Shanghai', ref_date)
    assert result == '[]', f"Expected '[]', got {result}"
    logger.info(f"  ✓ Empty array → '[]'")

    result = SQLAggregator._convert_utc_times_json_to_local(None, 'Asia/Shanghai', ref_date)
    assert result == '[]', f"Expected '[]' for None, got {result}"
    logger.info(f"  ✓ None input → '[]'")

    logger.info("✅ Timezone conversion: all cases passed")


def test_cgm_event_methods_classification():
    """Test that CGM event methods are correctly classified"""
    from .aggregators.sql_aggregator import SQLAggregator

    agg = SQLAggregator()

    # These should be routed to event detection path
    assert 'hypo_event_count' in agg._cgm_event_methods
    assert 'hypo_event_times' in agg._cgm_event_methods
    assert 'hypo_event_details' in agg._cgm_event_methods
    logger.info(f"  ✓ hypo_event_count/times/details classified as event methods")

    # GMI should be in its own category, not in event methods
    assert 'gmi_14d' in agg._cgm_gmi_methods
    assert 'gmi_14d' not in agg._cgm_event_methods
    logger.info(f"  ✓ gmi_14d classified as GMI method (separate from events)")

    # These should NOT be in event methods (standard GROUP BY path)
    standard_methods = ['avg', 'max', 'min', 'time_of_max', 'time_of_min',
                        'pct_below_70', 'pct_above_180', 'tir_70_180',
                        'pct_above_140', 'tir_70_140']
    for m in standard_methods:
        assert m not in agg._cgm_event_methods, f"'{m}' should NOT be an event method"
    logger.info(f"  ✓ Standard methods correctly excluded from event methods")

    logger.info("✅ Event method classification: correct")


def test_tir_tbr_tar_sum():
    """Verify TIR + TBR + TAR = 100% mathematically"""
    # Using the SQL clause patterns
    from .aggregators.sql_aggregator import SQLAggregator
    agg = SQLAggregator()

    tir = agg._parse_threshold_method('tir_70_180')
    tbr = agg._parse_threshold_method('pct_below_70')
    tar = agg._parse_threshold_method('pct_above_180')

    # TIR: BETWEEN 70 AND 180 (inclusive)
    assert 'BETWEEN 70 AND 180' in tir[2]
    # TBR: < 70
    assert '< 70' in tbr[2]
    # TAR: > 180
    assert '> 180' in tar[2]

    # Mathematically: <70 + [70,180] + >180 covers all real numbers
    # So TIR + TBR + TAR = 100% (within rounding)
    logger.info("  ✓ TBR(<70) + TIR([70,180]) + TAR(>180) covers entire range")

    # Also verify 140 threshold completeness: <70 + [70,140] + >140 = 100%
    tir_140 = agg._parse_threshold_method('tir_70_140')
    tar_140 = agg._parse_threshold_method('pct_above_140')
    assert 'BETWEEN 70 AND 140' in tir_140[2]
    assert '> 140' in tar_140[2]
    logger.info("  ✓ TBR(<70) + TIR([70,140]) + TAR(>140) covers entire range")

    logger.info("✅ TIR+TBR+TAR completeness: verified")


async def test_db_aggregation():
    """End-to-end test: run aggregation on real user data and validate"""
    from .aggregators.sql_aggregator import SQLAggregator
    from .models import CalculationTask
    from .rule_generator import get_rules_by_source_indicator

    agg = SQLAggregator()

    # User 212, 2025-12-13, Asia/Shanghai
    # data_begin_utc = 2025-12-12 16:00:00 (local midnight)
    user_id = '212'
    data_begin_utc = datetime(2025, 12, 12, 16, 0, 0)
    timezone = 'Asia/Shanghai'

    rules = get_rules_by_source_indicator('bloodGlucoses')
    bg_rules = {r.aggregation_type: r for r in rules}

    # Build tasks for all methods
    tasks = []
    for rule in rules:
        task = CalculationTask(
            user_id=user_id,
            source_indicator='bloodGlucoses',
            target_indicator=rule.target_indicator,
            aggregation_type=rule.aggregation_type,
            data_begin_utc=data_begin_utc,
            timezone=timezone,
            update_time=datetime.now()
        )
        tasks.append(task)

    logger.info(f"Created {len(tasks)} tasks for bloodGlucoses")

    # Execute
    summaries = await agg.calculate_batch_aggregations(tasks)

    logger.info(f"Generated {len(summaries)} summary records")

    # Validate results
    result_map = {}
    for s in summaries:
        indicator = s['indicator']
        result_map[indicator] = s['value']
        logger.info(f"  {indicator} = {s['value']}")

    # Check expected indicators exist
    expected_indicators = [
        'dailyAvgBloodGlucoses.apple_health',
        'dailyMaxBloodGlucoses.apple_health',
        'dailyMinBloodGlucoses.apple_health',
        'dailyLastBloodGlucoses.apple_health',
        'dailyTimeOfMaxBloodGlucoses.apple_health',
        'dailyTimeOfMinBloodGlucoses.apple_health',
        'dailyTir70180BloodGlucoses.apple_health',
        'dailyPctBelow70BloodGlucoses.apple_health',
        'dailyPctAbove180BloodGlucoses.apple_health',
        'dailyTir70140BloodGlucoses.apple_health',
        'dailyPctAbove140BloodGlucoses.apple_health',
        'dailyHypoEventCountBloodGlucoses.apple_health',
        'dailyHypoEventTimesBloodGlucoses.apple_health',
        'dailyHypoEventDetailsBloodGlucoses.apple_health',
    ]

    for ind in expected_indicators:
        assert ind in result_map, f"Missing indicator: {ind}"

    # Cross-validate values against known SQL results
    avg = float(result_map['dailyAvgBloodGlucoses.apple_health'])
    assert 85 < avg < 90, f"avg should be ~87.32, got {avg}"

    max_val = float(result_map['dailyMaxBloodGlucoses.apple_health'])
    assert 205 < max_val < 215, f"max should be ~208.98, got {max_val}"

    tir = float(result_map['dailyTir70180BloodGlucoses.apple_health'])
    tbr = float(result_map['dailyPctBelow70BloodGlucoses.apple_health'])
    tar = float(result_map['dailyPctAbove180BloodGlucoses.apple_health'])
    total = tir + tbr + tar
    assert 99.9 <= total <= 100.1, f"TIR+TBR+TAR should be ~100%, got {total}"
    logger.info(f"  ✓ TIR({tir}) + TBR({tbr}) + TAR({tar}) = {total}")

    # time_of_max should be local time (Asia/Shanghai = UTC+8)
    time_max = result_map['dailyTimeOfMaxBloodGlucoses.apple_health']
    assert time_max == '11:25', f"time_of_max should be 11:25 (local), got {time_max}"
    logger.info(f"  ✓ time_of_max = {time_max} (local)")

    # Event detection: 2025-12-13 has 3 hypo events
    event_count = int(result_map['dailyHypoEventCountBloodGlucoses.apple_health'])
    assert event_count == 3, f"Expected 3 hypo events, got {event_count}"
    logger.info(f"  ✓ hypo_event_count = {event_count}")

    event_times = json.loads(result_map['dailyHypoEventTimesBloodGlucoses.apple_health'])
    assert len(event_times) == 3, f"Expected 3 event times, got {len(event_times)}"
    # event_times should remain as string array (backward compatible)
    assert isinstance(event_times[0], str), f"Expected string, got {type(event_times[0])}"
    logger.info(f"  ✓ hypo_event_times = {event_times} (local, string array)")

    # Event details: new indicator with start/end/duration_min objects
    event_details_key = 'dailyHypoEventDetailsBloodGlucoses.apple_health'
    assert event_details_key in result_map, f"Missing indicator: {event_details_key}"
    event_details = json.loads(result_map[event_details_key])
    assert len(event_details) == 3, f"Expected 3 event details, got {len(event_details)}"
    assert isinstance(event_details[0], dict), f"Expected event object, got {type(event_details[0])}"
    assert 'start' in event_details[0], "Event detail should have 'start' field"
    assert 'end' in event_details[0], "Event detail should have 'end' field"
    assert 'duration_min' in event_details[0], "Event detail should have 'duration_min' field"
    assert event_details[0]['duration_min'] >= 15, f"Duration should be >= 15 min, got {event_details[0]['duration_min']}"
    logger.info(f"  ✓ hypo_event_details = {event_details} (local, object format)")

    # Validate 140 threshold indicators
    tir_140 = float(result_map['dailyTir70140BloodGlucoses.apple_health'])
    tar_140 = float(result_map['dailyPctAbove140BloodGlucoses.apple_health'])
    total_140 = tir_140 + tar_140 + tbr  # reuse tbr (<70) from above
    assert 99.9 <= total_140 <= 100.1, f"TIR_140+TAR_140+TBR should be ~100%, got {total_140}"
    logger.info(f"  ✓ TIR_140({tir_140}) + TAR_140({tar_140}) + TBR({tbr}) = {total_140}")

    logger.info("✅ DB aggregation end-to-end: all values validated")


def run_unit_tests():
    """Run all non-DB unit tests"""
    logger.info("=" * 60)
    logger.info("Running CGM Indicator Unit Tests")
    logger.info("=" * 60)

    tests = [
        ("Rule Generation", test_rule_generation),
        ("Threshold Method Parsing", test_parse_threshold_method),
        ("Timezone Conversion", test_timezone_conversion),
        ("Event Method Classification", test_cgm_event_methods_classification),
        ("TIR+TBR+TAR Completeness", test_tir_tbr_tar_sum),
    ]

    passed = 0
    failed = 0
    for name, test_fn in tests:
        try:
            logger.info(f"\n--- {name} ---")
            test_fn()
            passed += 1
        except Exception as e:
            logger.error(f"❌ {name} FAILED: {e}")
            failed += 1

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Unit Tests: {passed} passed, {failed} failed")
    logger.info(f"{'=' * 60}")
    return failed == 0


async def run_db_tests():
    """Run DB-dependent tests"""
    logger.info(f"\n{'=' * 60}")
    logger.info("Running CGM DB Integration Tests")
    logger.info(f"{'=' * 60}")

    try:
        await test_db_aggregation()
        return True
    except Exception as e:
        logger.error(f"❌ DB test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    # Initialize config for DB access
    from ....utils.config import Config
    try:
        Config('config/config.test-inlocal.yaml')
    except Exception:
        try:
            Config('/app/config/config.yaml')
        except Exception:
            pass

    unit_ok = run_unit_tests()
    db_ok = await run_db_tests()

    if unit_ok and db_ok:
        logger.info("\n🎉 ALL TESTS PASSED")
    else:
        logger.error("\n💥 SOME TESTS FAILED")


if __name__ == "__main__":
    asyncio.run(main())

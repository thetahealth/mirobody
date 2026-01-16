"""
Aggregate Indicator Module

Automatically calculate summary indicators from series data.

This module provides scheduled jobs to aggregate time-series health data into daily summaries.
Rules are defined in code (via IndicatorInfo.aggregation_methods) for easy maintenance and version control.

Key Features:
- Incremental calculation based on data updates
- Automatic rule generation from IndicatorInfo
- Timezone-aware time window calculation
- Idempotent operations (safe to re-run)
- SQL aggregation for maximum performance
- Dependency injection for flexibility

Example Usage:
    from mirobody.pulse.core.aggregate_indicator import AggregateIndicatorService
    
    service = AggregateIndicatorService()
    await service.process_incremental()

See README.md for detailed documentation.
"""

from .aggregators import SQLAggregator, AggregatorProtocol
from .models import (
    AggregationRule,
    TimeWindow,
    AggregationType,
    ProcessingStats
)
from .rule_generator import (
    get_all_aggregation_rules,
    register_custom_rule,
    get_rules_by_source_indicator,
    get_source_indicators
)
from .service import AggregateIndicatorService
from .task import AggregateIndicatorTask

__version__ = "1.0.0"
__all__ = [
    "AggregateIndicatorService",
    "AggregateIndicatorTask",
    "get_all_aggregation_rules",
    "register_custom_rule",
    "get_rules_by_source_indicator",
    "get_source_indicators",
    "AggregationRule",
    "TimeWindow",
    "AggregationType",
    "ProcessingStats",
    "SQLAggregator",
    "AggregatorProtocol",
]

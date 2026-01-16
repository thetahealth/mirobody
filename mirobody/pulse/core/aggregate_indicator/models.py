"""
Data models for Aggregate Indicator module

Defines core data structures including enums, dataclasses, and protocols.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class TimeWindow(Enum):
    """Time window for aggregation"""
    DAILY = "daily"
    WEEKLY = "weekly"  # Future: rolling 7-day window
    HOURLY = "hourly"  # Future: hourly aggregation
    MONTHLY = "monthly"  # Future: monthly aggregation


class AggregationType(Enum):
    """Aggregation type enumeration"""
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    TOTAL = "total"  # Alias for SUM, preferred for naming
    SUM = "sum"  # Equivalent to TOTAL
    COUNT = "count"
    LAST = "last"  # Latest value by time
    FIRST = "first"  # Earliest value by time
    STDDEV = "stddev"  # Standard deviation
    VARIANCE = "variance"  # Variance
    MEDIAN = "median"  # 50th percentile
    P95 = "p95"  # 95th percentile


@dataclass
class AggregationRule:
    """
    Aggregation rule definition
    
    Defines how to convert a series indicator to a summary indicator.
    """
    source_indicator: str  # Source indicator name (e.g., "heartRates")
    target_indicator: str  # Target indicator name (e.g., "dailyAvgHeartRates")
    aggregation_type: str  # Aggregation method (e.g., "avg")
    time_window: str = "daily"  # Time window (default: daily)
    enabled: bool = True  # Whether this rule is active
    priority: int = 50  # Execution priority (higher = earlier)

    def __post_init__(self):
        """Validate rule after initialization"""
        if not self.source_indicator:
            raise ValueError("source_indicator cannot be empty")
        if not self.target_indicator:
            raise ValueError("target_indicator cannot be empty")
        if not self.aggregation_type:
            raise ValueError("aggregation_type cannot be empty")


@dataclass
class AggregationContext:
    """
    Context for aggregation calculation
    
    Contains all information needed to execute aggregation for a user.
    """
    user_id: str
    indicators: List[str]
    day_start: datetime
    day_end: datetime
    timezone: str
    aggregation_methods: Set[str]
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class CalculationTask:
    """
    Represents a single aggregation calculation task
    
    Identified from trigger data, contains what needs to be calculated.
    data_begin_utc is the starting time point for data collection in UTC:
    - For normal data: 00:00 of the user's local date, converted to UTC
    - For sleep data: 18:00 of the user's local date, converted to UTC
    
    Example: User in America/Los_Angeles (-8), local date 2025-10-01
    - Normal data: data_begin_utc = 2025-09-30 16:00:00 (local 2025-10-01 00:00:00)
    - Sleep data: data_begin_utc = 2025-10-01 10:00:00 (local 2025-10-01 18:00:00)
    
    timezone is needed to convert UTC times back to user's local time for th_series_data storage.
    """
    user_id: str
    source_indicator: str
    target_indicator: str
    aggregation_type: str
    data_begin_utc: datetime  # Starting time point in UTC (for efficient querying with index)
    timezone: str  # User's timezone (e.g., 'America/Los_Angeles') - needed to convert back to local time
    update_time: datetime  # Update time from trigger record


@dataclass
class AggregationResult:
    """Result of aggregation calculation"""
    user_id: str
    indicator: str
    value: str
    start_time: datetime
    end_time: datetime
    source: str = "aggregate_indicator"
    task_id: str = "aggregate_indicator"
    comment: str = ""
    source_table: str = ""
    source_table_id: str = ""
    indicator_id: str = ""


@dataclass
class ProcessingStats:
    """Statistics for a processing run"""
    executed_at: datetime
    summaries_created: int
    users_affected: int
    execution_time_ms: float
    mode: str = "normal"  # normal | force | cold_start
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "executed_at": self.executed_at.isoformat(),
            "summaries_created": self.summaries_created,
            "users_affected": self.users_affected,
            "execution_time_ms": self.execution_time_ms,
            "mode": self.mode,
            "errors": self.errors
        }

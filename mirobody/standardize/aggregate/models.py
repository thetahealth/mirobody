"""The two records the aggregation pass works with: a rule from the catalogue,
and a task found in `series_data`."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class AggregationRule:
    """How a series indicator becomes a daily summary indicator."""

    source_indicator: str  # e.g. "heartRates"
    target_indicator: str  # e.g. "dailyAvgHeartRates"
    aggregation_type: str  # e.g. "avg"; the methods are SQLAggregator's

    def __post_init__(self):
        if not self.source_indicator:
            raise ValueError("source_indicator cannot be empty")
        if not self.target_indicator:
            raise ValueError("target_indicator cannot be empty")
        if not self.aggregation_type:
            raise ValueError("aggregation_type cannot be empty")


@dataclass
class CalculationTask:
    """One rule to run over one person's local day.

    `data_begin_utc` is the instant the reading's day began, in UTC: local
    00:00 for a plain day, local 18:00 for a metric on the 18:00 window (a
    night, dated by its evening). A reading at 2025-10-01 02:00 in
    America/Los_Angeles (PDT):

    - on a plain day: 2025-10-01 07:00:00 UTC (local 2025-10-01 00:00)
    - on the 18:00 window: 2025-10-01 01:00:00 UTC (local 2025-09-30 18:00)

    `timezone` places the day's end and turns the results back into local
    times.
    """
    user_id: str
    source_indicator: str
    target_indicator: str
    aggregation_type: str
    data_begin_utc: datetime
    timezone: str
    update_time: datetime  # the newest update_time of the rows behind it: the cursor

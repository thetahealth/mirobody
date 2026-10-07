"""The aggregation rules, read off the indicator catalogue.

Each SERIES indicator declares its `aggregation_methods` in `IndicatorInfo`;
every method becomes one rule from the indicator to its daily summary, named
`daily{Method}{Indicator}` (`naming.build_indicator_name`):

    heartRates + ['avg', 'max', 'min'] -> dailyAvgHeartRates, dailyMaxHeartRates, dailyMinHeartRates
    steps + ['total']                  -> dailyTotalSteps
"""

import logging

from .models import AggregationRule
from .naming import build_indicator_name
from mirobody.translate import StandardIndicator, HealthDataType

logger = logging.getLogger(__name__)

_CACHED_RULES: list[AggregationRule] | None = None


def generate_rules_from_indicators() -> list[AggregationRule]:
    """One rule per aggregation method of every SERIES indicator."""
    rules = []
    for indicator_enum in StandardIndicator:
        info = indicator_enum.value
        if info.data_type != HealthDataType.SERIES or not info.aggregation_methods:
            continue
        for method in info.aggregation_methods:
            rules.append(AggregationRule(
                source_indicator=info.name,
                target_indicator=build_indicator_name("day", method, info.name),
                aggregation_type=method,
            ))
    logger.info(f"Auto-generated {len(rules)} aggregation rules from IndicatorInfo definitions")
    return rules


def get_all_aggregation_rules() -> list[AggregationRule]:
    """Every rule, generated once per process."""
    global _CACHED_RULES
    if _CACHED_RULES is None:
        _CACHED_RULES = generate_rules_from_indicators()
    return _CACHED_RULES


def get_rules_by_source_indicator(source_indicator: str) -> list[AggregationRule]:
    """The rules that aggregate `source_indicator` (e.g. "heartRates")."""
    return [rule for rule in get_all_aggregation_rules() if rule.source_indicator == source_indicator]

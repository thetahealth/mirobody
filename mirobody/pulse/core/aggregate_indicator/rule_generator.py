"""
Rule Generator

Automatically generates aggregation rules from IndicatorInfo definitions.
Supports custom rule injection for special cases.
"""

import logging
from typing import List, Optional

from .models import AggregationRule
from ..indicators_info import StandardIndicator, HealthDataType

# Global custom rules registry
_CUSTOM_RULES: List[AggregationRule] = []

# Cached rules (to avoid regenerating on every call)
_CACHED_RULES: Optional[List[AggregationRule]] = None


def register_custom_rule(rule: AggregationRule):
    """
    Register a custom aggregation rule
    
    Use this to add special aggregation rules that cannot be auto-generated
    from IndicatorInfo.
    
    Args:
        rule: Custom AggregationRule instance
        
    Example:
        # Custom resting heart rate (nighttime only)
        custom_rule = AggregationRule(
            source_indicator="heartRates",
            target_indicator="dailyRestingHeartRate",
            aggregation_type="avg",
            time_window="daily"
        )
        register_custom_rule(custom_rule)
    """
    _CUSTOM_RULES.append(rule)
    logging.info(
        f"Registered custom rule: {rule.source_indicator} -> {rule.target_indicator} "
        f"({rule.aggregation_type})"
    )


def generate_rules_from_indicators() -> List[AggregationRule]:
    """
    Auto-generate aggregation rules from IndicatorInfo definitions
    
    Reads aggregation_methods from each SERIES indicator and generates rules.
    Follows naming convention: daily{Method}{Indicator} (camelCase)
    
    Returns:
        List of auto-generated AggregationRule objects
        
    Examples:
        heartRates + ['avg', 'max', 'min'] →
            dailyAvgHeartRates, dailyMaxHeartRates, dailyMinHeartRates
        
        steps + ['total'] →
            dailyTotalSteps
        
        bodyMasss + ['last'] →
            dailyLastBodyMasss
    """
    rules = []

    for indicator_enum in StandardIndicator:
        indicator_info = indicator_enum.value

        # Only process SERIES type indicators
        if indicator_info.data_type != HealthDataType.SERIES:
            continue

        # Skip if no aggregation methods defined
        aggregation_methods = getattr(indicator_info, 'aggregation_methods', None)
        if not aggregation_methods:
            continue

        source_name = indicator_info.name

        for method in aggregation_methods:
            # Generate target indicator name using camelCase convention
            # Pattern: daily{Method}{SourceName}
            # Examples: dailyAvgHeartRates, dailyTotalSteps, dailyLastBodyMasss

            # Capitalize method name
            if method in ['total', 'sum']:
                method_capitalized = 'Total'  # Both map to 'Total'
            else:
                method_capitalized = method.capitalize()

            # Capitalize first letter of source name
            source_capitalized = source_name[0].upper() + source_name[1:] if source_name else ''

            # Build target name
            target_name = f"daily{method_capitalized}{source_capitalized}"

            # Create rule
            rule = AggregationRule(
                source_indicator=source_name,
                target_indicator=target_name,
                aggregation_type=method,
                time_window="daily",
                enabled=True,
                priority=50
            )

            rules.append(rule)

    logging.info(
        f"Auto-generated {len(rules)} aggregation rules from IndicatorInfo definitions"
    )

    return rules


def get_all_aggregation_rules() -> List[AggregationRule]:
    """
    Get all aggregation rules (auto-generated + custom)
    
    Uses cached rules after first generation for performance.
    
    Returns:
        Combined list of all aggregation rules, sorted by priority
    """
    global _CACHED_RULES

    # Return cached rules if available
    if _CACHED_RULES is not None:
        return _CACHED_RULES

    # Auto-generated rules
    auto_rules = generate_rules_from_indicators()

    # Merge with custom rules
    all_rules = auto_rules + _CUSTOM_RULES

    # Sort by priority (higher priority first)
    all_rules.sort(key=lambda r: r.priority, reverse=True)

    # Cache the rules
    _CACHED_RULES = all_rules

    logging.info(
        f"Generated and cached {len(all_rules)} aggregation rules "
        f"(auto: {len(auto_rules)}, custom: {len(_CUSTOM_RULES)})"
    )

    return all_rules


def get_rules_by_source_indicator(source_indicator: str) -> List[AggregationRule]:
    """
    Get all rules for a specific source indicator
    
    Args:
        source_indicator: Source indicator name (e.g., "heartRates")
        
    Returns:
        List of matching AggregationRule objects
    """
    all_rules = get_all_aggregation_rules()

    return [
        rule for rule in all_rules
        if rule.source_indicator == source_indicator and rule.enabled
    ]


def get_source_indicators() -> List[str]:
    """
    Get list of all source indicators that have aggregation rules
    
    Returns:
        List of unique source indicator names
    """
    all_rules = get_all_aggregation_rules()

    # Get unique source indicators
    source_indicators = list(set(rule.source_indicator for rule in all_rules))

    return sorted(source_indicators)

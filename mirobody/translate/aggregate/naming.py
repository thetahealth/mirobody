"""
Indicator Naming Utilities

Shared naming logic for building aggregated indicator names.
Used by rule_generator (auto-generating rules from IndicatorInfo)
and statistics_service (mapping client-submitted statistics).

Pattern: {prefix}{Method}{SourceIndicator}
Examples:
    ("day", "total", "steps")       → "dailyTotalSteps"
    ("hour", "avg", "heartRates")   → "hourlyAvgHeartRates"
    ("week", "max", "heartRates")   → "weeklyMaxHeartRates"
    ("month", "last", "bodyMasss")  → "monthlyLastBodyMasss"
"""


GROUPING_PREFIX = {
    "hour": "hourly",
    "day": "daily",
    "week": "weekly",
    "month": "monthly",
}

# A method's name is its CamelCase (`pct_below_70` -> `PctBelow70`), except
# `sum`, which names the same figure as `total` (Apple's statistics upload
# sends `sum`, the catalogue declares `total`).
METHOD_NAME_MAP = {'sum': 'Total'}


def _snake_to_camel(s: str) -> str:
    """Convert snake_case to CamelCase: 'pct_below_70' → 'PctBelow70'"""
    return ''.join(part.capitalize() for part in s.split('_'))


def build_indicator_name(grouping: str, method: str, source_indicator: str) -> str:
    """
    Build an aggregated indicator name from grouping, method, and source indicator.

    Args:
        grouping: Time grouping key ("hour", "day", "week", "month")
        method: Aggregation method (e.g., "avg", "total", "max", "min", "last")
        source_indicator: Source indicator name in lowerCamelCase (e.g., "heartRates", "steps")

    Returns:
        Aggregated indicator name (e.g., "dailyAvgHeartRates", "weeklyTotalSteps")

    Raises:
        ValueError: If grouping is not recognized
    """
    prefix = GROUPING_PREFIX.get(grouping)
    if prefix is None:
        raise ValueError(f"Unknown grouping: {grouping!r}, expected one of {list(GROUPING_PREFIX.keys())}")

    method_capitalized = METHOD_NAME_MAP.get(method)
    if method_capitalized is None:
        method_capitalized = _snake_to_camel(method)

    source_capitalized = source_indicator[0].upper() + source_indicator[1:] if source_indicator else ''

    return f"{prefix}{method_capitalized}{source_capitalized}"

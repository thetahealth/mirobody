"""The indicator catalogue as the reference implementation has always seen it.

``StandardIndicator``, ``Categories``, ``IndicatorInfo`` and the helper
functions keep their names, members and shapes — every provider, the
aggregator, the Apple upload path and two downstream repositories reference
``StandardIndicator.HEART_RATE.value.name``. What changed is where the data
lives: this module used to BE the catalogue (3,131 lines of hand-written
members); it is now a projection of ``mirobody.kernel.metrics`` (``res/metrics.tsv``),
the library-layer catalogue that also carries the shape (``state_class``,
``aggregation_policy``), the LOINC code and the local-day window that the
enum never had.

The Chinese labels (``name_zh`` / ``description_zh``) come from
``res/labels/zh.tsv`` through ``metrics.register_labels``: the catalogue is
English, and a deployment injects the languages it serves. Loading the
shipped ``zh`` file here is what keeps ``get_all_indicators_info()`` — the
payload the web client renders — byte-for-byte what it was.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from ...kernel import metrics
logger = logging.getLogger(__name__)


class HealthDataType(Enum):
    """
    Enum representing the type of health data.

    Attributes:
        SUMMARY: Data that is a single, aggregated value over a period (e.g., daily step count, average heart rate).
        SERIES: Data consisting of a sequence of timestamped data points (e.g., a minute-by-minute heart rate log).
    """
    SUMMARY = "summary"
    SERIES = "series"
    MIX = "mix"


@dataclass
class CategoryInfo:
    """Category information"""
    name: str
    name_zh: str


@dataclass
class IndicatorInfo:
    """Indicator information"""
    category: CategoryInfo
    standard_unit: str
    data_type: HealthDataType = HealthDataType.SERIES
    name: str = ""  # lowerCamelCase
    name_zh: str = ""
    description: str = ""
    description_zh: str = ""
    aggregation_methods: list[str] | None = None  # For aggregate_indicator module
    """
    Aggregation methods for converting series data to summary data
    (``'avg'``, ``'max'``, ``'min'``, ``'total'``, ``'last'``, … plus the
    derived ones the CGM and heart-rate aggregators implement). Generated
    target indicators follow the pattern ``daily{Method}{Indicator}``:
    ``heartRates + ['avg']`` → ``dailyAvgHeartRates``.

    The catalogue row behind this entry also carries the *shape*
    (``metrics.METRICS[name].state_class`` / ``aggregation_policy``); the
    method list stays here because ``pulse/aggregate/rule_generator`` is
    written against it.
    """


# The enum spellings the category members have always had; the catalogue
# stores only the display name.
_CATEGORY_MEMBERS = {
    "Vital Signs": "VITAL_SIGNS",
    "Body Composition": "BODY_COMPOSITION",
    "Activity Metrics": "ACTIVITY",
    "Metabolic Metrics": "METABOLIC",
    "Sleep Metrics": "SLEEP",
    "Performance Metrics": "PERFORMANCE",
    "Medical Metrics": "MEDICAL",
    "Device Specific": "DEVICE_SPECIFIC",
    "Nutrition Intake": "NUTRITION",
    "Lifestyle": "LIFESTYLE",
    "Health Metrics": "HEALTH",
    "Mental Health": "MENTAL",
    "Reproductive Health": "REPRODUCTIVE",
}

metrics.load_labels_resource("zh")

_category_infos: dict[str, CategoryInfo] = {
    display: CategoryInfo(name=display, name_zh=metrics.label(member, "zh", display))
    for display, member in _CATEGORY_MEMBERS.items()
}
for _display in metrics.CATEGORIES:
    if _display not in _category_infos:
        _category_infos[_display] = CategoryInfo(name=_display, name_zh=_display)

Categories = Enum(  # type: ignore[misc]
    "Categories",
    {_CATEGORY_MEMBERS.get(display, display.upper().replace(" ", "_")): info for display, info in _category_infos.items()},
    module=__name__,
)
Categories.__doc__ = "Health indicator categories with embedded CategoryInfo"


def _info(m: metrics.Metric) -> IndicatorInfo:
    return IndicatorInfo(
        category=_category_infos[m.category],
        standard_unit=m.standard_unit,
        data_type=HealthDataType(m.data_type),
        name=m.name,
        name_zh=metrics.label(m.member, "zh"),
        description=m.description,
        description_zh=metrics.description(m.member, "zh"),
        aggregation_methods=list(m.aggregation_methods) or None,
    )


StandardIndicator = Enum(  # type: ignore[misc]
    "StandardIndicator",
    {m.member: _info(m) for m in metrics.ROWS},
    module=__name__,
)
StandardIndicator.__doc__ = "Standard health indicators — one member per catalogue row (``res/metrics.tsv``)."
StandardIndicator.identifier = property(lambda self: self.value.name)  # type: ignore[attr-defined]
StandardIndicator.identifier.__doc__ = "Return the string identifier for backward compatibility."


# ============================================================================
# UTILITY VARIABLES AND FUNCTIONS
# ============================================================================

# Valid indicators set for fast lookup (unique names only for backward compatibility)
VALID_INDICATORS: set[str] = {indicator.identifier for indicator in StandardIndicator}

# Create a dictionary for efficient lookup
_INDICATOR_LOOKUP = {
    indicator.value.name: indicator.value for indicator in StandardIndicator
}

# Case-insensitive lookup: lowercase → canonical name (W1.4)
_INDICATOR_NAME_NORMALIZE = {
    indicator.value.name.lower(): indicator.value.name
    for indicator in StandardIndicator if indicator.value.name
}


def normalize_indicator_name(raw_name: str) -> str:
    """Normalize indicator name to canonical case from StandardIndicator.

    Case-insensitive match: 'bloodglucoses' → 'bloodGlucoses'.
    Unrecognized names are returned as-is.
    """
    if not raw_name:
        return raw_name
    return _INDICATOR_NAME_NORMALIZE.get(raw_name.lower(), raw_name)


def is_summary_indicator(indicator: str) -> bool:
    """A defined indicator whose data_type is SUMMARY or MIX."""
    if not indicator:
        return False
    std_indicator = get_indicator_by_str(indicator)
    if std_indicator is not None:
        return std_indicator.value.data_type in (HealthDataType.SUMMARY, HealthDataType.MIX)
    return False


def is_series_indicator(indicator: str) -> bool:
    """A defined indicator whose data_type is SERIES or MIX; an empty name counts as series."""
    if not indicator:
        return True
    std_indicator = get_indicator_by_str(indicator)
    if std_indicator is not None:
        return std_indicator.value.data_type in (HealthDataType.SERIES, HealthDataType.MIX)
    return False


def is_valid_indicator(indicator: str) -> bool:
    """Check if indicator is a valid standard indicator"""
    return indicator in VALID_INDICATORS


def get_standard_unit(indicator: str) -> str:
    """Get standard unit for indicator"""
    info = _INDICATOR_LOOKUP.get(indicator)
    if info:
        return info.standard_unit
    raise ValueError(f"Unknown indicator: {indicator}")


def get_indicator_by_str(indicator: str) -> StandardIndicator | None:
    """
    Get StandardIndicator enum member by string identifier

    Args:
        indicator: The indicator string to search for

    Returns:
        StandardIndicator enum member if found, None otherwise
    """
    if not indicator:
        return None

    for std_indicator in StandardIndicator:
        if std_indicator.value.name == indicator:
            return std_indicator
    logger.warning(f"indicator {indicator} not found in StandardIndicator")
    return None


def get_indicators_in_same_categories(
    indicator_names: set[str],
    data_types: set[HealthDataType] | None = None,
) -> set[str]:
    """
    Expand a set of indicator names to all StandardIndicator names sharing their
    categories.

    Used by the data-repair reconcile to sweep sibling indicators in the same
    family. Example: a corrected sleep re-upload may only re-confirm
    `sleepAnalysis_Asleep(Deep)`, but TH-449-style corruption left duplicate rows
    under sibling stage indicators (`sleepAnalysis_Awake`, `_InBed`, ...). Expanding
    to the whole SLEEP category lets the sweep remove those siblings while still
    scoping to the relevant family (categories the batch never touched are excluded).

    Args:
        indicator_names: indicator names present in the repair batch.
        data_types: optional filter; only return indicators whose data_type is in
            this set (e.g. {HealthDataType.SERIES, HealthDataType.MIX} for series_data,
            {HealthDataType.SUMMARY, HealthDataType.MIX} for th_series_data).

    Returns:
        Canonical indicator names in the same categories (filtered by data_type).
        Runtime-generated aggregate indicator names (not in StandardIndicator) are
        never included, so this never matches derived aggregate rows.
    """
    category_names: set[str] = set()
    for name in indicator_names:
        std = get_indicator_by_str(name)
        if std is not None:
            category_names.add(std.value.category.name)

    if not category_names:
        return set()

    result: set[str] = set()
    for std in StandardIndicator:
        if std.value.category.name in category_names:
            if data_types is None or std.value.data_type in data_types:
                result.add(std.value.name)
    return result


def get_all_indicators_info() -> dict[str, Any]:
    """
    Get all indicator information (for frontend display)

    Returns:
        Dictionary containing all indicator information
    """
    result = {
        "categories": {},
        "indicators": {},
        "total_indicators": 0,
        "generated_at": datetime.now().isoformat(),
    }

    # Group indicators by category
    category_indicators = {}
    for indicator in StandardIndicator:
        category_key = indicator.value.category.name.lower()
        if category_key not in category_indicators:
            category_indicators[category_key] = {
                "category_info": indicator.value.category,
                "indicators": []
            }
        category_indicators[category_key]["indicators"].append(indicator)

    for category_key, data in category_indicators.items():
        category_info = data["category_info"]
        indicators_in_category = data["indicators"]

        # Deduplication: a name declared under several members appears once
        unique_indicators_in_category = []
        seen_names = set()
        for indicator in indicators_in_category:
            indicator_name = indicator.value.name
            if indicator_name not in seen_names:
                seen_names.add(indicator_name)
                unique_indicators_in_category.append(indicator)

        result["categories"][category_key] = {
            "name": category_info.name_zh,
            "name_en": category_info.name,
            "count": len(unique_indicators_in_category),
            "indicators": [],
        }

        for indicator in unique_indicators_in_category:
            indicator_data = {
                "key": indicator.value.name,
                "name": indicator.value.name_zh,
                "name_en": indicator.value.name,
                "description": indicator.value.description_zh,
                "standard_unit": indicator.value.standard_unit,
                "supported_units": [indicator.value.standard_unit],
                "category": category_key,
            }
            result["categories"][category_key]["indicators"].append(indicator_data)
            result["indicators"][indicator.value.name] = indicator_data

    result["total_indicators"] = len(result["indicators"])
    return result

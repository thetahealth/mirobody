"""② Translate: what a value MEANS.

    fold.py                  the identity of a printed name
    parse.py                 kind, number, comparator, unit
    local_day.py             the ONE implementation of "which day is this"
    series.py                what may be plotted on one axis
    code.py                  name + unit + value kind -> a Coding, never a guess
    indicators_info.py       the indicator catalogue
    canonical_units.py       a reading in the unit the catalogue declares
                             for its indicator (NOT `mirobody.units`, the
                             UCUM engine it borrows its arithmetic from)
    value_range_validator.py what counts as a plausible value
    aggregate/               a day of points to one number, and which source
                             publishes it
    derive/                  quantities nothing measured: sleep efficiency,
                             heart-rate range

Stage two of Collect, Translate, Agent. The input is what a report or a device
printed, verbatim; the output is what an analyst can group and compare: a
folded identity, a typed value, a UCUM unit, a local day, a LOINC code where
the vocabulary supports one, and a series key either way.

① Collect stores what a device or a document said, verbatim, and this stage
decides what it means. The five modules at the top of that list are the seam
1.4.4 left room for, and they are pure: no database, no clock, no model call.
That is what lets a coding be replayed under a newer vocabulary release
against the same frozen input and give the same answer, forever. The caller
owns the side effects — `mirobody.collect.observations` for the reference
server.

`aggregate/` is here because a daily total is the SAME quantity on a different
time axis, which is a LOINC axis change, and because the rules were already
here: `IndicatorInfo.aggregation_methods` declares them and `aggregate/` only
executes them. `derive/` sits beside it: sleep efficiency is total sleep over
time in bed, heart-rate range is max minus min. Nothing wore a sensor for
either. They are not collection, which is the point: ① Collect guarantees that
what a source said is stored cleanly and can be traced back, and computes
nothing on top.

`collect/` imports this package in 11 files, because a provider declares its
metrics with `StandardIndicator`; `mirobody.collect` therefore still
re-exports `StandardIndicator` and `UNIT_CONVERSIONS`, so a provider plugin
keeps one import path.

Nothing here is third-party: stdlib plus `mirobody` only. Keep it that way,
the same rule the library layer lives by.

Lazy (PEP 562) for everything that reaches the database: importing this
package must not pull the server stack. The pure seam is imported eagerly
because it costs a few stdlib modules and is what most callers came for.
"""

from typing import TYPE_CHECKING

from .fold import name_key, unit_key
from .local_day import local_day, resolve_tz, window_for, zone_for
from .outcome import (
    LOINC_SYSTEM,
    OUTCOME_CODED,
    OUTCOME_NEEDS_INPUT,
    OUTCOME_REFUSED,
    Alias,
    Coding,
)
from .parse import Parsed, parse_range, parse_value
from .series import Axes, local_series_id, property_dim, series_id
from .code import code, decision_id, release

#: The pure seam, imported above. `code` is the function, not the module;
#: `from mirobody.translate.code import code` still reaches the module.
_SEAM = [
    "Alias",
    "Axes",
    "Coding",
    "LOINC_SYSTEM",
    "OUTCOME_CODED",
    "OUTCOME_NEEDS_INPUT",
    "OUTCOME_REFUSED",
    "Parsed",
    "code",
    "decision_id",
    "local_day",
    "local_series_id",
    "name_key",
    "parse_range",
    "parse_value",
    "property_dim",
    "release",
    "resolve_tz",
    "series_id",
    "unit_key",
    "window_for",
    "zone_for",
]

# name -> submodule that defines it. Every symbol another package needs is
# here: 14 of them, measured, not guessed. A caller outside this package
# imports `mirobody.translate`, so 1.5.0 can rewrite the modules behind these
# names without a call-site edit anywhere else.
_EXPORTS = {
    # the catalogue
    "StandardIndicator": "indicators_info",
    "HealthDataType": "indicators_info",
    "get_all_indicators_info": "indicators_info",
    "get_indicator_by_str": "indicators_info",
    "get_indicators_in_same_categories": "indicators_info",
    "get_standard_unit": "indicators_info",
    "is_valid_indicator": "indicators_info",
    "is_series_indicator": "indicators_info",
    "is_summary_indicator": "indicators_info",
    "normalize_indicator_name": "indicators_info",
    # canonical units
    "UNIT_CONVERSIONS": "canonical_units",
    "convert_to_standard": "canonical_units",
    "get_all_units_info": "canonical_units",
    # ranges
    "ValueRangeValidator": "value_range_validator",
    "start_aggregate_indicator_scheduler": "aggregate.startup",
    "start_derived_scheduler": "derive.task",
    "AggregateIndicatorService": "aggregate.service",
    "AggregateDatabaseService": "aggregate.database_service",
    "build_indicator_name": "aggregate.naming",
    "get_all_aggregation_rules": "aggregate.rule_generator",
}
__all__ = [*_SEAM, *_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .indicators_info import (
        HealthDataType,
        StandardIndicator,
        get_all_indicators_info,
        get_indicator_by_str,
        get_indicators_in_same_categories,
        get_standard_unit,
        is_series_indicator,
        is_summary_indicator,
        is_valid_indicator,
        normalize_indicator_name,
    )
    from .aggregate.database_service import AggregateDatabaseService
    from .aggregate.naming import build_indicator_name
    from .aggregate.rule_generator import get_all_aggregation_rules
    from .aggregate.service import AggregateIndicatorService
    from .aggregate.startup import start_aggregate_indicator_scheduler
    from .derive.task import start_derived_scheduler
    from .canonical_units import UNIT_CONVERSIONS, convert_to_standard, get_all_units_info
    from .value_range_validator import ValueRangeValidator


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value

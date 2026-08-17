"""
Standardization layer — what a value *means*.

The indicator catalogue, unit conversion, value-range validation, the
indicator→fhir_id mapping, and the registry task that publishes the catalogue
to the database. Extracted from `pulse/core`, where these five sat between
auth, scheduler and push-service infrastructure and "standardization" was not
a place you could point to — it was a mental list of which core files counted.

Dependency rule that keeps the split meaningful: this package imports from
`mirobody.utils` and (for task wiring only, in `std_indicator_registry`) from
`pulse.core` — but the catalogue/units/validator modules themselves must not
depend on `pulse.core`, so they stay importable as pure data + DB reads.

Lazy (PEP 562), matching `mirobody/pulse/__init__.py` and
`mirobody/pulse/core/__init__.py`: importing this package must not pull the
server stack; each export pays its own import cost at first use.
"""

from typing import TYPE_CHECKING

# name -> submodule that defines it
_EXPORTS = {
    'StandardIndicator'       : 'indicators_info',
    'get_all_indicators_info' : 'indicators_info',
    'get_standard_unit'       : 'indicators_info',
    'is_valid_indicator'      : 'indicators_info',
    'convert_to_standard'     : 'units',
    'get_all_units_info'      : 'units',
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .indicators_info import StandardIndicator, get_all_indicators_info, get_standard_unit, is_valid_indicator
    from .units import convert_to_standard, get_all_units_info


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value

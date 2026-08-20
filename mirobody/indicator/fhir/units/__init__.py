"""Unit normalization for indicator resolve.

Two-step pipeline:

  free text ─► :func:`normalize_unit` ─► canonical UCUM
                                           │
                                           ▼
                                       :func:`unit_family`
                                           │
                                           ▼
                                       LOINC PROPERTY enum
                                       (MCnc, SCnc, NCnc, ...)

The LOINC PROPERTY axis is what actually disambiguates Mass/volume vs
Moles/volume LOINC codes that share component+time+system. By going
through this module, callers get one canonical form to store and one
small enum to filter on, without ever parsing UCUM grammar themselves.

See :mod:`.families` for the canonical-unit→family table and
:mod:`.normalize` for the alias / cleanup pipeline.

**Family is not convertibility.** :func:`unit_family` answers a LOINC PROPERTY
question, and using it to decide whether two units can be interconverted is
wrong in both directions: `kg/m2` (BMI) and `mg/dL` are both `MCnc` yet cannot
convert, while `U/L` (CCnc) and `[IU]/L` (ACnc) are 1:1 identical. That is what
:mod:`.convert` is for — it parses dimensions instead of classifying properties.
"""

from .families import (
    AMBIGUOUS_UNITS,
    UCUM_FAMILY,
    unit_families,
    unit_family,
)
from .convert import (
    MOLAR_MASS, conversion_factor, convert_value, convertible, partition_units,
    scale,
)
from .normalize import (
    ParsedQuantity, normalize_unit, parse_value_unit, scan_value_units,
)

__all__ = [
    "normalize_unit",
    "parse_value_unit",
    "scan_value_units",
    "ParsedQuantity",
    "unit_family",
    "unit_families",
    "UCUM_FAMILY",
    "AMBIGUOUS_UNITS",
    "convert_value",
    "convertible",
    "conversion_factor",
    "partition_units",
    "scale",
    "MOLAR_MASS",
]

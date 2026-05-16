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
"""

from .families import (
    AMBIGUOUS_UNITS,
    UCUM_FAMILY,
    unit_families,
    unit_family,
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
]

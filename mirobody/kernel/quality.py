"""Quality gates: the checks that decide whether a fact may enter analysis.

Two rules bound what belongs here. First, a gate rejects only what is
*physically or logically impossible* — an end before its start, a
percentage above 100, a total longer than the span it covers, a value whose
unit has a different dimension from the metric's. "Heart rate 190 is high"
is a reference range, an open-ended clinical asset this library does not
maintain. Second, every rejection carries a structured reason code, never a
free-text exception: a quarantine that can only be grepped is a pile the
next engineer cannot triage.

Pure functions; the consumer decides what a code means for its queue
(retry, park, drop).
"""

from __future__ import annotations

import math

from . import metrics
from .. import units
from .series import Fact

#: Reason codes. Consumers persist these literals on quarantined rows.
ERR_UNIT_DIMENSION_CONFLICT = "unit_dimension_conflict"
ERR_IMPOSSIBLE_TIME_RANGE = "impossible_time_range"
ERR_IMPOSSIBLE_VALUE = "impossible_value"
ERR_NO_TRUSTED_MAPPING = "no_trusted_mapping"
ERR_TRANSIENT = "transient"

#: Quality flags: the fact is admitted, with a note.
FLAG_UNIT_CONVERTED = "unit_converted"
FLAG_UNVERIFIED_UNIT = "unverified_unit"

#: An interval longer than this is a misused interval (a week filed as one
#: measurement), not a long measurement. Sleep and a marathon fit in 36h.
MAX_INTERVAL_MS = 36 * 3600 * 1000
#: Device clocks run fast; a measurement a day in the future is bad data.
FUTURE_TOLERANCE_MS = 24 * 3600 * 1000


def time_gate(fact: Fact, now_ms: int, *, max_interval_ms: int = MAX_INTERVAL_MS, future_tolerance_ms: int = FUTURE_TOLERANCE_MS) -> str:
    """``""`` when the timing is possible, else ``ERR_IMPOSSIBLE_TIME_RANGE``.
    Never fabricates a time: a fact without a start is rejected, not stamped
    with "now"."""
    if fact.effective_start_ms <= 0:
        return ERR_IMPOSSIBLE_TIME_RANGE
    if fact.effective_end_ms and fact.effective_end_ms < fact.effective_start_ms:
        return ERR_IMPOSSIBLE_TIME_RANGE
    if fact.effective_end_ms and fact.effective_end_ms - fact.effective_start_ms > max_interval_ms:
        return ERR_IMPOSSIBLE_TIME_RANGE
    if fact.effective_start_ms > now_ms + future_tolerance_ms:
        return ERR_IMPOSSIBLE_TIME_RANGE
    return ""


def value_gate(value: float | None, unit: str) -> str:
    """``""`` when the value is possible for its unit, else
    ``ERR_IMPOSSIBLE_VALUE``. Only dimension-level impossibilities: a
    percentage outside [0, 100], a non-finite number."""
    if value is None:
        return ""
    if value != value or value in (math.inf, -math.inf):
        return ERR_IMPOSSIBLE_VALUE
    if unit == "%" and not (0.0 <= value <= 100.0):
        return ERR_IMPOSSIBLE_VALUE
    return ""


def reconcile_unit(raw_unit: str, expected_ucum: str, value: float | None) -> tuple[float | None, str, str, str]:
    """Bring a fact's unit to the metric's canonical unit.

    Returns ``(value, unit, flag, error)``. Convertible units are converted
    and flagged ``unit_converted``; two units the engine *both* knows with
    different dimensions are an ``ERR_UNIT_DIMENSION_CONFLICT`` (a
    temperature filed under a mass); a unit the engine does not know is
    admitted as-is with ``unverified_unit`` — an unfamiliar but correct unit
    must not lock real data in quarantine.
    """
    incoming = units.normalize_unit(raw_unit) or raw_unit
    if not expected_ucum or not incoming or incoming == expected_ucum:
        return value, expected_ucum or incoming, "", ""
    if units.convertible(incoming, expected_ucum):
        converted = units.convert_value(value, incoming, expected_ucum) if value is not None else None
        return (converted if converted is not None else value), expected_ucum, FLAG_UNIT_CONVERTED, ""
    if units.unit_family(incoming) and units.unit_family(expected_ucum):
        return value, incoming, "", ERR_UNIT_DIMENSION_CONFLICT
    return value, incoming, FLAG_UNVERIFIED_UNIT, ""


def overcount_suspect(total_ms: float, span_ms: float, *, tolerance_ratio: float = 1.0, floor_ms: float = 0.0) -> bool:
    """A total duration that exceeds the wall-clock span it was measured in.
    This is arithmetic, not a heuristic: the union of intervals inside a span
    cannot be longer than the span. Fires on every aggregation pass, so a
    re-aggregation of a repaired cell is checked again, not just the first."""
    return total_ms > span_ms * tolerance_ratio + floor_ms


def is_echo(value: float | str | None, last_value: float | str | None) -> bool:
    """A source re-reporting the value it last stored. A profile field on a
    wearable comes back on every sync with today's date; storing it again
    manufactures a fresh-looking candidate that can beat a real scale."""
    if last_value is None:
        return False
    try:
        return math.isclose(float(value), float(last_value), rel_tol=1e-9, abs_tol=1e-9)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return value == last_value


def cross_source_ratio(a: float, b: float) -> float:
    """``max/min`` of two positive values, ``inf`` when one is zero — the
    signal a monitor uses to flag two sources disagreeing by orders of
    magnitude (a unit slipped: minutes stored as milliseconds)."""
    if a <= 0 or b <= 0:
        return math.inf
    return max(a, b) / min(a, b)


def shape_for(system: str, metric_key: str) -> metrics.Mapping | None:
    """Convenience: the catalogue mapping a normaliser needs, or ``None`` —
    which the caller turns into ``ERR_NO_TRUSTED_MAPPING``."""
    return metrics.mapping_for(system, metric_key)


__all__ = [
    "ERR_UNIT_DIMENSION_CONFLICT", "ERR_IMPOSSIBLE_TIME_RANGE", "ERR_IMPOSSIBLE_VALUE", "ERR_NO_TRUSTED_MAPPING", "ERR_TRANSIENT",
    "FLAG_UNIT_CONVERTED", "FLAG_UNVERIFIED_UNIT", "MAX_INTERVAL_MS", "FUTURE_TOLERANCE_MS",
    "time_gate", "value_gate", "reconcile_unit", "overcount_suspect", "is_echo", "cross_source_ratio", "shape_for",
]

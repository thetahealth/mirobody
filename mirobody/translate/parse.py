"""The typed layer of a value: what kind of thing was printed, and its number.

A report prints "5.62", "<0.5", "阴性", "++", "120/80", "见描述". The
original text is stored as printed and never edited; this module derives the
columns an analyst filters on beside it. The kinds follow FHIR Observation's
value[x] choice, collapsed to what a table can hold:

    quantity    a number, optionally with a comparator and a UCUM unit
    ordinal     a graded marker: +, ++, trace, 微量
    nominal     a label: positive, negative, O+, 阴性
    narrative   text that is none of the above, including "120/80"
    absent      nothing printed; `data_absent_reason` says why

A quantity ALWAYS carries a number (the table checks it), and a number is
never invented: "120/80" is two quantities the extractor should have split,
and until it does the row is narrative rather than a blood pressure of 120.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from mirobody.units import normalize_unit, parse_value_unit
from mirobody.value_scale import classify_value

KIND_QUANTITY = "quantity"
KIND_ORDINAL = "ordinal"
KIND_NOMINAL = "nominal"
KIND_NARRATIVE = "narrative"
KIND_ABSENT = "absent"
KINDS = (KIND_QUANTITY, KIND_ORDINAL, KIND_NOMINAL, KIND_NARRATIVE, KIND_ABSENT)

#: FHIR data-absent-reason for "the report printed nothing here".
ABSENT_UNKNOWN = "unknown"

_COMPARATORS = {"<": "<", "<=": "<=", "≤": "<=", ">": ">", ">=": ">=", "≥": ">="}
_SCALE_TO_KIND = {"qn": KIND_QUANTITY, "ord": KIND_ORDINAL, "nom": KIND_NOMINAL, "nar": KIND_NARRATIVE}

_NUM = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
_RANGE = re.compile(rf"^\s*({_NUM})\s*(?:-|~|～|－|–|—|to|至)\s*({_NUM})")
_BOUND = re.compile(rf"^\s*([<>≤≥]=?)\s*({_NUM})")
#: The comparator and number a value cell opens with; what follows is the
#: printed unit, or prose. Text that opens with a divisor, a power or a
#: multiplication continues a unit rather than following one.
_NUMBER_HEAD = re.compile(rf"^\s*[-<>=+~≤≥≈]*\s*{_NUM}")
_CONTINUES_UNIT = re.compile(r"^(?:[/^·×]|[*.](?=[0-9A-Za-z{(]))")


def _split_unit(tail: str, ucum: str) -> tuple[str, str]:
    """`(printed, rest)`: the longest prefix of `tail` that reads as `ucum`,
    and what follows it."""
    for end in range(len(tail), 0, -1):
        if normalize_unit(tail[:end]) == ucum:
            return tail[:end].strip(), tail[end:].strip()
    return tail, ""


@dataclass(frozen=True)
class Parsed:
    """The typed layer of one value. `unit_ucum` is `""` when the reading has
    no unit or its unit did not normalize; the printed unit is kept by the
    caller either way."""

    value_kind: str
    value_num: float | None = None
    comparator: str = ""
    unit_ucum: str = ""
    data_absent_reason: str = ""
    #: The unit as printed inside the value cell ("5.62 mmol/L" gives
    #: "mmol/L"), for the caller to keep as the verbatim unit when the unit
    #: column was empty. Empty when the unit came from its own column.
    unit_tail: str = ""


def parse_value(value_text: str, unit_text: str = "") -> Parsed:
    """`value_text` as printed (with or without its unit inside) and the unit
    column as printed, to the typed layer.

    The unit printed in its own column wins over one glued to the value:
    "5.62" + "mmol/L" and "5.62 mmol/L" + "" both give `mmol/L`. A value that
    parses as a number followed by something that is NOT a unit ("120/80",
    "5.6 (H)") is not a quantity: the trailing text is information this
    function must not discard by keeping only the number.
    """
    text = (value_text or "").strip()
    unit_ucum = (normalize_unit(unit_text) or "") if unit_text else ""
    if not text:
        return Parsed(KIND_ABSENT, data_absent_reason=ABSENT_UNKNOWN)

    parsed = parse_value_unit(text)
    if parsed.value is not None and math.isfinite(parsed.value):
        comparator = _COMPARATORS.get(parsed.comparator, "")
        if unit_ucum:
            return Parsed(KIND_QUANTITY, parsed.value, comparator, unit_ucum)
        head = _NUMBER_HEAD.match(text)
        tail = text[head.end():].strip() if head else ""
        if not tail:
            return Parsed(KIND_QUANTITY, parsed.value, comparator, parsed.unit or "")
        whole = normalize_unit(tail) or ""
        if whole or not parsed.unit:
            return Parsed(KIND_QUANTITY, parsed.value, comparator, whole, unit_tail=tail if whole else "")
        printed, rest = _split_unit(tail, parsed.unit)
        # "150 mg/dL ↑" and "70克葡萄糖" read a unit and then a flag or a noun.
        # "88 mL/(min.1,73 m2)" reads "mL" and then MORE UNIT: the longest
        # readable prefix is a different unit, not a reading of this one, so
        # the typed unit stays empty and the tail is kept for the verbatim
        # column.
        if _CONTINUES_UNIT.match(rest):
            return Parsed(KIND_QUANTITY, parsed.value, comparator, "", unit_tail=tail)
        return Parsed(KIND_QUANTITY, parsed.value, comparator, parsed.unit, unit_tail=printed)

    kind = _SCALE_TO_KIND.get(classify_value(text) or "", KIND_NARRATIVE)
    if kind == KIND_QUANTITY:
        # A digit-led value the unit parser refused: a pair, a date, a code.
        kind = KIND_NARRATIVE
    return Parsed(kind, unit_ucum=unit_ucum)


def parse_range(ref_text: str) -> tuple[float | None, float | None]:
    """`(low, high)` read off a printed reference range, either bound `None`
    when the text does not state it. "4.0-10.0", "4.0～10.0 ×10⁹/L", "<5.0",
    "≥ 60", "40 to 60". Prose ("阴性", "see report") gives `(None, None)`."""
    text = (ref_text or "").strip()
    if not text:
        return None, None
    m = _RANGE.match(text)
    if m:
        low, high = float(m.group(1)), float(m.group(2))
        return (low, high) if low <= high else (high, low)
    m = _BOUND.match(text)
    if m:
        number = float(m.group(2))
        return (None, number) if m.group(1).startswith(("<", "≤")) else (number, None)
    return None, None


__all__ = [
    "ABSENT_UNKNOWN",
    "KINDS",
    "KIND_ABSENT",
    "KIND_NARRATIVE",
    "KIND_NOMINAL",
    "KIND_ORDINAL",
    "KIND_QUANTITY",
    "Parsed",
    "parse_range",
    "parse_value",
]

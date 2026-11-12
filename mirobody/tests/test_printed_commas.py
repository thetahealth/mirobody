"""A comma inside a printed number is read by one rule (`units.numerals`).

The value parser read every comma as a decimal mark, so `250,000 /uL` was a
platelet count of 250; the evidence check and the answer check read the same
comma as thousands. These cases pin the rule and that every reader uses it.
The save path (stored number and evidence verdict together) is covered in
`test_extraction_regressions.py`, which needs the application extras.

`kernel.citations` cannot import the rule (`test_the_module_is_stdlib_only`)
and carries its own copy; where that copy still reads differently, the case
is a strict xfail, so a fix there turns it green here.
"""

from __future__ import annotations

import pytest

from mirobody import standardize
from mirobody.kernel import citations
from mirobody.units import numerals

#: printed digits, their plain reading (None: no reading as one number)
READINGS = [
    ("250,000", "250000"),          # a US platelet count
    ("12,500", "12500"),
    ("1,250", "1250"),              # three digits after one comma: thousands
    ("1,250,000", "1250000"),
    ("2,50,000", "250000"),         # lakh grouping
    ("1,250.5", "1250.5"),
    ("1,25", "1.25"),               # a European decimal comma
    ("5,6", "5.6"),
    ("0,125", "0.125"),             # a grouping never starts with 0
    ("1234,567", "1234.567"),       # nor has four digits before its comma
    ("1,2345", "1.2345"),
    ("72,66,65", None),             # a list
    ("1,25.5", None),
    ("1,250,00", None),
    ("5.6", "5.6"),
    ("250", "250"),
]


@pytest.mark.parametrize("printed,reading", READINGS)
def test_a_comma_has_one_reading(printed, reading):
    assert numerals.plain(printed) == reading


@pytest.mark.parametrize("printed,reading", [(p, r) for p, r in READINGS if r is not None])
def test_the_value_parser_stores_the_reading(printed, reading):
    assert standardize.parse_value(printed, "/uL").value_num == float(reading)


#: Where `citations.numbers` (stdlib-only, its own copy) still differs.
_CITATIONS_DIFFER = {
    "1,250,000": "reads one comma per number: 1250 and 0",
    "2,50,000": "reads one comma per number: 2.5 and 0",
    "1,250.5": "reads 1250 and drops the fraction",
    "0,125": "reads three digits after a comma as thousands even after a 0",
    "1234,567": "reads three digits after a comma as thousands after four digits",
}


@pytest.mark.parametrize("printed,reading", [
    pytest.param(p, r, marks=pytest.mark.xfail(strict=True, reason=_CITATIONS_DIFFER[p]))
    if p in _CITATIONS_DIFFER else (p, r)
    for p, r in READINGS if r is not None
])
def test_the_answer_check_reads_a_comma_as_the_value_was_stored(printed, reading):
    """An answer quoting a stored value is checked against the number the
    parser stored; `250,000` must not be an unsupported 250000 beside a
    stored 250, or the reverse."""
    assert citations.numbers(f"the count was {printed}") == [float(reading)]


def test_a_list_is_not_one_value():
    assert standardize.parse_value("72,66,65", "/uL").value_num is None


@pytest.mark.parametrize("cell,value_num,unit_tail", [
    ("250,000 /uL", 250000.0, "/uL"),   # kept `,000 /uL` as its printed unit
    ("7,8 mmol/L", 7.8, "mmol/L"),
])
def test_the_printed_unit_is_what_follows_the_whole_number(cell, value_num, unit_tail):
    parsed = standardize.parse_value(cell)
    assert (parsed.value_num, parsed.unit_tail) == (value_num, unit_tail)


@pytest.mark.parametrize("ref,bounds", [
    ("150,000-400,000", (150000.0, 400000.0)),
    ("1,50,000 - 4,50,000", (150000.0, 450000.0)),
    ("2,15-2,55", (2.15, 2.55)),
    ("<5,0", (None, 5.0)),
    ("72,66,65", (None, None)),
])
def test_a_range_printed_with_commas_is_read(ref, bounds):
    """Both conventions read as no range at all."""
    assert standardize.parse_range(ref) == bounds


def test_fold_credits_a_decimal_comma_as_a_separator_only_when_asked():
    text = "Ca 1,25; PLT 250,000; 2026-08-01,72"
    assert numerals.fold(text) == "Ca 1.25; PLT 250000; 2026-08-01.72"
    assert numerals.fold(text, separators=True) == "Ca 1,25; PLT 250000; 2026-08-01,72"

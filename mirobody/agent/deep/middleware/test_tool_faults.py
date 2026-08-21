"""Golden cases for the malformed-argument salvage.

The bug this guards: a model re-emits the same unparseable arguments every time
it is asked to fix them, the repair budget runs out, and the turn ends with an
empty answer over "Answer Completed". Observed against `query_health_indicators`
with `"start_time": 2024-08-21` (unquoted) and `"aggregate": none` (bare).

The salvage is deliberately narrow, so the "leave it alone" half of this file
matters as much as the other: it only ever sees text LangChain already refused,
and it must return None — not a guess — for anything it does not recognise.
"""

import pytest

from .tool_faults import _salvage_json_args


#: Verbatim from a production log; the whole reason this function exists.
_REAL_WORLD = (
    '{"keywords": ["lipid panel","Total Cholesterol","LDL","HDL","Triglycerides"], '
    '"start_time": 2024-08-21, "end_time": 2026-08-21, "aggregate": none}'
)


def test_the_call_that_motivated_this():
    got = _salvage_json_args(_REAL_WORLD)
    assert got == {
        "keywords": ["lipid panel", "Total Cholesterol", "LDL", "HDL", "Triglycerides"],
        "start_time": "2024-08-21",
        "end_time": "2026-08-21",
        "aggregate": None,
    }


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('{"a": none}', {"a": None}),
        ('{"a": None}', {"a": None}),
        ('{"a": True, "b": False}', {"a": True, "b": False}),
        ('{"d": 2024-08-21}', {"d": "2024-08-21"}),
        ('{"d": 2024-08-21T10:30}', {"d": "2024-08-21T10:30"}),
        ('{"d": 2024-08-21 10:30:15}', {"d": "2024-08-21 10:30:15"}),
        ('{"a": 1,}', {"a": 1}),
        ('{"a": [1, 2,]}', {"a": [1, 2]}),
    ],
)
def test_shapes_we_repair(raw, expected):
    assert _salvage_json_args(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # Already valid: it never reaches us in practice, and if it does we must
        # not pretend to have fixed something.
        '{"a": 1}',
        '{"a": "none"}',
        '{"a": "2024-08-21"}',
        # Nothing we know how to repair. Returning None sends it back to the
        # model, which is the honest answer.
        "not json at all",
        '{"a": undefined}',
        '{"a": NaN}',
        "",
        None,
        # A JSON array is valid JSON but not an argument mapping.
        "[1, 2, 3]",
    ],
)
def test_shapes_we_leave_alone(raw):
    assert _salvage_json_args(raw) is None


def test_a_quoted_none_inside_a_string_is_not_rewritten():
    """`"none"` is the real default of `aggregate`; only the bare form is a bug."""
    assert _salvage_json_args('{"aggregate": "none", "x": none}') == {
        "aggregate": "none",
        "x": None,
    }

"""#302: a Chinese per-day count written with a digit (`每日2次`) was read as
the bare `每日`/`每天` = once-daily rule, because the digit forms were only
wired up for `每天`, not `每日`/`一天`/`一日`/`1日`, and only for a couple of the
four counts. `一日3次，一次1片` lost the frequency entirely and kept the dose,
which `parse_dose_instruction`'s docstring forbids ("no partial parses").

These pin the issue's own table: every prefix (每天/每日/一天/一日/1日) crossed
with a digit or numeral count, spaces around the digit, a full-width comma
between dose and frequency, two different named counts in one sig (`None`),
and a count the table has no slot shape for (`None`, not once-daily).
"""

from __future__ import annotations

import pytest

from mirobody.kernel.meds import Dose, parse_dose_instruction


def _summary(text: str) -> tuple[int, int | None, int, Dose | None] | None:
    parsed = parse_dose_instruction(text)
    if parsed is None:
        return None
    instruction = parsed[0]
    return (instruction.doses_per_day, instruction.period_days, len(instruction.slots()), instruction.dose)


_TABLET = Dose(1.0, "{tablet}")

# (text, (doses_per_day, period_days, slot_count, dose)) — the issue's own table.
CASES = [
    ("每天2次", (2, None, 2, None)),
    ("每天 2 次", (2, None, 2, None)),
    ("每日2次", (2, None, 2, None)),
    ("每日3次", (3, None, 3, None)),
    ("每日 1 次", (0, 1, 1, None)),
    ("一天2次", (2, None, 2, None)),
    ("一日2次", (2, None, 2, None)),
    ("一日1次", (0, 1, 1, None)),
    ("1日1次", (0, 1, 1, None)),
    ("一日3次，一次1片", (3, None, 3, _TABLET)),
    ("每日2次，每次1片", (2, None, 2, _TABLET)),
    ("每天三次", (3, None, 3, None)),
    ("每天4次", (4, None, 4, None)),
    ("每日4次", (4, None, 4, None)),
    ("一天四次", (4, None, 4, None)),
    ("一日四次", (4, None, 4, None)),
    ("1日4次", (4, None, 4, None)),
    # Spaces around the digit and a full-width comma, both common in text
    # copied from forms and package inserts.
    ("每日 2 次,每次 1 片", (2, None, 2, _TABLET)),
    ("每日２次", (2, None, 2, None)),  # full-width digit, folded by NFKC
]


@pytest.mark.parametrize(("text", "expected"), CASES, ids=[c[0] for c in CASES])
def test_the_issues_table(text, expected):
    assert _summary(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "每日2次 每天三次",  # two different named counts: two regimens, not one
        "每日2次 一日三次",
    ],
)
def test_two_named_counts_in_one_sig_is_no_parse(text):
    assert parse_dose_instruction(text) is None


@pytest.mark.parametrize(
    "text",
    [
        "每日10次",  # a real count, but outside the table's four slot shapes
        "一天五次",
        "每日十次",
    ],
)
def test_a_count_the_table_cannot_read_is_no_parse(text):
    assert parse_dose_instruction(text) is None


def test_an_unreadable_count_does_not_leave_a_partial_parse():
    """The frequency can't be read, so the dose that's also in the sentence
    must not surface alone — exactly the shape #302 reported for `一日3次`
    before the fix, now reproduced with a count outside the table instead."""
    assert parse_dose_instruction("每日10次，每次1片") is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Existing Chinese and English sigs: unchanged by the fix.
        ("每天两次", (2, None, 2, None)),
        ("每日一次", (0, 1, 1, None)),
        ("一日两次", (2, None, 2, None)),
        ("bid", (2, None, 2, None)),
        ("tid", (3, None, 3, None)),
        ("qid", (4, None, 4, None)),
        ("500 mg twice daily", (2, None, 2, Dose(500.0, "mg"))),
        ("500mg 每天早晚", (2, None, 2, Dose(500.0, "mg"))),
        ("500mg 每天早中晚", (3, None, 3, Dose(500.0, "mg"))),
        ("q8h", (3, None, 3, None)),
    ],
)
def test_existing_sigs_are_unaffected(text, expected):
    assert _summary(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "1-2 tablets",
        "1 in the morning and 0.5 in the evening",
        "with food",
    ],
)
def test_still_no_parse_for_the_documented_non_cases(text):
    assert parse_dose_instruction(text) is None

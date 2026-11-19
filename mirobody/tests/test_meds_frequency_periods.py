"""The same bug as #302, one scale up: a period or a weekly count not
written in the handful of literal phrases ``_WEEKLY_RE``/``_EVERY_N_DAYS_RE``
spelled out fell back to the bare 每周/每天 = once rule instead of being read,
or being refused outright when ``Schedule`` has no field for it.

``每周2次`` ("twice a week") used to read as ``period_days=7`` (once a week),
silently dropping the "2"; ``一日3次` 's sibling at the period scale,
``每2日1次`` ("every 2 days"), did not match at all (``每\\s*(\\d+)\\s*天``
required 天, not 日, and had no numeral form) and returned ``None`` even
though "every 2 days" is a plain ``period_days=2`` schedule. ``隔一日一次``
("every other day", spelled out) fell through to the per-day grammar's
``一日`` prefix and came back once DAILY instead of every other day.

The fix reads prefix (每/一/隔) + an optional count + a period
(天/日/周/星期, and the English day/week/wk spellings) + an optional
trailing count + 次/times as one grammar, the same shape the per-day count
fix used. ``Schedule`` has an interval (``period_days``) and named weekdays,
but no "N times a week" field, so a weekly count other than one, and a
period together with a per-occurrence count other than one, both refuse
(``None``) rather than guess or default to once.
"""

from __future__ import annotations

import pytest

from mirobody.kernel.meds import parse_dose_instruction


def _summary(text: str) -> tuple[int, int | None, int, tuple[int, ...]] | None:
    parsed = parse_dose_instruction(text)
    if parsed is None:
        return None
    instruction = parsed[0]
    return (
        instruction.doses_per_day,
        instruction.period_days,
        len(instruction.slots()),
        tuple(sorted(instruction.weekdays)),
    )


# (text, (doses_per_day, period_days, slot_count, weekdays)) — the issue's own table,
# for every weekly count that used to read as "once a week".
UNREPRESENTABLE_WEEKLY_COUNTS = [
    "每周2次",
    "每周 2 次",
    "每周两次",
    "twice weekly",
    "一周2次",
    "2 times a week",
]


@pytest.mark.parametrize("text", UNREPRESENTABLE_WEEKLY_COUNTS)
def test_a_weekly_count_with_no_named_weekdays_is_no_parse(text):
    """``Schedule`` has no "N times a week" field: only an interval or named
    weekdays. A count other than one refuses rather than reading as the
    bare 每周/一周 = once-weekly rule (the #302 pattern, one scale up)."""
    assert parse_dose_instruction(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("隔一日一次", (0, 2, 1, ())),  # spelled out: still "every other day", not once daily
        ("隔天", (0, 2, 1, ())),
        ("隔日", (0, 2, 1, ())),
        ("every other day", (0, 2, 1, ())),
        ("qod", (0, 2, 1, ())),
        ("每2日1次", (0, 2, 1, ())),
        ("每两日一次", (0, 2, 1, ())),
        ("每2周1次", (0, 14, 1, ())),
        ("每两周一次", (0, 14, 1, ())),
        ("every 2 weeks", (0, 14, 1, ())),
        ("every 3 days", (0, 3, 1, ())),
        ("q3d", (0, 3, 1, ())),
    ],
)
def test_a_period_multiplier_or_gap_is_read(text, expected):
    assert _summary(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("每周", (0, 7, 1, ())),
        ("每星期", (0, 7, 1, ())),
        ("每周一次", (0, 7, 1, ())),
        ("每星期一次", (0, 7, 1, ())),
        ("一周一次", (0, 7, 1, ())),
        ("weekly", (0, 7, 1, ())),
        ("once a week", (0, 7, 1, ())),
        ("once weekly", (0, 7, 1, ())),
        ("every week", (0, 7, 1, ())),
    ],
)
def test_bare_or_explicit_once_weekly_is_unchanged(text, expected):
    """The already-correct cases: no count, or a count of exactly one."""
    assert _summary(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # A 每周 filler before a weekday list is still a weekday list, not a
        # second, conflicting "once weekly" statement (周一、三、五 is the
        # documented zh form; 每周一、三、五 adds the filler word).
        ("每周一、三、五", (0, None, 1, (1, 3, 5))),
        ("周一、三、五", (0, None, 1, (1, 3, 5))),
        ("on Monday and Thursday", (0, None, 1, (1, 4))),
    ],
)
def test_weekday_lists_are_unaffected(text, expected):
    assert _summary(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "每日2次 每天三次",  # pre-existing: two different per-day counts
        "每周2次 每天三次",  # new: a weekly count and a daily count, two different kinds
        "每2周1次 每3周1次",  # two different periods named
        "每2天3次",  # a period AND a per-occurrence count other than one: no field holds both
    ],
)
def test_two_named_frequencies_in_one_sig_is_no_parse(text):
    assert parse_dose_instruction(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Existing sigs the fix must not change.
        ("每天2次", (2, None, 2, ())),
        ("每日3次", (3, None, 3, ())),
        ("每天三次", (3, None, 3, ())),
        ("一日3次", (3, None, 3, ())),
        ("500 mg twice daily", (2, None, 2, ())),
        ("500mg 每天早晚", (2, None, 2, ())),
        ("500mg 每天早中晚", (3, None, 3, ())),
        ("q8h", (3, None, 3, ())),
        ("每8小时1次", (3, None, 3, ())),
        ("one every other day", (0, 2, 1, ())),
    ],
)
def test_existing_sigs_are_unaffected(text, expected):
    assert _summary(text) == expected

"""#349 and #350, and the rest of the family they belong to.

Both issues are the same defect in ``parse_dose_instruction``: the English
frequency grammar was a list of literal phrases, and a phrase it did not
know left a fragment behind that it did know, which then read as something
else, with no error.

* #349: ``500 mg 3 times daily`` and ``1 tablet 2x daily`` read as ONCE
  daily. The per-day rows spelled out ``three times daily`` but not the
  digit, so the bare ``daily`` left over matched the once-daily row. The
  same fallback made ``several times daily`` once daily, ``three times
  weekly`` and ``3x weekly`` once weekly, and ``每天数次`` once daily.
* #350: ``10 mg once a month`` read as every Monday. The weekday pattern
  matched any word starting with ``mon``/``sat``/``sun``/``fri``…, so
  ``month``, ``monitor``, ``months``, ``saturated``, ``sunlight`` and
  ``fridge`` were weekdays: a monthly dose became four a month, and ``daily,
  monitor blood pressure`` became two frequencies and no parse.

Found while fixing those, the same shape elsewhere: ``Mon-Fri`` and
``周一至周五`` read as the two end days only; ``1 to 2 tablets`` and
``每次1到2片`` read as the upper bound; ``q24h`` came back unscheduled (a
count of one fell through every branch); ``every 5 hours`` and ``once a
month`` kept the dose and dropped the frequency; ``1 tab t.i.d.`` never
matched (``\\b`` after the final dot needs a letter to follow); ``twice a
week on Mon and Thu`` and ``每周一次，周日服用`` were refused although the
named days ARE the schedule; and in FHIR's structured ``Timing``, two doses
every 3 days read as one, ``q4-6h`` as ``q4h``, twice a week at 08:00 as
every day at 08:00, and a weekday list dropped the period or count beside it.

The contract these pin is the docstring's: a frequency is read whole, or
the parse is ``None``. Never a smaller regimen, never the nearest guess.
"""

from __future__ import annotations

from datetime import date

import pytest

from mirobody.kernel.meds import (
    Dose,
    MedicationMention,
    describe_schedule,
    from_fhir_medication_statement,
    parse_dose_instruction,
    reconcile_mentions,
)

# (kind, doses on each due day, period_days, weekdays, clock times)
Summary = tuple[str, int, int | None, tuple[int, ...], tuple[str, ...]]

ONCE_DAILY: Summary = ("daily", 1, 1, (), ())
TWICE_DAILY: Summary = ("daily", 2, None, (), ())
THRICE_DAILY: Summary = ("daily", 3, None, (), ())
FOUR_DAILY: Summary = ("daily", 4, None, (), ())
WEEKLY: Summary = ("interval", 1, 7, (), ())
PER_DAY = {1: ONCE_DAILY, 2: TWICE_DAILY, 3: THRICE_DAILY, 4: FOUR_DAILY}


def _summary(text: str) -> Summary | None:
    parsed = parse_dose_instruction(text)
    if parsed is None:
        return None
    parts = describe_schedule(parsed[0])
    return (parts.kind, parts.doses_per_day, parts.period_days, parts.weekdays, parts.times)


def _interval(days: int) -> Summary:
    return ("interval", 1, days, (), ())


def _weekly(*days: int, per_day: int = 1, times: tuple[str, ...] = ()) -> Summary:
    return ("weekly", per_day, None, tuple(sorted(days)), times)


def _fixed(*times: str) -> Summary:
    return ("fixed_times", len(times), None, (), tuple(sorted(times)))


def _cases(table: list[tuple[str, Summary | None]]):
    return pytest.mark.parametrize(("text", "expected"), table, ids=[t for t, _ in table])


def _texts(texts: list[str]):
    return pytest.mark.parametrize("text", texts, ids=texts)


# --- #349: a count per day, said with a digit, a word or an "x" ----------------------------


@_cases([
    # The issue's own table.
    ("500 mg three times daily", THRICE_DAILY),
    ("500 mg 3 times daily", THRICE_DAILY),
    ("1 tablet 2x daily", TWICE_DAILY),
    ("2 times a day", TWICE_DAILY),
    # Every preposition a count is joined to its day with.
    ("2 times daily", TWICE_DAILY),
    ("two times daily", TWICE_DAILY),
    ("three times per day", THRICE_DAILY),
    ("twice per day", TWICE_DAILY),
    ("3 times per day", THRICE_DAILY),
    ("3 times each day", THRICE_DAILY),
    ("3 times every day", THRICE_DAILY),
    ("3 times in a day", THRICE_DAILY),
    ("3 times per 24 hours", THRICE_DAILY),
    ("3x/day", THRICE_DAILY),
    ("3x/d", THRICE_DAILY),
    ("3x a day", THRICE_DAILY),
    ("3 x daily", THRICE_DAILY),
    ("3×daily", THRICE_DAILY),
    ("4x daily", FOUR_DAILY),
    ("four times per day", FOUR_DAILY),
    ("thrice daily", THRICE_DAILY),
    ("1 time daily", ONCE_DAILY),
    ("1x daily", ONCE_DAILY),
    ("one time a day", ONCE_DAILY),
    ("once per day", ONCE_DAILY),
    ("once every day", ONCE_DAILY),
    ("THREE TIMES DAILY", THRICE_DAILY),
    # With a dose in front, in every form a sig carries one.
    ("2 tablets 3 times a day", THRICE_DAILY),
    ("take 1 capsule by mouth 2 times daily with food", TWICE_DAILY),
    ("10 units 3x daily before meals", THRICE_DAILY),
    ("2 puffs 4 times a day", FOUR_DAILY),
    ("500mg 2x/day", TWICE_DAILY),
])
def test_a_per_day_count_is_read_in_every_spelling(text, expected):
    assert _summary(text) == expected


COUNT_SPELLINGS = {
    1: ["1 time", "1x", "1 x", "one time", "once"],
    2: ["2 times", "2x", "2 x", "two times", "twice"],
    3: ["3 times", "3x", "3 x", "three times", "thrice"],
    4: ["4 times", "4x", "4 x", "four times"],
}
DAY_TAILS = ["daily", "a day", "per day", "/day", "each day", "every day"]
DOSE_PREFIXES = ["", "500 mg ", "1 tablet ", "2 puffs "]


@pytest.mark.parametrize("prefix", DOSE_PREFIXES)
@pytest.mark.parametrize("tail", DAY_TAILS)
@pytest.mark.parametrize(
    ("n", "count"), [(n, c) for n, spellings in COUNT_SPELLINGS.items() for c in spellings]
)
def test_every_count_spelling_crossed_with_every_day_tail(n, count, tail, prefix):
    """The cross product, so no spelling is left that falls back to the bare
    "daily" row: before #349, 50 of these 19 × 6 × 4 sigs read as once daily."""
    text = f"{prefix}{count} {tail}".replace(" /", "/")
    assert _summary(text) == PER_DAY[n], text


@_cases([
    ("1 tablet a day", ONCE_DAILY),
    ("1 tablet per day", ONCE_DAILY),
    ("everyday", ONCE_DAILY),
    ("1 tablet daily for a week", ONCE_DAILY),  # "a week" after "for" is a duration
])
def test_every_day_in_its_plain_spellings(text, expected):
    """Kept the dose and dropped the frequency before: "a day" and "per day"
    were only read after a spelled-out count."""
    assert _summary(text) == expected


@_texts([
    "5 times daily",
    "five times daily",
    "6x daily",
    "10 times a day",
    "12x per day",
    "0 times daily",
    "500 mg 5 times daily",
])
def test_a_per_day_count_with_no_slot_shape_refuses_rather_than_reading_once(text):
    """The same 1–4 the Chinese count accepts (每日10次 is ``None``)."""
    assert parse_dose_instruction(text) is None


@_texts([
    "several times daily",
    "a few times a day",
    "multiple times per day",
    "many times daily",
    "每天数次",
    "每日多次",
    "一天几次",
    "每天好几次",
    "每天十二次",
])
def test_a_count_with_no_number_refuses_rather_than_reading_once(text):
    assert parse_dose_instruction(text) is None


# --- #350: a weekday is a whole word ------------------------------------------------------


@_cases([
    # The issue's own table.
    ("5 mg daily, monitor blood pressure", ONCE_DAILY),
    # Every word a prefix match read as a weekday.
    ("1 tablet daily for 3 months", ONCE_DAILY),
    ("1 tablet daily, store in fridge", ONCE_DAILY),
    ("1 tablet daily, avoid sunlight", ONCE_DAILY),
    ("apply sunscreen daily", ONCE_DAILY),
    ("1 tablet daily, check oxygen saturation", ONCE_DAILY),
    ("1 tablet daily if saturated fat is high", ONCE_DAILY),
    ("1 tablet daily, avoid sun", ONCE_DAILY),  # "sun" alone is the word
    ("1 tablet daily, sat upright", ONCE_DAILY),
    ("1 tablet daily, monitoring weekly labs", None),  # the weekly is a real second frequency
    ("1 tablet daily, thumb", ONCE_DAILY),
    ("1 tablet daily for wedding", ONCE_DAILY),
    ("1 tablet daily, take with tuesday's dose", None),  # a real weekday: two frequencies
])
def test_a_word_that_starts_like_a_weekday_is_not_one(text, expected):
    assert _summary(text) == expected


@_cases([
    ("Monday", _weekly(1)),
    ("Mondays", _weekly(1)),
    ("mon", _weekly(1)),
    ("Mon.", _weekly(1)),
    ("tue", _weekly(2)),
    ("tues", _weekly(2)),
    ("Tuesday", _weekly(2)),
    ("Wednesday", _weekly(3)),
    ("weds", _weekly(3)),
    ("thu", _weekly(4)),
    ("thur", _weekly(4)),
    ("Thurs", _weekly(4)),
    ("Thursdays", _weekly(4)),
    ("fri", _weekly(5)),
    ("Friday", _weekly(5)),
    ("Saturday", _weekly(6)),
    ("Sunday", _weekly(7)),
    ("on Sun", _weekly(7)),
    ("every Sat", _weekly(6)),
    ("on wed", _weekly(3)),
    ("Sat/Sun", _weekly(6, 7)),
    ("Sat and Sun", _weekly(6, 7)),
    ("on Mon, Wed, Fri", _weekly(1, 3, 5)),
    ("on Mon, Wed, and Fri", _weekly(1, 3, 5)),
    ("Mon/Wed/Fri", _weekly(1, 3, 5)),
    ("Monday & Thursday", _weekly(1, 4)),
    ("1 tablet every Monday morning", _weekly(1)),
    ("70 mg weekly on Sunday", _weekly(7)),
])
def test_a_real_weekday_is_still_read(text, expected):
    assert _summary(text) == expected


@_cases([
    ("Mon-Fri", _weekly(1, 2, 3, 4, 5)),
    ("Mon - Fri", _weekly(1, 2, 3, 4, 5)),
    ("Monday through Friday", _weekly(1, 2, 3, 4, 5)),
    ("Monday to Friday", _weekly(1, 2, 3, 4, 5)),
    ("Mon thru Wed", _weekly(1, 2, 3)),
    ("Fri-Mon", _weekly(5, 6, 7, 1)),  # wraps past Sunday
    ("Sat-Sun", _weekly(6, 7)),
    ("Mon, Wed-Fri", _weekly(1, 3, 4, 5)),
    ("周一至周五", _weekly(1, 2, 3, 4, 5)),
    ("周一到周五", _weekly(1, 2, 3, 4, 5)),
    ("星期一至五", _weekly(1, 2, 3, 4, 5)),
    ("周一~周三", _weekly(1, 2, 3)),
    ("周六至周日", _weekly(6, 7)),
    ("每周一、三、五", _weekly(1, 3, 5)),  # a list is still a list
])
def test_a_weekday_range_is_every_day_in_it_not_the_two_ends(text, expected):
    assert _summary(text) == expected


@_texts(["Mon or Tue", "on Monday or Thursday", "Sat, or Sun"])
def test_days_offered_as_a_choice_is_no_parse(text):
    assert parse_dose_instruction(text) is None


# --- #350: months and years have no field -------------------------------------------------


@_texts([
    "10 mg once a month",
    "10 mg monthly",
    "once monthly",
    "every month",
    "each month",
    "every 2 months",
    "every 3 mos",
    "every other month",
    "q month",
    "q3mo",
    "twice a month",
    "2 times per month",
    "1x/month",
    "once a year",
    "yearly",
    "annually",
    "every 6 months",
    "quarterly",
    "biannually",
    "bimonthly",
    "on the first Monday of the month",
    "every other Monday",
    "every 2nd Tuesday",
    "每月一次",
    "每月1次，每次10mg",
    "每个月一次",
    "每两个月一次",
    "每3个月1次",
    "隔月一次",
    "每年一次",
    "每季度一次",
    "半年一次",
    "一个月一次",
])
def test_a_frequency_with_no_field_refuses_rather_than_keeping_the_dose_alone(text):
    """Not a Monday, and not the dose with the frequency silently dropped."""
    assert parse_dose_instruction(text) is None


@_cases([
    ("1 tablet daily for 3 months", ONCE_DAILY),
    ("1 tablet daily for a month", ONCE_DAILY),
    ("1 tablet daily for 1 year", ONCE_DAILY),
    ("1 tablet daily for a week", ONCE_DAILY),
    ("1 tablet daily x 30 days", ONCE_DAILY),
    ("1 tab po tid x 7 days", THRICE_DAILY),
    ("take 1 capsule three times a day for 10 days", THRICE_DAILY),
    ("3月1日起每日一次", ONCE_DAILY),  # a date, not a month frequency
])
def test_a_duration_is_not_a_frequency(text, expected):
    assert _summary(text) == expected


# --- a count per week: a field only once the days are named --------------------------------


@_texts([
    "three times a week",
    "three times weekly",
    "3x weekly",
    "3x/week",
    "3 times per week",
    "thrice weekly",
    "twice a week",
    "2x a week",
    "two times weekly",
    "biweekly",
    "semiweekly",
    "每周三次",
    "每周3次",
    "一周三次",
    "每周两次",
])
def test_a_weekly_count_with_no_days_refuses_rather_than_reading_once_weekly(text):
    assert parse_dose_instruction(text) is None


@_cases([
    ("1x weekly", WEEKLY),
    ("once a week", WEEKLY),
    ("once per week", WEEKLY),
    ("1 time a week", WEEKLY),
    ("one time weekly", WEEKLY),
    ("1 tablet a week", WEEKLY),
    ("10 mg per week", WEEKLY),
    ("qw", WEEKLY),
    ("qwk", WEEKLY),
    ("q1w", WEEKLY),
])
def test_once_a_week_in_every_spelling(text, expected):
    assert _summary(text) == expected


@_cases([
    ("twice a week on Mon and Thu", _weekly(1, 4)),
    ("2x weekly on Tuesday and Friday", _weekly(2, 5)),
    ("three times a week: Mon, Wed, Fri", _weekly(1, 3, 5)),
    ("once weekly on Sunday", _weekly(7)),
    ("once a week on Monday", _weekly(1)),
    ("每周一次，周日服用", _weekly(7)),
    ("每周两次，周一、周四", _weekly(1, 4)),
    ("每周3次：周一、三、五", _weekly(1, 3, 5)),
])
def test_a_weekly_count_restated_by_the_named_days_is_the_days(text, expected):
    """Refused before: the count and the days were counted as two frequencies."""
    assert _summary(text) == expected


@_texts([
    "twice a week on Monday",
    "once weekly on Mon and Thu",
    "three times a week on Mon and Fri",
    "每周两次，周一",
    "每周一次，周一、周四",
])
def test_a_weekly_count_that_disagrees_with_the_named_days_is_no_parse(text):
    assert parse_dose_instruction(text) is None


# --- hours ---------------------------------------------------------------------------------


@_cases([
    ("q24h", ONCE_DAILY),  # came back unscheduled
    ("every 24 hours", ONCE_DAILY),
    ("once every 24 hours", ONCE_DAILY),
    ("每24小时一次", ONCE_DAILY),
    ("每二十四小时一次", ONCE_DAILY),
    ("q12h", TWICE_DAILY),
    ("q12hrs", TWICE_DAILY),
    ("Q12H", TWICE_DAILY),
    ("every 12 hours", TWICE_DAILY),
    ("every twelve hours", TWICE_DAILY),
    ("每12小时1次", TWICE_DAILY),
    ("每十二小时一次", TWICE_DAILY),
    ("q8h", THRICE_DAILY),
    ("q 8 h", THRICE_DAILY),
    ("q8hr", THRICE_DAILY),
    ("every 8 hrs", THRICE_DAILY),
    ("every eight hours", THRICE_DAILY),
    ("每8小时1次", THRICE_DAILY),
    ("每八小时一次", THRICE_DAILY),
    ("每隔8小时一次", THRICE_DAILY),
    ("每 8 个小时一次", THRICE_DAILY),
    ("q6h", FOUR_DAILY),
    ("every 6 hours", FOUR_DAILY),
    ("q48h", _interval(2)),  # a multiple of a day is a period
    ("every 72 hours", _interval(3)),
    ("每48小时一次", _interval(2)),
])
def test_a_count_of_hours_is_a_count_per_day_or_a_period(text, expected):
    assert _summary(text) == expected


def test_q4h_keeps_its_six_numbered_slots():
    (i,) = parse_dose_instruction("q4h")
    assert i.slots() == ("1/6", "2/6", "3/6", "4/6", "5/6", "6/6")


@_texts([
    "every 5 hours",
    "q7h",
    "every 36 hours",
    "q10h",
    "every 0 hours",
    "500 mg every 5 hours",
    "每5小时一次",
    "每8小时2次",  # two doses per 8 hours: no field
    "twice every 12 hours",
    "every 4-6 hours",
    "every 4 to 6 hours",
    "q4-6h",
])
def test_hours_that_are_not_a_whole_count_per_day_refuse(text):
    assert parse_dose_instruction(text) is None


# --- periods said in words ------------------------------------------------------------------


@_cases([
    ("every two weeks", _interval(14)),
    ("every three days", _interval(3)),
    ("every second day", _interval(2)),
    ("every 2nd day", _interval(2)),
    ("every third day", _interval(3)),
    ("every 3rd day", _interval(3)),
    ("every other week", _interval(14)),
    ("every alternate day", _interval(2)),
    ("on alternate days", _interval(2)),
    ("alternate days", _interval(2)),
    ("every other night", _interval(2)),
    ("q.o.d.", _interval(2)),
    ("fortnightly", _interval(14)),
    ("once a fortnight", _interval(14)),
    ("every fortnight", _interval(14)),
    ("once every 3 days", _interval(3)),
    ("1 tablet once every other day", _interval(2)),
    ("every 4 weeks", _interval(28)),
    ("every 1 week", _interval(7)),
    ("q2w", _interval(14)),
    ("每十天一次", _interval(10)),
    ("每十四天一次", _interval(14)),
])
def test_a_period_in_words_or_ordinals_is_read(text, expected):
    assert _summary(text) == expected


@_texts([
    "twice every 3 days",
    "3 times every 2 weeks",
    "twice every other day",
    "every 2 to 3 days",
    "每2-3天一次",
])
def test_a_period_with_several_doses_in_it_refuses(text):
    assert parse_dose_instruction(text) is None


# --- named times of day -------------------------------------------------------------------


@_cases([
    ("every morning", ONCE_DAILY),
    ("each evening", ONCE_DAILY),
    ("in the morning", ONCE_DAILY),
    ("1 tablet in the evening", ONCE_DAILY),
    ("at bedtime", ONCE_DAILY),
    ("at night", ONCE_DAILY),
    ("nightly", ONCE_DAILY),
    ("before bed", ONCE_DAILY),
    ("qhs", ONCE_DAILY),
    ("q.h.s.", ONCE_DAILY),
    ("1 tab po hs", ONCE_DAILY),
    ("qam", ONCE_DAILY),
    ("qpm", ONCE_DAILY),
    ("at noon", ONCE_DAILY),
    ("once daily at bedtime", ONCE_DAILY),  # the Apple sample's own sig
    ("1 tablet daily in the morning", ONCE_DAILY),
    ("每晚一片", ONCE_DAILY),
    ("每晚", ONCE_DAILY),
    ("睡前一片", ONCE_DAILY),
    ("每晚睡前一片", ONCE_DAILY),
    ("晨起空腹一片", ONCE_DAILY),
    ("每天早上一次", ONCE_DAILY),
    ("中午一片", ONCE_DAILY),
    # Two different times of day are two a day, the 早晚 reading.
    ("1 tablet in the morning and at night", TWICE_DAILY),
    ("1 tablet morning and bedtime", TWICE_DAILY),
    ("bid (morning & evening)", TWICE_DAILY),
    ("早上一片，晚上一片", TWICE_DAILY),
    ("早上一片，中午一片，睡前一片", THRICE_DAILY),
    # A time of day qualifies a weekday or a period; it is not a second frequency.
    ("1 tablet every Monday morning", _weekly(1)),
    ("1 tablet every other day in the morning", _interval(2)),
    ("once weekly in the morning", WEEKLY),
])
def test_a_named_time_of_day_is_once_a_day_at_that_time(text, expected):
    assert _summary(text) == expected


@_texts([
    "twice daily in the morning",
    "tid at bedtime",
    "every 8 hours at bedtime",
    "早上一片，晚上半片",  # two doses
    "1 tablet in the morning and 0.5 tablet in the evening",
])
def test_a_time_of_day_that_contradicts_the_count_is_no_parse(text):
    assert parse_dose_instruction(text) is None


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("evening primrose oil 1000 mg", ("unscheduled", 0, None, (), ())),
        ("1 capsule evening primrose oil daily", ONCE_DAILY),
        ("Night Time cold relief 2 tablets", ("unscheduled", 0, None, (), ())),
    ],
)
def test_a_time_of_day_word_in_a_name_invents_no_schedule(text, expected):
    """A bare noun alone is not WHEN to take it; "in the evening" is."""
    assert _summary(text) == expected


@_cases([
    ("每日一次，晚饭后服用", ONCE_DAILY),  # a meal is not a time of day here
    ("1 tablet daily with breakfast", ONCE_DAILY),
    ("每天3次 饭后", THRICE_DAILY),
])
def test_a_meal_is_not_a_time_of_day(text, expected):
    assert _summary(text) == expected


# --- Latin abbreviations, dotted or not ----------------------------------------------------


@_cases([
    ("1 tab qd", ONCE_DAILY),
    ("1 tab q.d.", ONCE_DAILY),
    ("1 tab QD", ONCE_DAILY),
    ("1 tab bid", TWICE_DAILY),
    ("1 tab b.i.d.", TWICE_DAILY),
    ("1 tab b.i.d", TWICE_DAILY),
    ("1 tab BID.", TWICE_DAILY),
    ("1 tab bd", TWICE_DAILY),
    ("1 tab b.d.", TWICE_DAILY),
    ("1 tab tid", THRICE_DAILY),
    ("1 tab t.i.d.", THRICE_DAILY),  # never matched: \\b after the final dot needs a letter
    ("1 tab tds", THRICE_DAILY),
    ("1 tab t.d.s.", THRICE_DAILY),
    ("2 puffs qid", FOUR_DAILY),
    ("2 puffs q.i.d.", FOUR_DAILY),
    ("2 puffs qds", FOUR_DAILY),
    ("2 puffs q.d.s.", FOUR_DAILY),  # not q.d. (once) with an "s" left over
    ("1 tab t.i.d. with meals", THRICE_DAILY),
])
def test_a_latin_abbreviation_is_read_dotted_or_not(text, expected):
    assert _summary(text) == expected


def test_od_is_still_the_right_eye_not_once_daily():
    (i,) = parse_dose_instruction("1 drop od")
    assert i.schedule_kind() == "unscheduled"


# --- ranges are not doses -----------------------------------------------------------------


@_texts([
    "1 to 2 tablets",
    "1 or 2 tablets",
    "one or two tablets",
    "one to two tablets daily",
    "once or twice daily",
    "2 to 3 times daily",
    "two-three times a day",
    "1-2 tablets",
    "每次1到2片",
    "每次1至2片",
    "每次一到两片",
    "每日2~3次",
    "每周一到两次",
    "每周一至二次",
])
def test_a_range_in_words_or_zh_is_no_parse_not_its_upper_bound(text):
    assert parse_dose_instruction(text) is None


# --- clock times ----------------------------------------------------------------------------


@_cases([
    ("1 tablet at 8am", _fixed("08:00")),
    ("1 tablet at 8 AM", _fixed("08:00")),
    ("1 tablet at 8 pm", _fixed("20:00")),
    ("1 tablet at 8:30 p.m.", _fixed("20:30")),
    ("at 12 am", _fixed("00:00")),
    ("at 12 pm", _fixed("12:00")),
    ("1 tablet at 8 am and 8 pm", _fixed("08:00", "20:00")),
    ("twice daily at 08:00 and 20:00", _fixed("08:00", "20:00")),
    ("once daily at 08:00", _fixed("08:00")),
    ("q12h at 08:00 and 20:00", _fixed("08:00", "20:00")),
    ("1 tablet at 22:00 at bedtime", _fixed("22:00")),
    ("Mon and Thu at 08:00", _weekly(1, 4, times=("08:00",))),
])
def test_clock_times_including_the_twelve_hour_dial(text, expected):
    assert _summary(text) == expected


@_texts([
    "three times daily at 08:00 and 20:00",
    "once daily at 08:00 and 20:00",
    "q8h at 08:00",
])
def test_a_count_and_a_different_number_of_clock_times_is_no_parse(text):
    assert parse_dose_instruction(text) is None


# --- one frequency said twice, or two said once each ---------------------------------------


@_cases([
    ("1 tablet twice daily (every 12 hours)", TWICE_DAILY),
    ("q8h (three times daily)", THRICE_DAILY),
    ("once daily, every day", ONCE_DAILY),
    ("每天两次，每次一片", TWICE_DAILY),
    ("每日3次，每次1片", THRICE_DAILY),
])
def test_the_same_frequency_said_twice_agrees(text, expected):
    """Agreeing restatements used to be refused as "two frequencies"."""
    assert _summary(text) == expected


@_texts([
    "twice daily every 8 hours",
    "3 times daily, twice a day",
    "daily and every other day",
    "every 12 hours on Mondays",
    "twice daily on Mon and Thu",
    "every 2 weeks on Monday",
    "weekly, every other day",
    "bid, then tid",
])
def test_two_different_frequencies_are_no_parse(text):
    assert parse_dose_instruction(text) is None


@_texts([
    # The first step's bare "daily" names no count, so without the step word
    # the second step's count absorbed it and the taper read as its last step.
    "1 tablet daily for 3 days, then twice daily",
    "once daily for 1 week, followed by twice daily",
    "twice daily, thereafter once daily",
    "daily, increase to twice daily after 2 weeks",
    "taper: 3 times daily, reduce to twice daily",
    "每天一次，一周后改为每天两次",
    "每日1次，然后每日2次",
])
def test_a_step_or_taper_is_two_regimens(text):
    assert parse_dose_instruction(text) is None


@_cases([
    ("on weekdays", _weekly(1, 2, 3, 4, 5)),
    ("every weekday", _weekly(1, 2, 3, 4, 5)),
    ("on weekends", _weekly(6, 7)),
    ("every weekend", _weekly(6, 7)),
    ("工作日", _weekly(1, 2, 3, 4, 5)),
    ("周末", _weekly(6, 7)),
])
def test_weekdays_and_weekends_are_their_days(text, expected):
    assert _summary(text) == expected


@_texts([
    "daily except weekends",
    "every day except Sunday",
    "daily, excluding Saturdays",
    "q4h while awake",  # fewer than six a day, by an amount the sig does not say
    "every 4 hours while awake",
    "除周日外每天一次",
])
def test_a_frequency_with_an_exception_is_no_parse(text):
    assert parse_dose_instruction(text) is None


# --- the dose survives -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "dose"),
    [
        ("500 mg 3 times daily", Dose(500.0, "mg")),
        ("1 tablet 2x daily", Dose(1.0, "{tablet}")),
        ("2 puffs q.i.d.", Dose(2.0, "{puff}")),
        ("10 units qhs", Dose(10.0, "U")),
        ("5 mg daily, monitor blood pressure", Dose(5.0, "mg")),
        ("每晚一片", Dose(1.0, "{tablet}")),
    ],
)
def test_the_dose_is_kept_beside_the_frequency(text, dose):
    (i,) = parse_dose_instruction(text)
    assert i.dose == dose and i.text == text


# --- unchanged ------------------------------------------------------------------------------


@_cases([
    ("100mg bid", TWICE_DAILY),
    ("500 mg twice daily with food", TWICE_DAILY),
    ("每天两次，每次一片", TWICE_DAILY),
    ("500mg 每天早晚", TWICE_DAILY),
    ("早晚各一片", TWICE_DAILY),
    ("每天早中晚各一粒", THRICE_DAILY),
    ("早上 中午 晚上各一次", THRICE_DAILY),
    ("1 tablet morning and evening", TWICE_DAILY),
    ("半片 每日一次", ONCE_DAILY),
    ("隔日一次 1片", _interval(2)),
    ("every other day", _interval(2)),
    ("qod", _interval(2)),
    ("every 3 days", _interval(3)),
    ("q3d", _interval(3)),
    ("每2周1次", _interval(14)),
    ("每周一次", WEEKLY),
    ("weekly", WEEKLY),
    ("once weekly", WEEKLY),
    ("every week", WEEKLY),
    ("每周一、三", _weekly(1, 3)),
    ("take on Mon, Wed and Fri", _weekly(1, 3, 5)),
    ("on Monday and Thursday", _weekly(1, 4)),
    ("daily", ONCE_DAILY),
    ("every day", ONCE_DAILY),
    ("each day", ONCE_DAILY),
    ("2 tablets daily", ONCE_DAILY),  # a dose of two, once
    ("take at the same time every day", ONCE_DAILY),
    ("每天", ONCE_DAILY),
    ("一日3次", THRICE_DAILY),
    ("每周三", _weekly(3)),
    ("10 units at 08:00 and 20:00", _fixed("08:00", "20:00")),
])
def test_what_already_read_correctly_still_does(text, expected):
    assert _summary(text) == expected


def test_as_needed_keeps_winning_over_any_count():
    for text in ("2 puffs qid prn", "1 tablet at bedtime as needed for sleep", "1 tab q4h prn", "必要时睡前一片"):
        (i,) = parse_dose_instruction(text)
        assert i.as_needed and i.schedule_kind() == "prn", text


# --- the three callers ----------------------------------------------------------------------


def _statement(dosage: dict) -> dict:
    return {
        "resourceType": "MedicationStatement",
        "status": "active",
        "medicationCodeableConcept": {"text": "Metformin"},
        "effectivePeriod": {"start": "2026-01-05"},
        "dosage": [dosage],
    }


def _fhir(dosage: dict) -> Summary:
    plan = from_fhir_medication_statement(
        _statement(dosage), default_start=date(2026, 1, 5), today=date(2026, 6, 20), subject_id="u"
    )
    parts = describe_schedule(plan.schedule[0])
    return (parts.kind, parts.doses_per_day, parts.period_days, parts.weekdays, parts.times)


UNSCHEDULED: Summary = ("unscheduled", 0, None, (), ())


@_cases([
    ("500 mg 3 times daily", THRICE_DAILY),
    ("1 tablet 2x daily", TWICE_DAILY),
    ("10 mg once a month", UNSCHEDULED),
    ("5 mg daily, monitor blood pressure", ONCE_DAILY),
])
def test_fhir_dosage_text_reads_through_the_same_grammar(text, expected):
    assert _fhir({"text": text}) == expected


def _repeat(**repeat) -> dict:
    return {"timing": {"repeat": repeat}}


@pytest.mark.parametrize(
    ("label", "dosage", "expected"),
    [
        ("q24h", _repeat(frequency=1, period=24, periodUnit="h"), ONCE_DAILY),
        ("q8h, frequency absent", _repeat(period=8, periodUnit="h"), THRICE_DAILY),
        ("q48h", _repeat(frequency=1, period=48, periodUnit="h"), _interval(2)),
        ("bid", _repeat(frequency=2, period=1, periodUnit="d"), TWICE_DAILY),
        ("bid at two times", _repeat(frequency=2, period=1, periodUnit="d", timeOfDay=["08:00", "20:00"]),
         _fixed("08:00", "20:00")),
        ("every 2 days", _repeat(frequency=1, period=2, periodUnit="d"), _interval(2)),
        ("weekly", _repeat(frequency=1, period=1, periodUnit="wk"), WEEKLY),
        ("Mon and Thu", _repeat(frequency=1, period=1, periodUnit="d", dayOfWeek=["mon", "thu"]), _weekly(1, 4)),
        ("twice a week, Mon and Thu", _repeat(frequency=2, period=1, periodUnit="wk", dayOfWeek=["mon", "thu"]),
         _weekly(1, 4)),
        ("Mon, twice that day at two times",
         _repeat(frequency=2, period=1, periodUnit="d", dayOfWeek=["mon"], timeOfDay=["08:00", "20:00"]),
         _weekly(1, per_day=2, times=("08:00", "20:00"))),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_fhir_structured_timing_that_has_a_field(label, dosage, expected):
    assert _fhir(dosage) == expected


@pytest.mark.parametrize(
    ("label", "dosage"),
    [
        ("two doses every 3 days", _repeat(frequency=2, period=3, periodUnit="d")),
        ("1-2 times a day", _repeat(frequency=1, frequencyMax=2, period=1, periodUnit="d")),
        ("q4-6h", _repeat(frequency=1, period=4, periodMax=6, periodUnit="h")),
        ("every 5 hours", _repeat(frequency=1, period=5, periodUnit="h")),
        ("twice a week, no days", _repeat(frequency=2, period=1, periodUnit="wk")),
        ("twice a week at 08:00", _repeat(frequency=2, period=1, periodUnit="wk", timeOfDay=["08:00"])),
        ("Monday every 2 weeks", _repeat(frequency=1, period=2, periodUnit="wk", dayOfWeek=["mon"])),
        ("three a week on two days", _repeat(frequency=3, period=1, periodUnit="wk", dayOfWeek=["mon", "thu"])),
        ("Monday, twice that day, no times", _repeat(frequency=2, period=1, periodUnit="d", dayOfWeek=["mon"])),
        ("q8h on Mondays", _repeat(frequency=1, period=8, periodUnit="h", dayOfWeek=["mon"])),
        ("Monday monthly", _repeat(frequency=1, period=1, periodUnit="mo", dayOfWeek=["mon"])),
        ("monthly", _repeat(frequency=1, period=1, periodUnit="mo")),
        ("bid at three times",
         _repeat(frequency=2, period=1, periodUnit="d", timeOfDay=["08:00", "14:00", "20:00"])),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_fhir_structured_timing_with_no_field_stays_unscheduled(label, dosage):
    """Each of these used to come back as a smaller regimen than the one written."""
    assert _fhir(dosage) == UNSCHEDULED


def test_fhir_as_needed_with_a_ranged_interval_is_still_as_needed():
    dosage = {"asNeededBoolean": True, **_repeat(frequency=1, period=4, periodMax=6, periodUnit="h")}
    assert _fhir(dosage)[0] == "prn"


@pytest.mark.parametrize(
    ("frequency", "expected"),
    [
        ("3 times daily", THRICE_DAILY),
        ("2x daily", TWICE_DAILY),
        ("once a month", UNSCHEDULED),
        ("daily, monitor blood pressure", ONCE_DAILY),
        ("每晚", ONCE_DAILY),
    ],
)
def test_a_journal_mention_reads_its_frequency_through_the_same_grammar(frequency, expected):
    mention = MedicationMention(name="Metformin", dose_text="500 mg", frequency_text=frequency)
    out = reconcile_mentions([], [mention], record_date=date(2026, 6, 20), source_record_id="r1", subject_id="u")
    (plan,) = out.create
    parts = describe_schedule(plan.schedule[0])
    assert (parts.kind, parts.doses_per_day, parts.period_days, parts.weekdays, parts.times) == expected
    assert plan.schedule[0].dose == Dose(500.0, "mg") or expected == UNSCHEDULED

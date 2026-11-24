"""Medications as their own entity: a plan, the doses it schedules, and what
was actually taken. None of it is a reading and none of it belongs in a
series table.

The vocabulary is FHIR R4B's (``MedicationRequest`` for a prescription,
``MedicationStatement`` for a plan, ``MedicationAdministration`` for a dose
event, ``Dosage``/``Timing`` for the instruction) narrowed to what a personal
health record can actually know. Three invariants that production systems
learned the hard way are load-bearing here:

* **A dose slot is identified by ``(plan, local_date, slot)``, never by a UTC
  instant.** On the night daylight-saving time starts, a 02:30 slot and a
  03:30 slot fall on the same instant; keyed by instant one of them vanishes.
  The instant is derived at projection time, with an explicit policy for the
  gap, and a time-zone move re-projects the same keys onto new instants.
* **``pending`` and ``missed`` are derived states, never stored.** What is
  stored is what happened: ``taken`` or ``skipped`` (with a reason). Whether an
  unanswered slot is still ``due`` or already ``missed`` depends on *now* and on
  a grace rule, so :func:`slot_state` takes both.
* **Adherence needs ``now``.** A dose scheduled for tonight is not missed at
  noon. The percentage is over elapsed slots only, and is ``None`` (not 0) 
  when nothing has elapsed or the plan is as-needed.

Nothing here prints a drug name by accident: free-text fields are excluded
from ``repr``, and a concept's key is a hash, so a logged ``Reconcile`` shows
counts and keys, never what the person takes.

Storage (``MedicationStore``, ``DoseLogStore``) and terminology lookup
(``Terminology``) are ports the consumer implements; this module ships no
drug database. Pure: stdlib plus ``mirobody.units`` and ``mirobody.kernel.series``.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, tzinfo
from importlib import resources
from typing import Literal, NamedTuple, Protocol

from mirobody import units
from . import query
from .series import day_bounds_ms, stable_hash, zone

MS = 1000

# --- terminology ------------------------------------------------------------------

SYSTEM_RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"
SYSTEM_ATC = "http://www.whocc.no/atc"
SYSTEM_SNOMED = "http://snomed.info/sct"
SYSTEM_NDC = "http://hl7.org/fhir/sid/ndc"


@dataclass(frozen=True)
class Coding:
    """One code from one terminology (FHIR ``Coding``). ``tty`` is RxNorm's
    term type (``SCD``, ``SBD``, ``IN``…) when known."""

    system: str
    code: str
    display: str = ""
    tty: str = ""


def normalize_name(text: str | None) -> str:
    """The comparison form of a medication name that has no code.

    Deterministic and convergent only: Unicode compatibility folding (so
    full-width ``５００ｍｇ`` and ``㎎`` become ASCII: a superset of a plain
    full-width fold), case folding, and dropping all whitespace. Deliberately
    *no* alias or brand→generic mapping, that is an open-ended asset behind
    the ``Terminology`` port. Two spellings of one drug make two plans; a user
    sees that and deletes one, which is safer than a wrong merge.
    """
    folded = unicodedata.normalize("NFKC", (text or "").strip())
    return "".join(folded.split()).casefold()


@dataclass(frozen=True)
class MedicationConcept:
    """What the medication is: free text as the person wrote it, plus any
    codes a terminology service attached. ``text`` and ``strength`` are kept
    out of ``repr``, they are health data."""

    text: str = field(repr=False)
    codes: tuple[Coding, ...] = ()
    form: str = ""
    strength: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not normalize_name(self.text) and not self.codes:
            raise ValueError("a medication concept needs a name or a code")

    @property
    def rxnorm_codes(self) -> frozenset[str]:
        return frozenset(c.code for c in self.codes if c.system == SYSTEM_RXNORM and c.code)

    @property
    def concept_key(self) -> str:
        """``rxnorm:<sorted codes>`` when coded, else ``text:<hash of the
        normalised name>``. Order-independent for multi-coded concepts; a
        coded concept and a text-only one never compare equal, even for the
        same drug: auto-merging them is how a wrong code silently rewrites a
        person's medication list. The text key is a hash so the key can be
        logged without the name."""
        codes = self.rxnorm_codes
        if codes:
            return "rxnorm:" + ",".join(sorted(codes))
        return "text:" + stable_hash(normalize_name(self.text))[:16]

    def same_drug(self, other: MedicationConcept) -> bool:
        """Equal keys, or a shared RxNorm code (an ingredient code next to a
        clinical-drug code is the same drug, not two)."""
        if self.concept_key == other.concept_key:
            return True
        return bool(self.rxnorm_codes & other.rxnorm_codes)


class Terminology(Protocol):
    """A lookup the consumer wires in (RxNorm, a national list, an in-house
    table). The framework ships none."""

    def lookup(self, text: str) -> tuple[Coding, ...]: ...


# --- dose and dose units ------------------------------------------------------------

FAMILY_COUNT = "count"
FAMILY_MASS = "mass"
FAMILY_VOLUME = "volume"
FAMILY_ACTIVITY = "activity"
FAMILY_OTHER = "other"

_FAMILY_BRIDGE = {"Mass": FAMILY_MASS, "Vol": FAMILY_VOLUME, "Arb": FAMILY_ACTIVITY}
_ACTIVITY_UNITS = frozenset({"U", "[IU]", "[iU]"})  # enzyme / international units: insulin, vitamin D, heparin
#: Results the unit engine gives for words that are not dose units in a
#: medication context: a bare count (``次`` is "times", a frequency) and time.
_REJECTED_FAMILIES = frozenset({"Time", "Num", "Ratio"})

#: Spellings the UCUM engine does not know but dose text uses constantly.
_SPELLING_BRIDGE = {
    "meq": "meq",
    "unit": "U",
    "units": "U",
    "iu": "[IU]",
    "cc": "mL",
    "tsp": "[tsp_us]",
    "tbsp": "[tbs_us]",
}


def _load_forms() -> dict[str, str]:
    text = resources.files("mirobody").joinpath("res", "dose_forms.tsv").read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(text), delimiter="\t"):
        for alias in row["aliases"].split("|"):
            out[alias.casefold()] = row["annotation"]
    return out


#: Dose forms → UCUM annotations, with English and Chinese aliases. A closed,
#: versioned table (``res/dose_forms.tsv``); ambiguous aliases are documented
#: there rather than guessed here.
DOSE_FORMS: dict[str, str] = _load_forms()
_FORM_ANNOTATIONS = frozenset(DOSE_FORMS.values())


def _form(text: str) -> str | None:
    key = text.strip().casefold()
    if key in DOSE_FORMS:
        return DOSE_FORMS[key]
    if text.startswith("{") and text.endswith("}") and text in _FORM_ANNOTATIONS:
        return text
    return None


def normalize_dose_unit(text: str | None) -> str | None:
    """A dose unit as written → UCUM, or ``None`` when it is not one.

    Dose forms become UCUM annotations (``tablet`` → ``{tablet}``, ``片`` →
    ``{tablet}``); a per-form strength keeps its denominator
    (``mg/actuat`` → ``mg/{actuat}``) instead of degrading to a bare mass;
    everything else goes through :func:`units.normalize_unit`, then a short
    spelling bridge (``mEq``, ``IU``, ``units``, ``cc``). Words the engine
    reads as a plain count or a time (``次``, ``days``) are refused: in a dose
    they are frequencies, not amounts.
    """
    raw = (text or "").strip()
    if not raw:
        return None
    form = _form(raw)
    if form:
        return form
    if "/" in raw:
        num, _, den = raw.partition("/")
        den_form = _form(den.strip())
        if den_form:
            num_unit = normalize_dose_unit(num)
            return f"{num_unit}/{den_form}" if num_unit else None
        return units.normalize_unit(raw)
    key = raw.casefold()
    if key in _SPELLING_BRIDGE:
        return _SPELLING_BRIDGE[key]
    unit = units.normalize_unit(raw)
    if unit is None or units.unit_family(unit) in _REJECTED_FAMILIES:
        return None
    return unit


def dose_unit_family(unit: str | None) -> str:
    """``count`` (a form: tablets, puffs), ``mass``, ``volume``, ``activity``
    (U, [IU]) or ``other``. A per-form strength (``mg/{actuat}``) is the
    family of its numerator."""
    if not unit:
        return FAMILY_OTHER
    if unit.startswith("{"):
        return FAMILY_COUNT
    if "/" in unit and unit.rsplit("/", 1)[1].startswith("{"):
        unit = unit.rsplit("/", 1)[0]
    if unit in _ACTIVITY_UNITS:
        return FAMILY_ACTIVITY
    return _FAMILY_BRIDGE.get(units.unit_family(unit) or "", FAMILY_OTHER)


@dataclass(frozen=True)
class Dose:
    """An amount in a UCUM unit. ``unit`` is already normalised; build one
    from text with :func:`dose_from_text`."""

    value: float
    unit: str

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("a dose cannot be negative")
        if not self.unit:
            raise ValueError("a dose needs a unit")

    @property
    def family(self) -> str:
        return dose_unit_family(self.unit)


def dose_from_text(value: float | str, unit_text: str) -> Dose | None:
    """A dose from a number and a unit as written; ``None`` when the unit is
    not a dose unit."""
    unit = normalize_dose_unit(unit_text)
    if unit is None:
        return None
    try:
        return Dose(float(value), unit)
    except (TypeError, ValueError):
        return None


# --- instruction and schedule kinds ------------------------------------------------

KIND_FIXED_TIMES = "fixed_times"  # daily at listed clock times
KIND_DAILY = "daily"  # n times a day, clock times unknown
KIND_INTERVAL = "interval"  # every N days
KIND_WEEKLY = "weekly"  # on listed weekdays
KIND_PRN = "prn"  # as needed
KIND_UNSCHEDULED = "unscheduled"  # nothing known about timing

_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")

#: A dose scheduled for a day at no particular time.
SLOT_DAY = "day"


def normalize_time(text: str) -> str:
    """``8:00`` → ``08:00``; seconds kept when given; ``24:00`` is not a time
    of day and is refused."""
    m = _TIME_RE.match(text.strip())
    if not m:
        raise ValueError(f"not a clock time: {text!r}")
    h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
    if not (0 <= h <= 23 and 0 <= mi <= 59 and 0 <= s <= 59):
        raise ValueError(f"clock time out of range: {text!r}")
    return f"{h:02d}:{mi:02d}" + (f":{s:02d}" if m.group(3) else "")


def normalize_slot(slot: str) -> str:
    """A slot name in its canonical spelling (clock slots normalised, the
    ``#n`` disambiguator kept)."""
    base, sep, suffix = slot.partition("#")
    if base == SLOT_DAY or "/" in base:
        return slot
    return normalize_time(base) + (sep + suffix if sep else "")


@dataclass(frozen=True)
class DoseInstruction:
    """How the medication is taken (FHIR ``Dosage`` narrowed).

    Exactly one timing shape is allowed: clock ``times`` (optionally every
    ``period_days``), ``doses_per_day`` without times, ``weekdays`` (optionally
    with times), or ``as_needed``. Mixing them is rejected, so a projection
    never has to guess which one wins. Hourly regimens (``q8h``) are
    represented as ``doses_per_day`` with numbered slots: the clock times are
    not known, and the kernel does not invent them.
    """

    dose: Dose | None = None
    times: tuple[str, ...] = ()
    doses_per_day: int = 0
    period_days: int | None = None
    weekdays: frozenset[int] = frozenset()
    as_needed: bool = False
    as_needed_for: str = field(default="", repr=False)
    max_dose_per_day: Dose | None = None
    text: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        times = tuple(sorted({normalize_time(t) for t in self.times}))
        object.__setattr__(self, "times", times)
        object.__setattr__(self, "weekdays", frozenset(self.weekdays))
        if any(not 1 <= d <= 7 for d in self.weekdays):
            raise ValueError("weekdays are ISO 1..7 (Monday=1)")
        if self.period_days is not None and self.period_days < 1:
            raise ValueError("period_days must be >= 1")
        if self.doses_per_day < 0:
            raise ValueError("doses_per_day must be >= 0")
        if self.as_needed and (self.times or self.doses_per_day or self.period_days or self.weekdays):
            raise ValueError("an as-needed instruction carries no schedule")
        if self.weekdays and self.period_days is not None:
            raise ValueError("weekdays and period_days are two different schedules")
        if self.times and self.doses_per_day:
            raise ValueError("give clock times or a count per day, not both")
        if self.max_dose_per_day is not None and not self.as_needed:
            raise ValueError("max_dose_per_day is for as-needed instructions")

    def schedule_kind(self) -> str:
        if self.as_needed:
            return KIND_PRN
        if self.weekdays:
            return KIND_WEEKLY
        if self.period_days is not None and self.period_days > 1:
            return KIND_INTERVAL
        if self.times:
            return KIND_FIXED_TIMES
        if self.doses_per_day or self.period_days == 1:
            return KIND_DAILY
        return KIND_UNSCHEDULED

    def slots(self) -> tuple[str, ...]:
        """The slot names one due day carries."""
        if self.times:
            return self.times
        if self.doses_per_day > 1:
            return tuple(f"{i}/{self.doses_per_day}" for i in range(1, self.doses_per_day + 1))
        return (SLOT_DAY,)


Schedule = tuple[DoseInstruction, ...]


def period_from_gap(gap_days: int) -> int:
    """Some stores keep the *gap* between doses (``0`` = every day, ``1`` =
    every other day); a period is the gap plus one."""
    if gap_days < 0:
        raise ValueError("gap_days must be >= 0")
    return gap_days + 1


def weekday_from(value: int | str, convention: Literal["iso", "zero_monday", "zero_sunday"] = "iso") -> int:
    """An external weekday → ISO (Monday=1 … Sunday=7). FHIR ``dayOfWeek``
    codes (``mon``…``sun``) and English names are accepted as strings."""
    if isinstance(value, str):
        idx = ("mon", "tue", "wed", "thu", "fri", "sat", "sun").index(value.strip().casefold()[:3])
        return idx + 1
    v = int(value)
    if convention == "iso":
        out = v
    elif convention == "zero_monday":
        out = v + 1
    elif convention == "zero_sunday":
        out = 7 if v == 0 else v
    else:
        raise ValueError(f"unknown weekday convention {convention!r}")
    if not 1 <= out <= 7:
        raise ValueError(f"weekday out of range: {value!r} ({convention})")
    return out


@dataclass(frozen=True)
class ScheduleParts:
    """A description of a schedule with no words in it: labels render it.
    ``doses_per_day`` counts the doses on each day the schedule is due (0 for
    as-needed and unscheduled), whichever way the instruction stores them."""

    kind: str
    times: tuple[str, ...]
    doses_per_day: int
    period_days: int | None
    weekdays: tuple[int, ...]
    as_needed: bool


def describe_schedule(instruction: DoseInstruction) -> ScheduleParts:
    kind = instruction.schedule_kind()
    return ScheduleParts(
        kind=kind,
        times=instruction.times,
        # Counted from the slots: once daily is stored as `period_days=1` with
        # no count, and the stored fields alone read "0x/day".
        doses_per_day=0 if kind in (KIND_PRN, KIND_UNSCHEDULED) else len(instruction.slots()),
        period_days=instruction.period_days,
        weekdays=tuple(sorted(instruction.weekdays)),
        as_needed=instruction.as_needed,
    )


# --- parsing free text ---------------------------------------------------------------

_NUM = r"(\d+(?:\.\d+)?)"
_ZH_NUM = {
    "半": 0.5,
    "一": 1,
    "两": 2,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}
_FORMS_ZH = "片|粒|胶囊|滴|吸|喷|贴|袋|包|栓|支|瓶|勺|剂"
_DOSE_RE = re.compile(_NUM + r"\s*([A-Za-z\[\]{}%µ/]+|" + _FORMS_ZH + ")")
_DOSE_ZH_RE = re.compile(r"([半一两二三四五六七八九十])\s*(" + _FORMS_ZH + ")")
_TIMES_RE = re.compile(r"\b(\d{1,2}:\d{2})\b")
#: "8am", "8 pm", "8:30 p.m.". Read before ``_TIMES_RE`` and blanked, so
#: "8:30 pm" is one time (20:30) and not also 08:30.
_TIMES_12H_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\b\.?", re.I)
#: A count in digits or zh numerals up to 99 (十二, 二十四): a frequency is
#: written "每十二小时" as often as "每12小时". 半 is never a count.
_ZH_INT = r"\d+|[一两二三四五六七八九]?十[一二三四五六七八九]?|[一两二三四五六七八九]"
#: A dose range in digits: "1-2", "1~2", and the zh 至/到 spellings ("1到2片"
#: read as 2 tablets before they were here).
_RANGE_RE = re.compile(_NUM + r"\s*[-–—~至到]\s*" + _NUM)
#: The same range in zh numerals: 一到两片, 每周一到两次. 一至五 after 周/星期/礼拜 is
#: a weekday range (周一至五), unless a 次 follows (每周一至二次 is a count).
_ZH_RANGE_RE = re.compile(
    r"(?<![周期拜])[半一两二三四五六七八九十]\s*[-–—~至到]\s*[一两二三四五六七八九十]"
    r"|[一两二三四五六七八九十]\s*[-–—~至到]\s*[一两二三四五六七八九十]\s*次"
)
_EN_NUMBERS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
}
_EN_ORDINALS = {
    "second": 2, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6, "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10,
}
_EN_ADVERBS = {"once": 1, "twice": 2, "thrice": 3}
_EN_INT = r"\d+|" + "|".join(_EN_NUMBERS)
_EN_NTH = r"\d+(?:st|nd|rd|th)?|" + "|".join((*_EN_NUMBERS, *_EN_ORDINALS))
#: "3 times", "3x", "three times", "once": how many doses one period holds.
#: A count is never a bare number: "2 tablets daily" is a dose, not a count.
_EN_COUNT = rf"(?:(?:{_EN_INT})\s*(?:times?|x|×)|once|twice|thrice)"
#: A count in words with a range in it: "one or two tablets", "once or twice
#: daily", "2 to 3 times". The digits-and-dash form is ``_RANGE_RE``; a clock
#: time ("08:00 to 20:00") is not a dose range.
_EN_WORD_RANGE_RE = re.compile(
    rf"\b(?:{_EN_INT}|once|twice|thrice)\s*(?:-|–|to|or)\s*(?:{_EN_INT}|twice|thrice)\b(?!\s*:)", re.I
)


def _zh_int(token: str) -> int:
    """A digit run or a zh numeral up to 99 (``十二``, ``二十四``) to an int."""
    if token.isdigit():
        return int(token)
    tens, sep, ones = token.partition("十")
    if not sep:
        return int(_ZH_NUM[token])
    return (int(_ZH_NUM[tens]) if tens else 1) * 10 + (int(_ZH_NUM[ones]) if ones else 0)


def _zh_count(token: str | None, default: int) -> int:
    """``_zh_int``, or ``default`` when ``token`` is absent: a frequency's
    trailing count is often left implicit (每周, 隔日 both mean one occurrence)."""
    return default if token is None else _zh_int(token)


def _en_int(token: str) -> int:
    """``8``, ``2nd``, ``eight``, ``second``, ``3x``, ``three times``, ``twice``
    to an int: the number a count or a period multiplier starts with."""
    t = token.casefold().strip()
    if t in _EN_ADVERBS:
        return _EN_ADVERBS[t]
    m = re.match(r"\d+", t)
    if m:
        return int(m.group(0))
    word = re.match(r"[a-z]+", t).group(0)
    return _EN_NUMBERS.get(word) or _EN_ORDINALS[word]


def _latin(*forms: str) -> str:
    """A Latin sig abbreviation, dotted or not, as a whole token. ``\\b`` after a
    final dot only matched when a letter followed, so "1 tab t.i.d." at the end
    of a sig was never read."""
    return r"(?<![\w.])(?:" + "|".join(forms) + r")(?![\w.])"


#: Every frequency that is said with a number of hours: "every 8 hours",
#: "q8h", "q 8 hrs", "every eight hours". A count of hours that divides a day
#: is so many doses a day; a multiple of a day is a period; anything else
#: (every 5 hours, every 36 hours) has no field and refuses.
_ZH_HOURS_RE = re.compile(rf"每\s*(?:隔\s*)?({_ZH_INT})\s*个?\s*(?:小时|钟头|h)(?:\s*({_ZH_INT})\s*次)?", re.I)
#: "every/q N days|weeks|hours", with an optional count of doses per period in
#: front ("twice every 3 days" is two per period, which no field holds).
_EN_EVERY_N_RE = re.compile(
    rf"(?:\b(?P<occ>{_EN_COUNT})\s+)?"
    rf"(?:\bevery\s+(?P<n>{_EN_NTH})\s*|\bq\s*(?P<qn>\d+)\s*)"
    r"(?P<unit>days?|d|weeks?|wks?|w|hours?|hrs?|h)\b",
    re.I,
)
#: "every other day/week/night", "on alternate days", "qod".
_EN_EVERY_OTHER_RE = re.compile(
    rf"(?:\b(?P<occ>{_EN_COUNT})\s+)?"
    r"(?:\bevery\s+(?:other|alternate)\s+(?P<unit>day|week|night|morning|evening)s?\b"
    r"|\b(?:on\s+)?alternate\s+(?P<unit2>day|week)s\b"
    r"|" + _latin(r"q\.?o\.?d\.?") + ")",
    re.I,
)
_FORTNIGHTLY_RE = re.compile(r"\b(?:once\s+)?(?:fortnightly|(?:every|each|a)\s+fortnight)\b", re.I)
#: One count + one period: "3 times daily", "2x/day", "two times a day", "once
#: per week", "twice a month". Before this was one token, a digit count was
#: missed and the bare "daily" after it read as once daily (#349).
_EN_PER = r"(?:a|an|per|each|every|in\s+a)"
_EN_COUNT_RE = re.compile(
    rf"\b(?P<count>{_EN_COUNT})\s*(?:"
    rf"(?P<day>(?:{_EN_PER}\s+day|/\s*d(?:ay)?|daily|(?:per|in|every|each)\s+24\s*(?:hours?|hrs?|h))\b)"
    rf"|(?P<week>(?:{_EN_PER}\s+(?:week|wk)|/\s*(?:week|wk|w)|weekly)\b)"
    rf"|(?P<month>(?:{_EN_PER}\s+(?:month|mo)|/\s*(?:month|mo)|monthly)\b)"
    rf"|(?P<year>(?:{_EN_PER}\s+(?:year|yr)|/\s*(?:year|yr)|yearly|annually)\b))",
    re.I,
)
#: Said outright, but ``Schedule`` has no field for it: months and years (a
#: month is not 30 days), bi-/semi- words that mean both "twice a" and
#: "every other", and a weekday that is not every week ("every other
#: Monday", "the first Monday of the month"). A duration ("for 3 months") has
#: none of these prefixes.
_EN_NO_FIELD_PERIOD_RE = re.compile(
    r"\b(?:monthly|bimonthly|semimonthly|semi-monthly|quarterly|yearly|annually|biannually|semiannually"
    r"|semi-annually|biweekly|bi-weekly|semiweekly|semi-weekly)\b"
    rf"|\b(?:every|each|per|q)\s*(?:other\s+|(?:{_EN_NTH})\s*)?(?:months?|mos?|years?|yrs?|quarters?)\b"
    r"|\bof\s+the\s+month\b"
    rf"|\bevery\s+(?:other|alternate|{_EN_NTH})\s+(?:mon|tue|wed|thu|fri|sat|sun)",
    re.I,
)
_ZH_NO_FIELD_PERIOD_RE = re.compile(
    rf"(?:每|隔)\s*(?:{_ZH_INT}|半)?\s*个?\s*(?:月|年|季度?)"
    rf"|(?:半|{_ZH_INT})\s*个?\s*(?:月|年|季度)\s*(?:{_ZH_INT})\s*次"
)
#: A count with no number in it: "several times a day", 每天数次. It used to
#: leave the bare "daily"/每天 behind, which read as once.
_EN_VAGUE_COUNT_RE = re.compile(
    r"\b(?:several|multiple|many|few|some|numerous|various)\s+times\b", re.I
)
_ZH_VAGUE_COUNT_RE = re.compile(r"(?:每天|每日|一天|一日|每周|每星期|一周|一星期)\s*(?:数|多|几|若干|好几)\s*次")
#: A step or a taper ("daily for 3 days, then twice daily", 然后改为): two
#: regimens. The first step's bare "daily" names no count, so without this
#: the second step's count absorbed it and the taper read as its last step.
_STEP_RE = re.compile(
    r"\b(?:then|thereafter|followed\s+by|taper(?:ing)?|increase\s+to|decrease\s+to|reduce\s+to)\b"
    r"|然后|随后|改为|减为|减至|增至|增加至|加至|加到",
    re.I,
)
#: An exception the schedule cannot hold: "daily except weekends", "q4h while
#: awake" (fewer than six a day, by an amount no sig says), 除周日外.
_EXCEPTION_RE = re.compile(
    r"\b(?:except|excluding|other\s+than|apart\s+from|but\s+not)\b|\bwhile\s+awake\b|除了?\s*(?:周|星期|礼拜)",
    re.I,
)

#: Weekday names and abbreviations as whole words. A prefix match read
#: "month", "monitor", "saturated" and "sunlight" as weekdays (#350).
_EN_DAY_RE = re.compile(
    r"\b(?:(?:mon|tues|wednes|thurs|fri|satur|sun)days?|mon|tues?|weds?|thu(?:rs?)?|fri|sat|sun)\b\.?", re.I
)
#: Abbreviations that are also ordinary words ("avoid sun", "sat up"): a
#: weekday only next to another day ("Sat/Sun") or after on/every/each.
_EN_DAY_WORDS = frozenset({"wed", "sat", "sun"})
_EN_DAY_LIST_SEP = re.compile(r"\s*(?:,|/|&|\+|and)?\s*(?:and\s+)?", re.I)
_EN_DAY_RANGE_SEP = re.compile(r"\s*(?:-|–|—|~|to|through|thru|until|till)\s*", re.I)
_EN_DAY_OR_SEP = re.compile(r"\s*,?\s*or\s+", re.I)
_WEEK_PARTS_RE = re.compile(r"\b(weekdays?|weekends?)\b|(工作日)|(周末)", re.I)
_ZH_WEEKDAYS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "日": 7, "天": 7}
_ZH_WEEKDAY_RE = re.compile(
    r"(?:周|星期|礼拜)([一二三四五六日天][一二三四五六日天、,，和及\s]*)(?!次)"
)  # 周一、三、五; 每周一次 is "once weekly"
#: 周一至周五, 星期一到五: every day from one to the other, not the two ends.
_ZH_WEEKDAY_RANGE_RE = re.compile(
    r"(?:周|星期|礼拜)([一二三四五六日天])\s*[-–—~至到]\s*(?:周|星期|礼拜)?([一二三四五六日天])(?!\s*次)"
)
_PRN_RE = re.compile(
    r"\b(?:prn|as needed|when needed|if needed|as required|when required)\b|必要时|必要的时候|按需|需要时|需要的时候",
    re.I,
)
#: The per-day counts said without a number: Latin abbreviations, and the
#: named times of day that pair up. Most specific first, each blanked once
#: matched. ``od`` is deliberately absent: in ophthalmic sigs it means the
#: right eye, not once daily. 早晚 (morning and evening) and 早中晚 are how a
#: Chinese sig most often says two and three a day; without them 每天早晚 read
#: as once. The worded and digit counts ("3 times daily", 每日2次) are one
#: token each (``_EN_COUNT_RE``, ``_PER_DAY_COUNT_RE``), so the bare
#: "daily"/每天 row never has to also guess a count.
_PER_DAY = (
    (re.compile(_latin(r"q\.?i\.?d\.?", r"q\.?d\.?s\.?"), re.I), 4),
    (
        re.compile(
            _latin(r"t\.?i\.?d\.?", r"t\.?d\.?s\.?")
            + r"|早[\s、,，和]*中[\s、,，和]*晚|早上[\s、,，和]*中午[\s、,，和]*晚上",
            re.I,
        ),
        3,
    ),
    (
        re.compile(
            _latin(r"b\.?i\.?d\.?", r"b\.?d\.?")
            + r"|\bmorning\s+and\s+(?:evening|night)\b|早[\s、,，和]*晚|早上[\s、,，和]*晚上",
            re.I,
        ),
        2,
    ),
    (re.compile(_latin(r"q\.?d\.?"), re.I), 1),
)
#: "Every day" with no count: it qualifies a count ("每天早晚" is two a day)
#: rather than competing with one, and is once a day only when nothing else
#: says how many. "for a day" is a duration.
_DAILY_RE = re.compile(
    r"\b(?:daily|everyday|every\s+day|each\s+day|per\s+day|(?<!\bfor\s)a\s+day)\b|每天|每日", re.I
)
#: One prefix + one count + 次: "每日2次", "一日三次", "1日 1 次". Out-of-range
#: counts still match (每日10次, 一天五次) so the caller below can refuse them
#: instead of silently dropping the frequency, which would leave only the
#: dose behind (the partial parse #302 reported).
_PER_DAY_COUNT_RE = re.compile(rf"(?:每天|每日|一天|一日|1日)\s*({_ZH_INT})\s*次")
#: "每/隔" + an optional count + "天/日/周/星期" + an optional trailing count + 次:
#: "每2天", "每两周1次", "隔日", "隔一日一次". Bare 每天/每日/每周/每星期 (no digit
#: between the prefix and the unit) is left to the callers below: a trailing
#: count on those means something else (每天2次 is twice a DAY, not a 2-day
#: period) and 每周/每星期 alone, with nothing after, is weekly. 一/1 are not
#: prefixes here: that explicit-period form is "每2天", never "一2天".
_ZH_PERIOD_RE = re.compile(rf"(每|隔)\s*({_ZH_INT})?\s*(天|日|周|星期)(?:\s*({_ZH_INT})\s*次)?")
#: An explicit weekly count: "每周2次", "一周两次". ``Schedule`` has no "N times
#: a week" field (only an interval or named weekdays), so unless N is 1 or
#: the weekdays are named, this is refused below.
_ZH_WEEKLY_COUNT_RE = re.compile(rf"(?:每周|每星期|一周|一星期)\s*({_ZH_INT})\s*次")
#: Bare, no count said: "every day" by itself for weeks.
_WEEKLY_RE = re.compile(
    r"\b(?:weekly|every\s+week|each\s+week|per\s+week|(?<!\bfor\s)a\s+week)\b"
    r"|" + _latin(r"q\.?\s*w(?:ee)?k?\.?") + r"|每周|每星期|每礼拜",
    re.I,
)
#: A named time of day. One alone is once a day at that time ("at bedtime",
#: 每晚, "qam"), and qualifies a weekday or a period without competing with it
#: ("every Monday morning"). Two or more different ones are that many a day
#: ("早上一片，晚上一片"), the same reading the 早晚 row gives. Meals (晚饭, with
#: breakfast) are not here: "with meals" is three a day as often as one.
_TIMES_OF_DAY = (
    ("morning", re.compile(r"\bmornings?\b|" + _latin(r"q\.?a\.?m\.?") + r"|早上|早晨|清晨|晨起|上午|每早|每晨", re.I)),
    ("noon", re.compile(r"\b(?:noon|midday)\b|中午", re.I)),
    ("afternoon", re.compile(r"\bafternoons?\b|下午", re.I)),
    ("evening", re.compile(r"\bevenings?\b|" + _latin(r"q\.?p\.?m\.?") + r"|傍晚", re.I)),
    (
        "night",
        re.compile(
            r"\b(?:nights?|nightly|bedtime)\b|\bbefore\s+(?:bed|sleep(?:ing)?)\b|" + _latin(r"(?:q\.?)?h\.?s\.?")
            + r"|晚上|夜间|夜里|每晚|睡前|睡觉前|就寝前",
            re.I,
        ),
    ),
)
#: A time of day said as WHEN to take it, not a bare noun: alone, "evening"
#: is as likely "evening primrose oil" as a time, and must not invent a
#: once-daily schedule. Bare nouns still count when two different ones
#: appear ("1 tablet morning and bedtime"), since then they cannot both be
#: a name.
_TIME_OF_DAY_SAID_RE = re.compile(
    r"\b(?:in\s+the|every|each|at|before|after|this)\s+(?:mornings?|evenings?|afternoons?|noon|midday|nights?|bed"
    r"(?:time)?|sleep(?:ing)?)\b|\b(?:nightly|bedtime)\b|"
    + _latin(r"q\.?a\.?m\.?", r"q\.?p\.?m\.?", r"(?:q\.?)?h\.?s\.?")
    + r"|早上|早晨|清晨|晨起|上午|每早|每晨|中午|下午|傍晚|晚上|夜间|夜里|每晚|睡前|睡觉前|就寝前",
    re.I,
)


def _day_span(first: int, last: int) -> list[int]:
    """ISO weekdays from ``first`` to ``last`` inclusive, wrapping past Sunday."""
    return [(first - 1 + i) % 7 + 1 for i in range((last - first) % 7 + 1)]


def _en_weekdays(text: str) -> set[int] | None:
    """The English weekdays named in ``text``. A run of days joined by commas,
    "and" or "/" is a list; joined by "-", "to" or "through" it is a range
    ("Mon-Fri" is five days, not two). ``None`` when days are offered as a
    choice ("Mon or Tue"): one of them, but not which."""
    days: set[int] = set()
    found = list(_EN_DAY_RE.finditer(text))
    i = 0
    while i < len(found):
        run = [found[i]]
        ranges: list[bool] = []
        while i + 1 < len(found):
            between = text[found[i].end():found[i + 1].start()]
            if _EN_DAY_OR_SEP.fullmatch(between):
                return None
            if _EN_DAY_RANGE_SEP.fullmatch(between):
                ranges.append(True)
            elif _EN_DAY_LIST_SEP.fullmatch(between):
                ranges.append(False)
            else:
                break
            i += 1
            run.append(found[i])
        i += 1
        word = run[0].group(0).rstrip(".").casefold()
        if (
            len(run) == 1
            and word in _EN_DAY_WORDS
            and not re.search(r"\b(?:on|every|each)\s+$", text[: run[0].start()], re.I)
        ):
            continue  # "avoid sun": the word, not Sunday
        isos = [weekday_from(m.group(0)) for m in run]
        days.add(isos[0])
        for prev, cur, is_range in zip(isos[:-1], isos[1:], ranges, strict=True):
            days.update(_day_span(prev, cur) if is_range else (cur,))
    return days


def _week_parts(text: str) -> set[int]:
    """"on weekdays", "every weekend", 工作日, 周末 as their ISO days."""
    days: set[int] = set()
    for en, workdays, weekend in _WEEK_PARTS_RE.findall(text):
        if workdays or en.casefold().startswith("weekday"):
            days.update(range(1, 6))
        elif weekend or en:
            days.update((6, 7))
    return days


def _zh_weekdays(text: str) -> set[int]:
    days: set[int] = set()

    def _range(m: re.Match[str]) -> str:
        days.update(_day_span(_ZH_WEEKDAYS[m.group(1)], _ZH_WEEKDAYS[m.group(2)]))
        return " "

    text = _ZH_WEEKDAY_RANGE_RE.sub(_range, text)
    for run in _ZH_WEEKDAY_RE.findall(text):
        days.update(_ZH_WEEKDAYS[ch] for ch in run if ch in _ZH_WEEKDAYS)
    return days


def parse_dose_instruction(text: str | None) -> Schedule | None:
    """Structure from free text such as ``"500 mg twice daily with food"`` or
    ``"每天两次，每次一片"``.

    The grammar is small and closed (``docs/medications.md``): one dose
    (number + unit or form), one frequency (n per day / every N days, weeks
    or hours / weekdays / weekly / as needed), optional clock times. Returns
    a one-element schedule, or ``None`` when nothing is recognised: the
    caller keeps the original text either way. **No partial parses**: a dose
    range (``"1-2 tablets"``, ``"one or two"``), two different doses, two
    frequencies, two regimens, or a frequency ``Schedule`` has no field for
    ("twice weekly" with no weekdays named; "every 2 days, 3 times"; "5 times
    daily"; "once a month"; "every 5 hours") return ``None`` rather than half
    a regimen or the nearest guess.
    """
    raw = unicodedata.normalize("NFKC", (text or "")).strip()
    if not raw:
        return None
    if any(p.search(raw) for p in (_RANGE_RE, _ZH_RANGE_RE, _EN_WORD_RANGE_RE, _STEP_RE, _EXCEPTION_RE)):
        return None  # a range, a step or an exception: none of them is one regimen with one field each
    doses = [dose_from_text(v, u) for v, u in _DOSE_RE.findall(raw)]
    doses += [dose_from_text(_ZH_NUM[v], u) for v, u in _DOSE_ZH_RE.findall(raw)]
    doses = [d for d in doses if d is not None]
    if len({(d.value, d.unit) for d in doses}) > 1:
        return None  # two different amounts: two regimens, or a range in words
    dose = doses[0] if doses else None
    as_needed = bool(_PRN_RE.search(raw))

    clock: list[str] = []

    def _clock_12h(m: re.Match[str]) -> str:
        hour, minute = int(m.group(1)), int(m.group(2) or 0)
        if not 1 <= hour <= 12:
            return m.group(0)
        clock.append(f"{hour % 12 + (12 if m.group(3).lower() == 'p' else 0):02d}:{minute:02d}")
        return " "

    rest = _TIMES_12H_RE.sub(_clock_12h, raw)
    times = (*clock, *_TIMES_RE.findall(rest))

    # Everything a frequency can say, each way it can be said. A phrase is
    # blanked from ``rest`` once read, so "twice daily" is not also "daily".
    unrepresentable = False  # a frequency named outright, but ``Schedule`` has no field to put it in
    counts: set[int] = set()  # doses per due day
    said_daily = False
    period_candidates: set[int] = set()  # period_days from any "every N period" / gap / weekly phrase
    said_period = False
    weekly_counts: set[int] = set()  # "N times a week": a field only once the weekdays are named

    def _hours(hours: int, occurrence: int) -> None:
        nonlocal unrepresentable, said_daily, said_period
        if occurrence != 1 or hours < 1:
            unrepresentable = True
        elif 24 % hours == 0:
            said_daily = True
            counts.add(24 // hours)
        elif hours % 24 == 0:
            said_period = True
            period_candidates.add(hours // 24)
        else:
            unrepresentable = True  # every 5 hours, every 36 hours: no whole number of doses a day

    def _zh_hours_sub(m: re.Match[str]) -> str:
        _hours(_zh_int(m.group(1)), _zh_count(m.group(2), 1))
        return " "

    def _every_n_sub(m: re.Match[str]) -> str:
        nonlocal unrepresentable, said_period
        occurrence = _en_int(m["occ"]) if m["occ"] else 1
        n = _en_int(m["n"] or m["qn"])
        unit = m["unit"].casefold()
        if unit.startswith("h"):
            _hours(n, occurrence)
            return " "
        said_period = True
        if occurrence != 1 or n < 1:
            unrepresentable = True  # "twice every 3 days": no field holds a period and a count
        else:
            period_candidates.add(n * (7 if unit.startswith("w") else 1))
        return " "

    def _every_other_sub(m: re.Match[str]) -> str:
        nonlocal unrepresentable, said_period
        said_period = True
        if m["occ"] and _en_int(m["occ"]) != 1:
            unrepresentable = True
        else:
            unit_days = 7 if (m["unit"] or m["unit2"] or "").casefold() == "week" else 1
            period_candidates.add(period_from_gap(1) * unit_days)
        return " "

    def _zh_period_sub(m: re.Match[str]) -> str:
        nonlocal unrepresentable, said_period
        prefix, mult_token, unit, occ_token = m.group(1), m.group(2), m.group(3), m.group(4)
        if prefix == "每" and mult_token is None:
            return m.group(0)  # bare 每天/每日/每周/每星期: left for the rules below, count and all
        said_period = True
        mult = _zh_count(mult_token, 1)
        unit_days = 7 if unit in ("周", "星期") else 1
        if _zh_count(occ_token, 1) != 1:
            unrepresentable = True  # "每2天3次": a period AND several doses per occurrence, no field holds both
        else:
            period_candidates.add((period_from_gap(mult) if prefix == "隔" else mult) * unit_days)
        return " "

    def _en_count_sub(m: re.Match[str]) -> str:
        nonlocal unrepresentable, said_daily
        n = _en_int(m["count"])
        if m["day"]:
            said_daily = True
            if 1 <= n <= 4:
                counts.add(n)
            else:
                unrepresentable = True  # "5 times daily": the same 1–4 the zh count accepts
        elif m["week"]:
            weekly_counts.add(n)
        else:
            unrepresentable = True  # "once a month": a month is not a whole number of days
        return " "

    rest = _ZH_HOURS_RE.sub(_zh_hours_sub, rest)
    rest = _EN_EVERY_N_RE.sub(_every_n_sub, rest)
    rest = _EN_EVERY_OTHER_RE.sub(_every_other_sub, rest)
    rest = _ZH_PERIOD_RE.sub(_zh_period_sub, rest)
    if _FORTNIGHTLY_RE.search(rest):
        said_period = True
        period_candidates.add(14)
        rest = _FORTNIGHTLY_RE.sub(" ", rest)
    rest = _EN_COUNT_RE.sub(_en_count_sub, rest)
    for token in _ZH_WEEKLY_COUNT_RE.findall(rest):
        weekly_counts.add(_zh_int(token))
    rest = _ZH_WEEKLY_COUNT_RE.sub(" ", rest)
    if any(p.search(rest) for p in (_EN_NO_FIELD_PERIOD_RE, _ZH_NO_FIELD_PERIOD_RE,
                                    _EN_VAGUE_COUNT_RE, _ZH_VAGUE_COUNT_RE)):
        unrepresentable = True

    en_days = _en_weekdays(raw)
    if en_days is None:
        return None  # "Mon or Tue": a choice of day, not a schedule
    weekdays = en_days | _zh_weekdays(raw) | _week_parts(raw)
    if weekly_counts:
        if len(weekly_counts) > 1:
            return None
        (per_week,) = weekly_counts
        if weekdays:
            if per_week != len(weekdays):
                return None  # "twice a week on Monday": the count and the days disagree
        elif per_week == 1:
            said_period = True
            period_candidates.add(7)
        else:
            unrepresentable = True  # "每周2次": no "N times a week" field unless the weekdays are named
    elif not weekdays and _WEEKLY_RE.search(rest):
        said_period = True
        period_candidates.add(7)
    rest = _WEEKLY_RE.sub(" ", rest)

    for token in _PER_DAY_COUNT_RE.findall(rest):
        n = _zh_int(token)
        said_daily = True
        if 1 <= n <= 4:
            counts.add(n)
        else:
            unrepresentable = True  # e.g. "每日10次": a real count, not one the table has a slot shape for
    rest = _PER_DAY_COUNT_RE.sub(" ", rest)  # consumed either way, so the bare 每天/每日 row below can't re-read it
    for pattern, n in _PER_DAY:
        if pattern.search(rest):
            said_daily = True
            counts.add(n)
            rest = pattern.sub(" ", rest)
    if _DAILY_RE.search(rest):
        said_daily = True
        rest = _DAILY_RE.sub(" ", rest)
    times_of_day = {name for name, pattern in _TIMES_OF_DAY if pattern.search(rest)}
    if len(times_of_day) > 1 or (
        times_of_day and _TIME_OF_DAY_SAID_RE.search(rest) and not (weekdays or said_period)
    ):
        said_daily = True
        counts.add(len(times_of_day))

    if unrepresentable or len(counts) > 1 or len(period_candidates) > 1:
        return None
    if bool(weekdays) + said_period + said_daily > 1:
        return None  # two frequencies is no parse, even when one would have won
    period_days = next(iter(period_candidates)) if period_candidates else None
    doses_per_day = 0
    if said_daily:
        n = next(iter(counts)) if counts else 1
        if counts and times and n != len(times):
            return None  # "three times daily at 08:00 and 20:00"
        if n == 1:
            period_days = 1
        else:
            doses_per_day = n
    recognised = dose is not None or times or as_needed or period_days or doses_per_day or weekdays
    if not recognised:
        return None
    try:
        instr = _instruction(dose=dose, times=times, doses_per_day=doses_per_day, period_days=period_days,
                             weekdays=frozenset(weekdays), as_needed=as_needed, text=raw)
    except ValueError:
        return None
    return (instr,)


def _instruction(
    *,
    dose: Dose | None,
    times: tuple[str, ...],
    doses_per_day: int,
    period_days: int | None,
    weekdays: frozenset[int],
    as_needed: bool,
    text: str,
    max_dose_per_day: Dose | None = None,
) -> DoseInstruction:
    """The one timing shape `DoseInstruction` allows, chosen from fields read
    out of a sig or a FHIR dosage, most specific first: as needed, weekdays,
    clock times, a count per day, a period. Raises ``ValueError`` when the
    fields still contradict each other. That includes a weekday list beside a
    period or a count ("every 2 weeks on Monday", "twice a day on Mondays"
    with no clock times): a weekday schedule has room for neither, and
    returning the weekdays alone dropped them without a word."""
    if as_needed:
        return DoseInstruction(dose=dose, as_needed=True, max_dose_per_day=max_dose_per_day, text=text)
    if doses_per_day > 1 and times and doses_per_day != len(times):
        raise ValueError("a count per day and a different number of clock times")
    if weekdays:
        if (period_days or 1) > 1 or (doses_per_day > 1 and not times):
            raise ValueError("a weekday schedule holds clock times, not a period or a bare count")
        return DoseInstruction(dose=dose, weekdays=weekdays, times=times, text=text)
    if times:
        # A period of one day is every day: fixed times, not an interval.
        return DoseInstruction(
            dose=dose, times=times, period_days=period_days if (period_days or 1) > 1 else None, text=text
        )
    if doses_per_day > 1:
        return DoseInstruction(dose=dose, doses_per_day=doses_per_day, text=text)
    return DoseInstruction(dose=dose, period_days=period_days, text=text)


# --- plans, prescriptions, courses ------------------------------------------------

PLAN_ACTIVE = "active"
PLAN_STOPPED = "stopped"
PLAN_ENTERED_IN_ERROR = "entered_in_error"
STORED_PLAN_STATUSES = frozenset({PLAN_ACTIVE, PLAN_STOPPED, PLAN_ENTERED_IN_ERROR})

#: Effective (derived) statuses, FHIR MedicationStatement words.
EFFECTIVE_INTENDED = "intended"
EFFECTIVE_ACTIVE = "active"
EFFECTIVE_COMPLETED = "completed"
EFFECTIVE_STOPPED = "stopped"
EFFECTIVE_ENTERED_IN_ERROR = "entered_in_error"


@dataclass(frozen=True)
class MedicationPlan:
    """A person's standing plan to take a medication (FHIR
    ``MedicationStatement``). ``status`` is what is *stored*: ``active``,
    ``stopped`` or ``entered_in_error``; ``intended`` and ``completed`` are
    derived from the dates by :func:`effective_status`.

    ``start`` is required: a plan with no start cannot be projected or
    placed on a timeline. ``schedule`` holds one instruction per regimen
    (morning 1 tablet, evening ½ tablet is two). ``confirmed`` is ``True`` for
    what the person entered; extraction and import pass ``False`` explicitly
    and UIs/tools label it: projections do not care. ``entered_in_error`` is
    FHIR's "recorded in error", terminal and retroactive: a consumer whose
    "cancel" is reversible maps it to ``stopped`` with its own flag.
    """

    plan_id: str
    concept: MedicationConcept
    schedule: Schedule
    start: date
    end: date | None = None
    status: str = PLAN_ACTIVE
    classification: str = ""  # free text: a consumer's own vocabulary (e.g. prescription / otc / supplement)
    confirmed: bool = True
    order_id: str = ""
    source: str = ""
    source_record_id: str = ""
    subject_id: str = ""
    stopped_on: date | None = None

    def __post_init__(self) -> None:
        if not self.plan_id:
            raise ValueError("a plan needs a plan_id")
        if self.status not in STORED_PLAN_STATUSES:
            raise ValueError(f"stored plan status must be one of {sorted(STORED_PLAN_STATUSES)}, got {self.status!r}")
        if self.end is not None and self.end < self.start:
            raise ValueError("plan end precedes start")
        object.__setattr__(self, "schedule", tuple(self.schedule))
        if not self.schedule:
            raise ValueError("a plan needs at least one instruction (an empty DoseInstruction() is fine)")

    @property
    def last_day(self) -> date | None:
        """The last day the plan was in effect: the earlier of its end and the
        day it was stopped; ``None`` while it is open-ended. A plan can be
        stopped after its end date (the stored status stays ``active`` past
        it), and five call sites read ``stopped_on or end``, the later one."""
        if self.stopped_on is None or self.end is None:
            return self.stopped_on or self.end
        return min(self.stopped_on, self.end)


def effective_status(plan: MedicationPlan, today: date) -> str:
    if plan.status == PLAN_ENTERED_IN_ERROR:
        return EFFECTIVE_ENTERED_IN_ERROR
    if plan.status == PLAN_STOPPED:
        return EFFECTIVE_STOPPED
    if today < plan.start:
        return EFFECTIVE_INTENDED
    if plan.end is not None and today > plan.end:
        return EFFECTIVE_COMPLETED
    return EFFECTIVE_ACTIVE


@dataclass(frozen=True)
class Course:
    """A period during which a plan was followed: the analysis-side
    "exposure" (OMOP ``drug_exposure``) that lines up with readings on a
    timeline. ``closed_by`` says why it ended, or ``None`` while open; an
    open course is materialised at "today in the subject's zone" by whoever
    exports it."""

    plan_id: str
    order_id: str
    start: date
    end: date | None
    closed_by: Literal["stopped", "superseded", "completed", "entered_in_error"] | None = None


def plan_status_transition(
    plan: MedicationPlan, event: Literal["stop", "resume", "void"], *, today: date
) -> tuple[MedicationPlan, Course | None]:
    """The legal moves, and the course a move closes:

    * ``active --stop--> stopped`` (only once the plan has started); closes
      ``Course(start, today, "stopped")``.
    * ``stopped --resume--> active``: a *new* course starts today (the plan's
      ``start`` moves to ``today`` and ``end`` clears) and the closed course
      of the previous period is returned so the store can keep it.
    * ``* --void--> entered_in_error`` (not from ``entered_in_error``).

    Anything else raises: an illegal transition is a bug in the caller, not a
    state to record.
    """
    if event == "void":
        if plan.status == PLAN_ENTERED_IN_ERROR:
            raise ValueError("plan is already entered_in_error")
        return replace(plan, status=PLAN_ENTERED_IN_ERROR), None
    if event == "stop":
        if plan.status != PLAN_ACTIVE:
            raise ValueError(f"cannot stop a plan that is {plan.status}")
        if today < plan.start:
            raise ValueError("cannot stop a plan before it starts; void it or change its start")
        end = today if plan.end is None or plan.end > today else plan.end
        return replace(plan, status=PLAN_STOPPED, stopped_on=today, end=end), Course(
            plan.plan_id, plan.order_id, plan.start, end, "stopped"
        )
    if event == "resume":
        if plan.status != PLAN_STOPPED:
            raise ValueError(f"cannot resume a plan that is {plan.status}")
        closed = Course(plan.plan_id, plan.order_id, plan.start, plan.last_day, "stopped")
        return replace(plan, status=PLAN_ACTIVE, stopped_on=None, start=today, end=None), closed
    raise ValueError(f"unknown plan event {event!r}")


REVIEW_PENDING = "pending"
REVIEW_ACCEPTED = "accepted"
REVIEW_REJECTED = "rejected"
REVIEW_SUPERSEDED = "superseded"
REVIEW_STATES = frozenset({REVIEW_PENDING, REVIEW_ACCEPTED, REVIEW_REJECTED, REVIEW_SUPERSEDED})


@dataclass(frozen=True)
class Prescription:
    """A clinician's order (FHIR ``MedicationRequest``), kept apart from the
    plan the person actually follows. ``order_status`` and ``intent`` are the
    resource's own values, verbatim; ``review`` is *our* state: whether the
    person has accepted it into a plan. ``order_id`` is the caller's: a
    server-local resource id collides across two EHRs."""

    order_id: str
    concept: MedicationConcept
    schedule: Schedule
    order_status: str = ""
    intent: str = "order"
    review: str = REVIEW_PENDING
    plan_id: str = ""
    prior_order_id: str = ""
    effective_start: date | None = None
    effective_end: date | None = None
    source: str = ""

    def __post_init__(self) -> None:
        if self.review not in REVIEW_STATES:
            raise ValueError(f"review must be one of {sorted(REVIEW_STATES)}")
        object.__setattr__(self, "schedule", tuple(self.schedule) or (DoseInstruction(),))


def supersede(old: Prescription, new: Prescription, *, on: date) -> tuple[Prescription, Prescription, Course]:
    """A new prescription replaces an accepted one on ``on``: the old one
    becomes ``superseded`` and its course closes; the new one is accepted,
    chained to the old, and starts on ``on`` unless it says otherwise."""
    if old.review != REVIEW_ACCEPTED:
        raise ValueError("only an accepted prescription can be superseded")
    if old.effective_start is not None and on < old.effective_start:
        raise ValueError("supersede date precedes the old prescription's start")
    closed = replace(old, review=REVIEW_SUPERSEDED, effective_end=on)
    accepted = replace(
        new, review=REVIEW_ACCEPTED, prior_order_id=old.order_id, effective_start=new.effective_start or on
    )
    course = Course(old.plan_id, old.order_id, old.effective_start or on, on, "superseded")
    return closed, accepted, course


def courses(plan: MedicationPlan, *, today: date) -> tuple[Course, ...]:
    """The exposure period a plan's *current* dates imply; earlier periods
    closed by ``plan_status_transition`` live in the store. Nothing for a
    plan recorded in error or not yet started."""
    eff = effective_status(plan, today)
    if eff in (EFFECTIVE_ENTERED_IN_ERROR, EFFECTIVE_INTENDED):
        return ()
    if eff == EFFECTIVE_STOPPED:
        return (Course(plan.plan_id, plan.order_id, plan.start, plan.last_day, "stopped"),)
    if eff == EFFECTIVE_COMPLETED:
        return (Course(plan.plan_id, plan.order_id, plan.start, plan.end, "completed"),)
    return (Course(plan.plan_id, plan.order_id, plan.start, None, None),)


# --- schedule projection ----------------------------------------------------------

GAP_SHIFT_FORWARD = "shift_forward"  # a 02:30 that does not exist becomes 03:30
GAP_SKIP = "skip"  # ...or the slot has no instant that day


def _zone(tz: str) -> tzinfo:
    """The subject's zone, strictly: a dose placed in UTC because its zone
    was unreadable is a dose on the wrong day."""
    return zone(tz, strict=True)


def slot_instant(d: date, slot: str, tz: str, *, gap: str = GAP_SHIFT_FORWARD) -> int | None:
    """The instant a slot falls on for local day ``d`` in ``tz``. A ``day``
    or numbered slot is the local day's start. A clock time that does not
    exist that day (the spring-forward gap) shifts forward under
    ``shift_forward`` or is ``None`` under ``skip``; an ambiguous time (the
    fall-back hour) is its first occurrence."""
    zone = _zone(tz)
    base = slot.partition("#")[0]
    if base == SLOT_DAY or "/" in base:
        return day_bounds_ms(d, tz)[0]
    parts = [int(p) for p in base.split(":")]
    h, mi, s = parts[0], parts[1], (parts[2] if len(parts) > 2 else 0)
    wall = datetime(d.year, d.month, d.day, h, mi, s, tzinfo=zone)
    ms = int(wall.timestamp() * MS)
    back = datetime.fromtimestamp(ms / MS, zone)
    if (back.hour, back.minute, back.second) != (h, mi, s):
        return None if gap == GAP_SKIP else ms
    return ms


class SlotKey(NamedTuple):
    """A slot's identity. A plain ``(plan_id, local_date, slot)`` tuple is
    equal to it and hashes the same, so a store may pass either."""

    plan_id: str
    local_date: date
    slot: str


@dataclass(frozen=True)
class DoseSlot:
    """One planned intake. Identity is ``key`` (a :class:`SlotKey`), never
    the instant. ``utc_ms`` is derived at projection time
    (``None`` for a skipped daylight-saving gap) and is what reminders and
    :func:`slot_state` use."""

    plan_id: str
    local_date: date
    slot: str
    tz: str
    dose: Dose | None = None
    utc_ms: int | None = None

    @classmethod
    def at(
        cls,
        plan_id: str,
        local_date: date,
        slot: str,
        tz: str,
        dose: Dose | None = None,
        *,
        gap: str = GAP_SHIFT_FORWARD,
    ) -> DoseSlot:
        slot = normalize_slot(slot)
        return cls(plan_id, local_date, slot, tz, dose, slot_instant(local_date, slot, tz, gap=gap))

    @property
    def key(self) -> SlotKey:
        return SlotKey(self.plan_id, self.local_date, self.slot)


def _due_on(instruction: DoseInstruction, plan_start: date, d: date) -> bool:
    if d < plan_start:
        return False
    kind = instruction.schedule_kind()
    if kind == KIND_WEEKLY:
        return d.isoweekday() in instruction.weekdays
    if kind == KIND_INTERVAL:
        return (d - plan_start).days % (instruction.period_days or 1) == 0
    return kind in (KIND_FIXED_TIMES, KIND_DAILY)


def _slot_names(schedule: Schedule) -> tuple[tuple[DoseInstruction, str], ...]:
    """Every (instruction, slot) of one due day; a slot name shared by two
    instructions gets ``#i`` so the two doses keep separate identities. An
    as-needed or unscheduled instruction has no slot: counted, its ``day``
    renamed a once-daily ``day`` to ``day#0``, and adding an as-needed dose
    to a plan moved every key of its daily one."""
    scheduled = [
        (i, instr) for i, instr in enumerate(schedule)
        if instr.schedule_kind() not in (KIND_PRN, KIND_UNSCHEDULED)
    ]
    counts: dict[str, int] = {}
    for _, instr in scheduled:
        for s in instr.slots():
            counts[s] = counts.get(s, 0) + 1
    out: list[tuple[DoseInstruction, str]] = []
    for i, instr in scheduled:
        for s in instr.slots():
            out.append((instr, f"{s}#{i}" if counts[s] > 1 else s))
    return tuple(out)


def project_schedule(
    plan: MedicationPlan, *, start: date, end: date, tz: str, max_days: int = 62, gap: str = GAP_SHIFT_FORWARD
) -> tuple[DoseSlot, ...]:
    """Every dose the plan schedules on the local days ``start..end``
    (both inclusive), clipped to the plan's own dates and, for a stopped
    plan, to the day it was stopped: history stays answerable. Interval
    schedules anchor on ``plan.start``. An unknown zone or an inverted
    window raises; a plan recorded in error, an as-needed or an unscheduled
    plan projects to nothing; a window longer than ``max_days`` is refused
    rather than silently truncated."""
    _zone(tz)
    if end < start:
        raise ValueError("window end precedes start")
    if (end - start).days + 1 > max_days:
        raise ValueError(f"window of {(end - start).days + 1} days exceeds max_days={max_days}")
    if plan.status == PLAN_ENTERED_IN_ERROR:
        return ()
    first = max(start, plan.start)
    last = end if plan.last_day is None else min(end, plan.last_day)
    names = _slot_names(plan.schedule)
    out: list[DoseSlot] = []
    d = first
    while d <= last:
        for instr, slot in names:
            if not _due_on(instr, plan.start, d):
                continue
            out.append(DoseSlot(plan.plan_id, d, slot, tz, instr.dose, slot_instant(d, slot, tz, gap=gap)))
        d += timedelta(days=1)
    return tuple(out)


def diff_projection(
    existing: Iterable[DoseSlot], projected: Iterable[DoseSlot]
) -> tuple[tuple[DoseSlot, ...], tuple[DoseSlot, ...]]:
    """What a store must add and remove to match a fresh projection. Keyed
    by :attr:`DoseSlot.key`, so a time-zone change re-projects the same keys
    onto new instants without churning rows. Two obligations stay with the
    store: never remove a key that has a dose event, and only remove slots
    dated on or after the subject's current local day."""
    have = {d.key: d for d in existing}
    want = {d.key: d for d in projected}

    def order(k: SlotKey) -> tuple[date, str, str]:
        # The plan id breaks ties: without it two plans' 08:00 came out in
        # set order, which the string hash seed changes from run to run.
        return (k.local_date, k.slot, k.plan_id)

    to_add = tuple(want[k] for k in sorted(want.keys() - have.keys(), key=order))
    to_remove = tuple(have[k] for k in sorted(have.keys() - want.keys(), key=order))
    return to_add, to_remove


# --- dose events and derived states ---------------------------------------------

EVENT_TAKEN = "taken"
EVENT_SKIPPED = "skipped"
STORED_EVENT_STATUSES = frozenset({EVENT_TAKEN, EVENT_SKIPPED})

STATE_UPCOMING = "upcoming"
STATE_DUE = "due"
STATE_TAKEN = "taken"
STATE_SKIPPED = "skipped"
STATE_MISSED = "missed"
STATE_UNSCHEDULABLE = "unschedulable"  # a skipped daylight-saving gap: no instant that day

GRACE_END_OF_LOCAL_DAY = "end_of_local_day"


@dataclass(frozen=True)
class DoseEvent:
    """What happened (FHIR ``MedicationAdministration``): a dose was taken or
    deliberately skipped, at an instant, possibly against a scheduled slot.
    ``slot_key=None`` is an unscheduled intake (an as-needed dose, an extra).
    ``reason`` is the person's words and stays out of ``repr``."""

    event_id: str
    plan_id: str
    status: str
    taken_at_ms: int
    tz: str
    slot_key: SlotKey | None = None
    dose: Dose | None = None
    recorded_by: str = "user"  # user | caregiver | device | import
    reason: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.status not in STORED_EVENT_STATUSES:
            raise ValueError(f"a stored dose event is {sorted(STORED_EVENT_STATUSES)}, never a derived state")
        if self.slot_key is not None:
            plan_id, local_date, slot = self.slot_key
            if plan_id != self.plan_id:
                raise ValueError("slot_key belongs to another plan")
            object.__setattr__(self, "slot_key", SlotKey(plan_id, local_date, normalize_slot(slot)))


def _answers(events: Iterable[DoseEvent]) -> dict[SlotKey, DoseEvent]:
    """The event that answers each slot: the latest by ``taken_at_ms``,
    whatever order the store returns (the reference store's is newest first,
    and the plan view used to keep the last one it saw, the oldest)."""
    answered: dict[SlotKey, DoseEvent] = {}
    for e in events:
        if e.slot_key is None:
            continue
        prev = answered.get(e.slot_key)
        if prev is None or e.taken_at_ms > prev.taken_at_ms:
            answered[e.slot_key] = e
    return answered


def _deadline_ms(slot: DoseSlot, due_ms: int, grace: str | int) -> int:
    if grace == GRACE_END_OF_LOCAL_DAY:
        return day_bounds_ms(slot.local_date, slot.tz)[1] - 1
    return due_ms + int(grace) * 60 * MS


def slot_state(
    slot: DoseSlot, event: DoseEvent | None, *, now_ms: int, grace: str | int = GRACE_END_OF_LOCAL_DAY
) -> str:
    """``taken`` / ``skipped`` when an event answers the slot; otherwise
    ``upcoming`` before its instant, ``due`` until the grace deadline (end of
    the slot's local day in the slot's zone, or ``grace`` minutes after the
    instant), then ``missed``; ``unschedulable`` when the slot has no
    instant. Derived: call it, do not store it."""
    if event is not None:
        return STATE_TAKEN if event.status == EVENT_TAKEN else STATE_SKIPPED
    if slot.utc_ms is None:
        return STATE_UNSCHEDULABLE
    if now_ms < slot.utc_ms:
        return STATE_UPCOMING
    if now_ms <= _deadline_ms(slot, slot.utc_ms, grace):
        return STATE_DUE
    return STATE_MISSED


@dataclass(frozen=True)
class Adherence:
    """Counts over one window. ``percent`` is ``100 · taken / (taken + skipped
    + missed)``, one decimal, over *elapsed* slots only: ``None`` when none
    have elapsed (an as-needed plan, or a window that has not started).
    ``extra`` are taken doses with no slot in the window; ``extra_skipped``
    are skips with no slot (they answer nothing); ``unschedulable`` slots
    are excluded from every other count. Roll-ups across plans add counts;
    they never average percentages."""

    plan_id: str
    window: tuple[date, date]
    scheduled: int
    taken: int
    skipped: int
    missed: int
    pending: int
    extra: int
    percent: float | None
    extra_skipped: int = 0
    unschedulable: int = 0


def adherence(
    plan: MedicationPlan,
    scheduled: Iterable[DoseSlot],
    events: Iterable[DoseEvent],
    *,
    window: tuple[date, date],
    now_ms: int,
    tz: str,
    grace: str | int = GRACE_END_OF_LOCAL_DAY,
) -> Adherence:
    """Adherence for ``plan`` over ``window`` as of ``now_ms``. Events match
    slots by key (the latest event per slot wins); ``tz`` places slot-less
    events on a local date. The grace deadline is judged in each slot's own
    zone: the zone the person was in when the dose was due."""
    lo, hi = window
    zone = _zone(tz)
    slots = [d for d in scheduled if d.plan_id == plan.plan_id and lo <= d.local_date <= hi]
    by_key = {d.key: d for d in slots}
    mine = [e for e in events if e.plan_id == plan.plan_id]
    answered = _answers(e for e in mine if e.slot_key in by_key)
    extra = extra_skipped = 0
    for e in mine:
        if e.slot_key is not None and e.slot_key in by_key:
            continue
        if lo <= datetime.fromtimestamp(e.taken_at_ms / MS, zone).date() <= hi:
            if e.status == EVENT_TAKEN:
                extra += 1
            else:
                extra_skipped += 1
    taken = skipped = missed = pending = unschedulable = 0
    for d in slots:
        state = slot_state(d, answered.get(d.key), now_ms=now_ms, grace=grace)
        if state == STATE_TAKEN:
            taken += 1
        elif state == STATE_SKIPPED:
            skipped += 1
        elif state == STATE_MISSED:
            missed += 1
        elif state == STATE_UNSCHEDULABLE:
            unschedulable += 1
        else:
            pending += 1
    elapsed = taken + skipped + missed
    percent = round(100.0 * taken / elapsed, 1) if elapsed else None
    return Adherence(
        plan.plan_id,
        window,
        len(slots) - unschedulable,
        taken,
        skipped,
        missed,
        pending,
        extra,
        percent,
        extra_skipped,
        unschedulable,
    )


# --- mentions in free text → plans ----------------------------------------------

ASSERTION_TAKING = "taking"
ASSERTION_STOPPED = "stopped"
ASSERTION_NEGATED = "negated"  # "I don't take X"
ASSERTION_HYPOTHETICAL = "hypothetical"  # "the doctor might start me on X"
ASSERTIONS = frozenset({ASSERTION_TAKING, ASSERTION_STOPPED, ASSERTION_NEGATED, ASSERTION_HYPOTHETICAL})
SUBJECTS = frozenset({"self", "other", "unknown"})


@dataclass(frozen=True)
class MedicationMention:
    """A medication an extraction found in a person's text. The fields an
    extractor fills; the *decision* to make a plan is :func:`reconcile_mentions`'s.
    Every free-text field is out of ``repr``; ``quote`` is the supporting
    span and never belongs in a log."""

    name: str = field(repr=False)
    dose_text: str = field(default="", repr=False)
    frequency_text: str = field(default="", repr=False)
    instructions_text: str = field(default="", repr=False)
    started_on: date | None = None
    ended_on: date | None = None
    assertion: str = ASSERTION_TAKING
    subject: str = "self"
    as_needed: bool = False
    quote: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.assertion not in ASSERTIONS:
            raise ValueError(f"assertion must be one of {sorted(ASSERTIONS)}")
        if self.subject not in SUBJECTS:
            raise ValueError(f"subject must be one of {sorted(SUBJECTS)}")

    @property
    def concept_key(self) -> str:
        return MedicationConcept(self.name).concept_key if normalize_name(self.name) else ""


@dataclass(frozen=True)
class Reconcile:
    """What one record's mentions imply for a person's plans. Keys are
    concept keys (hashes), so the whole result can be logged."""

    create: tuple[MedicationPlan, ...] = ()
    already_known: tuple[str, ...] = ()
    stopped_known: tuple[str, ...] = ()  # the person says they stopped; the plan exists: the caller decides
    stopped_unknown: tuple[str, ...] = ()  # says stopped, no plan on file, never create one
    ignored: tuple[tuple[str, str], ...] = ()  # (concept_key, reason)

    def counts(self) -> dict[str, int]:
        return {
            "create": len(self.create),
            "already_known": len(self.already_known),
            "stopped_known": len(self.stopped_known),
            "stopped_unknown": len(self.stopped_unknown),
            "ignored": len(self.ignored),
        }


def _schedule_from_mention(m: MedicationMention) -> Schedule:
    text = " ".join(t for t in (m.dose_text, m.frequency_text, m.instructions_text) if t).strip()
    parsed = parse_dose_instruction(text) if text else None
    if parsed:
        instr = parsed[0]
        if m.as_needed and not instr.as_needed:
            instr = DoseInstruction(dose=instr.dose, as_needed=True, text=text)
        return (instr,)
    return (DoseInstruction(as_needed=m.as_needed, text=text),)


def plan_id_for(subject_id: str, source_record_id: str, concept_key: str) -> str:
    """The idempotent id of a plan created from a record: re-extracting the
    same record yields the same ids whatever order the mentions come in."""
    return stable_hash((subject_id, source_record_id, concept_key))[:16]


def reconcile_mentions(
    existing: Iterable[MedicationPlan | str],
    mentions: Sequence[MedicationMention],
    *,
    record_date: date,
    source_record_id: str,
    subject_id: str,
    max_cards: int = 8,
) -> Reconcile:
    """Decide, for one record, which mentions become new (unconfirmed) plans.

    ``existing`` is the person's plans, or just their concept keys (a
    consumer whose cards are not ``MedicationPlan`` objects passes keys).
    Negated, hypothetical and third-person mentions are ignored; a "stopped"
    mention never creates a plan; a mention whose concept matches an existing
    plan is already known; at most ``max_cards`` plans are created from one
    record (a text naming twenty drugs is a leaflet, not a medication list).
    Mentions are de-duplicated by concept within the record, first one wins.
    """
    known: set[str] = set()
    for p in existing:
        if isinstance(p, str):
            known.add(p)
        elif p.status != PLAN_ENTERED_IN_ERROR:
            known.add(p.concept.concept_key)
    seen: set[str] = set()
    create: list[MedicationPlan] = []
    already: list[str] = []
    stopped_known: list[str] = []
    stopped_unknown: list[str] = []
    ignored: list[tuple[str, str]] = []
    for m in mentions:
        key = m.concept_key
        if not key:
            ignored.append(("", "empty_name"))
            continue
        if key in seen:
            ignored.append((key, "duplicate_in_record"))
            continue
        seen.add(key)
        if m.assertion in (ASSERTION_NEGATED, ASSERTION_HYPOTHETICAL):
            ignored.append((key, m.assertion))
            continue
        if m.subject != "self":
            ignored.append((key, f"subject_{m.subject}"))
            continue
        if m.assertion == ASSERTION_STOPPED:
            (stopped_known if key in known else stopped_unknown).append(key)
            continue
        if key in known:
            already.append(key)
            continue
        if len(create) >= max_cards:
            ignored.append((key, "max_cards"))
            continue
        start = m.started_on or record_date
        end = m.ended_on if m.ended_on and m.ended_on >= start else None
        create.append(
            MedicationPlan(
                plan_id=plan_id_for(subject_id, source_record_id, key),
                concept=MedicationConcept(text=m.name.strip()),
                schedule=_schedule_from_mention(m),
                start=start,
                end=end,
                status=PLAN_ACTIVE,
                confirmed=False,
                source="record",
                source_record_id=source_record_id,
                subject_id=subject_id,
            )
        )
        known.add(key)
    return Reconcile(tuple(create), tuple(already), tuple(stopped_known), tuple(stopped_unknown), tuple(ignored))


# --- FHIR in -------------------------------------------------------------------------


def _codings(cc: Mapping | None) -> tuple[Coding, ...]:
    if not isinstance(cc, Mapping):
        return ()
    return tuple(
        Coding(system=str(c.get("system", "")), code=str(c.get("code", "")), display=str(c.get("display", "")))
        for c in cc.get("coding", []) or []
        if isinstance(c, Mapping) and c.get("code")
    )


def _concept_from_fhir(resource: Mapping) -> MedicationConcept | None:
    """The medication of a Request/Statement/Order, wherever the resource
    keeps it: ``medicationCodeableConcept`` (R4), ``medication.concept`` (R5),
    ``medicationReference`` resolved against ``contained`` (how Apple Health
    and most EHR exports carry it), or the reference's ``display``. Every
    coding is kept. ``None`` when nothing names the drug."""
    cc = resource.get("medicationCodeableConcept")
    ref = resource.get("medicationReference")
    if not cc and isinstance(resource.get("medication"), Mapping):
        med = resource["medication"]
        cc = med.get("concept")
        ref = ref or med.get("reference")
    codings = _codings(cc)
    text = str(cc.get("text", "") or "") if isinstance(cc, Mapping) else ""
    if not codings and not text and isinstance(ref, Mapping):
        target = str(ref.get("reference", "") or "")
        wanted = target.lstrip("#").rsplit("/", 1)[-1]
        for contained in resource.get("contained", []) or []:
            if (
                isinstance(contained, Mapping)
                and contained.get("resourceType") == "Medication"
                and str(contained.get("id", "")) == wanted
            ):
                codings = _codings(contained.get("code"))
                text = str((contained.get("code") or {}).get("text", "") or "")
                break
        text = text or str(ref.get("display", "") or "")
    text = text or next((c.display for c in codings if c.display), "")
    if not normalize_name(text) and not codings:
        return None
    return MedicationConcept(text=text, codes=codings)


def _instruction_from_fhir_dosage(d: Mapping) -> DoseInstruction:
    dose = None
    for dr in d.get("doseAndRate", []) or []:
        q = dr.get("doseQuantity") if isinstance(dr, Mapping) else None
        if q and q.get("value") is not None:
            dose = dose_from_text(q["value"], str(q.get("code") or q.get("unit") or ""))
            break  # a doseRange stays None: the kernel does not pick a point in a range
    as_needed = (
        bool(d.get("asNeededBoolean"))
        or "asNeededCodeableConcept" in d
        or bool(d.get("asNeeded"))
        or "asNeededFor" in d
    )
    max_per_day = None
    mdp = d.get("maxDosePerPeriod")
    if isinstance(mdp, Mapping) and as_needed:
        num, den = mdp.get("numerator") or {}, mdp.get("denominator") or {}
        if (
            den.get("value") == 1
            and str(den.get("code") or den.get("unit") or "") in ("d", "day", "days")
            and num.get("value") is not None
        ):
            max_per_day = dose_from_text(num["value"], str(num.get("code") or num.get("unit") or ""))
    repeat = (d.get("timing") or {}).get("repeat") or {}
    times = tuple(str(t) for t in repeat.get("timeOfDay", []) or [])
    weekdays = frozenset(weekday_from(w) for w in repeat.get("dayOfWeek", []) or [])
    freq = int(repeat.get("frequency") or 0)
    per = freq or 1  # FHIR: no frequency is once per period
    period = repeat.get("period")
    unit = repeat.get("periodUnit")
    period_days: int | None = None
    doses_per_day = 0
    # A frequency the schedule has no field for. Every case below used to come
    # back as a different, smaller regimen (two doses every 3 days as one;
    # q4-6h as q4h; twice a week at 08:00 as every day at 08:00), so the
    # timing is dropped instead and the instruction stays unscheduled.
    unreadable = False
    if repeat.get("frequencyMax") is not None or repeat.get("periodMax") is not None:
        unreadable = True  # "1-2 times a day", "every 4-6 hours": the kernel does not pick a point in a range
    elif not period:
        pass
    elif unit == "d":
        if per == 1:
            period_days = int(period)
        elif float(period) == 1:
            doses_per_day = per
        else:
            unreadable = True  # N doses every M days
    elif unit == "wk":
        if weekdays:
            # The named days are the week; any other period, or a count that is not them, has no field.
            unreadable = float(period) != 1 or per not in (1, len(weekdays))
        elif per == 1:
            period_days = 7 * int(period)
        else:
            unreadable = True  # "twice a week" with no days named
    elif unit == "h":
        hours = float(period) / per
        if 24 % hours == 0:
            doses_per_day = int(24 // hours)
        elif hours % 24 == 0:
            period_days = int(hours // 24)
        else:
            unreadable = True  # every 5 hours, every 36 hours
    else:
        unreadable = True  # months, years, minutes: no field, and a month is not 30 days
    if doses_per_day == 1:
        # q24h: once a day is a one-day period. A count of one used to fall
        # through every branch of `_instruction` and leave it unscheduled.
        doses_per_day, period_days = 0, 1
    text = str(d.get("text", "") or "")
    structured = bool(times or weekdays or freq or period or as_needed or dose)
    if unreadable:
        times, weekdays, doses_per_day, period_days = (), frozenset(), 0, None
    if not structured and text:
        # FHIR R4B: `Dosage.text` is the SIG, and the spec expects it to carry
        # the instruction when the structured fields do not. A dosage that is
        # only text is the common shape from a pharmacy system, and reading it
        # with this module's own closed grammar is not a guess: it returns
        # `None` rather than half a regimen, and then this falls through to the
        # same empty instruction it would have produced anyway.
        parsed = parse_dose_instruction(text)
        if parsed is not None:
            return replace(parsed[0], text=text)
    try:
        return _instruction(dose=dose, times=times, doses_per_day=doses_per_day, period_days=period_days,
                            weekdays=weekdays, as_needed=as_needed, text=text, max_dose_per_day=max_per_day)
    except ValueError:
        return DoseInstruction(dose=dose, text=text)


def _schedule_from_fhir(dosages: object) -> Schedule:
    """Every ``dosage``/``dosageInstruction`` entry becomes one instruction:
    a morning/evening regimen is two."""
    if not isinstance(dosages, list) or not dosages:
        return (DoseInstruction(),)
    return tuple(_instruction_from_fhir_dosage(d) for d in dosages if isinstance(d, Mapping)) or (DoseInstruction(),)


def _fhir_date(value: object) -> date | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _bounds(dosages: object) -> tuple[date | None, date | None]:
    if isinstance(dosages, list):
        for d in dosages:
            if isinstance(d, Mapping):
                bp = ((d.get("timing") or {}).get("repeat") or {}).get("boundsPeriod") or {}
                if bp:
                    return _fhir_date(bp.get("start")), _fhir_date(bp.get("end"))
    return None, None


def from_fhir_medication_request(resource: Mapping, *, order_id: str = "") -> Prescription | None:
    """A ``MedicationRequest`` (or a DSTU2 ``MedicationOrder``) → a
    prescription awaiting the person's review, or ``None`` when the resource
    does not name a drug. ``status``/``intent`` are copied verbatim; dates
    come from the dosage bounds, then the dispense validity period, then
    ``authoredOn``/``dateWritten``."""
    concept = _concept_from_fhir(resource)
    if concept is None:
        return None
    dosages = resource.get("dosageInstruction")
    b_start, b_end = _bounds(dosages)
    validity = (resource.get("dispenseRequest") or {}).get("validityPeriod") or {}
    return Prescription(
        order_id=order_id or str(resource.get("id") or ""),
        concept=concept,
        schedule=_schedule_from_fhir(dosages),
        order_status=str(resource.get("status") or ""),
        intent=str(resource.get("intent") or "order"),
        review=REVIEW_PENDING,
        effective_start=b_start
        or _fhir_date(validity.get("start"))
        or _fhir_date(resource.get("authoredOn"))
        or _fhir_date(resource.get("dateWritten")),
        effective_end=b_end or _fhir_date(validity.get("end")),
        source=f"fhir:{resource.get('resourceType') or 'MedicationRequest'}",
    )


def from_fhir_medication_statement(
    resource: Mapping, *, default_start: date, today: date, plan_id: str = "", subject_id: str = ""
) -> MedicationPlan | None:
    """A ``MedicationStatement`` → a plan, or ``None`` for a statement that
    is not a plan to import (R4B status table):

    * ``active`` / ``unknown``-with-dates → active, dates from
      ``effectivePeriod`` (or ``effectiveDateTime``, or ``dateAsserted``, or
      ``default_start``);
    * ``completed`` → active with an ``end`` (period end, else asserted, else
      ``default_start``), never open-ended;
    * ``intended`` → only when its start is after ``today``; otherwise ``None``;
    * ``on-hold`` → stopped on the asserted date; ``stopped`` → stopped;
    * ``not-taken``, ``entered-in-error``, ``unknown`` without dates → ``None``;
    * R5 ``recorded``/``draft`` → ``ValueError`` (not supported).
    """
    status_in = str(resource.get("status") or "active")
    if status_in in ("recorded", "draft"):
        raise ValueError(f"R5 MedicationStatement status {status_in!r} is not supported")
    concept = _concept_from_fhir(resource)
    if concept is None or status_in in ("not-taken", "entered-in-error"):
        return None
    period = resource.get("effectivePeriod") or {}
    asserted = _fhir_date(resource.get("dateAsserted"))
    p_start, p_end = _fhir_date(period.get("start")), _fhir_date(period.get("end"))
    start = p_start or _fhir_date(resource.get("effectiveDateTime")) or asserted or default_start
    end = p_end
    stopped_on: date | None = None
    status = PLAN_ACTIVE
    if status_in == "unknown" and not (p_start or p_end):
        return None
    if status_in == "completed":
        end = end or asserted or default_start
    elif status_in == "intended":
        if start <= today:
            return None
    elif status_in in ("stopped", "on-hold"):
        status = PLAN_STOPPED
        stopped_on = end or asserted or default_start
        end = stopped_on
    if end is not None and end < start:
        end = start
    if stopped_on is not None and stopped_on < start:
        stopped_on = start
    return MedicationPlan(
        # NEVER the resource's own `id`. A FHIR resource id is unique within
        # the server that issued it, and a plan id is unique across every
        # person a consumer stores: two people importing from the same clinic
        # collide, and one medication list overwrites another. `plan_id_for`
        # keeps the idempotency that made the raw id tempting (re-importing
        # the same record yields the same id) and scopes it to the subject.
        plan_id=plan_id or plan_id_for(subject_id, str(resource.get("id") or "fhir"), concept.concept_key),
        concept=concept,
        schedule=_schedule_from_fhir(resource.get("dosage")),
        start=start,
        end=end,
        status=status,
        confirmed=False,
        source="fhir:MedicationStatement",
        subject_id=subject_id,
        stopped_on=stopped_on,
    )


# --- ports ---------------------------------------------------------------------------


class MedicationStore(Protocol):
    """Where plans live. ``list(active_only=True)`` is by *stored* status
    (``active``, which includes not-yet-started and completed plans, those
    are derived); the caller applies :func:`effective_status` with the
    subject's local date, never ``date.today()``. Per-field corrections are
    :mod:`mirobody.kernel.overlay` overrides; ``overrides`` returns them so a reader
    can ``overlay.apply``; ``courses`` are the periods
    :func:`plan_status_transition` and :func:`supersede` closed."""

    def list(self, subject_id: str, *, active_only: bool = False) -> Sequence[MedicationPlan]: ...
    def get(self, plan_id: str) -> MedicationPlan | None: ...
    def put(self, plan: MedicationPlan) -> None: ...
    def overrides(self, plan_id: str) -> Sequence: ...
    def courses(self, plan_id: str) -> Sequence[Course]: ...


class DoseLogStore(Protocol):
    """Where dose events live. ``list`` is by the event's local date in the
    subject's zone, both ends inclusive."""

    def list(self, subject_id: str, window: tuple[date, date]) -> Sequence[DoseEvent]: ...
    def append(self, event: DoseEvent) -> None: ...


# --- the read tool: query_medications ------------------------------------------------
#
# Medications have their own tool because they have their own grammar: a plan
# has a lifecycle, a dose log has a day, a course has a reason it closed:
# none of which is a reading's view of a series. Four parameters, every one
# applicable to every call; the readings tool is `mirobody.kernel.query`.

TOOL_NAME = "query_medications"
VIEW_PLAN = "plan"
VIEW_LOG = "log"
VIEW_HISTORY = "history"
VIEWS = (VIEW_PLAN, VIEW_LOG, VIEW_HISTORY)
#: Rows one answer may carry, after each view's own order: the dose log and the
#: history newest first, the plan view active plans first, earliest start first.
#: A medication list is short by nature, a dose log is bounded by its window.
MAX_ROWS = 200
#: The dose log's window when the caller names no dates: this many days
#: ending today, both counted, as `query.LAST_N_DAYS_EXAMPLE` tells the model.
LOG_DEFAULT_DAYS = 30

TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "view": {
            "type": "string",
            "enum": list(VIEWS),
            "default": VIEW_PLAN,
            "description": (
                "plan: the medication list — what the person intends to take, with each plan's status and today's "
                "dose states (NOT an intake record). log: doses actually recorded taken or skipped. "
                "history: courses with their start, end and why they ended (for 'when did I switch')."
            ),
        },
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 20,
            "description": "Drug names or codes to narrow to (any language, partial is fine). Omit for every medication.",
        },
        "start": {
            "type": "string",
            "pattern": r"^\d{4}-\d{2}-\d{2}$",
            "description": (
                f"First local date, inclusive (YYYY-MM-DD). {query.LAST_N_DAYS_EXAMPLE} plan/history: plans or "
                "courses in effect at any point in the window; log: doses recorded in it (default: the last "
                f"{LOG_DEFAULT_DAYS} days)."
            ),
        },
        "end": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$", "description": "Last local date, inclusive."},
    },
}


@dataclass(frozen=True)
class MedicationsRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_query`."""

    view: str = VIEW_PLAN
    keywords: tuple[str, ...] = ()
    start: str = ""
    end: str = ""


def validate_query(args: Mapping[str, object]) -> tuple[query.Rejection, ...]:
    """Everything wrong with the raw arguments; empty means
    :func:`parse_query` will succeed."""
    out = query.reject_unknown(args, TOOL_SCHEMA)
    out.extend(query.reject_view(args, VIEWS))
    out.extend(query.reject_dates(args))
    return tuple(out)


def parse_query(args: Mapping[str, object]) -> MedicationsRequest:
    """Raw arguments → a :class:`MedicationsRequest`; ``ValueError`` when
    :func:`validate_query` finds anything."""
    problems = validate_query(args)
    if problems:
        raise ValueError("; ".join(f"{r.parameter}: {r.reason}" for r in problems))
    return MedicationsRequest(
        view=str(args.get("view") or VIEW_PLAN),
        keywords=query.normalize_list_arg(args.get("keywords")),
        start=str(args.get("start") or ""),
        end=str(args.get("end") or ""),
    )


def matches(concept: MedicationConcept, keywords: Sequence[str]) -> bool:
    """Whether a keyword names this medication: a normalised substring of the
    name, or a code / display the concept carries. No keywords matches all."""
    if not keywords:
        return True
    name = normalize_name(concept.text)
    surfaces = {name, concept.concept_key.casefold()}
    for c in concept.codes:
        surfaces.add((c.code or "").casefold())
        surfaces.add(normalize_name(c.display))
    for kw in keywords:
        k = normalize_name(kw)
        if k and any(k in surface for surface in surfaces if surface):
            return True
    return False


def _overlaps(start: date, end: date | None, window: tuple[date, date] | None) -> bool:
    if window is None:
        return True
    lo, hi = window
    return start <= hi and (end is None or end >= lo)


def schedule_text(schedule: Schedule) -> str:
    """A schedule the model can read, built from the STRUCTURE, never from
    the person's free text, which is health data and stays in the name."""
    parts: list[str] = []
    for instr in schedule:
        d = describe_schedule(instr)
        dose = f"{instr.dose.value:g} {instr.dose.unit}" if instr.dose else ""
        if d.kind == KIND_PRN:
            when = "as needed"
        elif d.kind == KIND_UNSCHEDULED:
            when = "unscheduled"
        else:
            # What a due day holds, then which days are due: a weekly or
            # interval plan's clock times were dropped.
            days = {
                KIND_WEEKLY: "wd" + ",".join(str(w) for w in d.weekdays),
                KIND_INTERVAL: f"every {d.period_days}d",
            }.get(d.kind, "")
            per_day = "/".join(d.times) if d.times else f"{d.doses_per_day}x" + ("" if days else "/day")
            when = " ".join(x for x in (per_day, days) if x)
        parts.append(" ".join(x for x in (dose, when) if x))
    return "; ".join(parts)


def plan_rows(
    plans: Sequence[MedicationPlan],
    todays_events: Sequence[DoseEvent],
    *,
    keywords: Sequence[str] = (),
    window: tuple[date, date] | None = None,
    today: date,
    now_ms: int,
    tz: str,
    limit: int = MAX_ROWS,
) -> list[dict]:
    """The ``plan`` view: one row per plan in effect at some point in
    ``window`` (all plans when ``None``), with its effective status and, for
    an active plan, every slot of ``today`` and its derived state. Pure:
    ``today``/``now_ms`` are the caller's clock in the subject's zone. At most
    ``limit`` rows, as in each view: a caller that asks for one more than it
    shows can tell a cut answer from a complete one."""
    out: list[dict] = []
    answered = _answers(todays_events)
    for plan in plans:
        if plan.status == PLAN_ENTERED_IN_ERROR or not matches(plan.concept, keywords):
            continue
        if not _overlaps(plan.start, plan.last_day, window):
            continue
        status = effective_status(plan, today)
        slots = project_schedule(plan, start=today, end=today, tz=tz) if status == EFFECTIVE_ACTIVE else ()
        out.append(
            {
                "medication": plan.concept.text,
                "status": status,
                "schedule": schedule_text(plan.schedule),
                "today": ", ".join(f"{s.slot}={slot_state(s, answered.get(s.key), now_ms=now_ms)}" for s in slots),
                "since": plan.start.isoformat(),
                "until": plan.last_day.isoformat() if plan.last_day else "",
                "source": plan.source,
                "plan_id": plan.plan_id,
                "provenance": "measured" if plan.confirmed else "computed",
            }
        )
    out.sort(key=lambda r: (r["status"] != EFFECTIVE_ACTIVE, r["since"]), reverse=False)
    return out[:limit]


def log_rows(
    events: Sequence[DoseEvent], plans: Mapping[str, MedicationPlan], *, keywords: Sequence[str] = (),
    limit: int = MAX_ROWS,
) -> list[dict]:
    """The ``log`` view: what was recorded, newest first, each dose named by
    its medication (a log of plan ids answers nothing a person asked)."""
    rows: list[dict] = []
    for e in events:
        plan = plans.get(e.plan_id)
        if plan is not None and not matches(plan.concept, keywords):
            continue
        if plan is None and keywords:
            continue
        at = datetime.fromtimestamp(e.taken_at_ms / 1000, _zone(e.tz))
        rows.append(
            {
                "date": at.date().isoformat(),
                "time": at.strftime("%H:%M"),
                "medication": plan.concept.text if plan else "",
                "status": e.status,
                "slot": e.slot_key.slot if e.slot_key else "",
                "dose": f"{e.dose.value:g} {e.dose.unit}" if e.dose else "",
                "recorded_by": e.recorded_by,
                "plan_id": e.plan_id,
                "provenance": "measured",
                "_at": e.taken_at_ms,
            }
        )
    rows.sort(key=lambda r: r.pop("_at"), reverse=True)
    return rows[:limit]


def history_rows(
    plans: Sequence[MedicationPlan],
    courses_by_plan: Mapping[str, Sequence[Course]],
    *,
    keywords: Sequence[str] = (),
    window: tuple[date, date] | None = None,
    limit: int = MAX_ROWS,
) -> list[dict]:
    """The ``history`` view: every course of every matching plan that was in
    effect at some point in ``window``, newest first, with why it closed
    (``open`` while it has not)."""
    out: list[dict] = []
    for plan in plans:
        if not matches(plan.concept, keywords):
            continue
        for course in courses_by_plan.get(plan.plan_id, ()):
            if not _overlaps(course.start, course.end, window):
                continue
            out.append(
                {
                    "medication": plan.concept.text,
                    "start": course.start.isoformat(),
                    "end": course.end.isoformat() if course.end else "",
                    "closed_by": course.closed_by or "open",
                    "plan_id": plan.plan_id,
                    "provenance": "measured",
                }
            )
    out.sort(key=lambda r: r["start"], reverse=True)
    return out[:limit]


#: The columns each view renders for the model, in order. `provenance` rides
#: in the envelope; `plan_id` is last because it is a handle, not a fact.
VIEW_COLUMNS: dict[str, tuple[str, ...]] = {
    VIEW_PLAN: ("medication", "status", "schedule", "today", "since", "until", "source", "plan_id"),
    VIEW_LOG: ("date", "time", "medication", "status", "slot", "dose", "recorded_by", "plan_id"),
    VIEW_HISTORY: ("medication", "start", "end", "closed_by", "plan_id"),
}

#: Said on every `plan` answer: a model that is not told this reports a
#: medication list as evidence of what was swallowed.
PLAN_NOTE = "a plan is what the person intends to take; it is not a record of doses taken"
LOG_NOTE = "a dose missing from the log is not evidence it was not taken"


__all__ = [
    "LOG_DEFAULT_DAYS",
    "LOG_NOTE",
    "MAX_ROWS",
    "PLAN_NOTE",
    "TOOL_NAME",
    "TOOL_SCHEMA",
    "VIEWS",
    "VIEW_COLUMNS",
    "VIEW_HISTORY",
    "VIEW_LOG",
    "VIEW_PLAN",
    "Adherence",
    "Coding",
    "Course",
    "Dose",
    "DoseEvent",
    "DoseInstruction",
    "DoseLogStore",
    "DoseSlot",
    "MedicationConcept",
    "MedicationMention",
    "MedicationPlan",
    "MedicationStore",
    "MedicationsRequest",
    "Prescription",
    "Reconcile",
    "Schedule",
    "ScheduleParts",
    "SlotKey",
    "Terminology",
    "adherence",
    "courses",
    "describe_schedule",
    "diff_projection",
    "dose_from_text",
    "dose_unit_family",
    "effective_status",
    "from_fhir_medication_request",
    "from_fhir_medication_statement",
    "history_rows",
    "log_rows",
    "matches",
    "normalize_dose_unit",
    "normalize_name",
    "normalize_slot",
    "normalize_time",
    "parse_dose_instruction",
    "parse_query",
    "period_from_gap",
    "plan_id_for",
    "plan_rows",
    "plan_status_transition",
    "project_schedule",
    "reconcile_mentions",
    "schedule_text",
    "slot_instant",
    "slot_state",
    "supersede",
    "validate_query",
    "weekday_from",
]

"""Medications as their own entity — a plan, the doses it schedules, and what
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
  noon. The percentage is over elapsed slots only, and is ``None`` — not 0 —
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
from datetime import date, datetime, timedelta
from importlib import resources
from typing import Literal, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import units
from .series import day_bounds_ms, stable_hash

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
    full-width ``５００ｍｇ`` and ``㎎`` become ASCII — a superset of a plain
    full-width fold), case folding, and dropping all whitespace. Deliberately
    *no* alias or brand→generic mapping — that is an open-ended asset behind
    the ``Terminology`` port. Two spellings of one drug make two plans; a user
    sees that and deletes one, which is safer than a wrong merge.
    """
    folded = unicodedata.normalize("NFKC", (text or "").strip())
    return "".join(folded.split()).casefold()


@dataclass(frozen=True)
class MedicationConcept:
    """What the medication is: free text as the person wrote it, plus any
    codes a terminology service attached. ``text`` and ``strength`` are kept
    out of ``repr`` — they are health data."""

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
        same drug — auto-merging them is how a wrong code silently rewrites a
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
    """A description of a schedule with no words in it — labels render it."""

    kind: str
    times: tuple[str, ...]
    doses_per_day: int
    period_days: int | None
    weekdays: tuple[int, ...]
    as_needed: bool


def describe_schedule(instruction: DoseInstruction) -> ScheduleParts:
    return ScheduleParts(
        kind=instruction.schedule_kind(),
        times=instruction.times,
        doses_per_day=instruction.doses_per_day or len(instruction.times),
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
_EVERY_N_DAYS_RE = re.compile(r"(?:every|q)\s*(\d+)\s*(?:days?|d)\b|每\s*(\d+)\s*天", re.I)
_EVERY_OTHER_DAY_RE = re.compile(r"\bevery other day\b|\bqod\b|隔天|隔日", re.I)
_EVERY_N_HOURS_RE = re.compile(r"(?:every|q)\s*(\d+)\s*(?:hours?|h)\b|每\s*(\d+)\s*(?:小时|h)", re.I)
_WEEKDAY_RE = re.compile(r"\b(mon|tue|wed|thu|fri|sat|sun)[a-z]*\b", re.I)
_ZH_WEEKDAYS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "日": 7, "天": 7}
_ZH_WEEKDAY_RE = re.compile(
    r"(?:周|星期|礼拜)([一二三四五六日天][一二三四五六日天、,，和及\s]*)(?!次)"
)  # 周一、三、五; 每周一次 is "once weekly"
_PRN_RE = re.compile(r"\b(?:prn|as needed|when needed|if needed)\b|必要时|按需|需要时", re.I)
#: Most specific first: "twice daily" contains "daily". ``od`` is deliberately
#: absent — in ophthalmic sigs it means the right eye, not once daily.
_PER_DAY = (
    (
        re.compile(r"\b(?:four times a day|four times daily|qid|q\.i\.d\.)\b|每天四次|每日四次|一天四次|每天4次", re.I),
        4,
    ),
    (
        re.compile(
            r"\b(?:three times a day|three times daily|tid|t\.i\.d\.)\b|每天三次|每日三次|一天三次|一日三次|每天3次",
            re.I,
        ),
        3,
    ),
    (
        re.compile(
            r"\b(?:twice daily|twice a day|two times a day|bid|b\.i\.d\.)\b|每天两次|每日两次|一天两次|一日两次|每天2次",
            re.I,
        ),
        2,
    ),
    (
        re.compile(
            r"\b(?:once daily|once a day|daily|qd|every day|each day)\b|每天一次|每日一次|一天一次|一日一次|每天|每日",
            re.I,
        ),
        1,
    ),
)
_WEEKLY_RE = re.compile(r"\b(?:once weekly|once a week|weekly|every week)\b|每周一次|每星期一次|每周", re.I)
_RANGE_RE = re.compile(_NUM + r"\s*[-–~]\s*" + _NUM)


def parse_dose_instruction(text: str | None) -> Schedule | None:
    """Structure from free text such as ``"500 mg twice daily with food"`` or
    ``"每天两次，每次一片"``.

    The grammar is small and closed (``docs/medications.md``): one dose
    (number + unit or form), one frequency (n per day / every N days / every N
    hours / weekdays / weekly / as needed), optional clock times. Returns a
    one-element schedule, or ``None`` when nothing is recognised — the caller
    keeps the original text either way. **No partial parses**: a dose range
    (``"1-2 tablets"``), two different doses, two frequencies, or two regimens
    in one sentence return ``None`` rather than half a regimen.
    """
    raw = unicodedata.normalize("NFKC", (text or "")).strip()
    if not raw:
        return None
    if _RANGE_RE.search(raw):
        return None
    doses = [dose_from_text(v, u) for v, u in _DOSE_RE.findall(raw)]
    doses += [dose_from_text(_ZH_NUM[v], u) for v, u in _DOSE_ZH_RE.findall(raw)]
    doses = [d for d in doses if d is not None]
    if len({(d.value, d.unit) for d in doses}) > 1:
        return None  # two different amounts: two regimens, or a range in words
    dose = doses[0] if doses else None
    times = tuple(_TIMES_RE.findall(raw))
    as_needed = bool(_PRN_RE.search(raw))
    period_days: int | None = None
    doses_per_day = 0
    weekdays: set[int] = set()
    frequencies = 0
    m = _EVERY_N_DAYS_RE.search(raw)
    if m:
        period_days = int(m.group(1) or m.group(2))
        frequencies += 1
    elif _EVERY_OTHER_DAY_RE.search(raw):
        period_days = 2
        frequencies += 1
    m = _EVERY_N_HOURS_RE.search(raw)
    if m:
        hours = int(m.group(1) or m.group(2))
        if hours and 24 % hours == 0:
            doses_per_day = 24 // hours
            frequencies += 1
    for day in _WEEKDAY_RE.findall(raw):
        weekdays.add(weekday_from(day))
    for run in _ZH_WEEKDAY_RE.findall(raw):
        weekdays.update(_ZH_WEEKDAYS[ch] for ch in run if ch in _ZH_WEEKDAYS)
    if weekdays:
        frequencies += 1
    elif _WEEKLY_RE.search(raw):
        period_days = 7
        frequencies += 1
    for pattern, n in _PER_DAY:
        if pattern.search(raw):
            frequencies += 1  # counted even when another frequency already won: two frequencies is no parse
            if not (period_days or weekdays or doses_per_day):
                doses_per_day = n
                if n == 1:
                    period_days = 1
            break
    if frequencies > 1:
        return None
    recognised = dose is not None or times or as_needed or period_days or doses_per_day or weekdays
    if not recognised:
        return None
    try:
        if as_needed:
            instr = DoseInstruction(dose=dose, as_needed=True, text=raw)
        elif weekdays:
            instr = DoseInstruction(dose=dose, weekdays=frozenset(weekdays), times=times, text=raw)
        elif times:
            instr = DoseInstruction(
                dose=dose, times=times, period_days=period_days if (period_days or 1) > 1 else None, text=raw
            )
        elif doses_per_day > 1:
            instr = DoseInstruction(dose=dose, doses_per_day=doses_per_day, text=raw)
        else:
            instr = DoseInstruction(dose=dose, period_days=period_days, text=raw)
    except ValueError:
        return None
    return (instr,)


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

    ``start`` is required — a plan with no start cannot be projected or
    placed on a timeline. ``schedule`` holds one instruction per regimen
    (morning 1 tablet, evening ½ tablet is two). ``confirmed`` is ``True`` for
    what the person entered; extraction and import pass ``False`` explicitly
    and UIs/tools label it — projections do not care. ``entered_in_error`` is
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
    """A period during which a plan was followed — the analysis-side
    "exposure" (OMOP ``drug_exposure``) that lines up with readings on a
    timeline. ``closed_by`` says why it ended, or ``None`` while open; an
    open course is materialised at "today in the subject's zone" by whoever
    exports it."""

    plan_id: str
    order_id: str
    start: date
    end: date | None
    closed_by: Literal["stopped", "superseded", "completed"] | None = None


def plan_status_transition(
    plan: MedicationPlan, event: Literal["stop", "resume", "void"], *, today: date
) -> tuple[MedicationPlan, Course | None]:
    """The legal moves, and the course a move closes:

    * ``active --stop--> stopped`` (only once the plan has started); closes
      ``Course(start, today, "stopped")``.
    * ``stopped --resume--> active``: a *new* course starts today — the plan's
      ``start`` moves to ``today`` and ``end`` clears — and the closed course
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
        closed = Course(plan.plan_id, plan.order_id, plan.start, plan.stopped_on or plan.end, "stopped")
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
    resource's own values, verbatim; ``review`` is *our* state — whether the
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
        return (Course(plan.plan_id, plan.order_id, plan.start, plan.stopped_on or plan.end, "stopped"),)
    if eff == EFFECTIVE_COMPLETED:
        return (Course(plan.plan_id, plan.order_id, plan.start, plan.end, "completed"),)
    return (Course(plan.plan_id, plan.order_id, plan.start, None, None),)


# --- schedule projection ----------------------------------------------------------

GAP_SHIFT_FORWARD = "shift_forward"  # a 02:30 that does not exist becomes 03:30
GAP_SKIP = "skip"  # ...or the slot has no instant that day


def _zone(tz: str) -> ZoneInfo:
    try:
        return ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"unknown time zone {tz!r}") from e


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


@dataclass(frozen=True)
class DoseSlot:
    """One planned intake. Identity is ``key`` — ``(plan_id, local_date,
    slot)`` — never the instant. ``utc_ms`` is derived at projection time
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
    def key(self) -> tuple[str, date, str]:
        return (self.plan_id, self.local_date, self.slot)


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
    instructions gets ``#i`` so the two doses keep separate identities."""
    counts: dict[str, int] = {}
    for instr in schedule:
        for s in instr.slots():
            counts[s] = counts.get(s, 0) + 1
    out: list[tuple[DoseInstruction, str]] = []
    for i, instr in enumerate(schedule):
        for s in instr.slots():
            out.append((instr, f"{s}#{i}" if counts[s] > 1 else s))
    return tuple(out)


def project_schedule(
    plan: MedicationPlan, *, start: date, end: date, tz: str, max_days: int = 62, gap: str = GAP_SHIFT_FORWARD
) -> tuple[DoseSlot, ...]:
    """Every dose the plan schedules on the local days ``start..end``
    (both inclusive), clipped to the plan's own dates and, for a stopped
    plan, to the day it was stopped — history stays answerable. Interval
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
    last = end if plan.end is None else min(end, plan.end)
    if plan.stopped_on is not None:
        last = min(last, plan.stopped_on)
    names = _slot_names(plan.schedule)
    out: list[DoseSlot] = []
    d = first
    while d <= last:
        for instr, slot in names:
            if instr.schedule_kind() in (KIND_PRN, KIND_UNSCHEDULED) or not _due_on(instr, plan.start, d):
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
    to_add = tuple(want[k] for k in sorted(want.keys() - have.keys(), key=lambda k: (k[1], k[2])))
    to_remove = tuple(have[k] for k in sorted(have.keys() - want.keys(), key=lambda k: (k[1], k[2])))
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
    slot_key: tuple[str, date, str] | None = None
    dose: Dose | None = None
    recorded_by: str = "user"  # user | caregiver | device | import
    reason: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if self.status not in STORED_EVENT_STATUSES:
            raise ValueError(f"a stored dose event is {sorted(STORED_EVENT_STATUSES)}, never a derived state")
        if self.slot_key is not None:
            if self.slot_key[0] != self.plan_id:
                raise ValueError("slot_key belongs to another plan")
            object.__setattr__(self, "slot_key", (self.slot_key[0], self.slot_key[1], normalize_slot(self.slot_key[2])))


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
    instant. Derived — call it, do not store it."""
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
    + missed)``, one decimal, over *elapsed* slots only — ``None`` when none
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
    zone — the zone the person was in when the dose was due."""
    lo, hi = window
    zone = _zone(tz)
    slots = [d for d in scheduled if d.plan_id == plan.plan_id and lo <= d.local_date <= hi]
    by_key = {d.key: d for d in slots}
    answered: dict[tuple[str, date, str], DoseEvent] = {}
    extra = extra_skipped = 0
    for e in events:
        if e.plan_id != plan.plan_id:
            continue
        if e.slot_key is not None and e.slot_key in by_key:
            prev = answered.get(e.slot_key)
            if prev is None or e.taken_at_ms > prev.taken_at_ms:
                answered[e.slot_key] = e
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
    stopped_known: tuple[str, ...] = ()  # the person says they stopped; the plan exists — the caller decides
    stopped_unknown: tuple[str, ...] = ()  # says stopped, no plan on file — never create one
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
    period = repeat.get("period")
    unit = repeat.get("periodUnit")
    period_days: int | None = None
    doses_per_day = 0
    if period and unit == "d":
        period_days = int(period)
    elif period and unit == "wk" and not weekdays and freq <= 1:
        period_days = 7 * int(period)
    elif period and unit == "h" and freq:
        hours = float(period) / freq
        doses_per_day = int(24 // hours) if hours and (24 % hours) == 0 else 0
    if freq > 1 and period_days == 1 and not times:
        doses_per_day, period_days = freq, None
    text = str(d.get("text", "") or "")
    structured = bool(times or weekdays or freq or period or as_needed or dose)
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
        if as_needed:
            return DoseInstruction(dose=dose, as_needed=True, max_dose_per_day=max_per_day, text=text)
        if weekdays:
            return DoseInstruction(dose=dose, weekdays=weekdays, times=times, text=text)
        if times:
            return DoseInstruction(
                dose=dose, times=times, period_days=period_days if (period_days or 1) > 1 else None, text=text
            )
        if doses_per_day > 1:
            return DoseInstruction(dose=dose, doses_per_day=doses_per_day, text=text)
        return DoseInstruction(dose=dose, period_days=period_days, text=text)
    except ValueError:
        return DoseInstruction(dose=dose, text=text)


def _schedule_from_fhir(dosages: object) -> Schedule:
    """Every ``dosage``/``dosageInstruction`` entry becomes one instruction —
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
      ``default_start``) — never open-ended;
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
        # keeps the idempotency that made the raw id tempting — re-importing
        # the same record yields the same id — and scopes it to the subject.
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
    (``active``, which includes not-yet-started and completed plans — those
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
# has a lifecycle, a dose log has a day, a course has a reason it closed —
# none of which is a resolution or an aggregate. Five parameters, every one
# applicable to every call; the readings tool is `mirobody.kernel.query`.

TOOL_NAME = "query_medications"
VIEW_PLAN = "plan"
VIEW_LOG = "log"
VIEW_HISTORY = "history"
VIEWS = (VIEW_PLAN, VIEW_LOG, VIEW_HISTORY)
#: Rows one answer may carry; the newest come first. A medication list is
#: short by nature, a dose log is bounded by its window.
MAX_ROWS = 200
#: The dose log's window when the caller names no dates.
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
                "First local date, inclusive (YYYY-MM-DD). plan/history: plans or courses in effect at any point in "
                "the window; log: doses recorded in it (default: the last 30 days)."
            ),
        },
        "end": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$", "description": "Last local date, inclusive."},
        "member": {
            "type": "string",
            "description": "Read another person's medications you are authorised to see (a care-circle member id). Omit for the caller.",
        },
    },
}


@dataclass(frozen=True)
class MedicationsRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_query`."""

    view: str = VIEW_PLAN
    keywords: tuple[str, ...] = ()
    start: str = ""
    end: str = ""
    member: str = ""


def validate_query(args: Mapping[str, object]) -> tuple:
    """Everything wrong with the raw arguments (``query.Rejection`` rows);
    empty means :func:`parse_query` will succeed."""
    from . import query

    out = query.reject_unknown(args, TOOL_SCHEMA)
    view = args.get("view")
    if view not in (None, "") and view not in VIEWS:
        out.append(query.Rejection("view", f"must be one of {', '.join(VIEWS)}"))
    out.extend(query.reject_dates(args))
    return tuple(out)


def parse_query(args: Mapping[str, object]) -> MedicationsRequest:
    """Raw arguments → a :class:`MedicationsRequest`; ``ValueError`` when
    :func:`validate_query` finds anything."""
    from . import query

    problems = validate_query(args)
    if problems:
        raise ValueError("; ".join(f"{r.parameter}: {r.reason}" for r in problems))
    return MedicationsRequest(
        view=str(args.get("view") or VIEW_PLAN),
        keywords=query.normalize_list_arg(args.get("keywords")),
        start=str(args.get("start") or ""),
        end=str(args.get("end") or ""),
        member=str(args.get("member") or ""),
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
    """A schedule the model can read, built from the STRUCTURE — never from
    the person's free text, which is health data and stays in the name."""
    parts: list[str] = []
    for instr in schedule:
        d = describe_schedule(instr)
        dose = f"{instr.dose.value:g} {instr.dose.unit}" if instr.dose else ""
        if d.kind == KIND_FIXED_TIMES:
            when = "/".join(d.times)
        elif d.kind == KIND_DAILY:
            when = f"{d.doses_per_day}x/day"
        elif d.kind == KIND_WEEKLY:
            when = "wd" + ",".join(str(w) for w in d.weekdays)
        elif d.kind == KIND_INTERVAL:
            when = f"every {d.period_days}d"
        elif d.kind == KIND_PRN:
            when = "as needed"
        else:
            when = "unscheduled"
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
) -> list[dict]:
    """The ``plan`` view: one row per plan in effect at some point in
    ``window`` (all plans when ``None``), with its effective status and, for
    an active plan, every slot of ``today`` and its derived state. Pure:
    ``today``/``now_ms`` are the caller's clock in the subject's zone."""
    out: list[dict] = []
    by_plan: dict[str, dict[tuple, DoseEvent]] = {}
    for e in todays_events:
        if e.slot_key:
            by_plan.setdefault(e.plan_id, {})[e.slot_key] = e
    for plan in plans:
        if plan.status == PLAN_ENTERED_IN_ERROR or not matches(plan.concept, keywords):
            continue
        if not _overlaps(plan.start, plan.stopped_on or plan.end, window):
            continue
        status = effective_status(plan, today)
        slots = project_schedule(plan, start=today, end=today, tz=tz) if status == EFFECTIVE_ACTIVE else ()
        answered = by_plan.get(plan.plan_id, {})
        out.append(
            {
                "medication": plan.concept.text,
                "status": status,
                "schedule": schedule_text(plan.schedule),
                "today": ", ".join(f"{s.slot}={slot_state(s, answered.get(s.key), now_ms=now_ms)}" for s in slots),
                "since": plan.start.isoformat(),
                "until": (plan.stopped_on or plan.end).isoformat() if (plan.stopped_on or plan.end) else "",
                "source": plan.source,
                "plan_id": plan.plan_id,
                "provenance": "measured" if plan.confirmed else "computed",
            }
        )
    out.sort(key=lambda r: (r["status"] != EFFECTIVE_ACTIVE, r["since"]), reverse=False)
    return out[:MAX_ROWS]


def log_rows(
    events: Sequence[DoseEvent], plans: Mapping[str, MedicationPlan], *, keywords: Sequence[str] = ()
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
                "slot": e.slot_key[2] if e.slot_key else "",
                "dose": f"{e.dose.value:g} {e.dose.unit}" if e.dose else "",
                "recorded_by": e.recorded_by,
                "plan_id": e.plan_id,
                "provenance": "measured",
                "_at": e.taken_at_ms,
            }
        )
    rows.sort(key=lambda r: r.pop("_at"), reverse=True)
    return rows[:MAX_ROWS]


def history_rows(
    plans: Sequence[MedicationPlan],
    courses_by_plan: Mapping[str, Sequence[Course]],
    *,
    keywords: Sequence[str] = (),
    window: tuple[date, date] | None = None,
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
    return out[:MAX_ROWS]


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

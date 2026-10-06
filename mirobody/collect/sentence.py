"""One sentence a person typed, as the entries it states.

"我头疼，血压150/95" is a complaint and two readings. A model SPLITS the
sentence and TYPES each part; it is never asked for a code. Coding happens
where every row is coded, at write time in `observations`, per kind: LOINC and
UCUM for a measurement, ICPC-3 S for a symptom, ICPC-3 D for a condition.
Asked for a code, a model guesses one; the vocabulary does not.

The answer is checked before anything is written (`plan`), and every part
that is not written comes back with its reason:

* a part must quote the sentence. A quote the sentence does not contain is a
  part the model made up;
* only what the person asserts about themselves is written: 没发烧 (negated),
  怕是感冒了 (hypothetical) and 我妈高血压 (someone else) are not;
* a medication goes to the medication record, not to an observation: the
  model reports the drug, dose and frequency as written (`mentions`), and
  `kernel.meds` parses the schedule and decides whether it is a new plan;
* anything else worth keeping (a meal, a mood) is a note, kept as written
  and never coded; a sentence the model finds nothing in is one note, so
  nothing typed into the box is lost;
* a time the model resolved ("昨晚") is used only when it parses and is not
  in the future; otherwise the entry takes the default time.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from mirobody.collect import observations as obs
from mirobody.kernel import meds

logger = logging.getLogger(__name__)

EXTRACTOR = "llm:journal-sentence@v3"

KIND_MEASUREMENT = "measurement"
KIND_SYMPTOM = "symptom"
KIND_CONDITION = "condition"
KIND_MEDICATION = "medication"
KIND_OTHER = "other"
PART_KINDS = (KIND_MEASUREMENT, KIND_SYMPTOM, KIND_CONDITION, KIND_MEDICATION, KIND_OTHER)

_WRITES = {
    KIND_MEASUREMENT: obs.KIND_MEASUREMENT,
    KIND_SYMPTOM: obs.KIND_SYMPTOM,
    KIND_CONDITION: obs.KIND_CONDITION,
    KIND_OTHER: obs.KIND_NOTE,
}

ASSERT_PRESENT = "present"
ASSERT_NEGATED = "negated"
ASSERT_HYPOTHETICAL = "hypothetical"
#: Medication only: the person says they no longer take it.
ASSERT_STOPPED = "stopped"
ASSERTIONS = (ASSERT_PRESENT, ASSERT_NEGATED, ASSERT_HYPOTHETICAL, ASSERT_STOPPED)

#: The sentence's assertion in `kernel.meds`' words.
_MENTION_ASSERTION = {
    ASSERT_PRESENT: meds.ASSERTION_TAKING,
    ASSERT_STOPPED: meds.ASSERTION_STOPPED,
    ASSERT_NEGATED: meds.ASSERTION_NEGATED,
    ASSERT_HYPOTHETICAL: meds.ASSERTION_HYPOTHETICAL,
}

SUBJECT_SELF = "self"
SUBJECT_OTHER = "other"

#: Why a part was not written. Tokens: the client translates them.
SKIP_NEGATED = "negated"
SKIP_HYPOTHETICAL = "hypothetical"
SKIP_SOMEONE_ELSE = "someone_else"
SKIP_NOT_A_RECORD = "not_a_record"
SKIP_NO_VALUE = "no_value"
SKIP_NOT_IN_SENTENCE = "not_in_sentence"
SKIP_TOO_LONG = "too_long"
#: An assertion outside `ASSERTIONS`: whether the thing is present, absent or
#: only wondered about is unknown, and guessing "present" would write "没发烧"
#: as a fever.
SKIP_UNCLEAR = "unclear"

#: Words that may negate what they stand next to. Read only when the model
#: gave no assertion at all: then a quote with one of these is not taken as
#: present. It also catches 不舒服 or 无力, which are symptoms; those are
#: skipped as unclear rather than written, and the person sees the skip.
_NEGATION_CUE = re.compile(
    r"[没沒不无無未非别別]|否认|否認|\b(?:no|not|never|none|without|denies|denied)\b|n't\b", re.IGNORECASE
)

#: What one sentence may be, and what one entry's name may be (the journal's
#: own limit for a single entry).
MAX_SENTENCE = 500
MAX_NAME = 200

#: A model's clock and the server's disagree by seconds; "just now" must not
#: read as the future.
_FUTURE_SLACK = timedelta(minutes=10)

_BP_NAME = re.compile(r"血压|blood\s*pressure|\bbp\b", re.IGNORECASE)
_BP_VALUE = re.compile(r"^\s*(\d{2,3}(?:\.\d+)?)\s*/\s*(\d{2,3}(?:\.\d+)?)\s*$")
_CJK = re.compile(r"[一-鿿]")

#: Closed at both levels (`additionalProperties: false`, every key required):
#: OpenAI's json_schema answers HTTP 400 to an open object, so the journal
#: read nothing on GPT-6 Luna, GPT-6 Sol, GPT-6.1 Sol or GPT-5.6 Terra
#: (through OpenRouter, 2026-10-06).
RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "quote": {
                        "type": "string",
                        "description": "The exact span of the sentence this entry comes from, copied character for character.",
                    },
                    "kind": {
                        "type": "string",
                        "enum": list(PART_KINDS),
                        "description": (
                            "measurement: a number with what it measures (血压150/95, 体温38.5, 空腹血糖6.1). "
                            "symptom: something felt (头疼, 腰痛, 发烧, 咳嗽, sore throat). "
                            "condition: a named diagnosis the person has or was given (高血压, 糖尿病, asthma). "
                            "medication: a drug or supplement taken, started or stopped (二甲双胍, 布洛芬, vitamin D). "
                            "other: anything else about their day worth keeping (a meal, a mood, how they slept, an activity)."
                        ),
                    },
                    "name": {
                        "type": "string",
                        "description": (
                            "What it is, in the person's own words and language, shortest form: 头疼, 腰痛, 高血压, "
                            "体温. Never translated, never a medical term they did not use, never a code."
                        ),
                    },
                    "value": {"type": "string", "description": "measurement only: the number as written (150, 38.5). Empty otherwise."},
                    "dose": {
                        "type": "string",
                        "description": "medication only: the amount per dose as written (500mg, 一片, 2 puffs). Empty otherwise or when not said.",
                    },
                    "frequency": {
                        "type": "string",
                        "description": "medication only: how often and when, as written (每天两次, 早晚各一次, 睡前, 需要时, twice daily). Empty otherwise or when not said.",
                    },
                    "unit": {
                        "type": "string",
                        "description": "measurement only: the unit as written, or the one the reading plainly has (mmHg for blood pressure, ℃ for a body temperature like 38.5). Empty when unknown.",
                    },
                    "assertion": {
                        "type": "string",
                        "enum": list(ASSERTIONS),
                        "description": "present: the person says it is so (for a medication: they take it). negated: they say it is not (没发烧, no cough, 不咳嗽了, 今天没吃药). hypothetical: a worry, a guess or a question (怕是感冒了, maybe the flu, 要不要吃点布洛芬). stopped: medication only, they no longer take it (停了二甲双胍, stopped the statin).",
                    },
                    "subject": {
                        "type": "string",
                        "enum": [SUBJECT_SELF, SUBJECT_OTHER],
                        "description": "self: about the person writing. other: about someone else (我妈头疼).",
                    },
                    "when": {
                        "type": "string",
                        "description": "Local 'YYYY-MM-DD HH:MM' when the sentence says when it happened (昨晚, this morning, 3 days ago), worked out from NOW. Empty when it does not say.",
                    },
                    "detail": {
                        "type": "string",
                        "description": "Severity, duration, location or other qualifiers, in the person's words (有点, 一整天, 左边). Empty when none.",
                    },
                },
                "required": ["quote", "kind", "name", "value", "unit", "dose", "frequency", "assertion", "subject", "when", "detail"],
            },
        },
    },
    "required": ["entries"],
}

_PROMPT = """You split what a person wrote about their own health into entries. You do not diagnose, \
interpret or code anything: a separate, deterministic step does that from the words you return.

Rules:
- One entry per thing the sentence states. Add nothing it does not state.
- Keep the person's words and language in `name`. Do not translate, do not substitute a medical term.
- `name` must make sense on its own, as it would in a symptom diary: keep the body part or thing the \
sentence ties it to (头发脱落, not 脱落; 牙齿松动, not 松动; 尿很臭, not 很臭; urine smells bad, not smells bad).
- An improvement, a normal result or a treatment's effect (好点了, 效果明显, 外观正常, feeling better) is \
not an entry.
- A blood pressure like 150/95 is TWO measurement entries: 收缩压 150 and 舒张压 95 (in English, \
systolic blood pressure and diastolic blood pressure), unit mmHg, both quoting the same span.
- "发烧38.5" is a symptom (发烧) and a measurement (体温 38.5 ℃), both quoting the same span.
- A medication is one entry: the drug in `name` as written, without the amount; the amount in \
`dose`; how often in `frequency` ("每天早晚吃二甲双胍500mg": name 二甲双胍, dose 500mg, frequency 每天早晚).
- Anything else about their day worth keeping (吃了一碗牛肉面, 心情不好, 睡得很差) is one "other" \
entry whose `name` is the span as written.
- A negation ("没发烧", "no fever", "不咳嗽了") is still an entry, with assertion "negated".
- A worry or guess ("怕是感冒了", "might be the flu") is an entry with assertion "hypothetical".
- About someone else ("我妈头疼") is an entry with subject "other".
- When RECORD OF is given, the writer is logging for that person, and subject "self" means THAT \
person however the writer refers to them (我爸, Dad, 他, her name, or no subject at all). Anyone else, \
the writer included, is "other".
- A time the sentence names is resolved against NOW into `when`; otherwise leave `when` empty.
- If nothing is stated worth an entry, return no entries."""


def _entry(**fields: str) -> dict[str, str]:
    """One answer entry with every required field, as the schema spells it."""
    blank = dict.fromkeys(RESPONSE_SCHEMA["properties"]["entries"]["items"]["required"], "")
    return {**blank, "assertion": ASSERT_PRESENT, "subject": SUBJECT_SELF, **fields}


#: Two worked answers, sent as earlier turns before the real sentence. Under
#: a json_schema (or json_object) grammar, MiniCPM5-2B closed `entries` at
#: once: `[]` for "Been leg cramps for 5 days." and 30 of the evaluation's
#: 31 entries (benchmarks/local_models, 2026-10-06). Unconstrained, the same
#: model found them, but opened its answer by echoing the schema's own
#: `{"type": "object", "properties": ...}`, so the grammar forced it off
#: its path. With these two turns first it answered all six repro
#: sentences in 1-4 s, in the writer's language. They show the rules a
#: small model needs to see rather than read: one entry per thing, the
#: person's words, a negation and someone else kept and marked, a time
#: resolved from NOW, and a meal as an `other` entry.
_EXAMPLES: tuple[tuple[str, dict[str, Any]], ...] = (
    (
        (
            "NOW: 2026-03-02 09:00 (CST)\n"
            "SENTENCE: Sore throat since yesterday, temp 38.2 this morning, my son has a cough too."
        ),
        {"entries": [
            _entry(quote="Sore throat since yesterday", kind=KIND_SYMPTOM, name="Sore throat", when="2026-03-01 09:00"),
            _entry(quote="temp 38.2 this morning", kind=KIND_MEASUREMENT, name="temp", value="38.2", unit="℃",
                   when="2026-03-02 08:00"),
            _entry(quote="my son has a cough too", kind=KIND_SYMPTOM, name="cough", subject=SUBJECT_OTHER),
        ]},
    ),
    (
        "NOW: 2026-03-02 21:00 (CST)\nSENTENCE: 没发烧，晚饭吃了一碗面，有点头晕",
        {"entries": [
            _entry(quote="没发烧", kind=KIND_SYMPTOM, name="发烧", assertion=ASSERT_NEGATED),
            _entry(quote="晚饭吃了一碗面", kind=KIND_OTHER, name="晚饭吃了一碗面"),
            _entry(quote="有点头晕", kind=KIND_SYMPTOM, name="头晕", detail="有点"),
        ]},
    ),
)


@dataclass(frozen=True)
class Part:
    """One entry as the model stated it, before any check."""

    quote: str
    kind: str
    name: str
    value: str = ""
    unit: str = ""
    assertion: str = ASSERT_PRESENT
    subject: str = SUBJECT_SELF
    when: str = ""
    detail: str = ""
    dose: str = ""
    frequency: str = ""


@dataclass(frozen=True)
class Skip:
    """A part that was not written, and why."""

    quote: str
    kind: str
    name: str
    reason: str


def available() -> bool:
    """Whether a text model is configured to read a sentence."""
    from mirobody.utils.config.llm import resolve_route

    return resolve_route("text") is not None


def messages_for(sentence: str, now: datetime, *, record_of: str | None = None) -> list[dict[str, str]]:
    """`record_of` names the person whose record this is when a caregiver
    writes for them. Without it, "我爸今天咳嗽" typed into Dad's record was
    about someone else, and every part of it was dropped."""
    head = f"NOW: {now:%Y-%m-%d %H:%M} ({now.tzname() or ''})\n"
    if record_of is not None:
        head += f"RECORD OF: {record_of or 'the person the writer is logging for'}\n"
    shown: list[dict[str, str]] = []
    for asked, answered in _EXAMPLES:
        shown += [
            {"role": "user", "content": asked},
            {"role": "assistant", "content": json.dumps(answered, ensure_ascii=False)},
        ]
    return [
        {"role": "system", "content": _PROMPT},
        *shown,
        {"role": "user", "content": f"{head}SENTENCE: {sentence}"},
    ]


async def read(sentence: str, *, now: datetime, record_of: str | None = None) -> tuple[list[Part], dict] | None:
    """The model's parts and its raw answer, or `None` when the call failed."""
    from mirobody.utils.llm import async_get_structured_output

    answer = await async_get_structured_output(
        messages=messages_for(sentence, now, record_of=record_of),
        response_format={"type": "json_schema", "json_schema": {"name": "journal_sentence", "schema": RESPONSE_SCHEMA}},
        temperature=0,
        max_tokens=2000,
    )
    if not isinstance(answer, Mapping):
        return None
    return parts_from(answer), dict(answer)


def parts_from(answer: Mapping[str, Any]) -> list[Part]:
    """The model's JSON as parts. A malformed entry is dropped here rather
    than guessed at: it has no quote to show the person."""
    out: list[Part] = []
    for item in answer.get("entries") or []:
        if not isinstance(item, Mapping):
            continue
        field = {k: str(item.get(k) or "").strip() for k in Part.__dataclass_fields__}
        if not (field["quote"] or field["name"]):
            continue
        # Not every provider enforces the schema, where `assertion` is required.
        # A different spelling is kept and refused below. A missing one is
        # read as present only when nothing in the quote could negate it:
        # "没发烧" with no assertion was written as a fever.
        said = field["assertion"].lower()
        if not said:
            said = "" if _NEGATION_CUE.search(field["quote"] or field["name"]) else ASSERT_PRESENT
        field["assertion"] = said
        out.append(Part(**field))
    return out


def plan(
    parts: Sequence[Part], *, sentence: str, now: datetime, default_at: datetime, tz: str = ""
) -> tuple[list[obs.Draft], list[Skip]]:
    """The observations to write and the parts to report as skipped. Pure.

    Medication parts are not here: `mentions` takes them. A sentence the model
    found nothing in becomes one note, so what was typed is kept.

    `tz` is the zone the writer said they are in, and `now` is read in the
    zone it resolves to. Every draft carries it, so "今天" typed in Shanghai
    is filed under the Shanghai day; filed in UTC it was the day before.
    """
    drafts: list[obs.Draft] = []
    skipped: list[Skip] = []
    haystack = _squash(sentence)
    if not parts and sentence.strip():
        return [_note(sentence.strip(), default_at, tz)], []
    for part in _split_blood_pressure(parts):
        if part.kind == KIND_MEDICATION:
            continue
        reason = _skip_reason(part, haystack)
        if reason:
            skipped.append(Skip(part.quote or part.name, part.kind, part.name, reason))
            continue
        at = _when(part.when, now=now, zone=now.tzinfo) or default_at
        if part.kind == KIND_OTHER:
            drafts.append(_note(part.quote or part.name, at, tz, detail=part.detail))
            continue
        drafts.append(obs.Draft(
            name_text=part.name,
            observed_start=at,
            value_text=part.value if part.kind == KIND_MEASUREMENT else "",
            unit_text=part.unit if part.kind == KIND_MEASUREMENT else "",
            note_text=part.detail,
            kind=_WRITES[part.kind],
            tz=tz,
        ))
    return drafts, skipped


def _note(text: str, at: datetime, tz: str, *, detail: str = "") -> obs.Draft:
    """A note as written. `name_text` holds it, as a complaint's words are held;
    past `MAX_NAME` the name is its opening and the whole text goes to the
    encrypted note, so a long entry is kept, not refused."""
    if len(text) <= MAX_NAME:
        return obs.Draft(name_text=text, observed_start=at, note_text=detail, kind=obs.KIND_NOTE, tz=tz)
    return obs.Draft(name_text=text[: MAX_NAME - 1] + "…", observed_start=at, note_text=text,
                     kind=obs.KIND_NOTE, tz=tz)


def mentions(parts: Sequence[Part], *, sentence: str, now: datetime) -> tuple[list[meds.MedicationMention], list[Skip]]:
    """The medication parts as `kernel.meds` mentions, and the ones refused.

    The same checks as every other part (a quote the sentence contains, about
    the person, asserted rather than negated or wondered about), so a skip
    carries its quote back to the person. What is left goes to
    `meds.reconcile_mentions`, which decides what becomes a plan."""
    out: list[meds.MedicationMention] = []
    skipped: list[Skip] = []
    haystack = _squash(sentence)
    for part in parts:
        if part.kind != KIND_MEDICATION:
            continue
        reason = _skip_reason(part, haystack)
        if reason:
            skipped.append(Skip(part.quote or part.name, part.kind, part.name, reason))
            continue
        started = _when(part.when, now=now, zone=now.tzinfo) if part.assertion == ASSERT_PRESENT else None
        out.append(meds.MedicationMention(
            name=part.name,
            dose_text=part.dose,
            frequency_text=part.frequency,
            instructions_text=part.detail,
            started_on=started.date() if started else None,
            assertion=_MENTION_ASSERTION[part.assertion],
            quote=part.quote or part.name,
        ))
    return out, skipped


def _skip_reason(part: Part, haystack: str) -> str:
    if part.kind not in _WRITES and part.kind != KIND_MEDICATION:
        return SKIP_NOT_A_RECORD
    if not (part.name or (part.kind == KIND_OTHER and part.quote)):
        return SKIP_NOT_A_RECORD
    if _squash(part.quote or part.name) not in haystack:
        return SKIP_NOT_IN_SENTENCE
    if part.subject == SUBJECT_OTHER:
        return SKIP_SOMEONE_ELSE
    if part.assertion not in ASSERTIONS:
        return SKIP_UNCLEAR
    if part.assertion == ASSERT_NEGATED:
        return SKIP_NEGATED
    if part.assertion == ASSERT_HYPOTHETICAL:
        return SKIP_HYPOTHETICAL
    if part.assertion == ASSERT_STOPPED and part.kind != KIND_MEDICATION:
        return SKIP_NOT_A_RECORD
    if part.kind == KIND_MEASUREMENT and not part.value:
        return SKIP_NO_VALUE
    if part.kind != KIND_OTHER and len(part.name) > MAX_NAME:
        return SKIP_TOO_LONG
    return ""


def _split_blood_pressure(parts: Sequence[Part]) -> list[Part]:
    """150/95 left as one value is two readings the model forgot to split.
    Split here, so a blood pressure never lands as one narrative value."""
    out: list[Part] = []
    for part in parts:
        m = _BP_VALUE.match(part.value) if part.kind == KIND_MEASUREMENT else None
        if not (m and _BP_NAME.search(part.name)):
            out.append(part)
            continue
        zh = bool(_CJK.search(part.name))
        names = ("收缩压", "舒张压") if zh else ("Systolic blood pressure", "Diastolic blood pressure")
        unit = part.unit or "mmHg"
        out.extend(Part(**{**part.__dict__, "name": n, "value": v, "unit": unit}) for n, v in zip(names, m.groups(), strict=True))
    return out


def _when(text: str, *, now: datetime, zone: Any) -> datetime | None:
    """A local 'YYYY-MM-DD HH:MM' (or a bare date) as an instant in the
    person's zone; `None` when it does not parse or is in the future."""
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            at = datetime.strptime(text, fmt).replace(tzinfo=zone)
            break
        except ValueError:
            continue
    else:
        return None
    return None if at > now + _FUTURE_SLACK else at


def _squash(text: str) -> str:
    """For the quote check only: width, case and spacing do not make a quote
    a different quote (１５０ is 150, "Sore Throat" is "sore throat")."""
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", text or "")).casefold()


__all__ = [
    "EXTRACTOR",
    "MAX_SENTENCE",
    "PART_KINDS",
    "Part",
    "RESPONSE_SCHEMA",
    "Skip",
    "available",
    "mentions",
    "messages_for",
    "parts_from",
    "plan",
    "read",
]

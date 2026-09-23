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
* a medication is not written here, because medications have their own
  record, and neither is anything else (a meal, a mood);
* a time the model resolved ("昨晚") is used only when it parses and is not
  in the future; otherwise the entry takes the default time.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from mirobody import translate
from mirobody.collect import observations as obs

logger = logging.getLogger(__name__)

EXTRACTOR = "llm:journal-sentence@v1"

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
}

ASSERT_PRESENT = "present"
ASSERT_NEGATED = "negated"
ASSERT_HYPOTHETICAL = "hypothetical"
ASSERTIONS = (ASSERT_PRESENT, ASSERT_NEGATED, ASSERT_HYPOTHETICAL)

SUBJECT_SELF = "self"
SUBJECT_OTHER = "other"

#: Why a part was not written. Tokens: the client translates them.
SKIP_NEGATED = "negated"
SKIP_HYPOTHETICAL = "hypothetical"
SKIP_SOMEONE_ELSE = "someone_else"
SKIP_MEDICATION = "medication"
SKIP_NOT_A_RECORD = "not_a_record"
SKIP_NO_VALUE = "no_value"
SKIP_NOT_IN_SENTENCE = "not_in_sentence"
SKIP_TOO_LONG = "too_long"

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

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
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
                            "medication: a drug taken or stopped. other: anything else (a meal, a mood, an activity)."
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
                    "unit": {
                        "type": "string",
                        "description": "measurement only: the unit as written, or the one the reading plainly has (mmHg for blood pressure, ℃ for a body temperature like 38.5). Empty when unknown.",
                    },
                    "assertion": {
                        "type": "string",
                        "enum": list(ASSERTIONS),
                        "description": "present: the person says it is so. negated: they say it is not (没发烧, no cough, 不咳嗽了). hypothetical: a worry, a guess or a question (怕是感冒了, maybe the flu).",
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
                "required": ["quote", "kind", "name", "value", "unit", "assertion", "subject", "when", "detail"],
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
- A blood pressure like 150/95 is TWO measurement entries: 收缩压 150 and 舒张压 95 (in English, \
systolic blood pressure and diastolic blood pressure), unit mmHg, both quoting the same span.
- "发烧38.5" is a symptom (发烧) and a measurement (体温 38.5 ℃), both quoting the same span.
- A negation ("没发烧", "no fever", "不咳嗽了") is still an entry, with assertion "negated".
- A worry or guess ("怕是感冒了", "might be the flu") is an entry with assertion "hypothetical".
- About someone else ("我妈头疼") is an entry with subject "other".
- A time the sentence names is resolved against NOW into `when`; otherwise leave `when` empty.
- If nothing health-related is stated, return no entries."""


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


def messages_for(sentence: str, now: datetime) -> list[dict[str, str]]:
    return [
        {"role": "system", "content": _PROMPT},
        {"role": "user", "content": f"NOW: {now:%Y-%m-%d %H:%M} ({now.tzname() or ''})\nSENTENCE: {sentence}"},
    ]


async def read(sentence: str, *, now: datetime) -> tuple[list[Part], dict] | None:
    """The model's parts and its raw answer, or `None` when the call failed."""
    from mirobody.utils.llm import async_get_structured_output

    answer = await async_get_structured_output(
        messages=messages_for(sentence, now),
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
        out.append(Part(**field))
    return out


def plan(
    parts: Sequence[Part], *, sentence: str, now: datetime, default_at: datetime
) -> tuple[list[obs.Draft], list[Skip]]:
    """The drafts to write and the parts to report as skipped. Pure."""
    drafts: list[obs.Draft] = []
    skipped: list[Skip] = []
    haystack = _squash(sentence)
    for part in _split_blood_pressure(parts):
        reason = _skip_reason(part, haystack)
        if reason:
            skipped.append(Skip(part.quote or part.name, part.kind, part.name, reason))
            continue
        at = _when(part.when, now=now, zone=now.tzinfo) or default_at
        drafts.append(obs.Draft(
            name_text=part.name,
            observed_start=at,
            value_text=part.value if part.kind == KIND_MEASUREMENT else "",
            unit_text=part.unit if part.kind == KIND_MEASUREMENT else "",
            note_text=part.detail,
            kind=_WRITES[part.kind],
        ))
    return drafts, skipped


def _skip_reason(part: Part, haystack: str) -> str:
    if part.kind == KIND_MEDICATION:
        return SKIP_MEDICATION
    if part.kind not in _WRITES or not part.name:
        return SKIP_NOT_A_RECORD
    if _squash(part.quote or part.name) not in haystack:
        return SKIP_NOT_IN_SENTENCE
    if part.subject == SUBJECT_OTHER:
        return SKIP_SOMEONE_ELSE
    if part.assertion == ASSERT_NEGATED:
        return SKIP_NEGATED
    if part.assertion == ASSERT_HYPOTHETICAL:
        return SKIP_HYPOTHETICAL
    if part.kind == KIND_MEASUREMENT and not part.value:
        return SKIP_NO_VALUE
    if len(part.name) > MAX_NAME:
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


def zone_now(*zones: str) -> datetime:
    """Now in the first zone that parses. The router passes the writer's own
    (the browser's) before the record's: "今早" is the morning where the person
    is typing, and a record with no zone set would otherwise read it in UTC."""
    for tz in zones:
        if not tz:
            continue
        try:
            return datetime.now(tz=translate.zone_for(tz))
        except ValueError:
            continue
    return datetime.now(tz=translate.zone_for("UTC"))


__all__ = [
    "EXTRACTOR",
    "MAX_SENTENCE",
    "PART_KINDS",
    "Part",
    "RESPONSE_SCHEMA",
    "Skip",
    "available",
    "messages_for",
    "parts_from",
    "plan",
    "read",
    "zone_now",
]

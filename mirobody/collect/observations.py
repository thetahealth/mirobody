"""The one writer of the observation model: every reading, whatever brought it in.

A lab report's rows, a wearable's daily figures, a number typed into the API
and the demo seed all arrive here as `Draft`s (what was printed, verbatim,
plus when) with a `Provenance` (where from, how measured), and leave as rows
of `th_observation` with their coding beside them. The schema and its
invariants are `mirobody/schema/30_observations.sql`; the pure parts
are `mirobody.translate`. What this module owns is the transaction.

What one `ingest()` call does, in ONE transaction:

1. freezes the structured input in `th_extraction` when the source was an
   extraction (an LLM's output, a vendor's batch), so a coding can be
   replayed against exactly what was read;
2. for each draft, inside its own savepoint: folds, parses, places the day,
   inserts the observation (a collision on the identity index is a retry,
   not a duplicate, and is counted as skipped), then codes it and writes
   `th_coding_current`, `th_coding_history`, `th_coding_decision` and, for any
   code that carries a display name, `th_concept`;
3. records the batch's verdict on the extraction row.

A draft that cannot be a row (no name, no time, an impossible time, a value
outside its unit's range) is counted by reason code and not written; the
reasons ride back in the `Report` and, for an extraction, on its row. Nothing
here is ever guessed in: no `now()` for a missing time, no unit for a bare
number, no code for an unmatched name.

`th_observation` is append-only. A correction is `amend()` (a new row that
points at the old one), a removal is `retract()` (the same, marked entered
in error), and the only DELETE is `erase()`, the privacy path. Readers see
the view `v_observation`, which already hides amended and retracted rows.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from functools import lru_cache
from datetime import date, datetime
from typing import Any

from mirobody import translate, units
from mirobody.kernel import metrics, quality, series
from mirobody.utils import db

logger = logging.getLogger(__name__)

KIND_MEASUREMENT = "measurement"
KIND_SYMPTOM = "symptom"
#: What a person has been told they have, as opposed to what they feel now.
#: The two code on different ICPC-3 components and must not share a kind.
KIND_CONDITION = "condition"
#: Anything else a person writes about their day (a meal, a mood), kept as
#: written and never coded (`translate.code_note`).
KIND_NOTE = "note"
KIND_FINDING = "finding"
KIND_ORGANIZER = "organizer"

MODALITY_LAB = "lab-report"
MODALITY_DEVICE = "device-sensed"
MODALITY_SELF = "self-reported"
MODALITY_MANUAL = "manual"
MODALITY_DERIVED = "derived"
MODALITY_UNVERIFIED = "llm-unverified"

SOURCE_FILE = "file"
SOURCE_DEVICE = "device"
SOURCE_MANUAL = "manual"
SOURCE_API = "api"

GRAIN_INSTANT = "instant"
GRAIN_WINDOW = "window"
GRAIN_DAY = "day"

#: The statistic a window or day row carries when its source did not name
#: one: the figure is the source's own, not a summary this code computed.
STAT_AS_REPORTED = "as-reported"

CAUSE_INGEST = "ingest"
#: A source re-sent a changed value for a row it wrote (`ON_CONFLICT_AMEND`),
#: or a row was retracted.
CAUSE_AMEND = "amend"
#: A person corrected a row (`amend()`). A source re-sending its value, the
#: old one or a new one, never amends a correction back.
CAUSE_CORRECT = "correct"
CAUSE_RECODE_RELEASE = "recode-release"
CAUSE_RECODE_RULES = "recode-rules"
CAUSE_RECODE_ALIAS = "recode-alias"

#: What a collision on the identity index means. `skip`: a retry, not a
#: duplicate (a report re-uploaded, a batch re-sent after a timeout).
#: `amend`: the source re-sent the truth (a device sync, a re-aggregation),
#: and a changed value amends the row it replaces, unless a person corrected it.
#: `reassert`: the person typed it again. A row they retracted no longer holds
#: the identity, so the new row amends the retraction. Under the other two a
#: retraction stands, and a re-sync does not bring back what was deleted.
ON_CONFLICT_SKIP = "skip"
ON_CONFLICT_AMEND = "amend"
ON_CONFLICT_REASSERT = "reassert"
#: `verify`: the batch copies rows another table still holds (the 1.5
#: migration), and that table may be dropped once every row is here. A row
#: with the same fingerprint anywhere in the identity's chain is the copy and
#: counts as skipped; any other row under the identity is counted as
#: `differs` and left alone. `skip` would call both "already present", and
#: a row written from a comment that did not decrypt (no unit, range or
#: method) then passed for the full one, and the original was dropped.
ON_CONFLICT_VERIFY = "verify"
#: `repair`: `verify`, except that a differing identity whose only row is the
#: one first written, never amended or retracted since, is amended with this
#: row. That repairs a copy written from a comment that did not decrypt,
#: without overriding anything a person or a later sync changed.
ON_CONFLICT_REPAIR = "repair"

#: What `ingest` did with each draft, in `Report.outcomes`.
OUTCOME_INSERTED = "inserted"
OUTCOME_SKIPPED = "skipped"
OUTCOME_DIFFERS = "differs"
OUTCOME_REJECTED = "rejected"

#: `task_id` values the aggregation passes stamp on the rows they publish.
AGGREGATE_TASK_IDS = frozenset({"aggregate_indicator", "derived_indicator", "derived_aggregator", "apple_health_statistics"})

#: Reason codes for drafts that are not written.
REJECT_NO_NAME = "no-name"
REJECT_NO_TIME = "no-time"
REJECT_WRITE_ERROR = "write-error"
#: A typed reading outside the ingestion range a device reading of the same
#: code is held to (`indicator_valid_rules`): 血压 400/300.
REJECT_OUT_OF_RANGE = "out-of-range"

#: The fields that carry meaning for `fingerprint`: the verbatim layer and
#: the time. Not the row id, not the coding, not `created_at`.
FINGERPRINT_FIELDS = (
    "name_text", "value_text", "unit_text", "ref_text", "flag_text", "method_text", "specimen_text",
    "observed_start", "observed_end", "tz", "source_ref", "source_record_id",
)

_ALIAS_SCOPE_GLOBAL = "global"
_ALIAS_SCOPE_CATALOG = "catalog"


@dataclass(frozen=True)
class Draft:
    """One reading as printed, and when it was observed.

    Text fields are verbatim: `name_text` is never translated, `value_text`
    keeps its comparator, `unit_text` and `ref_text` are what the report
    shows. `observed_start` is required; a naive datetime is wall clock in
    `tz` (a report's "2026-03-04 08:15"), an aware one is an instant. `tz`
    is what the SOURCE said about its zone, resolved against the person's
    own by `translate.resolve_tz`; empty means "the source did not say".
    """

    name_text: str
    observed_start: datetime | None
    observed_end: datetime | None = None
    value_text: str = ""
    unit_text: str = ""
    ref_text: str = ""
    flag_text: str = ""
    method_text: str = ""
    specimen_text: str = ""
    note_text: str = ""
    panel_text: str = ""
    kind: str = KIND_MEASUREMENT
    tz: str = ""
    grain: str = ""
    stat: str = ""
    window: str = ""
    source_record_id: str | None = None
    vendor_field: str | None = None
    row_ix: int | None = None
    member_of: int | None = None
    derived_from: tuple[int, ...] = ()


@dataclass(frozen=True)
class Provenance:
    """Where a batch came from and how its readings were obtained. One per
    `ingest()` call; every row of the batch shares it."""

    modality: str
    source_kind: str
    source_ref: str
    source_class: str = series.SOURCE_MEASURER
    vendor: str | None = None
    extractor: str = ""
    report_date: date | None = None
    date_source: str = ""


@dataclass
class Report:
    inserted: int = 0
    skipped: int = 0
    #: Under `verify` or `repair`: drafts whose identity holds a different row.
    differs: int = 0
    rejected: dict[str, int] = field(default_factory=dict)
    ids: list[int] = field(default_factory=list)
    extraction_id: int | None = None
    coded: int = 0
    #: One `OUTCOME_*` per draft, in the order given.
    outcomes: list[str] = field(default_factory=list)

    @property
    def written(self) -> int:
        return self.inserted

    def reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1
        self.outcomes.append(OUTCOME_REJECTED)


class Rejected(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# --- the pure part: a draft becomes the columns of a row ---------------------


def _clean(text: object) -> str:
    """Text as the table will hold it: no NUL (Postgres rejects it in `text`),
    no surrounding whitespace, never `None`."""
    if text is None:
        return ""
    return str(text).replace("\x00", "").strip()


def prepare(
    draft: Draft, provenance: Provenance, user_id: str, user_tz: str, *, now: datetime
) -> dict[str, Any]:
    """The columns of one observation, or `Rejected` with a reason code.

    Pure: `now` is a parameter so the future-time gate gives the same answer
    in a test as at midnight. Nothing is defaulted that the source did not
    say: a missing time is a rejection, not today.
    """
    name = _clean(draft.name_text)
    if not name:
        raise Rejected(REJECT_NO_NAME)
    if draft.observed_start is None:
        raise Rejected(REJECT_NO_TIME)

    tz, tz_source = translate.resolve_tz(draft.tz, user_tz)
    zone = translate.zone_for(tz)
    start = draft.observed_start if draft.observed_start.tzinfo else draft.observed_start.replace(tzinfo=zone)
    end = draft.observed_end if draft.observed_end is not None else start
    end = end if end.tzinfo else end.replace(tzinfo=zone)
    grain = draft.grain or (GRAIN_INSTANT if end == start else GRAIN_WINDOW)
    if grain == GRAIN_INSTANT:
        end = start

    value_text = _clean(draft.value_text)
    unit_text = _clean(draft.unit_text)
    parsed = translate.parse_value(value_text, unit_text)
    # A unit printed inside the value cell is still the printed unit.
    unit_text = unit_text or parsed.unit_tail
    fact = series.Fact(
        metric_key=name,
        value_num=parsed.value_num,
        effective_start_ms=int(start.timestamp() * 1000),
        effective_end_ms=int(end.timestamp() * 1000) if end != start else 0,
        unit=parsed.unit_ucum,
    )
    code = quality.time_gate(fact, int(now.timestamp() * 1000)) or quality.value_gate(parsed.value_num, parsed.unit_ucum)
    if code:
        raise Rejected(code)

    name_key = translate.name_key(name)
    unit_part = parsed.unit_ucum or translate.unit_key(unit_text)
    local_key = f"{name_key}|{unit_part}"
    stream_key = f"{local_key}|{provenance.source_kind}:{provenance.vendor or ''}"
    window = draft.window or translate.window_for(name)
    ref_low, ref_high = translate.parse_range(_clean(draft.ref_text))

    row: dict[str, Any] = {
        "user_id": str(user_id),
        "kind": draft.kind or KIND_MEASUREMENT,
        "modality": provenance.modality,
        "source_kind": provenance.source_kind,
        "source_ref": provenance.source_ref,
        "source_record_id": _clean(draft.source_record_id) or None,
        "row_ix": draft.row_ix,
        "vendor": provenance.vendor,
        "vendor_field": draft.vendor_field,
        "derived_from": list(draft.derived_from) if draft.derived_from else None,
        "member_of": draft.member_of,
        "panel_text": _clean(draft.panel_text),
        "observed_start": start,
        "observed_end": end,
        "tz": tz,
        "tz_source": tz_source,
        "local_date": translate.local_day(start, tz, window),
        "grain": grain,
        "stat": draft.stat or ("" if grain == GRAIN_INSTANT else STAT_AS_REPORTED),
        "name_text": name,
        "value_text": value_text,
        "unit_text": unit_text,
        "ref_text": _clean(draft.ref_text),
        "flag_text": _clean(draft.flag_text),
        "method_text": _clean(draft.method_text),
        "specimen_text": _clean(draft.specimen_text),
        "note_text": _clean(draft.note_text),
        "name_key": name_key,
        "value_kind": parsed.value_kind,
        "value_num": parsed.value_num,
        "comparator": parsed.comparator,
        "data_absent_reason": parsed.data_absent_reason,
        "unit_ucum": parsed.unit_ucum,
        "ref_low": ref_low,
        "ref_high": ref_high,
        "local_key": local_key,
        "stream_key": stream_key,
        "source_class": provenance.source_class,
        "amends": None,
        "status": "final",
    }
    row["fingerprint"] = series.stable_hash(*(row[f] for f in FINGERPRINT_FIELDS))
    return row


def catalog_alias(name: str) -> translate.Alias | None:
    """The device catalogue's answer for a metric name, as an alias: the
    metric's confident LOINC when it has one, its own namespace otherwise.
    An unverified code is evidence in `translate.devices`, not an identity.
    `None` for a name the catalogue does not know."""
    head = name.split(".", 1)[0]
    metric = metrics.METRICS.get(head) or metrics.METRICS.get(name)
    if metric is None:
        return None
    system, code = metric.canonical
    if system == metrics.SYSTEM_LOINC:
        return translate.Alias(_ALIAS_SCOPE_CATALOG, translate.LOINC_SYSTEM, code)
    return translate.Alias(_ALIAS_SCOPE_CATALOG, metrics.SYSTEM_DEVICE, code)


def coding_for(row: dict[str, Any], aliases: dict[tuple[str, str], translate.Alias]) -> translate.Coding:
    """The coding of one prepared row. A person's alias first, then the
    device catalogue for a device batch, then the vocabulary.

    The catalogue is keyed by vendor metric names, and several of them are
    ordinary words that mean something else on a lab report: `cholesterol` is
    dietary intake there and serum cholesterol here, `sodium` likewise, and
    `steps` is a per-sample delta (55423-8) where a person means the day
    (41950-7). Consulting it for a typed name filed all of those in the
    device namespace. It answers for a device batch, where the name did come
    from a vendor.
    """
    alias = aliases.get((row["name_key"], row["unit_ucum"])) or aliases.get((row["name_key"], ""))
    if row["kind"] == KIND_SYMPTOM:
        # A complaint is not an analyte, and the lexical resolver cannot
        # abstain from one: 发烧 reached 153 LOINC candidates and coded to
        # 103717-5, Crimean-Congo hemorrhagic fever virus RNA. The ICPC-3
        # components answer on their own vocabulary, one per kind.
        return translate.resolve_symptom(row["name_text"], alias=alias)
    if row["kind"] == KIND_CONDITION:
        return translate.resolve_condition(row["name_text"], alias=alias)
    if row["kind"] == KIND_NOTE:
        return translate.code_note()
    if alias is None and row["source_kind"] == SOURCE_DEVICE:
        alias = catalog_alias(row["name_text"]) or translate.Alias(
            _ALIAS_SCOPE_CATALOG, metrics.SYSTEM_DEVICE, row["name_text"].split(".", 1)[0]
        )
    return translate.code(
        row["name_text"],
        name_key=row["name_key"],
        local_key=row["local_key"],
        value_kind=row["value_kind"],
        value_text=row["value_text"],
        unit_text=row["unit_text"],
        unit_ucum=row["unit_ucum"],
        value_num=row["value_num"],
        alias=alias,
    )


# --- SQL ---------------------------------------------------------------------

_INSERT_EXTRACTION = """
INSERT INTO th_extraction (user_id, source_kind, source_ref, extractor, status, reason, payload, digest, report_date, date_source)
VALUES (:user_id, :source_kind, :source_ref, :extractor, 'ok', '', CAST(:payload AS jsonb), :digest, :report_date, :date_source)
ON CONFLICT (source_ref, digest) DO UPDATE SET created_at = th_extraction.created_at
RETURNING id
"""

_FINISH_EXTRACTION = "UPDATE th_extraction SET status = :status, reason = :reason WHERE id = :id"

_OBSERVATION_COLUMNS = (
    "user_id", "kind", "modality", "source_kind", "source_ref", "source_record_id", "extraction_id", "row_ix",
    "vendor", "vendor_field", "derived_from", "member_of", "panel_text",
    "observed_start", "observed_end", "tz", "tz_source", "local_date", "grain", "stat",
    "name_text", "value_text", "unit_text", "ref_text", "flag_text", "method_text", "specimen_text", "note_text",
    "name_key", "value_kind", "value_num", "comparator", "data_absent_reason", "unit_ucum", "ref_low", "ref_high",
    "local_key", "stream_key", "source_class", "fingerprint", "status", "amends",
)

# `encrypt_content('')` is NULL by design, and so is its answer when the
# key is unusable: the column is nullable so that a missing note is NULL and
# a note that could not be encrypted is caught by the round-trip check below.
_INSERT_OBSERVATION = (
    "INSERT INTO th_observation (" + ", ".join(_OBSERVATION_COLUMNS) + ") VALUES ("
    + ", ".join("encrypt_content(:note_text)" if c == "note_text" else f":{c}" for c in _OBSERVATION_COLUMNS)
    + """)
ON CONFLICT (user_id, name_key, observed_start, observed_end, source_ref,
             COALESCE(source_record_id, ''), COALESCE(member_of, 0), COALESCE(amends, 0)) DO NOTHING
RETURNING id, (note_text IS NOT NULL) AS has_note"""
)


class NoteNotEncrypted(RuntimeError):
    """`encrypt_content` answered NULL for a non-empty note: the connection's
    key is unusable. The row is refused rather than stored without its note."""

_INSERT_DECISION = """
INSERT INTO th_coding_decision (decision_id, name_key, unit_ucum, value_kind, release, rule, evidence)
VALUES (:decision_id, :name_key, :unit_ucum, :value_kind, :release, :rule, :evidence)
ON CONFLICT (decision_id) DO NOTHING
"""

_CODING_COLUMNS = "release, outcome, reason, code_system, code, series_id, group_id, value_canonical, unit_canonical, decision_id"
_CODING_VALUES = ":release, :outcome, :reason, :code_system, :code, :series_id, :group_id, :value_canonical, :unit_canonical, :decision_id"

_INSERT_CURRENT = f"INSERT INTO th_coding_current (observation_id, {_CODING_COLUMNS}) VALUES (:observation_id, {_CODING_VALUES})"
_INSERT_HISTORY = (
    f"INSERT INTO th_coding_history (observation_id, cause, {_CODING_COLUMNS}) VALUES (:observation_id, :cause, {_CODING_VALUES})"
)

_INSERT_CONCEPT = """
INSERT INTO th_concept (release, code_system, code, display, series_id,
                        loinc_component, loinc_property, loinc_time, loinc_system, loinc_scale, loinc_method)
VALUES (:release, :code_system, :code, :display, :series_id, :component, :property, :time, :system, :scale, :method)
ON CONFLICT (release, code_system, code) DO NOTHING
"""

_SELECT_ALIASES = """
SELECT scope, name_key, unit_ucum, code_system, code
  FROM th_coding_alias
 WHERE scope IN (:user_scope, :global_scope)
 ORDER BY (scope = :global_scope), id
"""

_SELECT_ROW = "SELECT * FROM th_observation WHERE id = :id AND user_id = :user_id"

_COPY_CURRENT = f"""
INSERT INTO th_coding_current (observation_id, {_CODING_COLUMNS})
SELECT :new_id, {_CODING_COLUMNS} FROM th_coding_current WHERE observation_id = :old_id
"""
_COPY_HISTORY = f"""
INSERT INTO th_coding_history (observation_id, cause, {_CODING_COLUMNS})
SELECT :new_id, :cause, {_CODING_COLUMNS} FROM th_coding_current WHERE observation_id = :old_id
"""

# The visible row that already holds an identity: the one a re-sent value
# amends, unless a person corrected it.
_SELECT_CURRENT = """
SELECT id, fingerprint,
       EXISTS (SELECT 1 FROM th_coding_history h
                WHERE h.observation_id = o.id AND h.cause = 'correct') AS corrected
  FROM th_observation o
 WHERE o.user_id = :user_id AND o.name_key = :name_key
   AND o.observed_start = :observed_start AND o.observed_end = :observed_end AND o.source_ref = :source_ref
   AND COALESCE(o.source_record_id, '') = COALESCE(:source_record_id, '')
   AND COALESCE(o.member_of, 0) = COALESCE(:member_of, 0)
   AND o.status = 'final'
   AND NOT EXISTS (SELECT 1 FROM th_observation n WHERE n.amends = o.id)
 ORDER BY o.id DESC LIMIT 1
"""

# Any row of the identity's chain, visible or not, holding this fingerprint.
_SELECT_EQUAL = """
SELECT 1 FROM th_observation o
 WHERE o.user_id = :user_id AND o.name_key = :name_key
   AND o.observed_start = :observed_start AND o.observed_end = :observed_end AND o.source_ref = :source_ref
   AND COALESCE(o.source_record_id, '') = COALESCE(:source_record_id, '')
   AND COALESCE(o.member_of, 0) = COALESCE(:member_of, 0)
   AND o.fingerprint = :fingerprint
 LIMIT 1
"""

# The identity's chain when it is one row, final and never amended: the row
# a repair may amend. Two or more rows mean someone changed it since.
_SELECT_UNTOUCHED = """
SELECT o.id, o.status, (SELECT count(*) FROM th_observation c
                         WHERE c.user_id = o.user_id AND c.name_key = o.name_key
                           AND c.observed_start = o.observed_start AND c.observed_end = o.observed_end
                           AND c.source_ref = o.source_ref
                           AND COALESCE(c.source_record_id, '') = COALESCE(o.source_record_id, '')
                           AND COALESCE(c.member_of, 0) = COALESCE(o.member_of, 0)) AS chain
  FROM th_observation o
 WHERE o.user_id = :user_id AND o.name_key = :name_key
   AND o.observed_start = :observed_start AND o.observed_end = :observed_end AND o.source_ref = :source_ref
   AND COALESCE(o.source_record_id, '') = COALESCE(:source_record_id, '')
   AND COALESCE(o.member_of, 0) = COALESCE(:member_of, 0)
   AND o.amends IS NULL
"""

# The retraction that ends an identity's chain: the row a re-typed entry amends.
_SELECT_RETRACTED = """
SELECT id FROM th_observation o
 WHERE o.user_id = :user_id AND o.name_key = :name_key
   AND o.observed_start = :observed_start AND o.observed_end = :observed_end AND o.source_ref = :source_ref
   AND COALESCE(o.source_record_id, '') = COALESCE(:source_record_id, '')
   AND COALESCE(o.member_of, 0) = COALESCE(:member_of, 0)
   AND o.status = 'entered-in-error'
   AND NOT EXISTS (SELECT 1 FROM th_observation n WHERE n.amends = o.id)
 ORDER BY o.id DESC LIMIT 1
"""

_SELECT_BY_SOURCE = """
SELECT id, observed_start, observed_end FROM v_observation
 WHERE user_id = :user_id AND source_ref = :source_ref
"""


# --- the writes --------------------------------------------------------------


def _digest(source_ref: str, extractor: str, payload: str) -> str:
    return hashlib.sha256("\x1f".join((source_ref, extractor, payload)).encode("utf-8")).hexdigest()


async def _load_aliases(tx: db.Transaction, user_id: str) -> dict[tuple[str, str], translate.Alias]:
    rows = await tx.execute(_SELECT_ALIASES, {"user_scope": f"user:{user_id}", "global_scope": _ALIAS_SCOPE_GLOBAL})
    out: dict[tuple[str, str], translate.Alias] = {}
    for r in rows or []:
        key = (str(r["name_key"]), str(r["unit_ucum"] or ""))
        # The user's row sorts first and wins the key.
        out.setdefault(key, translate.Alias(str(r["scope"]), r["code_system"], r["code"]))
    return out


def _coding_params(observation_id: int, coding: translate.Coding) -> dict[str, Any]:
    return {
        "observation_id": observation_id,
        "release": coding.release,
        "outcome": coding.outcome,
        "reason": coding.reason or None,
        "code_system": coding.code_system,
        "code": coding.code,
        "series_id": coding.series_id,
        "group_id": coding.group_id,
        "value_canonical": coding.value_canonical,
        "unit_canonical": coding.unit_canonical,
        "decision_id": coding.decision_id,
    }


async def _write_coding(tx: db.Transaction, observation_id: int, row: dict[str, Any], coding: translate.Coding, cause: str) -> None:
    await tx.execute(_INSERT_DECISION, {
        "decision_id": coding.decision_id,
        "name_key": row["name_key"],
        "unit_ucum": row["unit_ucum"],
        "value_kind": row["value_kind"],
        "release": coding.release,
        "rule": coding.rule,
        "evidence": list(coding.evidence),
    })
    params = _coding_params(observation_id, coding)
    await tx.execute(_INSERT_CURRENT, params)
    await tx.execute(_INSERT_HISTORY, {**params, "cause": cause})
    await _write_concept(tx, coding)


async def _write_concept(tx: db.Transaction, coding: translate.Coding) -> None:
    """Every code whose vocabulary gave it a NAME, not only the LOINC ones: an
    ICPC-3 coding has no axes, and the six `loinc_*` columns are nullable for
    that reason. Without a row here a symptom series has no standard name to
    show beside the words the person wrote. A device code, whose display is the
    code echoed back, gets none: the series keeps showing the vendor field.
    The writer and `recode` both call this, so a recoded row is named the same."""
    if not (coding.coded and coding.display and coding.display != coding.code):
        return
    axes = coding.axes
    await tx.execute(_INSERT_CONCEPT, {
        "release": coding.release,
        "code_system": coding.code_system,
        "code": coding.code,
        "display": coding.display,
        "series_id": coding.series_id,
        "component": axes.component if axes else None,
        "property": axes.property if axes else None,
        "time": axes.time if axes else None,
        "system": axes.system if axes else None,
        "scale": axes.scale if axes else None,
        "method": axes.method if axes else None,
    })


_range_rules: Any = None


async def _ranges() -> Any:
    """The ingestion ranges, loaded once per process. A load that fails
    passes every value, as it does for a device batch."""
    global _range_rules
    if _range_rules is None:
        rules = translate.ValueRangeValidator()
        await rules.load()
        _range_rules = rules
    return _range_rules


@lru_cache(maxsize=1)
def _metrics_by_code() -> dict[str, tuple[metrics.Metric, ...]]:
    out: dict[str, list[metrics.Metric]] = {}
    for m in metrics.ROWS:
        if m.loinc and m.confidence == metrics.CONFIDENT:
            out.setdefault(m.loinc, []).append(m)
    return {code: tuple(ms) for code, ms in out.items()}


def out_of_range(row: dict[str, Any], coding: translate.Coding, ranges: Any) -> bool:
    """Whether a coded reading breaks the range of a catalogue metric that
    carries its code, in that metric's unit. A typed reading was held to
    nothing: a sentence wrote a blood pressure of 400/300 that a device's
    reading of the same code would have been filtered for."""
    value = row.get("value_num")
    if value is None or not coding.coded or coding.code_system != translate.LOINC_SYSTEM:
        return False
    for metric in _metrics_by_code().get(coding.code or "", ()):
        unit = row.get("unit_ucum") or metric.unit_ucum
        converted = value if unit == metric.unit_ucum else units.convert_value(value, unit, metric.unit_ucum)
        if converted is not None and not ranges.validate(metric.name, converted).is_valid:
            return True
    return False


async def _insert(tx: db.Transaction, row: dict[str, Any], on_conflict: str) -> tuple[int | None, str]:
    """`(id, outcome)`: the new row's id and `OUTCOME_INSERTED`, or `None` and
    `OUTCOME_SKIPPED` when the identity is already held by an equal row (or,
    under `skip`, by any row), or `OUTCOME_DIFFERS` under `verify`/`repair`
    when it is held by a different one. Under `amend`, a changed value is
    inserted as an amendment, unless the row it would amend is a person's
    correction (`CAUSE_CORRECT`); under `reassert`, an identity whose row was
    retracted is written again; under `repair`, see `ON_CONFLICT_REPAIR`."""
    inserted = await tx.execute(_INSERT_OBSERVATION, row)
    if inserted:
        return _inserted_id(inserted[0], row), OUTCOME_INSERTED
    if on_conflict in (ON_CONFLICT_VERIFY, ON_CONFLICT_REPAIR):
        if await tx.execute(_SELECT_EQUAL, row):
            return None, OUTCOME_SKIPPED
        if on_conflict == ON_CONFLICT_VERIFY:
            return None, OUTCOME_DIFFERS
        current = await tx.execute(_SELECT_UNTOUCHED, row)
        if not current or int(current[0]["chain"]) != 1 or current[0]["status"] != "final":
            return None, OUTCOME_DIFFERS
    elif on_conflict == ON_CONFLICT_REASSERT:
        current = await tx.execute(_SELECT_RETRACTED, row)
        if not current:
            return None, OUTCOME_SKIPPED
    elif on_conflict == ON_CONFLICT_AMEND:
        current = await tx.execute(_SELECT_CURRENT, row)
        if not current or current[0]["corrected"] or str(current[0]["fingerprint"]) == row["fingerprint"]:
            return None, OUTCOME_SKIPPED
    else:
        return None, OUTCOME_SKIPPED
    amended = dict(row, amends=int(current[0]["id"]))
    inserted = await tx.execute(_INSERT_OBSERVATION, amended)
    if not inserted:
        return None, OUTCOME_SKIPPED
    return _inserted_id(inserted[0], row), OUTCOME_INSERTED


def _inserted_id(returned: dict[str, Any], row: dict[str, Any]) -> int:
    if row.get("note_text") and not returned.get("has_note"):
        raise NoteNotEncrypted("encrypt_content returned NULL for a non-empty note")
    return int(returned["id"])


async def ingest(
    user_id: str,
    drafts: list[Draft],
    provenance: Provenance | list[Provenance],
    *,
    user_tz: str,
    payload: Any = None,
    now: datetime | None = None,
    on_conflict: str = ON_CONFLICT_SKIP,
) -> Report:
    """Write a batch of drafts. See the module docstring for what one call
    does. `payload` is the extraction's verbatim output when the batch came
    from one (an LLM's JSON, a vendor's records); the drafts themselves are
    frozen when it is not given and `provenance.extractor` names one.
    `on_conflict` says what a row that already exists means: see
    `ON_CONFLICT_SKIP` and `ON_CONFLICT_AMEND`. A list of provenance records
    codes a mixed API batch in one transaction; extraction payloads require
    the single-provenance form."""
    report = Report()
    if not drafts:
        return report
    if isinstance(provenance, list):
        if len(provenance) != len(drafts) or any(p.extractor for p in provenance):
            raise ValueError("mixed provenance needs one non-extraction source per draft")
        sources = provenance
        extraction_source = None
    else:
        sources = [provenance] * len(drafts)
        extraction_source = provenance
    now = now or datetime.now(tz=translate.zone_for("UTC"))

    async with db.transaction() as tx:
        if extraction_source is not None and extraction_source.extractor:
            frozen = json.dumps(payload if payload is not None else [d.__dict__ for d in drafts], ensure_ascii=False, default=str)
            rows = await tx.execute(_INSERT_EXTRACTION, {
                "user_id": str(user_id),
                "source_kind": extraction_source.source_kind,
                "source_ref": extraction_source.source_ref,
                "extractor": extraction_source.extractor,
                "payload": frozen,
                "digest": _digest(extraction_source.source_ref, extraction_source.extractor, frozen),
                "report_date": extraction_source.report_date,
                "date_source": extraction_source.date_source,
            })
            report.extraction_id = int(rows[0]["id"]) if rows else None

        aliases = await _load_aliases(tx, str(user_id))
        ranges = await _ranges() if any(p.source_class == series.SOURCE_MANUAL for p in sources) else None
        for ix, (draft, row_source) in enumerate(zip(drafts, sources, strict=True)):
            try:
                row = prepare(draft, row_source, str(user_id), user_tz, now=now)
            except Rejected as e:
                report.reject(e.reason)
                continue
            row["extraction_id"] = report.extraction_id
            if row["row_ix"] is None:
                row["row_ix"] = ix
            try:
                async with tx.savepoint():
                    coding = coding_for(row, aliases)
                    if row_source.source_class == series.SOURCE_MANUAL and ranges is not None and out_of_range(row, coding, ranges):
                        report.reject(REJECT_OUT_OF_RANGE)
                        continue
                    observation_id, outcome = await _insert(tx, row, on_conflict)
                    if observation_id is None:
                        if outcome == OUTCOME_DIFFERS:
                            report.differs += 1
                        else:
                            report.skipped += 1
                        report.outcomes.append(outcome)
                        continue
                    await _write_coding(tx, observation_id, row, coding, CAUSE_INGEST if row.get("amends") is None else CAUSE_AMEND)
            except Exception as e:
                # Counts and a type: the row is health data and stays out of the log.
                logger.warning("observation not written: row_ix=%d error_type=%s", ix, type(e).__name__)
                report.reject(REJECT_WRITE_ERROR)
                continue
            report.inserted += 1
            report.ids.append(observation_id)
            report.outcomes.append(OUTCOME_INSERTED)
            if coding.coded:
                report.coded += 1

        if report.extraction_id is not None:
            status = "ok" if not report.rejected else ("failed" if not report.inserted and not report.skipped else "ok")
            reason = ", ".join(f"{k}={v}" for k, v in sorted(report.rejected.items()))
            await tx.execute(_FINISH_EXTRACTION, {"id": report.extraction_id, "status": status, "reason": reason})

    logger.info(
        "observations written: inserted=%d skipped=%d coded=%d rejected=%s",
        report.inserted, report.skipped, report.coded, report.rejected,
    )
    return report


async def retract(user_id: str, observation_ids: list[int], *, note: str = "") -> int:
    """Mark observations entered in error: a new row per id, pointing at the
    old one, which `v_observation` then hides. Returns how many were marked.
    A row already amended or retracted is left alone."""
    if not observation_ids:
        return 0
    async with db.transaction() as tx:
        return await _retract(tx, str(user_id), observation_ids, note)


async def _retract(tx: db.Transaction, user_id: str, observation_ids: list[int], note: str) -> int:
    count = 0
    for oid in observation_ids:
        rows = await tx.execute(_SELECT_ROW, {"id": int(oid), "user_id": user_id})
        if not rows:
            continue
        old = dict(rows[0])
        new = {c: old.get(c) for c in _OBSERVATION_COLUMNS}
        new.update({"status": "entered-in-error", "amends": int(oid), "note_text": note})
        inserted = await tx.execute(_INSERT_OBSERVATION, new)
        if not inserted:
            continue
        new_id = _inserted_id(inserted[0], new)
        await tx.execute(_COPY_CURRENT, {"new_id": new_id, "old_id": int(oid)})
        await tx.execute(_COPY_HISTORY, {"new_id": new_id, "old_id": int(oid), "cause": CAUSE_AMEND})
        count += 1
    return count


# What a device repair batch did not re-confirm: the visible readings of its
# vendors and names observed inside its window, under any batch but its own.
# `legacy_provenance` files a repair batch under `device:<vendor>:<task id>`.
_SELECT_UNCONFIRMED = """
SELECT id FROM v_observation
 WHERE user_id = :user_id AND source_kind = 'device'
   AND vendor = ANY(:vendors) AND name_text = ANY(:names)
   AND observed_start >= :start AND observed_start <= :end
   AND source_ref <> 'device:' || vendor || ':' || :task_id
"""


async def retract_unconfirmed(
    user_id: str, *, vendors: list[str], names: list[str], start: datetime, end: datetime, task_id: str
) -> int:
    """The sweep of a device repair batch (`task_id`, `repair-<uuid>`): retract
    every visible reading of `vendors` and `names` observed in `[start, end]`
    that another batch wrote. `start` and `end` are aware instants, compared
    with `observed_start` as such. Retracted, never deleted: each retraction
    is a row that says why (`repair:<task id>`). Returns how many."""
    if not vendors or not names:
        return 0
    async with db.transaction() as tx:
        rows = await tx.execute(_SELECT_UNCONFIRMED, {
            "user_id": str(user_id), "vendors": list(vendors), "names": list(names),
            "start": start, "end": end, "task_id": task_id,
        })
        return await _retract(tx, str(user_id), [int(r["id"]) for r in rows or []], f"repair:{task_id}")


async def amend(
    user_id: str,
    observation_id: int,
    *,
    value_text: str | None = None,
    unit_text: str | None = None,
    observed_start: datetime | None = None,
    observed_end: datetime | None = None,
    note: str = "",
    user_tz: str = "UTC",
    now: datetime | None = None,
) -> int | None:
    """Correct one observation: a new row with the change, pointing at the
    old one, re-coded from the corrected text. Returns the new id, or `None`
    when the id is not this person's or nothing changed."""
    async with db.transaction() as tx:
        rows = await tx.execute(_SELECT_ROW, {"id": int(observation_id), "user_id": str(user_id)})
        if not rows:
            return None
        old = dict(rows[0])
        provenance = Provenance(
            modality=old["modality"], source_kind=old["source_kind"], source_ref=old["source_ref"],
            source_class=old["source_class"], vendor=old.get("vendor"),
        )
        draft = Draft(
            name_text=old["name_text"],
            observed_start=observed_start if observed_start is not None else old["observed_start"],
            observed_end=observed_end if observed_end is not None else (old["observed_end"] if observed_start is None else None),
            value_text=old["value_text"] if value_text is None else value_text,
            unit_text=old["unit_text"] if unit_text is None else unit_text,
            ref_text=old["ref_text"], flag_text=old["flag_text"], method_text=old["method_text"],
            specimen_text=old["specimen_text"], note_text=note, panel_text=old["panel_text"],
            kind=old["kind"], tz=old["tz"], grain="" if observed_start is not None else old["grain"],
            stat=old["stat"], source_record_id=old.get("source_record_id"), vendor_field=old.get("vendor_field"),
            row_ix=old.get("row_ix"), member_of=old.get("member_of"),
            derived_from=tuple(old.get("derived_from") or ()),
        )
        row = prepare(draft, provenance, str(user_id), user_tz, now=now or datetime.now(tz=translate.zone_for("UTC")))
        row["extraction_id"] = old.get("extraction_id")
        row["amends"] = int(observation_id)
        inserted = await tx.execute(_INSERT_OBSERVATION, row)
        if not inserted:
            return None
        new_id = _inserted_id(inserted[0], row)
        aliases = await _load_aliases(tx, str(user_id))
        coding = coding_for(row, aliases)
        await _write_coding(tx, new_id, row, coding, CAUSE_CORRECT)
    return new_id


async def redate(user_id: str, source_ref: str, when: datetime, *, user_tz: str = "UTC") -> tuple[int, int]:
    """Move every visible observation of one source to `when` (a report whose
    date came from the upload time, answered by the person). Each move is an
    amendment. Returns `(moved, skipped)`, where a skipped row already sat on
    that time, or an equal row already holds the target identity."""
    rows = await _select_by_source(user_id, source_ref)
    moved = skipped = 0
    for r in rows:
        start = r["observed_start"]
        target = when if when.tzinfo else when.replace(tzinfo=start.tzinfo)
        if start == target and r["observed_end"] == target:
            skipped += 1
            continue
        new_id = await amend(user_id, int(r["id"]), observed_start=when, observed_end=when, user_tz=user_tz)
        if new_id is None:
            skipped += 1
        else:
            moved += 1
    return moved, skipped


async def _select_by_source(user_id: str, source_ref: str) -> list[dict[str, Any]]:
    async with db.transaction() as tx:
        rows = await tx.execute(_SELECT_BY_SOURCE, {"user_id": str(user_id), "source_ref": source_ref})
    return [dict(r) for r in rows or []]


def contains_pattern(text: str) -> str:
    """An ILIKE pattern that matches `text` literally anywhere in a value:
    `%` and `_` in a name are letters, not wildcards."""
    escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


# The rows an erase selects, and every row of their amendment chains in both
# directions. `v_observation` shows only a chain's last row, so deleting the
# id a reader was given left the original behind and it became visible again;
# deleting an older row alone broke the `amends` reference of the next one.
_ERASE_CHAINS = """
WITH RECURSIVE chain(id) AS (
    SELECT id FROM th_observation WHERE user_id = :user_id AND {where}
  UNION
    SELECT o.id
      FROM chain c
      JOIN th_observation x ON x.id = c.id
      JOIN th_observation o ON o.user_id = :user_id AND (o.id = x.amends OR o.amends = x.id)
)
DELETE FROM th_observation WHERE user_id = :user_id AND id IN (SELECT id FROM chain) RETURNING id
"""


async def erase(
    user_id: str,
    *,
    ids: list[int] | None = None,
    source_ref: str | None = None,
    name_contains: str | None = None,
    everything: bool = False,
) -> int:
    """The privacy path: physically delete a person's observations, by id,
    by source document, by a piece of the printed name (matched literally,
    case-insensitively), or all of them. Each selected row goes with its
    whole amendment chain, the corrected and retracted rows behind it
    included. Coding, day authority and check rows go with them (`ON DELETE
    CASCADE`).

    Erasing by name or everything also deletes the device points of those
    names in `series_data`, which the aggregation reads. The same readings
    still waiting in the retired 1.4 table are marked deleted too, or
    `mirobody migrate-observations` would write them back: by source
    document (a file's), by name and all of them; a row erased by id has a
    copy there only once it has been moved, and a moved row is never read
    again. Returns how many rows of `th_observation` were deleted, the hidden
    rows of each chain included."""
    params: dict[str, Any] = {"user_id": str(user_id)}
    if ids:
        where, params["ids"] = "id = ANY(:ids)", [int(i) for i in ids]
    elif source_ref:
        where, params["source_ref"] = "source_ref = :source_ref", source_ref
    elif name_contains:
        where, params["pattern"] = "name_text ILIKE :pattern", contains_pattern(name_contains)
    elif everything:
        where = "TRUE"
    else:
        return 0
    async with db.transaction() as tx:
        if everything:
            deleted = await tx.execute("DELETE FROM th_observation WHERE user_id = :user_id RETURNING id", params)
        else:
            deleted = await tx.execute(_ERASE_CHAINS.format(where=where), params)
        if source_ref:
            await tx.execute("DELETE FROM th_extraction WHERE user_id = :user_id AND source_ref = :source_ref", params)
        elif everything:
            await tx.execute("DELETE FROM th_extraction WHERE user_id = :user_id", params)
        if everything or name_contains:
            points = "TRUE" if everything else "indicator ILIKE :pattern"
            await tx.execute(f"DELETE FROM series_data WHERE user_id = :user_id AND {points}", params)
        retired_where = _retired_where(source_ref, name_contains, everything)
        if retired_where and (await tx.execute("SELECT to_regclass(:name) IS NOT NULL AS present", {"name": RETIRED_READINGS}))[0]["present"]:
            await tx.execute(
                f"UPDATE {RETIRED_READINGS} SET deleted = 1 WHERE user_id = :user_id AND deleted = 0 AND {retired_where}",
                {**params, "file_id": source_ref.removeprefix("th_files:") if source_ref else ""})
    return len(deleted or [])


#: Where 1.4 kept the readings, until `mirobody migrate-observations` has
#: moved every row (`collect/migrate_observations.py`).
RETIRED_READINGS = "th_series_data_retired_15"


def _retired_where(source_ref: str | None, name_contains: str | None, everything: bool) -> str:
    """The retired table's rows an `erase` covers, by the columns that held
    the same facts there; "" when none can be told apart (an erase by id)."""
    if source_ref:
        return "source_table = 'th_files' AND source_table_id = :file_id" if source_ref.startswith("th_files:") else ""
    if name_contains:
        return "indicator ILIKE :pattern"
    return "TRUE" if everything else ""


# The losing account's chains whose first row has an identity the winning
# account already holds (the same report uploaded to both), every later row of
# each chain included. Dropping the first row alone broke the `amends`
# reference of its correction and aborted the whole merge.
_MERGE_DROP_HELD = """
WITH RECURSIVE held(id) AS (
    SELECT l.id FROM th_observation l
     WHERE l.user_id = %(losing)s AND l.amends IS NULL
       AND EXISTS (
         SELECT 1 FROM th_observation w
          WHERE w.user_id = %(winning)s AND w.amends IS NULL
            AND w.name_key = l.name_key
            AND w.observed_start = l.observed_start AND w.observed_end = l.observed_end
            AND w.source_ref = l.source_ref
            AND COALESCE(w.source_record_id, '') = COALESCE(l.source_record_id, '')
            AND COALESCE(w.member_of, 0) = COALESCE(l.member_of, 0))
  UNION
    SELECT o.id FROM held h JOIN th_observation o ON o.amends = h.id
)
DELETE FROM th_observation WHERE id IN (SELECT id FROM held)
"""


async def merge_accounts(cur: Any, losing_user_id: str, winning_user_id: str) -> int:
    """Give the winning account every observation of the losing one, on the
    caller's psycopg cursor: an account merge is one transaction over many
    tables (`user/account_merge.py`), so this runs inside it.

    A chain of the same reading on both accounts keeps the winner's: the
    loser's chain goes whole, its corrections included. The rest moves over.
    The loser's day authority is deleted rather than moved, so it never names
    a row the winner's election did not choose. Returns the rows deleted and
    moved."""
    params = {"losing": str(losing_user_id), "winning": str(winning_user_id)}
    await cur.execute(_MERGE_DROP_HELD, params)
    total = cur.rowcount or 0
    await cur.execute("UPDATE th_observation SET user_id = %(winning)s WHERE user_id = %(losing)s", params)
    total += cur.rowcount or 0
    await cur.execute("DELETE FROM th_day_authority WHERE user_id = %(losing)s", params)
    return total + (cur.rowcount or 0)


# --- recoding: the same frozen rows under a newer vocabulary or rule -------

_SELECT_FOR_RECODE = """
SELECT o.id, o.kind, o.name_text, o.name_key, o.local_key, o.value_kind, o.value_text, o.unit_text, o.unit_ucum,
       o.value_num, o.source_kind, o.series_id, o.decision_id, o.release, o.outcome, o.code_system, o.code
  FROM v_observation o
 WHERE o.user_id = :user_id AND o.id > :after {name_filter}
 ORDER BY o.id
 LIMIT :batch
"""

_UPDATE_CURRENT = """
UPDATE th_coding_current
   SET release = :release, outcome = :outcome, reason = :reason, code_system = :code_system, code = :code,
       series_id = :series_id, group_id = :group_id, value_canonical = :value_canonical,
       unit_canonical = :unit_canonical, decision_id = :decision_id, coded_at = now()
 WHERE observation_id = :observation_id
"""

# A day authority names an observation inside a series; when the observation
# changes series the row is stale and election rebuilds it.
_DROP_AUTHORITY = "DELETE FROM th_day_authority WHERE observation_id = ANY(:ids)"

_UPSERT_ALIAS = """
INSERT INTO th_coding_alias (scope, name_key, unit_ucum, code_system, code, confirmed_by, note)
VALUES (:scope, :name_key, :unit_ucum, :code_system, :code, :confirmed_by, :note)
ON CONFLICT (scope, name_key, unit_ucum) DO UPDATE
   SET code_system = EXCLUDED.code_system, code = EXCLUDED.code, confirmed_by = EXCLUDED.confirmed_by,
       confirmed_at = now(), note = EXCLUDED.note
"""


@dataclass
class RecodeReport:
    scanned: int = 0
    changed: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)

    def count(self, outcome: str) -> None:
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1


def _same_coding(row: dict[str, Any], coding: translate.Coding) -> bool:
    return (
        str(row["outcome"]) == coding.outcome
        and (row["code"] or None) == coding.code
        and (row["code_system"] or None) == coding.code_system
        and str(row["series_id"]) == coding.series_id
        and str(row["release"]) == coding.release
        and str(row["decision_id"]) == coding.decision_id
    )


async def recode(
    user_id: str,
    *,
    cause: str = "",
    name_key: str | None = None,
    batch: int = 500,
) -> RecodeReport:
    """Replay `translate.code()` over one person's visible observations and
    replace the coding of every row whose answer changed.

    This is what `th_coding_history` is for: the observation never changes,
    the vocabulary release, the rules and the confirmed aliases do, and each
    replay appends a history row with its `cause`. `cause` defaults to
    `recode-release` when the row was coded under another release and
    `recode-rules` otherwise; `name_key` limits the pass to one printed name
    (after an alias was confirmed). Runs in batches, one transaction each,
    so a long history is recoded without holding a lock over all of it.
    """
    report = RecodeReport()
    current_release = translate.release()
    name_filter = " AND o.name_key = :name_key" if name_key else ""
    after = 0
    while True:
        async with db.transaction() as tx:
            params: dict[str, Any] = {"user_id": str(user_id), "after": after, "batch": batch}
            if name_key:
                params["name_key"] = name_key
            rows = await tx.execute(_SELECT_FOR_RECODE.format(name_filter=name_filter), params)
            if not rows:
                break
            aliases = await _load_aliases(tx, str(user_id))
            changed_ids: list[int] = []
            for r in rows:
                row = dict(r)
                # The driver hands `numeric` back as Decimal; the seam works in floats.
                row["value_num"] = None if row["value_num"] is None else float(row["value_num"])
                report.scanned += 1
                after = int(row["id"])
                coding = coding_for(row, aliases)
                if _same_coding(row, coding):
                    continue
                why = cause or (CAUSE_RECODE_RELEASE if str(row["release"]) != current_release else CAUSE_RECODE_RULES)
                await tx.execute(_INSERT_DECISION, {
                    "decision_id": coding.decision_id, "name_key": row["name_key"], "unit_ucum": row["unit_ucum"],
                    "value_kind": row["value_kind"], "release": coding.release, "rule": coding.rule,
                    "evidence": list(coding.evidence),
                })
                params_c = _coding_params(int(row["id"]), coding)
                await tx.execute(_UPDATE_CURRENT, params_c)
                await tx.execute(_INSERT_HISTORY, {**params_c, "cause": why})
                await _write_concept(tx, coding)
                report.changed += 1
                report.count(coding.outcome)
                if str(row["series_id"]) != coding.series_id:
                    changed_ids.append(int(row["id"]))
            if changed_ids:
                await tx.execute(_DROP_AUTHORITY, {"ids": changed_ids})
            if len(rows) < batch:
                break
    logger.info("recode: scanned=%d changed=%d outcomes=%s", report.scanned, report.changed, report.outcomes)
    return report


async def confirm_alias(
    user_id: str,
    name_key: str,
    unit_ucum: str,
    code: str | None,
    *,
    confirmed_by: str,
    code_system: str | None = translate.LOINC_SYSTEM,
    scope: str | None = None,
    note: str = "",
) -> RecodeReport:
    """Record what a person said a printed name means, then recode the rows
    that carry it. `code=None` confirms "not a standard item": the rows are
    refused rather than left waiting for input. `scope` defaults to this
    person; `global` applies to everyone and is a curator's call."""
    async with db.transaction() as tx:
        await tx.execute(_UPSERT_ALIAS, {
            "scope": scope or f"user:{user_id}", "name_key": name_key, "unit_ucum": unit_ucum or "",
            "code_system": code_system if code else None, "code": code,
            "confirmed_by": confirmed_by, "note": note or None,
        })
    return await recode(str(user_id), cause=CAUSE_RECODE_ALIAS, name_key=name_key)


# --- rows in the shape the collect layer still produces --------------------

_DAY_STATS = (("dailytotal", "sum"), ("dailysum", "sum"), ("dailyavg", "mean"), ("dailyaverage", "mean"),
              ("dailymean", "mean"), ("dailymin", "minimum"), ("dailymax", "maximum"), ("dailycount", "count"))


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T", 1))
        except ValueError:
            return None
    return None


def _stat_for(name: str) -> str:
    head = name.split(".", 1)[0].casefold()
    for prefix, stat in _DAY_STATS:
        if head.startswith(prefix):
            return stat
    return STAT_AS_REPORTED


def legacy_draft(row: dict[str, Any]) -> Draft:
    """A `th_series_data`-shaped dict (indicator, value, start_time, end_time,
    source, comment, task_id, source_table, source_table_id, unit or
    fhir_mapping_info) to a `Draft`. The collect layer still produces this
    shape; the migration reads it off the retired table."""
    unit = _clean(row.get("unit"))
    if not unit:
        info = row.get("fhir_mapping_info")
        try:
            info = json.loads(info) if isinstance(info, str) else (info or {})
        except ValueError:
            info = {}
        unit = _clean(info.get("unit")) if isinstance(info, dict) else ""
    start = _as_datetime(row.get("start_time"))
    end = _as_datetime(row.get("end_time")) or start
    grain = stat = ""
    if start is not None and end is not None and end != start:
        span_h = (end - start).total_seconds() / 3600
        grain = GRAIN_DAY if span_h >= 23 else GRAIN_WINDOW
        stat = _stat_for(str(row.get("indicator") or ""))
    source_table = str(row.get("source_table") or "")
    keep_note = source_table in ("th_files", "api", "demo_seed")
    return Draft(
        name_text=str(row.get("indicator") or ""),
        observed_start=start,
        observed_end=end,
        value_text=str(row.get("value") if row.get("value") is not None else ""),
        unit_text=unit,
        ref_text=_clean(row.get("reference_range")),
        flag_text=_clean(row.get("flag")),
        method_text=_clean(row.get("detection_method")),
        note_text=_clean(row.get("comment")) if keep_note else "",
        tz=_clean(row.get("timezone")),
        grain=grain,
        stat=stat,
        source_record_id=(_clean(row.get("source_table_id")) or None) if source_table not in ("th_files", "api", "demo_seed") else None,
    )


def legacy_provenance(row: dict[str, Any]) -> Provenance:
    """Where a `th_series_data`-shaped row came from, by the columns that
    used to say so (`source_table`, `task_id`, `source`)."""
    source_table = str(row.get("source_table") or "")
    source_table_id = str(row.get("source_table_id") or "")
    source = str(row.get("source") or "")
    task_id = str(row.get("task_id") or "")
    if source_table == "th_files":
        return Provenance(MODALITY_LAB, SOURCE_FILE, f"th_files:{source_table_id}", vendor=None)
    if source_table == "api":
        return Provenance(MODALITY_MANUAL, SOURCE_API, f"api:{source_table_id or source or 'records'}", source_class=series.SOURCE_MANUAL)
    if source_table == "demo_seed":
        # The seed's own rows are a wearable's: weekly weigh-ins, daily steps,
        # resting heart rate, blood pressure. Reading them back as manual kept
        # them out of the device catalogue, so 2,013 of 2,019 demo readings
        # carried no code and an uploaded report's blood pressure would not
        # join the seed's. Its file-backed rows say `th_files` above.
        return Provenance(MODALITY_DEVICE, SOURCE_DEVICE, f"demo:{source_table_id}", source_class=series.SOURCE_MEASURER, vendor=source or None)
    if task_id in AGGREGATE_TASK_IDS:
        return Provenance(MODALITY_DEVICE, SOURCE_DEVICE, f"aggregate:{source}", source_class=series.SOURCE_AGGREGATOR, vendor=source or None)
    ref = f"device:{source}"
    if task_id.startswith("repair-"):
        ref = f"{ref}:{task_id}"
    source_class = str(row.get("source_class") or series.SOURCE_MEASURER)
    return Provenance(MODALITY_DEVICE, SOURCE_DEVICE, ref, source_class=source_class, vendor=source or None)


async def user_tz(user_id: str) -> str:
    """The person's IANA zone, `UTC` when unset: through `user.get_user`,
    where `is_del = false` lives."""
    from mirobody.user.user import get_user

    try:
        row = await get_user(user_id=user_id)
    except (TypeError, ValueError):
        # An id that is not an account id (a demo or test person): no zone.
        row = None
    return ((row or {}).get("tz") or "").strip() or "UTC"


async def ingest_legacy(rows: list[dict[str, Any]], *, on_conflict: str = ON_CONFLICT_AMEND) -> Report:
    """Write rows in the collect layer's dict shape, grouped by person and
    provenance, and total the reports. `outcomes` is in the order of `rows`."""
    groups: dict[tuple[str, Provenance], list[tuple[int, Draft]]] = {}
    for ix, r in enumerate(rows):
        prov = legacy_provenance(r)
        groups.setdefault((str(r.get("user_id") or ""), prov), []).append((ix, legacy_draft(r)))
    total = Report()
    outcomes: list[str] = [OUTCOME_REJECTED] * len(rows)
    zones: dict[str, str] = {}
    for (uid, prov), indexed in groups.items():
        if uid not in zones:
            zones[uid] = await user_tz(uid)
        report = await ingest(uid, [d for _, d in indexed], prov, user_tz=zones[uid], on_conflict=on_conflict)
        total.inserted += report.inserted
        total.skipped += report.skipped
        total.differs += report.differs
        total.coded += report.coded
        for reason, n in report.rejected.items():
            total.rejected[reason] = total.rejected.get(reason, 0) + n
        total.ids.extend(report.ids)
        for (ix, _), outcome in zip(indexed, report.outcomes, strict=True):
            outcomes[ix] = outcome
    total.outcomes = outcomes
    return total


async def legacy_present(rows: list[dict[str, Any]]) -> list[bool]:
    """Whether a row with each legacy row's fingerprint is already stored,
    writing nothing (`mirobody migrate-observations --verify-only`). A row
    the new model would reject is not present."""
    present = [False] * len(rows)
    zones: dict[str, str] = {}
    now = datetime.now(tz=translate.zone_for("UTC"))
    async with db.transaction() as tx:
        for ix, r in enumerate(rows):
            uid = str(r.get("user_id") or "")
            if uid not in zones:
                zones[uid] = await user_tz(uid)
            try:
                row = prepare(legacy_draft(r), legacy_provenance(r), uid, zones[uid], now=now)
            except Rejected:
                continue
            present[ix] = bool(await tx.execute(_SELECT_EQUAL, row))
    return present


async def ingest_legacy_rows(rows: list[dict[str, Any]], *, on_conflict: str = ON_CONFLICT_AMEND) -> int:
    """`ingest_legacy`, returning only how many observations were written."""
    return (await ingest_legacy(rows, on_conflict=on_conflict)).inserted


__all__ = [
    "AGGREGATE_TASK_IDS",
    "CAUSE_AMEND",
    "CAUSE_CORRECT",
    "CAUSE_INGEST",
    "CAUSE_RECODE_ALIAS",
    "CAUSE_RECODE_RELEASE",
    "CAUSE_RECODE_RULES",
    "Draft",
    "FINGERPRINT_FIELDS",
    "GRAIN_DAY",
    "GRAIN_INSTANT",
    "GRAIN_WINDOW",
    "KIND_CONDITION",
    "KIND_NOTE",
    "KIND_FINDING",
    "KIND_MEASUREMENT",
    "KIND_ORGANIZER",
    "KIND_SYMPTOM",
    "MODALITY_DERIVED",
    "MODALITY_DEVICE",
    "MODALITY_LAB",
    "MODALITY_MANUAL",
    "MODALITY_SELF",
    "MODALITY_UNVERIFIED",
    "NoteNotEncrypted",
    "ON_CONFLICT_AMEND",
    "ON_CONFLICT_REASSERT",
    "ON_CONFLICT_REPAIR",
    "ON_CONFLICT_SKIP",
    "ON_CONFLICT_VERIFY",
    "OUTCOME_DIFFERS",
    "OUTCOME_INSERTED",
    "OUTCOME_REJECTED",
    "OUTCOME_SKIPPED",
    "Provenance",
    "RecodeReport",
    "REJECT_NO_NAME",
    "REJECT_NO_TIME",
    "REJECT_WRITE_ERROR",
    "Rejected",
    "RETIRED_READINGS",
    "Report",
    "SOURCE_API",
    "SOURCE_DEVICE",
    "SOURCE_FILE",
    "SOURCE_MANUAL",
    "STAT_AS_REPORTED",
    "amend",
    "catalog_alias",
    "coding_for",
    "confirm_alias",
    "contains_pattern",
    "erase",
    "ingest",
    "ingest_legacy",
    "ingest_legacy_rows",
    "legacy_draft",
    "legacy_present",
    "legacy_provenance",
    "merge_accounts",
    "prepare",
    "recode",
    "redate",
    "retract",
    "retract_unconfirmed",
    "user_tz",
]

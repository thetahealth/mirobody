"""The one writer of the observation model: every reading, whatever brought it in.

A lab report's rows, a wearable's daily figures, a number typed into the API
and the demo seed all arrive here as `Draft`s (what was printed, verbatim,
plus when) with a `Provenance` (where from, how measured), and leave as rows
of `th_observation` with their coding beside them. The schema and its
invariants are `mirobody/schema/a6_observation_model.sql`; the pure parts
are `mirobody.translate`. What this module owns is the transaction.

What one `ingest()` call does, in ONE transaction:

1. freezes the structured input in `th_extraction` when the source was an
   extraction (an LLM's output, a vendor's batch), so a coding can be
   replayed against exactly what was read;
2. for each draft, inside its own savepoint: folds, parses, places the day,
   inserts the observation (a collision on the identity index is a retry,
   not a duplicate, and is counted as skipped), then codes it and writes
   `th_coding_current`, `th_coding_history`, `th_coding_decision` and, for a
   LOINC code, `th_concept`;
3. refreshes `th_series`, the catalogue an assistant reads first, for the
   series the batch touched;
4. records the batch's verdict on the extraction row.

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
from datetime import date, datetime
from typing import Any

from mirobody import translate
from mirobody.kernel import metrics, quality, series
from mirobody.utils import db

logger = logging.getLogger(__name__)

KIND_MEASUREMENT = "measurement"
KIND_SYMPTOM = "symptom"
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
CAUSE_AMEND = "amend"

#: Reason codes for drafts that are not written.
REJECT_NO_NAME = "no-name"
REJECT_NO_TIME = "no-time"
REJECT_WRITE_ERROR = "write-error"

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
    rejected: dict[str, int] = field(default_factory=dict)
    ids: list[int] = field(default_factory=list)
    series: set[str] = field(default_factory=set)
    extraction_id: int | None = None
    coded: int = 0

    @property
    def written(self) -> int:
        return self.inserted

    def reject(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1


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
    metric's LOINC when it has one, its own namespace otherwise. `None` for
    a name the catalogue does not know."""
    head = name.split(".", 1)[0]
    metric = metrics.METRICS.get(head) or metrics.METRICS.get(name)
    if metric is None:
        return None
    if metric.loinc:
        return translate.Alias(_ALIAS_SCOPE_CATALOG, translate.LOINC_SYSTEM, metric.loinc)
    return translate.Alias(_ALIAS_SCOPE_CATALOG, metrics.SYSTEM_DEVICE, metric.name)


def coding_for(row: dict[str, Any], aliases: dict[tuple[str, str], translate.Alias]) -> translate.Coding:
    """The coding of one prepared row. A person's alias first, then the
    device catalogue for a device batch, then the vocabulary."""
    alias = aliases.get((row["name_key"], row["unit_ucum"])) or aliases.get((row["name_key"], ""))
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

# `encrypt_content('')` is NULL by design; the column is NOT NULL.
_INSERT_OBSERVATION = (
    "INSERT INTO th_observation (" + ", ".join(_OBSERVATION_COLUMNS) + ") VALUES ("
    + ", ".join(
        "COALESCE(encrypt_content(:note_text), '')" if c == "note_text" else f":{c}" for c in _OBSERVATION_COLUMNS
    )
    + """)
ON CONFLICT (user_id, name_key, observed_start, observed_end, source_ref,
             COALESCE(source_record_id, ''), COALESCE(member_of, 0), COALESCE(amends, 0)) DO NOTHING
RETURNING id"""
)

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

_REFRESH_SERIES = """
INSERT INTO th_series (user_id, series_id, standard, code_system, code, display, display_zh, unit_canonical, kind,
                       modalities, n, first_at, last_at, value_min, value_max, last_value, last_value_text,
                       reason, release, refreshed_at)
SELECT o.user_id,
       o.series_id,
       o.series_id NOT LIKE 'local:%',
       MAX(o.code_system),
       MAX(o.code),
       COALESCE(MAX(o.display), (ARRAY_AGG(o.name_text ORDER BY o.observed_start DESC))[1]),
       MAX(o.display_zh),
       (ARRAY_AGG(COALESCE(o.unit_canonical, o.unit_ucum) ORDER BY o.observed_start DESC))[1],
       (ARRAY_AGG(o.kind ORDER BY o.observed_start DESC))[1],
       ARRAY(SELECT DISTINCT m FROM unnest(ARRAY_AGG(o.modality)) AS m ORDER BY m),
       COUNT(*),
       MIN(o.observed_start),
       MAX(o.observed_end),
       MIN(COALESCE(o.value_canonical, o.value_num)),
       MAX(COALESCE(o.value_canonical, o.value_num)),
       (ARRAY_AGG(COALESCE(o.value_canonical, o.value_num) ORDER BY o.observed_start DESC))[1],
       (ARRAY_AGG(o.value_text ORDER BY o.observed_start DESC))[1],
       MAX(o.reason),
       MAX(o.release),
       now()
  FROM v_observation o
 WHERE o.user_id = :user_id AND o.series_id = ANY(:series_ids)
 GROUP BY o.user_id, o.series_id
ON CONFLICT (user_id, series_id) DO UPDATE SET
    standard = EXCLUDED.standard, code_system = EXCLUDED.code_system, code = EXCLUDED.code,
    display = EXCLUDED.display, display_zh = EXCLUDED.display_zh, unit_canonical = EXCLUDED.unit_canonical,
    kind = EXCLUDED.kind, modalities = EXCLUDED.modalities, n = EXCLUDED.n,
    first_at = EXCLUDED.first_at, last_at = EXCLUDED.last_at,
    value_min = EXCLUDED.value_min, value_max = EXCLUDED.value_max,
    last_value = EXCLUDED.last_value, last_value_text = EXCLUDED.last_value_text,
    reason = EXCLUDED.reason, release = EXCLUDED.release, refreshed_at = now()
"""

_PRUNE_SERIES = """
DELETE FROM th_series s
 WHERE s.user_id = :user_id AND s.series_id = ANY(:series_ids)
   AND NOT EXISTS (SELECT 1 FROM v_observation o WHERE o.user_id = s.user_id AND o.series_id = s.series_id)
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

_SERIES_OF = "SELECT series_id FROM th_coding_current WHERE observation_id = ANY(:ids)"


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
    if coding.coded and coding.code_system == translate.LOINC_SYSTEM and coding.axes is not None:
        await tx.execute(_INSERT_CONCEPT, {
            "release": coding.release,
            "code_system": coding.code_system,
            "code": coding.code,
            "display": coding.display or coding.code,
            "series_id": coding.series_id,
            "component": coding.axes.component,
            "property": coding.axes.property,
            "time": coding.axes.time,
            "system": coding.axes.system,
            "scale": coding.axes.scale,
            "method": coding.axes.method,
        })


async def refresh_series(tx: db.Transaction, user_id: str, series_ids: set[str]) -> None:
    """Recompute `th_series` for the series a write touched, and drop the
    entries whose last observation is gone."""
    if not series_ids:
        return
    ids = sorted(series_ids)
    await tx.execute(_REFRESH_SERIES, {"user_id": str(user_id), "series_ids": ids})
    await tx.execute(_PRUNE_SERIES, {"user_id": str(user_id), "series_ids": ids})


async def ingest(
    user_id: str,
    drafts: list[Draft],
    provenance: Provenance,
    *,
    user_tz: str,
    payload: Any = None,
    now: datetime | None = None,
) -> Report:
    """Write a batch of drafts. See the module docstring for what one call
    does. `payload` is the extraction's verbatim output when the batch came
    from one (an LLM's JSON, a vendor's records); the drafts themselves are
    frozen when it is not given and `provenance.extractor` names one."""
    report = Report()
    if not drafts:
        return report
    now = now or datetime.now(tz=translate.zone_for("UTC"))

    async with db.transaction() as tx:
        if provenance.extractor:
            frozen = json.dumps(payload if payload is not None else [d.__dict__ for d in drafts], ensure_ascii=False, default=str)
            rows = await tx.execute(_INSERT_EXTRACTION, {
                "user_id": str(user_id),
                "source_kind": provenance.source_kind,
                "source_ref": provenance.source_ref,
                "extractor": provenance.extractor,
                "payload": frozen,
                "digest": _digest(provenance.source_ref, provenance.extractor, frozen),
                "report_date": provenance.report_date,
                "date_source": provenance.date_source,
            })
            report.extraction_id = int(rows[0]["id"]) if rows else None

        aliases = await _load_aliases(tx, str(user_id))
        for ix, draft in enumerate(drafts):
            try:
                row = prepare(draft, provenance, str(user_id), user_tz, now=now)
            except Rejected as e:
                report.reject(e.reason)
                continue
            row["extraction_id"] = report.extraction_id
            if row["row_ix"] is None:
                row["row_ix"] = ix
            try:
                async with tx.savepoint():
                    inserted = await tx.execute(_INSERT_OBSERVATION, row)
                    if not inserted:
                        report.skipped += 1
                        continue
                    observation_id = int(inserted[0]["id"])
                    coding = coding_for(row, aliases)
                    await _write_coding(tx, observation_id, row, coding, CAUSE_INGEST)
            except Exception as e:
                # Counts and a type: the row is health data and stays out of the log.
                logger.warning("observation not written: row_ix=%d error_type=%s", ix, type(e).__name__)
                report.reject(REJECT_WRITE_ERROR)
                continue
            report.inserted += 1
            report.ids.append(observation_id)
            report.series.add(coding.series_id)
            if coding.coded:
                report.coded += 1

        await refresh_series(tx, str(user_id), report.series)
        if report.extraction_id is not None:
            status = "ok" if not report.rejected else ("failed" if not report.inserted and not report.skipped else "ok")
            reason = ", ".join(f"{k}={v}" for k, v in sorted(report.rejected.items()))
            await tx.execute(_FINISH_EXTRACTION, {"id": report.extraction_id, "status": status, "reason": reason})

    logger.info(
        "observations written: inserted=%d skipped=%d coded=%d rejected=%s",
        report.inserted, report.skipped, report.coded, report.rejected or {},
    )
    return report


async def retract(user_id: str, observation_ids: list[int], *, note: str = "") -> int:
    """Mark observations entered in error: a new row per id, pointing at the
    old one, which `v_observation` then hides. Returns how many were marked.
    A row already amended or retracted is left alone."""
    if not observation_ids:
        return 0
    touched: set[str] = set()
    count = 0
    async with db.transaction() as tx:
        for oid in observation_ids:
            rows = await tx.execute(_SELECT_ROW, {"id": int(oid), "user_id": str(user_id)})
            if not rows:
                continue
            old = dict(rows[0])
            new = {c: old.get(c) for c in _OBSERVATION_COLUMNS}
            new.update({"status": "entered-in-error", "amends": int(oid), "note_text": note})
            inserted = await tx.execute(_INSERT_OBSERVATION, new)
            if not inserted:
                continue
            new_id = int(inserted[0]["id"])
            await tx.execute(_COPY_CURRENT, {"new_id": new_id, "old_id": int(oid)})
            await tx.execute(_COPY_HISTORY, {"new_id": new_id, "old_id": int(oid), "cause": CAUSE_AMEND})
            series_rows = await tx.execute(_SERIES_OF, {"ids": [int(oid)]})
            touched.update(str(r["series_id"]) for r in series_rows or [])
            count += 1
        await refresh_series(tx, str(user_id), touched)
    return count


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
        new_id = int(inserted[0]["id"])
        aliases = await _load_aliases(tx, str(user_id))
        coding = coding_for(row, aliases)
        await _write_coding(tx, new_id, row, coding, CAUSE_AMEND)
        series_rows = await tx.execute(_SERIES_OF, {"ids": [int(observation_id)]})
        touched = {coding.series_id, *(str(r["series_id"]) for r in series_rows or [])}
        await refresh_series(tx, str(user_id), touched)
    return new_id


async def erase(
    user_id: str,
    *,
    ids: list[int] | None = None,
    source_ref: str | None = None,
    name_pattern: str | None = None,
    everything: bool = False,
) -> int:
    """The privacy path: physically delete a person's observations, by id,
    by source document, by a name pattern, or all of them. Coding, day
    authority and check rows go with them (`ON DELETE CASCADE`); the series
    catalogue is recomputed. Returns how many observations were deleted."""
    if ids:
        where, params = "id = ANY(:ids)", {"ids": [int(i) for i in ids]}
    elif source_ref:
        where, params = "source_ref = :source_ref", {"source_ref": source_ref}
    elif name_pattern:
        where, params = "name_text ILIKE :pattern", {"pattern": name_pattern}
    elif everything:
        where, params = "TRUE", {}
    else:
        return 0
    params["user_id"] = str(user_id)
    async with db.transaction() as tx:
        series_rows = await tx.execute(
            f"SELECT DISTINCT k.series_id FROM th_coding_current k JOIN th_observation o ON o.id = k.observation_id"
            f" WHERE o.user_id = :user_id AND {where}",
            params,
        )
        touched = {str(r["series_id"]) for r in series_rows or []}
        deleted = await tx.execute(f"DELETE FROM th_observation WHERE user_id = :user_id AND {where} RETURNING id", params)
        if source_ref:
            await tx.execute("DELETE FROM th_extraction WHERE user_id = :user_id AND source_ref = :source_ref", params)
        elif everything:
            await tx.execute("DELETE FROM th_extraction WHERE user_id = :user_id", params)
        await refresh_series(tx, str(user_id), touched)
    return len(deleted or [])


__all__ = [
    "CAUSE_AMEND",
    "CAUSE_INGEST",
    "Draft",
    "FINGERPRINT_FIELDS",
    "GRAIN_DAY",
    "GRAIN_INSTANT",
    "GRAIN_WINDOW",
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
    "Provenance",
    "REJECT_NO_NAME",
    "REJECT_NO_TIME",
    "REJECT_WRITE_ERROR",
    "Rejected",
    "Report",
    "SOURCE_API",
    "SOURCE_DEVICE",
    "SOURCE_FILE",
    "SOURCE_MANUAL",
    "STAT_AS_REPORTED",
    "amend",
    "catalog_alias",
    "coding_for",
    "erase",
    "ingest",
    "prepare",
    "refresh_series",
    "retract",
]

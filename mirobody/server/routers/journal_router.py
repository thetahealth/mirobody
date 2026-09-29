"""Logging what a person reports, and reading the log back by day.

The write side of the ICPC-3 axes. A person types what is wrong in their own
words; the words are stored verbatim and `translate` says which code they
name, or abstains. `kind` picks the axis: a `symptom` is what they feel now
and resolves on ICPC-3's S component, a `condition` is what they have been
diagnosed with and resolves on its D component. No new table: both are one
self-reported observation, which the model has had room for since 1.5.0, and
every invariant `collect.observations` enforces for a lab row holds here too.

`POST /journal` takes one entry. `POST /journal/sentence` takes whatever a
person typed ("我头疼，血压150/95，每天早晚吃二甲双胍500mg，午饭吃了面") and
writes what it states: the splitting is `collect.sentence`, an extraction step
with its own checks, and the coding is the same per-kind coding every row
gets. A reading stated in a sentence is a `measurement` row and is listed here
beside the complaints. A medication goes to the medication record through
`kernel.meds.reconcile_mentions` (a new plan, marked as from the journal, or a
stop), and is listed on the day it was written. Anything else (a meal, a mood)
is a `note`, kept as written: food has no vocabulary worth inventing one for.

`target_user_id` is declared and authorized on every route here: a
parameter a client sends and a route does not declare is dropped without a
word, and the write files under the caller.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter, Depends, Header, Query
from pydantic import BaseModel, Field

from mirobody import translate
from mirobody.collect import PostgresMedicationStore, apply_medication_mentions, observations, sentence
from mirobody.kernel import meds, series
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.user.care_circle import CareCircleDenied, resolve_subject, shared_with_me
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["journal"])

#: Every row this router writes shares it, so the identity index treats a
#: double submit of the same entry at the same instant as the retry it is.
#: An entry the person retracted does not count: they may log it again.
SOURCE_REF = "journal"
ON_CONFLICT = observations.ON_CONFLICT_REASSERT

#: The axis a `kind` writes on. `translate` owns the resolving; this map is
#: only which observation kind the caller may ask for.
KINDS = {
    "symptom": observations.KIND_SYMPTOM,
    "condition": observations.KIND_CONDITION,
    "note": observations.KIND_NOTE,
}

#: The kind a journal-made plan is listed under. Not an observation kind: a
#: plan lives in the medication record, and is only listed here.
KIND_MEDICATION = "medication"
MEDICATION_SOURCE = "journal"

MAX_DAYS = 400
MAX_ROWS = 2000

#: A reading typed into the journal is listed with it; a reading from a file
#: or a device is not, it belongs to the Indicators tab. `kinds` is cast
#: because it is empty when only readings are asked for.
_LISTED = """
   AND (o.kind = ANY(CAST(:kinds AS text[]))
        OR (o.kind = 'measurement' AND o.source_ref = :journal AND :with_readings))
"""

_SELECT = f"""
SELECT o.id, o.kind, o.local_date, o.observed_start, o.name_text, o.value_text, o.unit_text,
       o.code_system, o.code, o.display, o.outcome, o.reason, o.series_id,
       decrypt_content(o.note_text) AS note_text
  FROM v_observation o
 WHERE o.user_id = :user_id{_LISTED}
   AND o.local_date BETWEEN :from_date AND :to_date
 ORDER BY o.observed_start DESC
 LIMIT :limit
"""

_SELECT_WRITTEN = """
SELECT o.id, o.kind, o.observed_start, o.name_text, o.value_text, o.unit_text, o.code_system,
       o.code, o.display, o.outcome, o.reason, o.series_id, decrypt_content(o.note_text) AS note_text
  FROM v_observation o
 WHERE o.user_id = :user_id AND o.id = ANY(:ids)
 ORDER BY o.observed_start DESC, o.id
"""

_SELECT_ONE = """
SELECT o.outcome, o.reason, o.code_system, o.code, o.display, o.series_id, o.release
  FROM v_observation o
 WHERE o.id = :id AND o.user_id = :user_id
"""


_TZ = "The writer's IANA zone: 今天 is their day. Defaults to the X-Timezone header, then the record's."


class JournalSentence(BaseModel):
    text: str = Field(..., min_length=1, max_length=sentence.MAX_SENTENCE, description="What the person typed")
    observed_at: datetime | None = Field(None, description="When, for an entry whose words name no time. Defaults to now.")
    tz: str | None = Field(None, max_length=64, description=_TZ)
    target_user_id: str | None = Field(None, description="Log into this person's record; needs a write grant")


class JournalEntry(BaseModel):
    text: str = Field(..., min_length=1, max_length=200, description="The entry, in the person's own words")
    kind: str = Field("symptom", description='"symptom" (felt now) or "condition" (diagnosed)')
    observed_at: datetime | None = Field(None, description="When it was felt. Defaults to now.")
    note: str | None = Field(None, max_length=2000, description="Anything else worth keeping. Stored encrypted.")
    tz: str | None = Field(None, max_length=64, description=_TZ)
    target_user_id: str | None = Field(None, description="Log into this person's record; needs a write grant")


def _day(text: str | None) -> date | None:
    """`local_date` is a DATE column and the range arithmetic below is between
    days. `coerce.parse_date` answers a datetime, so one unconverted end of the
    range makes `end - start` a TypeError on the mixed call."""
    from mirobody.utils.coerce import parse_date

    when = parse_date(text) if text else None
    return when.date() if when else None


_BAD_ZONE = "tz must be an IANA zone name, like Asia/Shanghai."


def _bad_zone(tz: str | None) -> bool:
    """A `tz` the caller typed and no zone answers to. The web client's
    `X-Timezone` header still falls back quietly; a body field that says
    `Mars/Olympus` was filed under UTC and answered 200."""
    if not tz:
        return False
    try:
        translate.zone_for(tz)
    except ValueError:
        return True
    return False


def _writer_zone(record_tz: str, *said: str | None) -> tuple[str, datetime]:
    """`(zone, now)`: the zone the writer is in, the body's `tz` and else the
    web client's `X-Timezone` (sent on every request), and now in it. The
    zone is empty when neither parses: the drafts then fall back to the
    record's zone, which `resolve_tz` marks as the default it is."""
    for tz in said:
        if not tz or len(tz) > 64:
            continue
        try:
            return tz, datetime.now(tz=translate.zone_for(tz))
        except ValueError:
            continue
    return "", datetime.now(tz=translate.zone_for(translate.resolve_tz("", record_tz)[0]))


async def _label(caller: str, owner: str) -> str:
    """What the caller's circle calls the record's person ("Dad", a name), or
    empty when it calls them nothing."""
    for person in await shared_with_me(caller):
        if str(person.get("user_id")) == owner:
            return str(person.get("nickname") or person.get("name") or "")
    return ""


async def _subject(caller: str, target: str | None, *, write: bool) -> str | None:
    """Whose record this call touches, or `None` when the grant is missing."""
    owner = str(target or caller)
    if owner == str(caller):
        return owner
    try:
        await resolve_subject(caller, owner, require_write=write)
    except CareCircleDenied:
        return None
    return owner


@router.post("/journal")
async def log_entry(
    entry: JournalEntry,
    user_id: str = Depends(verify_token),
    x_timezone: str | None = Header(None, include_in_schema=False),
):
    """Log one entry. The answer carries the coding, so a client can show the
    standard name beside the words and put an abstention in a review queue
    rather than discarding it."""
    kind = KINDS.get(entry.kind)
    if kind is None:
        return ErrorResponse(code=400, msg=f"kind must be one of: {', '.join(sorted(KINDS))}.")
    if _bad_zone(entry.tz):
        return ErrorResponse(code=400, msg=_BAD_ZONE)
    owner = await _subject(user_id, entry.target_user_id, write=True)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot write to that record.")

    tz = await observations.user_tz(owner)
    said, now = _writer_zone(tz, entry.tz, x_timezone)
    draft = observations.Draft(
        name_text=entry.text,
        observed_start=entry.observed_at or now,
        kind=kind,
        note_text=entry.note or "",
        tz=said,
    )
    provenance = observations.Provenance(
        modality=observations.MODALITY_SELF,
        source_kind=observations.SOURCE_MANUAL,
        source_ref=SOURCE_REF,
        source_class=series.SOURCE_MANUAL,
    )
    try:
        report = await observations.ingest(owner, [draft], provenance, user_tz=tz, on_conflict=ON_CONFLICT)
    except Exception as e:
        # A type name and nothing else: a driver exception quotes the SQL and
        # its parameters, and the parameter here is what the person typed.
        logger.error("[log_entry] error_type=%s", type(e).__name__)
        return ErrorResponse(code=500, msg="This entry could not be saved.")

    if not report.inserted:
        if report.skipped:
            return StandardResponse(msg="Already logged.", data={"written": 0, "skipped": report.skipped})
        reason = ", ".join(sorted(report.rejected)) or "unknown"
        return ErrorResponse(code=400, msg=f"This entry could not be stored: {reason}")

    # Read the coding back rather than resolving a second time: a confirmed
    # alias is applied during the write, and a recompute without it would
    # report a code the row does not carry.
    stored = await execute_query(_SELECT_ONE, {"id": report.ids[0], "user_id": owner})
    row = (stored or [{}])[0]
    return StandardResponse(data={
        "id": report.ids[0],
        "text": entry.text,
        "kind": entry.kind,
        "coded": row.get("outcome") == "coded",
        "code_system": row.get("code_system"),
        "code": row.get("code"),
        "display": row.get("display") or "",
        "series_id": row.get("series_id"),
        "reason": row.get("reason") or "",
        "release": row.get("release"),
    })


def _entry(row: dict) -> dict:
    return {
        "id": row["id"],
        "kind": row["kind"],
        "at": row["observed_start"].isoformat() if row["observed_start"] else None,
        "text": row["name_text"],
        "value": row.get("value_text") or "",
        "unit": row.get("unit_text") or "",
        "display": row["display"] or "",
        "code_system": row["code_system"],
        "code": row["code"],
        "series_id": row["series_id"],
        "coded": row["outcome"] == "coded",
        "reason": row["reason"] or "",
        "note": row["note_text"] or "",
    }


@router.post("/journal/sentence")
async def log_sentence(
    entry: JournalSentence,
    user_id: str = Depends(verify_token),
    x_timezone: str | None = Header(None, include_in_schema=False),
):
    """Write every entry one sentence states. The answer lists what was
    written, with its coding, and every part that was not, with the reason
    (negated, someone else, a medication, ...), so the person sees what the
    sentence became rather than trusting it."""
    if _bad_zone(entry.tz):
        return ErrorResponse(code=400, msg=_BAD_ZONE)
    owner = await _subject(user_id, entry.target_user_id, write=True)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot write to that record.")
    if not sentence.available():
        return ErrorResponse(code=503, msg="Reading a sentence needs a text model (UTILS_TEXT_MODEL). Log one entry instead.")

    tz = await observations.user_tz(owner)
    said, now = _writer_zone(tz, entry.tz, x_timezone)
    record_of = None if owner == str(user_id) else await _label(user_id, owner)
    answer = await sentence.read(entry.text, now=now, record_of=record_of)
    if answer is None:
        return ErrorResponse(code=502, msg="The sentence could not be read. Try again, or log one entry.")
    parts, raw = answer
    drafts, skipped = sentence.plan(parts, sentence=entry.text, now=now, default_at=entry.observed_at or now, tz=said)
    mentions, refused = sentence.mentions(parts, sentence=entry.text, now=now)
    skipped = [*skipped, *refused]

    report = observations.Report()
    if drafts:
        provenance = observations.Provenance(
            modality=observations.MODALITY_SELF,
            source_kind=observations.SOURCE_MANUAL,
            source_ref=SOURCE_REF,
            source_class=series.SOURCE_MANUAL,
            extractor=sentence.EXTRACTOR,
        )
        try:
            report = await observations.ingest(
                owner, drafts, provenance, user_tz=tz, on_conflict=ON_CONFLICT,
                payload={"model": raw, "received_at": now.isoformat()},
            )
        except Exception as e:
            logger.error("[log_sentence] error_type=%s", type(e).__name__)
            return ErrorResponse(code=500, msg="This sentence could not be saved.")

    medications: list[dict] = []
    medications_failed = False
    if mentions:
        # The instant the observations were written at, so a sentence's plans
        # and entries are one record, and the writer's day, so "started today"
        # is theirs. Sending the sentence again is a new record; it adds no
        # second plan, because `reconcile_mentions` finds the drug listed.
        record_id = f"journal:{series.stable_hash(owner, entry.text, now.isoformat())[:16]}"
        try:
            outcomes = await apply_medication_mentions(
                owner, mentions, record_date=now.date(), source_record_id=record_id)
        except Exception as e:
            logger.error("[log_sentence] medications error_type=%s", type(e).__name__)
            medications_failed = True
        else:
            medications = [
                {"text": o.name, "quote": o.quote, "action": o.action, "plan_id": o.plan_id, "reason": o.reason}
                for o in outcomes
            ]

    rows = await execute_query(_SELECT_WRITTEN, {"user_id": owner, "ids": report.ids}) if report.ids else []
    return StandardResponse(data={
        "written": [_entry(r) for r in rows or []],
        "medications": medications,
        "medications_failed": medications_failed,
        "skipped": [s.__dict__ for s in skipped],
        "already_logged": report.skipped,
        "rejected": dict(report.rejected),
    })


@router.get("/journal")
async def list_entries(
    user_id: str = Depends(verify_token),
    from_date: str | None = Query(None, alias="from", description='"YYYY-MM-DD", inclusive'),
    to_date: str | None = Query(None, alias="to", description='"YYYY-MM-DD", inclusive'),
    kind: str | None = Query(None, description='Only this kind ("measurement" for readings typed here); omit for every kind'),
    target_user_id: str | None = Query(None, description="Read this person's record; needs a grant"),
    tz: str | None = Query(None, max_length=64, description="The reader's IANA zone: the default `to` is their today"),
    x_timezone: str | None = Header(None, include_in_schema=False),
):
    """The log, newest first, grouped by the day it was felt.

    Each entry carries both names: `text` is what the person wrote and
    `display` is what the classification calls it. An entry the vocabulary
    could not place keeps its words and reports why, because dropping it from
    the list would hide the half of the log that most needs a person's eye.
    """
    owner = await _subject(user_id, target_user_id, write=False)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot read that record.")

    start, end = _day(from_date), _day(to_date)
    if (from_date and start is None) or (to_date and end is None):
        return ErrorResponse(code=400, msg="from and to must be YYYY-MM-DD.")
    if _bad_zone(tz):
        return ErrorResponse(code=400, msg=_BAD_ZONE)
    if end is None:
        # The reader's today, not the server's: in UTC+8 the first eight hours
        # of a day were past the server's date, and that day's entries hidden.
        end = _writer_zone(await observations.user_tz(owner), tz, x_timezone)[1].date()
    start = start or (end - timedelta(days=30))
    if start > end:
        return ErrorResponse(code=400, msg="from is after to.")
    if (end - start).days > MAX_DAYS:
        return ErrorResponse(code=400, msg=f"That range is longer than {MAX_DAYS} days.")

    if kind is not None and kind not in KINDS and kind not in (observations.KIND_MEASUREMENT, KIND_MEDICATION):
        listed = sorted([*KINDS, observations.KIND_MEASUREMENT, KIND_MEDICATION])
        return ErrorResponse(code=400, msg=f"kind must be one of: {', '.join(listed)}.")
    kinds = [KINDS[kind]] if kind in KINDS else ([] if kind else sorted(KINDS.values()))
    if kind == KIND_MEDICATION:
        kinds = []
    try:
        rows = await execute_query(_SELECT, {
            "user_id": owner, "kinds": kinds, "journal": SOURCE_REF,
            "with_readings": kind in (None, observations.KIND_MEASUREMENT),
            "from_date": start, "to_date": end, "limit": MAX_ROWS,
        })
        plans = await _medications(owner, start, end, tz or x_timezone or "") \
            if kind in (None, KIND_MEDICATION) else []
    except Exception as e:
        logger.error("[list_journal] error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
        return ErrorResponse(code=500, msg="The journal could not be loaded.")

    days: dict[str, list[dict]] = {}
    for row in rows or []:
        days.setdefault(str(row["local_date"]), []).append(_entry(row))
    if kind in (None, KIND_MEDICATION):
        for day, item in plans:
            days.setdefault(day, []).append(item)
        # As instants: a reading's `at` is UTC and a plan's is local, so their
        # strings do not sort.
        for entries in days.values():
            entries.sort(key=_instant_of, reverse=True)
    return StandardResponse(data={
        "from": start.isoformat(),
        "to": end.isoformat(),
        "count": sum(len(v) for v in days.values()),
        "days": [{"date": d, "entries": days[d]} for d in sorted(days, reverse=True)],
    })


async def _medications(owner: str, start: date, end: date, said: str) -> list[tuple[str, dict]]:
    """The plans the journal made in `[start, end]`, each on the day it was
    written in the reader's zone, as entries."""
    record_tz = await observations.user_tz(owner)
    zone_name, _ = _writer_zone(record_tz, said)
    zone = translate.zone_for(zone_name or translate.resolve_tz("", record_tz)[0])
    since = datetime.combine(start, datetime.min.time(), tzinfo=zone)
    until = datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=zone)
    made = await PostgresMedicationStore().created_by(
        owner, MEDICATION_SOURCE, since=since, until=until, limit=MAX_ROWS)
    out = []
    for plan, created in made:
        local = created.astimezone(zone)
        out.append((local.date().isoformat(), _medication_entry(plan, local)))
    return out


def _instant_of(entry: dict) -> datetime:
    """An entry's `at` as an instant; a naive one is UTC, which is what the
    table's timestamps are."""
    if not entry.get("at"):
        return datetime.min.replace(tzinfo=UTC)
    at = datetime.fromisoformat(entry["at"])
    return at if at.tzinfo else at.replace(tzinfo=UTC)


def _medication_entry(plan: meds.MedicationPlan, created: datetime) -> dict:
    """A plan in the shape of a journal entry, plus what the list needs to
    say it: `plan_id` to open or remove it, the schedule's structure (the
    client words it), and its status."""
    return {
        "id": None,
        "plan_id": plan.plan_id,
        "kind": KIND_MEDICATION,
        "at": created.isoformat(),
        "text": plan.concept.text,
        "value": plan.concept.strength,
        "unit": "",
        "display": "",
        "code_system": None,
        "code": None,
        "series_id": None,
        "coded": False,
        "reason": "",
        "note": "; ".join(i.text for i in plan.schedule if i.text),
        "schedule": [
            {
                "dose": {"value": i.dose.value, "unit": i.dose.unit} if i.dose else None,
                "times": list(i.times), "doses_per_day": i.doses_per_day, "period_days": i.period_days,
                "weekdays": sorted(i.weekdays), "as_needed": i.as_needed,
            }
            for i in plan.schedule
        ],
        "status": plan.status,
        "confirmed": plan.confirmed,
    }


@router.delete("/journal/medication/{plan_id}")
async def retract_medication(
    plan_id: str,
    user_id: str = Depends(verify_token),
    target_user_id: str | None = Query(None, description="Retract from this person's record; needs a write grant"),
):
    """Remove a plan the journal made, with the grant the journal wrote it
    under: a caregiver who logged "Dad started X" can take it back. It is
    marked entered-in-error, as any removed plan is; a plan the person made
    elsewhere is not the journal's to remove."""
    owner = await _subject(user_id, target_user_id, write=True)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot write to that record.")
    store = PostgresMedicationStore()
    try:
        plan = await store.get(plan_id)
        if plan is None or plan.subject_id != owner or plan.source != MEDICATION_SOURCE \
                or plan.status == meds.PLAN_ENTERED_IN_ERROR:
            return ErrorResponse(code=404, msg="No such entry.")
        _, now = _writer_zone(await observations.user_tz(owner))
        await store.transition(owner, plan_id, "void", today=now.date())
    except Exception as e:
        logger.error("[retract_medication] error_type=%s", type(e).__name__)
        return ErrorResponse(code=500, msg="This entry could not be retracted.")
    return StandardResponse(data={"retracted": 1})


@router.delete("/journal/{observation_id}")
async def retract_entry(
    observation_id: int,
    user_id: str = Depends(verify_token),
    target_user_id: str | None = Query(None, description="Retract from this person's record; needs a write grant"),
):
    """Mark one entry entered in error. The row stays: `th_observation` is
    append-only and the view hides it, so a log that was corrected still says
    so to anyone auditing it."""
    owner = await _subject(user_id, target_user_id, write=True)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot write to that record.")
    try:
        count = await observations.retract(owner, [observation_id])
    except Exception as e:
        logger.error("[retract_entry] error_type=%s", type(e).__name__)
        return ErrorResponse(code=500, msg="This entry could not be retracted.")
    if not count:
        return ErrorResponse(code=404, msg="No such entry.")
    return StandardResponse(data={"retracted": count})

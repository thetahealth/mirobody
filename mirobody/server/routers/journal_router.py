"""Logging what a person reports, and reading the log back by day.

The write side of the ICPC-3 axes. A person types what is wrong in their own
words; the words are stored verbatim and `translate` says which code they
name, or abstains. `kind` picks the axis: a `symptom` is what they feel now
and resolves on ICPC-3's S component, a `condition` is what they have been
diagnosed with and resolves on its D component. No new table: both are one
self-reported observation, which the model has had room for since 1.5.0, and
every invariant `collect.observations` enforces for a lab row holds here too.

Two things this router deliberately does NOT do:

* it does not read a sentence. `text` is the complaint, not a diary entry.
  Pulling terms out of prose is an extraction decision and belongs with the
  other extraction decisions, where the prompt and the schema can refuse a
  negation ("no fever") instead of coding it;
* it does not take a meal or a dose. Medication has a domain model already
  (`kernel/meds.py`) and food has no vocabulary worth inventing one for.

`target_user_id` is declared and authorized. On `POST /files/upload` the same
parameter is not declared at all, so FastAPI drops it and a proxy upload
silently files under the caller instead: a parameter a client sends and a
route ignores is the shape that hides an authorization question.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from mirobody import translate
from mirobody.collect import observations
from mirobody.kernel import series
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.user.care_circle import CareCircleDenied, resolve_subject
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["journal"])

#: Every row this router writes shares it, so the identity index treats a
#: double submit of the same entry at the same instant as the retry it is.
#: The axis a `kind` writes on. `translate` owns the resolving; this map is
#: only which observation kind the caller may ask for.
SOURCE_REF = "journal"

KINDS = {
    "symptom": observations.KIND_SYMPTOM,
    "condition": observations.KIND_CONDITION,
}

MAX_DAYS = 400
MAX_ROWS = 2000

_SELECT = """
SELECT o.id, o.kind, o.local_date, o.observed_start, o.name_text, o.code_system, o.code,
       o.display, o.outcome, o.reason, o.series_id, decrypt_content(o.note_text) AS note_text
  FROM v_observation o
 WHERE o.user_id = :user_id
   AND o.kind = ANY(:kinds)
   AND o.local_date BETWEEN :from_date AND :to_date
 ORDER BY o.observed_start DESC
 LIMIT :limit
"""

_SELECT_ONE = """
SELECT o.outcome, o.reason, o.code_system, o.code, o.display, o.series_id, o.release
  FROM v_observation o
 WHERE o.id = :id AND o.user_id = :user_id
"""


class JournalEntry(BaseModel):
    text: str = Field(..., min_length=1, max_length=200, description="The entry, in the person's own words")
    kind: str = Field("symptom", description='"symptom" (felt now) or "condition" (diagnosed)')
    observed_at: datetime | None = Field(None, description="When it was felt. Defaults to now.")
    note: str | None = Field(None, max_length=2000, description="Anything else worth keeping. Stored encrypted.")
    target_user_id: str | None = Field(None, description="Log into this person's record; needs a write grant")


def _day(text: str | None) -> date | None:
    """`local_date` is a DATE column and the range arithmetic below is between
    days. `coerce.parse_date` answers a datetime, so one unconverted end of the
    range makes `end - start` a TypeError on the mixed call."""
    from mirobody.utils.coerce import parse_date

    when = parse_date(text) if text else None
    return when.date() if when else None


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
async def log_entry(entry: JournalEntry, user_id: str = Depends(verify_token)):
    """Log one entry. The answer carries the coding, so a client can show the
    standard name beside the words and put an abstention in a review queue
    rather than discarding it."""
    kind = KINDS.get(entry.kind)
    if kind is None:
        return ErrorResponse(code=400, msg=f"kind must be one of: {', '.join(sorted(KINDS))}.")
    owner = await _subject(user_id, entry.target_user_id, write=True)
    if owner is None:
        return ErrorResponse(code=403, msg="You cannot write to that record.")

    tz = await observations.user_tz(owner)
    draft = observations.Draft(
        name_text=entry.text,
        observed_start=entry.observed_at or datetime.now(tz=translate.zone_for(tz)),
        kind=kind,
        note_text=entry.note or "",
    )
    provenance = observations.Provenance(
        modality=observations.MODALITY_SELF,
        source_kind=observations.SOURCE_MANUAL,
        source_ref=SOURCE_REF,
        source_class=series.SOURCE_MANUAL,
    )
    try:
        report = await observations.ingest(owner, [draft], provenance, user_tz=tz)
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


@router.get("/journal")
async def list_entries(
    user_id: str = Depends(verify_token),
    from_date: str | None = Query(None, alias="from", description='"YYYY-MM-DD", inclusive'),
    to_date: str | None = Query(None, alias="to", description='"YYYY-MM-DD", inclusive'),
    kind: str | None = Query(None, description='Only this kind; omit for every kind'),
    target_user_id: str | None = Query(None, description="Read this person's record; needs a grant"),
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
    end = end or datetime.now().date()
    start = start or (end - timedelta(days=30))
    if (end - start).days > MAX_DAYS:
        return ErrorResponse(code=400, msg=f"That range is longer than {MAX_DAYS} days.")

    if kind is not None and kind not in KINDS:
        return ErrorResponse(code=400, msg=f"kind must be one of: {', '.join(sorted(KINDS))}.")
    kinds = [KINDS[kind]] if kind else sorted(KINDS.values())
    rows = await execute_query(_SELECT, {
        "user_id": owner, "kinds": kinds,
        "from_date": start, "to_date": end, "limit": MAX_ROWS,
    })

    days: dict[str, list[dict]] = {}
    for row in rows or []:
        days.setdefault(str(row["local_date"]), []).append({
            "id": row["id"],
            "kind": row["kind"],
            "at": row["observed_start"].isoformat() if row["observed_start"] else None,
            "text": row["name_text"],
            "display": row["display"] or "",
            "code_system": row["code_system"],
            "code": row["code"],
            "series_id": row["series_id"],
            "coded": row["outcome"] == "coded",
            "reason": row["reason"] or "",
            "note": row["note_text"] or "",
        })
    return StandardResponse(data={
        "from": start.isoformat(),
        "to": end.isoformat(),
        "count": sum(len(v) for v in days.values()),
        "days": [{"date": d, "entries": days[d]} for d in sorted(days, reverse=True)],
    })


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

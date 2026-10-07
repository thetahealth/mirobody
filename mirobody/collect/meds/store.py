"""`MedicationStore`, `DoseLogStore` and the override log, on Postgres.

Three rules the SQL here follows, all of them from `mirobody.kernel.meds`:

* **Nothing derived is stored.** `due`, `missed`, `upcoming` and
  `unschedulable` are what `meds.slot_state` answers from the clock, and
  adherence is what `meds.adherence` counts. A `status` column holding
  `missed` would be wrong the moment the person marks the dose taken.
* **Free text is encrypted, keys are not.** The drug name, the strength and
  the reason for a skip go through `encrypt_content`; `concept_key` (a code
  list or a hash of the normalised name) is what the indexes and the logs
  carry.
* **Corrections are a layer.** `th_override` is append-only and the stored
  row is never rewritten; `overrides()` hands the layer back so a reader can
  `overlay.apply` it.

The methods are `async` because the store is. `mirobody.kernel.meds` declares the
ports with plain `def` so an in-memory implementation stays possible; a
caller that may get either awaits with `query._awaited`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime

from mirobody.kernel import meds, overlay, series
from mirobody.utils import db, execute_query

logger = logging.getLogger(__name__)

TARGET_PLAN = "medication_plan"
TARGET_DOSE_EVENT = "dose_event"


# --- (de)serialisation: meds dataclasses <-> table rows ----------------------


def _dose_to_json(dose: meds.Dose | None) -> dict | None:
    return None if dose is None else {"value": dose.value, "unit": dose.unit}


def _dose_from_json(raw: object) -> meds.Dose | None:
    if not isinstance(raw, dict) or raw.get("value") is None:
        return None
    try:
        return meds.Dose(float(raw["value"]), str(raw.get("unit") or ""))
    except (TypeError, ValueError):
        return None


def instruction_to_json(instr: meds.DoseInstruction) -> dict:
    """One `DoseInstruction`'s structure as JSON. `text` and `as_needed_for`
    are the person's own words and are not here: `schedule` is jsonb, which
    `encrypt_content` cannot take. `text` rides in the encrypted
    `instructions_text` column (`_plan_params`); `as_needed_for` is not
    stored."""
    return {
        "dose": _dose_to_json(instr.dose),
        "times": list(instr.times),
        "doses_per_day": instr.doses_per_day,
        "period_days": instr.period_days,
        "weekdays": sorted(instr.weekdays),
        "as_needed": instr.as_needed,
        "max_dose_per_day": _dose_to_json(instr.max_dose_per_day),
    }


def instruction_from_json(raw: dict) -> meds.DoseInstruction:
    return meds.DoseInstruction(
        dose=_dose_from_json(raw.get("dose")),
        times=tuple(raw.get("times") or ()),
        doses_per_day=int(raw.get("doses_per_day") or 0),
        period_days=raw.get("period_days"),
        weekdays=frozenset(raw.get("weekdays") or ()),
        as_needed=bool(raw.get("as_needed")),
        max_dose_per_day=_dose_from_json(raw.get("max_dose_per_day")),
    )


def _codes_to_json(concept: meds.MedicationConcept) -> list[dict]:
    return [{"system": c.system, "code": c.code, "display": c.display, "tty": c.tty} for c in concept.codes]


def _loads(raw: object, default: object) -> object:
    """psycopg gives a `jsonb` back as a Python object; a driver configured
    otherwise gives the text. Accept both rather than depending on which."""
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return default
    return default if raw is None else raw


def plan_from_row(row: dict) -> meds.MedicationPlan:
    codes = tuple(
        meds.Coding(str(c.get("system") or ""), str(c.get("code") or ""), str(c.get("display") or ""), str(c.get("tty") or ""))
        for c in (_loads(row.get("codes"), []) or [])
        if isinstance(c, dict)
    )
    concept = meds.MedicationConcept(
        text=str(row.get("concept_text") or ""),
        codes=codes,
        form=str(row.get("concept_form") or ""),
        strength=str(row.get("concept_strength") or ""),
    )
    words = _loads(row.get("instructions_text"), []) or []
    schedule = tuple(
        replace(instruction_from_json(i), text=str(words[n]) if n < len(words) and words[n] else "")
        for n, i in enumerate(i for i in (_loads(row.get("schedule"), []) or []) if isinstance(i, dict))
    ) or (meds.DoseInstruction(),)
    return meds.MedicationPlan(
        plan_id=str(row["plan_id"]),
        concept=concept,
        schedule=schedule,
        start=row["start_date"],
        end=row.get("end_date"),
        status=str(row.get("status") or meds.PLAN_ACTIVE),
        classification=str(row.get("classification") or ""),
        confirmed=bool(row.get("confirmed", True)),
        order_id=str(row.get("order_id") or ""),
        source=str(row.get("source") or ""),
        source_record_id=str(row.get("source_record_id") or ""),
        subject_id=str(row.get("user_id") or ""),
        stopped_on=row.get("stopped_on"),
    )


def event_from_row(row: dict) -> meds.DoseEvent:
    slot_key = None
    if row.get("slot_date") and row.get("slot_name"):
        slot_key = (str(row["plan_id"]), row["slot_date"], str(row["slot_name"]))
    dose = None
    if row.get("dose_value") is not None and row.get("dose_unit"):
        dose = meds.Dose(float(row["dose_value"]), str(row["dose_unit"]))
    return meds.DoseEvent(
        event_id=str(row["event_id"]),
        plan_id=str(row["plan_id"]),
        status=str(row["status"]),
        taken_at_ms=int(row["taken_at_ms"]),
        tz=str(row.get("tz") or "UTC"),
        slot_key=slot_key,
        dose=dose,
        recorded_by=str(row.get("recorded_by") or "user"),
        reason=str(row.get("reason") or ""),
    )


_PLAN_COLUMNS = """
    plan_id, user_id, concept_key, decrypt_content(concept_text) AS concept_text,
    decrypt_content(concept_strength) AS concept_strength, concept_form, codes, schedule,
    start_date, end_date, status, classification, confirmed, order_id, source,
    source_record_id, stopped_on, decrypt_content(instructions_text) AS instructions_text,
    create_time
"""

_EVENT_COLUMNS = """
    event_id, plan_id, user_id, status, taken_at_ms, tz, local_date, slot_date, slot_name,
    dose_value, dose_unit, recorded_by, decrypt_content(reason) AS reason
"""


_PUT_PLAN = """
INSERT INTO th_medication_plan (
    plan_id, user_id, concept_key, concept_text, concept_strength, concept_form,
    codes, schedule, start_date, end_date, status, classification, confirmed,
    order_id, source, source_record_id, stopped_on, instructions_text, create_time, update_time, deleted
) VALUES (
    :plan_id, :user_id, :concept_key, encrypt_content(:concept_text),
    encrypt_content(:concept_strength), :concept_form, CAST(:codes AS jsonb),
    CAST(:schedule AS jsonb), :start_date, :end_date, :status, :classification,
    :confirmed, :order_id, :source, :source_record_id, :stopped_on,
    encrypt_content(:instructions_text), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
)
ON CONFLICT (plan_id) DO UPDATE SET
    concept_key = EXCLUDED.concept_key,
    concept_text = EXCLUDED.concept_text,
    concept_strength = EXCLUDED.concept_strength,
    concept_form = EXCLUDED.concept_form,
    codes = EXCLUDED.codes,
    schedule = EXCLUDED.schedule,
    start_date = EXCLUDED.start_date,
    end_date = EXCLUDED.end_date,
    status = EXCLUDED.status,
    classification = EXCLUDED.classification,
    confirmed = EXCLUDED.confirmed,
    order_id = EXCLUDED.order_id,
    source = EXCLUDED.source,
    source_record_id = EXCLUDED.source_record_id,
    stopped_on = EXCLUDED.stopped_on,
    instructions_text = EXCLUDED.instructions_text,
    update_time = CURRENT_TIMESTAMP,
    deleted = 0
"""


#: `_PUT_PLAN` for a plan that must be new: an id that exists writes nothing.
_NEW_PLAN = _PUT_PLAN[: _PUT_PLAN.index("ON CONFLICT")] + "ON CONFLICT (plan_id) DO NOTHING RETURNING plan_id\n"


def _plan_params(plan: meds.MedicationPlan) -> dict:
    return {
        "plan_id": plan.plan_id,
        "user_id": plan.subject_id,
        "concept_key": plan.concept.concept_key,
        "concept_text": plan.concept.text,
        "concept_strength": plan.concept.strength,
        "concept_form": plan.concept.form,
        "codes": json.dumps(_codes_to_json(plan.concept), ensure_ascii=False),
        "schedule": json.dumps([instruction_to_json(i) for i in plan.schedule], ensure_ascii=False),
        "start_date": plan.start,
        "end_date": plan.end,
        "status": plan.status,
        "classification": plan.classification,
        "confirmed": plan.confirmed,
        "order_id": plan.order_id,
        "source": plan.source,
        "source_record_id": plan.source_record_id,
        "stopped_on": plan.stopped_on,
        "instructions_text": json.dumps([i.text for i in plan.schedule], ensure_ascii=False)
        if any(i.text for i in plan.schedule) else None,
    }


#: A course's id is its plan, its start and its place in the plan's history.
#: Keyed by plan and start alone, a plan stopped and resumed on one day gave the
#: new course the id of the one just closed: it could not be written, and the
#: next stop merged the two. The open course is found by `closed_by IS NULL`,
#: never by rebuilding its id.
_INSERT_COURSE = """
INSERT INTO th_medication_course (course_id, plan_id, user_id, order_id, start_date, end_date, closed_by)
VALUES (:course_id, :plan_id, :user_id, :order_id, :start_date, :end_date, :closed_by)
"""


async def _next_course_id(tx, plan_id: str, start: date) -> str:
    """Called with the plan row locked, so two writers cannot take one number."""
    rows = await tx.execute(
        "SELECT COUNT(*) AS n FROM th_medication_course WHERE plan_id = :pid", {"pid": plan_id})
    return f"{plan_id}:{start.isoformat()}:{int(rows[0]['n']) if rows else 0}"


async def _open_course(tx, subject_id: str, plan: meds.MedicationPlan, start: date) -> None:
    await tx.execute(_INSERT_COURSE, {
        "course_id": await _next_course_id(tx, plan.plan_id, start),
        "plan_id": plan.plan_id, "user_id": str(subject_id), "order_id": plan.order_id,
        "start_date": start, "end_date": None, "closed_by": None,
    })


async def _close_course(tx, subject_id: str, closed: meds.Course) -> None:
    """Close the open course; record the closed one when none was open (a plan
    stored before the web form existed has no course rows)."""
    rows = await tx.execute(
        "UPDATE th_medication_course SET end_date = :end_date, closed_by = :closed_by"
        " WHERE plan_id = :pid AND closed_by IS NULL RETURNING course_id",
        {"end_date": closed.end, "closed_by": closed.closed_by, "pid": closed.plan_id},
    )
    if rows:
        return
    await tx.execute(_INSERT_COURSE, {
        "course_id": await _next_course_id(tx, closed.plan_id, closed.start),
        "plan_id": closed.plan_id, "user_id": str(subject_id), "order_id": closed.order_id,
        "start_date": closed.start, "end_date": closed.end, "closed_by": closed.closed_by,
    })


async def _lock_plan(tx, subject_id: str, plan_id: str) -> meds.MedicationPlan:
    rows = await tx.execute(
        f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan "
        "WHERE plan_id = :pid AND user_id = :uid AND deleted = 0 FOR UPDATE",
        {"pid": str(plan_id), "uid": str(subject_id)},
    )
    if not rows:
        raise LookupError("medication plan not found")
    return plan_from_row(dict(rows[0]))


class PostgresMedicationStore:
    """`meds.MedicationStore` over `th_medication_plan` / `th_medication_course`."""

    async def list(self, subject_id: str, *, active_only: bool = False) -> Sequence[meds.MedicationPlan]:
        sql = f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan WHERE user_id = :uid AND deleted = 0 AND status <> :void"
        if active_only:
            sql += " AND status = :active"
        sql += " ORDER BY start_date DESC, plan_id"
        params = {"uid": str(subject_id), "void": meds.PLAN_ENTERED_IN_ERROR}
        if active_only:
            params["active"] = meds.PLAN_ACTIVE
        rows = await execute_query(sql, params, log_sql=False) or []
        return [plan_from_row(dict(r)) for r in rows]

    async def get(self, plan_id: str) -> meds.MedicationPlan | None:
        sql = f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan WHERE plan_id = :pid AND deleted = 0"
        rows = await execute_query(sql, {"pid": str(plan_id)}, log_sql=False) or []
        return plan_from_row(dict(rows[0])) if rows else None

    async def created_by(
        self, subject_id: str, source: str, *, since: datetime, until: datetime, limit: int = 2000
    ) -> list[tuple[meds.MedicationPlan, datetime]]:
        """The subject's plans `source` created in `[since, until)`, newest
        first, with when each was created: the journal lists the plans a
        sentence made on the day it was written, not the day they start."""
        rows = await execute_query(
            f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan"
            " WHERE user_id = :uid AND source = :source AND deleted = 0 AND status <> :void"
            " AND create_time >= :since AND create_time < :until ORDER BY create_time DESC LIMIT :limit",
            {"uid": str(subject_id), "source": source, "void": meds.PLAN_ENTERED_IN_ERROR,
             "since": since, "until": until, "limit": max(1, int(limit))},
            log_sql=False,
        ) or []
        return [(plan_from_row(dict(r)), r["create_time"]) for r in rows]

    async def owner(self, plan_id: str) -> str | None:
        rows = await execute_query(
            "SELECT user_id FROM th_medication_plan WHERE plan_id = :pid AND deleted = 0",
            {"pid": str(plan_id)}, log_sql=False,
        ) or []
        return str(rows[0]["user_id"]) if rows else None

    async def put(self, plan: meds.MedicationPlan) -> None:
        """Insert or replace one plan. The natural key is `plan_id`, which
        `meds.plan_id_for` derives from `(subject, concept_key, start)`, so a
        re-import of the same statement updates rather than duplicates."""
        await execute_query(_PUT_PLAN, _plan_params(plan), log_sql=False)

    async def create(self, plan: meds.MedicationPlan) -> bool:
        """A new plan and its opening course in one transaction; False, and
        nothing written, when a plan with this id exists. Written as two
        statements, a failure between them left an active plan with no open
        course, and its first stop then closed a course that was never opened."""
        async with db.transaction() as tx:
            if not await tx.execute(_NEW_PLAN, _plan_params(plan)):
                return False
            await _open_course(tx, plan.subject_id, plan, plan.start)
            return True

    async def revise(self, subject_id: str, plan_id: str, changes: dict) -> meds.MedicationPlan:
        """Apply the fields in `changes` to the subject's plan, under a row lock.

        The open course starts when the plan does, so a start that moves takes
        the open course with it. A stopped plan's start is its last course's
        and is history, so it does not move; nor may an active plan's start
        move back over a course already closed.
        """
        async with db.transaction() as tx:
            old = await _lock_plan(tx, subject_id, plan_id)
            if old.status == meds.PLAN_ENTERED_IN_ERROR:
                raise LookupError("medication plan not found")
            new = replace(old, **changes)
            if new.start != old.start:
                if old.status != meds.PLAN_ACTIVE:
                    raise ValueError("only an active plan's start date can change")
                closed = await tx.execute(
                    "SELECT MAX(end_date) AS last_end FROM th_medication_course"
                    " WHERE plan_id = :pid AND closed_by IS NOT NULL",
                    {"pid": old.plan_id},
                )
                last_end = closed[0]["last_end"] if closed else None
                if last_end is not None and new.start < last_end:
                    raise ValueError("start date overlaps a closed course")
                await tx.execute(
                    "UPDATE th_medication_course SET start_date = :start"
                    " WHERE plan_id = :pid AND closed_by IS NULL",
                    {"start": new.start, "pid": old.plan_id},
                )
            await tx.execute(_PUT_PLAN, _plan_params(new))
            return new

    async def overrides(self, plan_id: str) -> Sequence[overlay.Override]:
        return await _overrides_for(TARGET_PLAN, plan_id)

    async def courses(self, plan_id: str) -> Sequence[meds.Course]:
        rows = await execute_query(
            "SELECT plan_id, order_id, start_date, end_date, closed_by FROM th_medication_course"
            " WHERE plan_id = :pid ORDER BY start_date",
            {"pid": str(plan_id)},
            log_sql=False,
        ) or []
        return [
            meds.Course(str(r["plan_id"]), str(r["order_id"] or ""), r["start_date"], r["end_date"], r["closed_by"])
            for r in rows
        ]

    async def transition(self, subject_id: str, plan_id: str, event: str, *, today: date) -> meds.MedicationPlan:
        """Apply a kernel transition and its course changes in one transaction."""
        async with db.transaction() as tx:
            old = await _lock_plan(tx, subject_id, plan_id)
            updated, closed = meds.plan_status_transition(old, event, today=today)  # type: ignore[arg-type]
            await tx.execute(
                """
                UPDATE th_medication_plan SET start_date = :start_date, end_date = :end_date,
                    status = :status, stopped_on = :stopped_on, update_time = CURRENT_TIMESTAMP
                WHERE plan_id = :plan_id AND user_id = :user_id
                """,
                {
                    "start_date": updated.start, "end_date": updated.end,
                    "status": updated.status, "stopped_on": updated.stopped_on,
                    "plan_id": updated.plan_id, "user_id": str(subject_id),
                },
            )
            if event == "stop" and closed is not None:
                await _close_course(tx, subject_id, closed)
            elif event == "void":
                # An entry made in error was never followed: its open course
                # must not read as an exposure still running.
                await tx.execute(
                    "UPDATE th_medication_course SET end_date = :today, closed_by = 'entered_in_error'"
                    " WHERE plan_id = :pid AND closed_by IS NULL",
                    {"today": today, "pid": plan_id},
                )
            elif event == "resume":
                # The stop already closed the previous course; the kernel hands
                # it back for a store that recorded nothing then (a plan stopped
                # before the web form existed). Record it only in that case.
                done = closed is not None and await tx.execute(
                    "SELECT 1 FROM th_medication_course WHERE plan_id = :pid"
                    " AND closed_by IS NOT NULL AND start_date = :start LIMIT 1",
                    {"pid": updated.plan_id, "start": closed.start},
                )
                if closed is not None and not done:
                    await _close_course(tx, subject_id, closed)
                await _open_course(tx, subject_id, updated, updated.start)
            return updated


class PostgresDoseLogStore:
    """`meds.DoseLogStore` over `th_dose_event`."""

    async def list(self, subject_id: str, window: tuple[date, date]) -> Sequence[meds.DoseEvent]:
        lo, hi = window
        rows = await execute_query(
            f"SELECT {_EVENT_COLUMNS} FROM th_dose_event"
            " WHERE user_id = :uid AND deleted = 0 AND local_date BETWEEN :lo AND :hi"
            " ORDER BY taken_at_ms DESC",
            {"uid": str(subject_id), "lo": lo, "hi": hi},
            log_sql=False,
        ) or []
        return [event_from_row(dict(r)) for r in rows]

    async def append(self, event: meds.DoseEvent, *, subject_id: str = "") -> None:
        local_date = datetime.fromtimestamp(event.taken_at_ms / 1000, UTC).astimezone(series.zone(event.tz)).date()
        await execute_query(
            """
            INSERT INTO th_dose_event (
                event_id, plan_id, user_id, status, taken_at_ms, tz, local_date,
                slot_date, slot_name, dose_value, dose_unit, recorded_by, reason, create_time, deleted
            ) VALUES (
                :event_id, :plan_id, :user_id, :status, :taken_at_ms, :tz, :local_date,
                :slot_date, :slot_name, :dose_value, :dose_unit, :recorded_by,
                encrypt_content(:reason), CURRENT_TIMESTAMP, 0
            )
            ON CONFLICT (event_id) DO UPDATE SET
                status = EXCLUDED.status,
                taken_at_ms = EXCLUDED.taken_at_ms,
                tz = EXCLUDED.tz,
                local_date = EXCLUDED.local_date,
                slot_date = EXCLUDED.slot_date,
                slot_name = EXCLUDED.slot_name,
                dose_value = EXCLUDED.dose_value,
                dose_unit = EXCLUDED.dose_unit,
                recorded_by = EXCLUDED.recorded_by,
                reason = EXCLUDED.reason,
                deleted = 0
            """,
            {
                "event_id": event.event_id,
                "plan_id": event.plan_id,
                "user_id": str(subject_id),
                "status": event.status,
                "taken_at_ms": event.taken_at_ms,
                "tz": event.tz,
                "local_date": local_date,
                "slot_date": event.slot_key.local_date if event.slot_key else None,
                "slot_name": event.slot_key.slot if event.slot_key else None,
                "dose_value": event.dose.value if event.dose else None,
                "dose_unit": event.dose.unit if event.dose else "",
                "recorded_by": event.recorded_by,
                "reason": event.reason,
            },
            log_sql=False,
        )


class PostgresOverlayStore:
    """The correction layer (`th_override`), append-only.

    Not a `meds` port: overrides apply to readings and dose events too, and
    the vocabulary is `mirobody.kernel.overlay`. Kept here because the medication
    tables are the first thing that needs it.
    """

    async def append(self, subject_id: str, target_kind: str, override: overlay.Override) -> None:
        await execute_query(
            """
            INSERT INTO th_override (override_id, user_id, target_kind, target_id, field, value, actor, at_ms, seq, reason)
            VALUES (:override_id, :user_id, :target_kind, :target_id, :field, encrypt_content(:value),
                    :actor, :at_ms, :seq, encrypt_content(:reason))
            ON CONFLICT (override_id) DO NOTHING
            """,
            {
                "override_id": f"{target_kind}:{override.target_id}:{override.field}:{override.at_ms}:{override.seq}",
                "user_id": str(subject_id),
                "target_kind": target_kind,
                "target_id": override.target_id,
                "field": override.field,
                "value": None if override.value is None else json.dumps(override.value, ensure_ascii=False),
                "actor": override.actor,
                "at_ms": override.at_ms,
                "seq": override.seq,
                "reason": override.reason,
            },
            log_sql=False,
        )

    async def for_target(self, target_kind: str, target_id: str) -> Sequence[overlay.Override]:
        return await _overrides_for(target_kind, target_id)


async def _overrides_for(target_kind: str, target_id: str) -> list[overlay.Override]:
    rows = await execute_query(
        "SELECT target_id, field, decrypt_content(value) AS value, actor, at_ms, seq,"
        " decrypt_content(reason) AS reason FROM th_override"
        " WHERE target_kind = :kind AND target_id = :tid ORDER BY at_ms, seq",
        {"kind": target_kind, "tid": str(target_id)},
        log_sql=False,
    ) or []
    return [
        overlay.Override(
            target_id=str(r["target_id"]),
            field=str(r["field"] or ""),
            value=_loads(r["value"], None),
            at_ms=int(r["at_ms"]),
            actor=str(r["actor"] or ""),
            reason=str(r["reason"] or ""),
            seq=int(r["seq"] or 0),
        )
        for r in rows
    ]

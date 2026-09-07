"""`MedicationStore`, `DoseLogStore` and the override log, on Postgres.

Three rules the SQL here follows, all of them from `mirobody.kernel.meds`:

* **Nothing derived is stored.** `due`, `missed`, `upcoming` and
  `unschedulable` are what `meds.slot_state` answers from the clock, and
  adherence is what `meds.adherence` counts. A `status` column holding
  `missed` would be wrong the moment the person marks the dose taken.
* **Free text is encrypted, keys are not.** The drug name, the strength and
  the reason for a skip go through `encrypt_content`; `concept_key` — a code
  list or a hash of the normalised name — is what the indexes and the logs
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
from datetime import UTC, date, datetime

from ...kernel import meds, overlay, series
from ...utils import execute_query

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
    """One `DoseInstruction` as JSON. `as_needed_for` and `text` are the
    person's own words and ride encrypted with the rest of the schedule
    column — the whole `schedule` jsonb is written through `encrypt_content`
    is NOT possible (jsonb is not text), so those two fields are dropped here
    and kept in `concept_text` territory instead: a plan's free text lives in
    the encrypted columns, the schedule column carries only structure."""
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
    schedule = tuple(
        instruction_from_json(i) for i in (_loads(row.get("schedule"), []) or []) if isinstance(i, dict)
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
    source_record_id, stopped_on
"""

_EVENT_COLUMNS = """
    event_id, plan_id, user_id, status, taken_at_ms, tz, local_date, slot_date, slot_name,
    dose_value, dose_unit, recorded_by, decrypt_content(reason) AS reason
"""


class PostgresMedicationStore:
    """`meds.MedicationStore` over `th_medication_plan` / `th_medication_course`."""

    async def list(self, subject_id: str, *, active_only: bool = False) -> Sequence[meds.MedicationPlan]:
        sql = f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan WHERE user_id = :uid AND deleted = 0"
        if active_only:
            sql += f" AND status = '{meds.PLAN_ACTIVE}'"
        sql += " ORDER BY start_date DESC, plan_id"
        rows = await execute_query(sql, {"uid": str(subject_id)}, log_sql=False) or []
        return [plan_from_row(dict(r)) for r in rows]

    async def get(self, plan_id: str) -> meds.MedicationPlan | None:
        sql = f"SELECT {_PLAN_COLUMNS} FROM th_medication_plan WHERE plan_id = :pid AND deleted = 0"
        rows = await execute_query(sql, {"pid": str(plan_id)}, log_sql=False) or []
        return plan_from_row(dict(rows[0])) if rows else None

    async def put(self, plan: meds.MedicationPlan) -> None:
        """Insert or replace one plan. The natural key is `plan_id`, which
        `meds.plan_id_for` derives from `(subject, concept_key, start)` — so a
        re-import of the same statement updates rather than duplicates."""
        await execute_query(
            """
            INSERT INTO th_medication_plan (
                plan_id, user_id, concept_key, concept_text, concept_strength, concept_form,
                codes, schedule, start_date, end_date, status, classification, confirmed,
                order_id, source, source_record_id, stopped_on, create_time, update_time, deleted
            ) VALUES (
                :plan_id, :user_id, :concept_key, encrypt_content(:concept_text),
                encrypt_content(:concept_strength), :concept_form, CAST(:codes AS jsonb),
                CAST(:schedule AS jsonb), :start_date, :end_date, :status, :classification,
                :confirmed, :order_id, :source, :source_record_id, :stopped_on,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
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
                update_time = CURRENT_TIMESTAMP,
                deleted = 0
            """,
            {
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
            },
            log_sql=False,
        )

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

    async def add_course(self, subject_id: str, course: meds.Course) -> None:
        await execute_query(
            """
            INSERT INTO th_medication_course (course_id, plan_id, user_id, order_id, start_date, end_date, closed_by)
            VALUES (:course_id, :plan_id, :user_id, :order_id, :start_date, :end_date, :closed_by)
            ON CONFLICT (course_id) DO UPDATE SET
                end_date = EXCLUDED.end_date, closed_by = EXCLUDED.closed_by
            """,
            {
                "course_id": f"{course.plan_id}:{course.start.isoformat()}",
                "plan_id": course.plan_id,
                "user_id": str(subject_id),
                "order_id": course.order_id,
                "start_date": course.start,
                "end_date": course.end,
                "closed_by": course.closed_by,
            },
            log_sql=False,
        )


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
                "slot_date": event.slot_key[1] if event.slot_key else None,
                "slot_name": event.slot_key[2] if event.slot_key else None,
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

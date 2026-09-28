"""Owner-safe medication forms for the open-source web client.

The kernel owns schedule validation and lifecycle rules. This router only
converts JSON into those kernel objects, applies care-circle read access, and
returns the house ``{code, msg, data}`` envelope.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field, model_validator

from mirobody.collect import PostgresMedicationStore
from mirobody.kernel import meds, series
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.user.care_circle import CareCircleDenied, resolve_subject
from mirobody.user.user import get_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/medications", tags=["medications"])


class MedicationCodeInput(BaseModel):
    system: str = Field(min_length=1, max_length=200)
    code: str = Field(min_length=1, max_length=100)
    display: str = Field(default="", max_length=300)
    tty: str = Field(default="", max_length=32)


class DoseInput(BaseModel):
    value: float = Field(gt=0, le=100000)
    unit: str = Field(min_length=1, max_length=32)


class InstructionInput(BaseModel):
    dose: DoseInput | None = None
    times: list[str] = Field(default_factory=list, max_length=12)
    doses_per_day: int = Field(default=0, ge=0, le=24)
    period_days: int | None = Field(default=None, ge=1, le=365)
    weekdays: list[int] = Field(default_factory=list, max_length=7)
    as_needed: bool = False
    max_dose_per_day: DoseInput | None = None

    @model_validator(mode="after")
    def validate_instruction(self) -> InstructionInput:
        try:
            meds.DoseInstruction(
                dose=_dose(self.dose), times=tuple(self.times),
                doses_per_day=self.doses_per_day, period_days=self.period_days,
                weekdays=frozenset(self.weekdays), as_needed=self.as_needed,
                max_dose_per_day=_dose(self.max_dose_per_day),
            )
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return self


class MedicationInput(BaseModel):
    name: str = Field(min_length=1, max_length=500)
    form: str = Field(default="", max_length=100)
    strength: str = Field(default="", max_length=200)
    codes: list[MedicationCodeInput] = Field(default_factory=list, max_length=20)
    schedule: list[InstructionInput] = Field(min_length=1, max_length=12)
    start_date: date
    end_date: date | None = None
    classification: str = Field(default="", max_length=100)
    confirmed: bool = True

    @model_validator(mode="after")
    def validate_dates(self) -> MedicationInput:
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError("end_date precedes start_date")
        return self


def _dose(value: DoseInput | None) -> meds.Dose | None:
    return None if value is None else meds.Dose(value.value, value.unit)


def _plan_input(body: MedicationInput, subject_id: str, plan_id: str | None = None) -> meds.MedicationPlan:
    concept = meds.MedicationConcept(
        text=body.name.strip(),
        form=body.form.strip(),
        strength=body.strength.strip(),
        codes=tuple(meds.Coding(c.system, c.code, c.display, c.tty) for c in body.codes),
    )
    schedule = tuple(
        meds.DoseInstruction(
            dose=_dose(item.dose), times=tuple(item.times),
            doses_per_day=item.doses_per_day, period_days=item.period_days,
            weekdays=frozenset(item.weekdays), as_needed=item.as_needed,
            max_dose_per_day=_dose(item.max_dose_per_day),
        )
        for item in body.schedule
    )
    return meds.MedicationPlan(
        plan_id=plan_id or meds.plan_id_for(subject_id, str(uuid.uuid4()), concept.concept_key),
        concept=concept, schedule=schedule, start=body.start_date, end=body.end_date,
        classification=body.classification.strip(), confirmed=body.confirmed,
        source="web", subject_id=str(subject_id),
    )


async def _subject(caller: str, target: str | None) -> tuple[str | None, ErrorResponse | None]:
    if not target or str(target) == str(caller):
        return str(caller), None
    try:
        await resolve_subject(str(caller), str(target))
    except CareCircleDenied:
        return None, ErrorResponse(code=403, msg="Not permitted to read this member's medications.")
    return str(target), None


async def _owner_or_404(caller: str, plan_id: str) -> tuple[str | None, ErrorResponse | None]:
    store = PostgresMedicationStore()
    owner = await store.owner(plan_id)
    if owner is None:
        return None, ErrorResponse(code=404, msg="No such medication plan.")
    subject, error = await _subject(caller, owner)
    return subject, error


async def _zone(subject_id: str) -> str:
    user = await get_user(user_id=subject_id)
    return str((user or {}).get("tz") or "UTC")


async def _today(subject_id: str) -> date:
    return datetime.now().astimezone(series.zone(await _zone(subject_id))).date()


def _json_plan(plan: meds.MedicationPlan, *, today: date, courses: list[meds.Course] | None = None) -> dict[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "name": plan.concept.text,
        "form": plan.concept.form,
        "strength": plan.concept.strength,
        "codes": [{"system": c.system, "code": c.code, "display": c.display, "tty": c.tty} for c in plan.concept.codes],
        "schedule": [
            {
                "dose": {"value": i.dose.value, "unit": i.dose.unit} if i.dose else None,
                "times": list(i.times), "doses_per_day": i.doses_per_day,
                "period_days": i.period_days, "weekdays": sorted(i.weekdays),
                "as_needed": i.as_needed,
                "max_dose_per_day": {"value": i.max_dose_per_day.value, "unit": i.max_dose_per_day.unit} if i.max_dose_per_day else None,
            }
            for i in plan.schedule
        ],
        "start_date": plan.start.isoformat(),
        "end_date": plan.end.isoformat() if plan.end else None,
        "status": plan.status,
        "effective_status": meds.effective_status(plan, today),
        "classification": plan.classification,
        "confirmed": plan.confirmed,
        "source": plan.source,
        "courses": [
            {"start_date": c.start.isoformat(), "end_date": c.end.isoformat() if c.end else None, "closed_by": c.closed_by}
            for c in courses or ()
        ],
    }


@router.get("")
async def list_medications(
    target_user_id: str | None = Query(None),
    status: str | None = Query(None),
    user_id: str = Depends(verify_token),
):
    subject, error = await _subject(user_id, target_user_id)
    if error:
        return error
    store = PostgresMedicationStore()
    plans = await store.list(subject or user_id)
    if status:
        allowed = {meds.EFFECTIVE_ACTIVE, meds.EFFECTIVE_INTENDED, meds.EFFECTIVE_COMPLETED, meds.EFFECTIVE_STOPPED}
        if status not in allowed:
            return ErrorResponse(code=400, msg="status is invalid.")
    today = await _today(subject or user_id)
    rows = [_json_plan(p, today=today) for p in plans]
    if status:
        rows = [r for r in rows if r["effective_status"] == status]
    return StandardResponse(data={"items": rows, "total": len(rows)})


@router.get("/{plan_id}")
async def get_medication(plan_id: str, user_id: str = Depends(verify_token)):
    subject, error = await _owner_or_404(user_id, plan_id)
    if error:
        return error
    store = PostgresMedicationStore()
    plan = await store.get(plan_id)
    if plan is None or plan.status == meds.PLAN_ENTERED_IN_ERROR:
        return ErrorResponse(code=404, msg="No such medication plan.")
    return StandardResponse(data=_json_plan(plan, today=await _today(subject or user_id), courses=[*await store.courses(plan_id)]))


@router.get("/{plan_id}/courses")
async def medication_courses(plan_id: str, user_id: str = Depends(verify_token)):
    _, error = await _owner_or_404(user_id, plan_id)
    if error:
        return error
    courses = await PostgresMedicationStore().courses(plan_id)
    return StandardResponse(data={"items": [_json_plan_course(c) for c in courses]})


def _json_plan_course(course: meds.Course) -> dict[str, Any]:
    return {"plan_id": course.plan_id, "start_date": course.start.isoformat(), "end_date": course.end.isoformat() if course.end else None, "closed_by": course.closed_by}


@router.post("")
async def create_medication(body: MedicationInput, user_id: str = Depends(verify_token)):
    plan = _plan_input(body, str(user_id))
    store = PostgresMedicationStore()
    await store.put(plan)
    await store.add_course(str(user_id), meds.Course(plan.plan_id, plan.order_id, plan.start, None))
    return StandardResponse(data=_json_plan(plan, today=await _today(str(user_id))))


@router.patch("/{plan_id}")
async def update_medication(plan_id: str, body: MedicationInput, user_id: str = Depends(verify_token)):
    owner, error = await _owner_or_404(user_id, plan_id)
    if error or owner != str(user_id):
        return error or ErrorResponse(code=403, msg="Only the record owner can edit medications.")
    current = await PostgresMedicationStore().get(plan_id)
    if current is None or current.status == meds.PLAN_ENTERED_IN_ERROR:
        return ErrorResponse(code=404, msg="No such medication plan.")
    plan = _plan_input(body, str(user_id), plan_id=plan_id)
    plan = meds.MedicationPlan(**{**plan.__dict__, "status": current.status, "stopped_on": current.stopped_on, "source": current.source})
    store = PostgresMedicationStore()
    await store.put(plan)
    return StandardResponse(data=_json_plan(plan, today=datetime.now().date()))


async def _transition(plan_id: str, event: str, user_id: str):
    owner, error = await _owner_or_404(user_id, plan_id)
    if error:
        return error
    if owner != str(user_id):
        return ErrorResponse(code=403, msg="Only the record owner can change medications.")
    try:
        plan = await PostgresMedicationStore().transition(str(user_id), plan_id, event, today=await _today(str(user_id)))
    except (LookupError, ValueError) as exc:
        return ErrorResponse(code=400, msg=str(exc))
    return StandardResponse(data=_json_plan(plan, today=await _today(str(user_id))))


@router.post("/{plan_id}/stop")
async def stop_medication(plan_id: str, user_id: str = Depends(verify_token)):
    return await _transition(plan_id, "stop", user_id)


@router.post("/{plan_id}/resume")
async def resume_medication(plan_id: str, user_id: str = Depends(verify_token)):
    return await _transition(plan_id, "resume", user_id)


@router.delete("/{plan_id}")
async def delete_medication(plan_id: str, user_id: str = Depends(verify_token)):
    return await _transition(plan_id, "void", user_id)


__all__ = ["router"]

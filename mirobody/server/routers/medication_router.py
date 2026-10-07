"""Medication forms for the open-source web client.

The kernel owns schedule validation and lifecycle rules. This router only
converts JSON into those kernel objects, applies care-circle access (a read
grant to list, a write grant to change, the same rule the journal applies when
a caregiver logs "Dad stopped X"), and returns the house ``{code, msg, data}``
envelope.
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
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.auth import subject_for, verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.user.user import get_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/medications", tags=["medications"])



def _not_found() -> ErrorResponse:
    return ErrorResponse(code=404, msg="No such medication plan.")


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
    text: str = Field(default="", max_length=500, description="How to take it, in the person's words (饭后)")

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


class MedicationPatch(BaseModel):
    """The fields an edit changes; the rest stay as stored. A full replace
    dropped whatever the form does not show: the codes an import attached,
    the classification, every instruction after the first."""

    name: str | None = Field(default=None, min_length=1, max_length=500)
    form: str | None = Field(default=None, max_length=100)
    strength: str | None = Field(default=None, max_length=200)
    codes: list[MedicationCodeInput] | None = Field(default=None, max_length=20)
    schedule: list[InstructionInput] | None = Field(default=None, min_length=1, max_length=12)
    start_date: date | None = None
    end_date: date | None = None
    classification: str | None = Field(default=None, max_length=100)
    confirmed: bool | None = None


def _dose(value: DoseInput | None) -> meds.Dose | None:
    """The unit as the kernel spells it (片 and tablet are `{tablet}`), so one
    plan edited in two languages does not hold two units for one form; a unit
    the kernel does not know is kept as typed."""
    if value is None:
        return None
    return meds.Dose(value.value, meds.normalize_dose_unit(value.unit) or value.unit.strip())


def _schedule(items: list[InstructionInput]) -> tuple[meds.DoseInstruction, ...]:
    return tuple(
        meds.DoseInstruction(
            dose=_dose(item.dose), times=tuple(item.times),
            doses_per_day=item.doses_per_day, period_days=item.period_days,
            weekdays=frozenset(item.weekdays), as_needed=item.as_needed,
            max_dose_per_day=_dose(item.max_dose_per_day), text=item.text.strip(),
        )
        for item in items
    )


def _changes(body: MedicationPatch, current: meds.MedicationPlan) -> dict[str, Any]:
    """`MedicationPatch` as `dataclasses.replace` arguments over `current`."""
    given = body.model_fields_set
    changes: dict[str, Any] = {}
    if given & {"name", "form", "strength", "codes"}:
        c = current.concept
        changes["concept"] = meds.MedicationConcept(
            text=body.name.strip() if body.name is not None else c.text,
            form=body.form.strip() if body.form is not None else c.form,
            strength=body.strength.strip() if body.strength is not None else c.strength,
            codes=tuple(meds.Coding(x.system, x.code, x.display, x.tty) for x in body.codes)
            if body.codes is not None else c.codes,
        )
    if body.schedule is not None:
        changes["schedule"] = _schedule(body.schedule)
    if body.start_date is not None:
        changes["start"] = body.start_date
    if "end_date" in given:
        changes["end"] = body.end_date
    if body.classification is not None:
        changes["classification"] = body.classification.strip()
    if body.confirmed is not None:
        changes["confirmed"] = body.confirmed
    return changes


def _plan_input(body: MedicationInput, subject_id: str, plan_id: str | None = None) -> meds.MedicationPlan:
    concept = meds.MedicationConcept(
        text=body.name.strip(),
        form=body.form.strip(),
        strength=body.strength.strip(),
        codes=tuple(meds.Coding(c.system, c.code, c.display, c.tty) for c in body.codes),
    )
    schedule = _schedule(body.schedule)
    return meds.MedicationPlan(
        plan_id=plan_id or meds.plan_id_for(subject_id, str(uuid.uuid4()), concept.concept_key),
        concept=concept, schedule=schedule, start=body.start_date, end=body.end_date,
        classification=body.classification.strip(), confirmed=body.confirmed,
        source="web", subject_id=str(subject_id),
    )


async def _subject(caller: str, target: str | None, *, write: bool = False) -> tuple[str, ErrorResponse | None]:
    subject = await subject_for(caller, target, write=write)
    if subject is not None:
        return subject, None
    if write:
        return "", ErrorResponse(code=403, msg="This member has not shared write access to their medications.")
    return "", ErrorResponse(code=403, msg="Not permitted to read this member's medications.")


async def _owner_or_404(caller: str, plan_id: str) -> tuple[str | None, ErrorResponse | None]:
    """The plan's owner when the caller may read it. A plan the caller may not
    read answers exactly as a missing one does, so a guessed id tells nothing."""
    owner = await PostgresMedicationStore().owner(plan_id)
    if owner is None:
        return None, _not_found()
    _, error = await _subject(caller, owner)
    if error:
        return None, _not_found()
    return owner, None


async def _zone(subject_id: str) -> str:
    user = await get_user(user_id=subject_id)
    return str((user or {}).get("tz") or "UTC")


async def _today(subject_id: str) -> date:
    return datetime.now().astimezone(series.zone(await _zone(subject_id))).date()


def _json_plan(plan: meds.MedicationPlan, *, today: date, courses: list[meds.Course] | None = None) -> dict[str, Any]:
    """`effective_status` is computed against `today` on every read; nothing
    derived from the clock is stored."""
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
                "text": i.text,
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
        "courses": [_json_course(c) for c in courses or ()],
    }


def _json_course(course: meds.Course) -> dict[str, Any]:
    return {
        "plan_id": course.plan_id,
        "start_date": course.start.isoformat(),
        "end_date": course.end.isoformat() if course.end else None,
        "closed_by": course.closed_by,
    }


def _failed(action: str, exc: Exception, msg: str) -> ErrorResponse:
    # A type name only: a driver exception quotes the SQL with its bound
    # parameters, and those are a drug name and a dose.
    logger.error("medications %s failed: error_type=%s", action, type(exc).__name__,
                 exc_info=not is_driver_exception(exc))
    return ErrorResponse(code=500, msg=msg)


_STATUSES = {meds.EFFECTIVE_ACTIVE, meds.EFFECTIVE_INTENDED, meds.EFFECTIVE_COMPLETED, meds.EFFECTIVE_STOPPED}


@router.get("")
async def list_medications(
    target_user_id: str | None = Query(None),
    status: str | None = Query(None),
    user_id: str = Depends(verify_token),
):
    if status and status not in _STATUSES:
        return ErrorResponse(code=400, msg=f"status must be one of: {', '.join(sorted(_STATUSES))}.")
    subject, error = await _subject(user_id, target_user_id)
    if error:
        return error
    try:
        plans = await PostgresMedicationStore().list(subject)
        today = await _today(subject)
    except Exception as exc:
        return _failed("list", exc, "Medications could not be loaded.")
    rows = [_json_plan(p, today=today) for p in plans]
    if status:
        rows = [r for r in rows if r["effective_status"] == status]
    return StandardResponse(data={"items": rows, "total": len(rows)})


@router.get("/{plan_id}")
async def get_medication(plan_id: str, user_id: str = Depends(verify_token)):
    try:
        owner, error = await _owner_or_404(user_id, plan_id)
        if error:
            return error
        store = PostgresMedicationStore()
        plan = await store.get(plan_id)
        if plan is None or plan.status == meds.PLAN_ENTERED_IN_ERROR:
            return _not_found()
        courses = list(await store.courses(plan_id))
        today = await _today(owner)
    except Exception as exc:
        return _failed("detail", exc, "This medication could not be loaded.")
    return StandardResponse(data=_json_plan(plan, today=today, courses=courses))


@router.get("/{plan_id}/courses")
async def medication_courses(plan_id: str, user_id: str = Depends(verify_token)):
    try:
        _, error = await _owner_or_404(user_id, plan_id)
        if error:
            return error
        courses = await PostgresMedicationStore().courses(plan_id)
    except Exception as exc:
        return _failed("courses", exc, "This medication's history could not be loaded.")
    return StandardResponse(data={"items": [_json_course(c) for c in courses]})


@router.post("")
async def create_medication(
    body: MedicationInput,
    target_user_id: str | None = Query(None, description="Add to this person's record; needs a write grant"),
    user_id: str = Depends(verify_token),
):
    subject, error = await _subject(user_id, target_user_id, write=True)
    if error:
        return error
    plan = _plan_input(body, subject)
    try:
        await PostgresMedicationStore().create(plan)
        today = await _today(subject)
    except Exception as exc:
        return _failed("create", exc, "This medication could not be saved.")
    return StandardResponse(data=_json_plan(plan, today=today))


async def _writable(plan_id: str, user_id: str) -> tuple[str, ErrorResponse | None]:
    """The plan's owner, when the caller may change it: the owner, or a
    member the owner granted write access. A member who may only read it gets
    403; anyone else gets the same 404 as a plan that does not exist."""
    owner, error = await _owner_or_404(user_id, plan_id)
    if error:
        return "", error
    return await _subject(user_id, owner, write=True)


@router.patch("/{plan_id}")
async def update_medication(plan_id: str, body: MedicationPatch, user_id: str = Depends(verify_token)):
    store = PostgresMedicationStore()
    try:
        owner, error = await _writable(plan_id, user_id)
        if error:
            return error
        current = await store.get(plan_id)
        if current is None or current.status == meds.PLAN_ENTERED_IN_ERROR:
            return _not_found()
        plan = await store.revise(owner, plan_id, _changes(body, current))
        today = await _today(owner)
    except LookupError:
        return _not_found()
    except ValueError as exc:
        return ErrorResponse(code=400, msg=str(exc))
    except Exception as exc:
        return _failed("edit", exc, "This medication could not be saved.")
    return StandardResponse(data=_json_plan(plan, today=today))


async def _transition(plan_id: str, event: str, user_id: str):
    try:
        owner, error = await _writable(plan_id, user_id)
        if error:
            return error
        today = await _today(owner)
        plan = await PostgresMedicationStore().transition(owner, plan_id, event, today=today)
    except LookupError:
        return _not_found()
    except ValueError as exc:
        # The kernel's refusal of an illegal move ("cannot resume a plan that
        # is active"): a fixed sentence about states, never the drug.
        return ErrorResponse(code=400, msg=str(exc))
    except Exception as exc:
        return _failed(event, exc, "That medication change could not be completed.")
    return StandardResponse(data=_json_plan(plan, today=today))


@router.post("/{plan_id}/stop")
async def stop_medication(plan_id: str, user_id: str = Depends(verify_token)):
    return await _transition(plan_id, "stop", user_id)


@router.post("/{plan_id}/resume")
async def resume_medication(plan_id: str, user_id: str = Depends(verify_token)):
    """Opens a new course today; the stopped one stays in the history."""
    return await _transition(plan_id, "resume", user_id)


@router.delete("/{plan_id}")
async def delete_medication(plan_id: str, user_id: str = Depends(verify_token)):
    """Marks the plan entered-in-error. Nothing is erased: the default list
    leaves it out, and its courses remain."""
    return await _transition(plan_id, "void", user_id)


__all__ = ["router"]

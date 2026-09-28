"""Request-to-kernel checks for the medication web adapter."""

from datetime import date

import pytest

from mirobody.server.routers.medication_router import InstructionInput, MedicationInput, _plan_input


def test_medication_form_becomes_kernel_plan_with_a_stable_shape():
    body = MedicationInput(
        name="Metformin", strength="500 mg", form="tablet",
        schedule=[InstructionInput(dose={"value": 500, "unit": "mg"}, times=["08:00", "20:00"])],
        start_date=date(2026, 9, 28),
    )
    plan = _plan_input(body, "user-1")
    assert plan.subject_id == "user-1"
    assert plan.concept.text == "Metformin"
    assert plan.schedule[0].times == ("08:00", "20:00")


def test_medication_form_rejects_conflicting_schedule_shapes():
    with pytest.raises(ValueError):
        InstructionInput(times=["08:00"], doses_per_day=2)

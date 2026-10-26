"""A confirmed analyte still needs the unit that makes its number comparable."""

import importlib
from unittest.mock import AsyncMock

import pytest

from mirobody import standardize


def test_confirmed_glucose_without_a_unit_still_needs_input():
    coding = standardize.code(
        "FPG", name_key="fpg", local_key="fpg|", value_kind="quantity",
        value_text="4.76", value_num=4.76,
        alias=standardize.Alias("user:1", standardize.LOINC_SYSTEM, "14771-0"),
    )
    assert coding.outcome == "needs-input"
    assert coding.reason == "unit:missing"
    assert coding.series_id == "local:fpg|"


@pytest.mark.parametrize("unit,number,code", [
    ("mmol/L", 4.76, "14771-0"), ("mg/dL", 90.0, "1558-6"),
])
def test_confirmed_glucose_with_a_unit_has_a_canonical_value(unit, number, code):
    coding = standardize.code(
        "FPG", name_key="fpg", local_key=f"fpg|{unit}", value_kind="quantity",
        value_text=str(number), value_num=number, unit_text=unit, unit_ucum=unit,
        alias=standardize.Alias("user:1", standardize.LOINC_SYSTEM, "14771-0"),
    )
    assert coding.outcome == "coded"
    assert coding.code == code
    assert coding.value_canonical is not None


@pytest.fixture
def confirmation_route(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("psycopg_pool")
    pytest.importorskip("mandrill")
    router = importlib.import_module("mirobody.server.routers.indicator_router")
    utils = importlib.import_module("mirobody.utils")
    monkeypatch.setattr(utils, "execute_query", AsyncMock(return_value=[{
        "user_id": "1", "name_key": "fpg", "unit_ucum": "", "kind": "measurement",
        "value_kind": "quantity", "value_text": "4.76", "unit_text": "",
    }]))
    monkeypatch.setattr(router, "subject_for", AsyncMock(return_value="1"))
    observations = importlib.import_module("mirobody.collect.observations")
    confirm = AsyncMock(return_value=observations.RecodeReport(changed=2, outcomes={"coded": 2}))
    amend = AsyncMock(return_value=3)
    monkeypatch.setattr(observations, "confirm_alias", confirm)
    monkeypatch.setattr(observations, "amend", amend)
    return router, confirm, amend


async def test_confirmation_refuses_to_guess_a_missing_unit(confirmation_route):
    router, confirm, amend = confirmation_route
    response = await router.confirm_code(router.CodeConfirmation(id=1, code="14771-0"), "1")
    assert response.code == 400
    assert "unit" in response.msg.lower()
    confirm.assert_not_awaited()
    amend.assert_not_awaited()


async def test_confirmation_records_an_explicit_unit_as_an_amendment(confirmation_route):
    router, confirm, amend = confirmation_route
    response = await router.confirm_code(router.CodeConfirmation(id=1, code="14771-0", unit="mmol/L"), "1")
    assert response.code == 0
    assert response.data["id"] == 3
    amend.assert_awaited_once_with("1", 1, unit_text="mmol/L")
    confirm.assert_awaited_once_with("1", "fpg", "", "14771-0", confirmed_by="user:1")


async def test_confirmation_checks_write_access_before_any_amendment(confirmation_route, monkeypatch):
    router, confirm, amend = confirmation_route
    monkeypatch.setattr(router, "subject_for", AsyncMock(return_value=None))
    response = await router.confirm_code(router.CodeConfirmation(id=1, code="14771-0", unit="mmol/L"), "2")
    assert response.code == 404
    confirm.assert_not_awaited()
    amend.assert_not_awaited()


@pytest.mark.parametrize("unit", ["not-a-unit", "kg"])
async def test_confirmation_rejects_an_unknown_or_incompatible_unit(confirmation_route, unit):
    router, confirm, amend = confirmation_route
    response = await router.confirm_code(router.CodeConfirmation(id=1, code="14771-0", unit=unit), "1")
    assert response.code == 400
    confirm.assert_not_awaited()
    amend.assert_not_awaited()

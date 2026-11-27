import pytest

from mirobody import standardize
from mirobody.units import convert_value, normalize_unit, unit_family


@pytest.mark.parametrize(
    "value,source,target,expected",
    [
        (60, "U/L", "ukat/L", 1),
        (1, "ukat/L", "U/L", 60),
        (1, "ukat/L", "nkat/L", 1000),
        (1, "U/L", "mU/mL", 1),
    ],
)
def test_convert_activity(value, source, target, expected):
    assert convert_value(value, source, target) == pytest.approx(expected)


@pytest.mark.parametrize("suffix,family", [("", "CCnt"), ("/L", "CCnc")])
@pytest.mark.parametrize(
    "unit,factor",
    [("kat", 60_000_000), ("mkat", 60_000), ("ukat", 60), ("nkat", 0.06), ("pkat", 0.00006)],
)
def test_katal_forms(unit, factor, suffix, family):
    ucum = f"{unit}{suffix}"
    assert normalize_unit(ucum) == ucum
    assert unit_family(ucum) == family
    assert convert_value(1, ucum, f"U{suffix}") == pytest.approx(factor)


@pytest.mark.parametrize("micro", ["µ", "μ"], ids=["micro-sign", "greek-mu"])
def test_printed_microkatal(micro):
    ucum = normalize_unit(f"{micro}kat/L")
    assert ucum == "ukat/L"
    assert convert_value(1, ucum, "U/L") == pytest.approx(60)
    assert normalize_unit(f"{micro}kat") == "ukat"


def test_catalytic_concentration_cannot_convert_to_mass_concentration():
    assert convert_value(1, "ukat/L", "mg/L") is None


@pytest.mark.parametrize("value,unit", [("1", "µkat/L"), ("60", "U/L")])
def test_alt_canonical_activity(value, unit):
    name = "ALT"
    parsed = standardize.parse_value(value, unit)
    name_key = standardize.name_key(name)
    coding = standardize.code(
        name,
        name_key=name_key,
        local_key=f"{name_key}|{parsed.unit_ucum}",
        value_kind=parsed.value_kind,
        value_text=value,
        value_num=parsed.value_num,
        unit_text=unit,
        unit_ucum=parsed.unit_ucum,
    )
    assert coding.code == "1742-6"
    assert coding.value_canonical == pytest.approx(60.0)
    assert coding.unit_canonical == "U/L"

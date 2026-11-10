"""Vendor mappings preserve quantity, shape and printed glucose units."""

import pytest

from mirobody import standardize
from mirobody.kernel import metrics
from mirobody.standardize import devices


@pytest.mark.parametrize(("vendor", "kind", "field", "metric", "code", "unit"), [
    ("fitbit", "sleep", "minutesAwake", "waso", "103215-0", "ms"),
    ("fitbit", "sleep", "levels.summary.wake.minutes", "waso", "103215-0", "ms"),
    ("huawei", "com.huawei.health.record.sleep", "awake_time", "waso", "103215-0", "ms"),
    ("huawei", "com.huawei.instantaneous.altitude", "", "altitudes", "41905-1", "m"),
    ("fitbit", "activities-steps", "", "dailySteps", "41950-7", "{#}"),
])
def test_mapped_vendor_fields_keep_the_reviewed_quantity(vendor, kind, field, metric, code, unit):
    row = devices.lookup(vendor, kind, field)
    assert row is not None and row.metric == metric and row.loinc == code
    assert metrics.METRICS[metric].canonical == ("loinc", code)
    assert metrics.METRICS[metric].unit_ucum == unit


@pytest.mark.parametrize(("vendor", "kind", "field"), [
    ("apple", "HKQuantityTypeIdentifier", "walkingStepLength"),
    ("samsung", "SleepApneaType(?)", ""),
])
def test_unrepresented_length_and_apnoea_signs_are_not_other_quantities(vendor, kind, field):
    row = devices.lookup(vendor, kind, field)
    assert row is not None and not row.metric and not row.loinc


@pytest.mark.parametrize(("vendor", "kind", "field"), [
    ("health_connect", "BloodGlucoseRecord", ""),
    ("honor", "BLOOD_GLUCOSE", "measureValue"),
    ("huawei", "com.huawei.instantaneous.blood_glucose", ""),
])
@pytest.mark.parametrize(("value", "unit", "code"), [
    ("5.6", "mmol/L", "15074-8"),
    ("101", "mg/dL", "2339-0"),
])
def test_glucose_mapping_still_selects_the_printed_unit(vendor, kind, field, value, unit, code):
    row = devices.lookup(vendor, kind, field)
    assert row is not None and row.metric == "bloodGlucoseMoles"
    metric = metrics.METRICS[row.metric]
    _, catalogue_code = metric.canonical
    parsed = standardize.parse_value(value, unit)
    result = standardize.code(
        row.metric, name_key=standardize.name_key(row.metric), local_key=row.metric,
        value_kind=parsed.value_kind, value_text=value, unit_text=unit,
        unit_ucum=parsed.unit_ucum, value_num=parsed.value_num,
        alias=standardize.Alias("catalog", standardize.LOINC_SYSTEM, catalogue_code),
    )
    assert result.coded and result.code == code
    assert result.unit_canonical == "mol/L"
    expected = 0.0056 if unit == "mmol/L" else 0.005606
    assert result.value_canonical == pytest.approx(expected, rel=1e-4)

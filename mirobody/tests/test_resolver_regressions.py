"""Clinical identity regressions for ranking and CamelCase report names.

Synthetic readings exercise both the resolver and the pure coding seam used by
the observation writer. These tests need only the library and its LFS bundle.
"""

import pytest

from mirobody import standardize
from mirobody.engine import get_resolver, resolve_reading


def _coding(name: str, value: str, unit: str) -> standardize.Coding:
    parsed = standardize.parse_value(value, unit)
    name_key = standardize.name_key(name)
    return standardize.code(
        name,
        name_key=name_key,
        local_key=f"{name_key}|{parsed.unit_ucum}",
        value_kind=parsed.value_kind,
        value_text=value,
        value_num=parsed.value_num,
        unit_text=unit,
        unit_ucum=parsed.unit_ucum,
    )


@pytest.mark.parametrize(
    "name,value,unit,component,prop,system",
    [
        ("weight", "70", "kg", "body weight", "Mass", "^Patient"),
        ("temperature", "36.5", "°C", "body temperature", "Temp", "^Patient"),
        ("温度", "36.5", "°C", "body temperature", "Temp", "^Patient"),
        ("溫度", "36.5", "°C", "body temperature", "Temp", "^Patient"),
        ("platelet", "210", "10*9/L", "platelets", "NCnc", "Bld"),
        ("transthyretin", "25", "mg/dL", "prealbumin", "MCnc", "Ser/Plas"),
        ("globulin", "2.5", "g/dL", "globulin", "MCnc", "Ser"),
        ("urea", "5", "mmol/L", "urea", "SCnc", "Ser/Plas"),
        ("amphetamine", "200", "ng/mL", "amphetamine", "MCnc", "Ser/Plas"),
        ("Weight of Stone", "0.8", "g", "weight", "Mass", "Calculus"),
        ("Temperature Humidifier", "37", "°C", "temperature", "Temp", "Humidifier"),
    ],
)
def test_common_readings_keep_their_clinical_axes(name, value, unit, component, prop, system):
    hit = resolve_reading(name, value, unit)
    assert hit.resolved
    axes = get_resolver().axes_of(hit.loinc)
    assert axes is not None
    assert (axes[0], axes[1], axes[3]) == (component, prop, system)

    coding = _coding(name, value, unit)
    assert coding.outcome == standardize.OUTCOME_CODED
    assert coding.code == hit.loinc
    assert coding.value_canonical is not None
    assert coding.axes is not None
    assert (coding.axes.component, coding.axes.property, coding.axes.system) == (component, prop, system)


@pytest.mark.parametrize(
    "name,value,unit",
    [
        ("BloodGlucose-OGTT", "5", "mmol/L"),
        ("BloodGlucose-FPG", "5", "mmol/L"),
        ("BloodGlucose-HbA1c", "5", "mmol/L"),
        ("TotalCholesterol-HDL", "50", "mg/dL"),
        ("TotalCholesterol-LDL", "100", "mg/dL"),
        ("TotalCholesterol–HDL", "50", "mg/dL"),
        ("TotalProtein-CSF", "30", "mg/dL"),
        ("TotalProtein-csf", "30", "mg/dL"),
        ("BloodGlucose-Cap", "5", "mmol/L"),
    ],
)
def test_camelcase_qualifiers_are_not_dropped(name, value, unit):
    hit = resolve_reading(name, value, unit)
    assert not hit.resolved
    assert not hit.loinc
    coding = _coding(name, value, unit)
    assert coding.outcome == standardize.OUTCOME_NEEDS_INPUT
    assert coding.code is None


def test_specimen_vocabulary_keeps_the_catalogue_axis_spelling():
    resolver = get_resolver()
    assert "CSF" in resolver._systems()
    assert not resolver._trailing_token_is_an_abbreviation("Total Protein", "csf")


@pytest.mark.parametrize(
    "name,value,unit,code",
    [
        ("TotalCholesterol", "193", "mg/dL", "2093-3"),
        ("HemoglobinA1c", "5.6", "%", "4548-4"),
        ("Glycated Hemoglobin-HbA1c", "5.6", "%", "4548-4"),
        ("AlanineAminotransferase-ALT", "25", "U/L", "1742-6"),
        ("TotalCholesterol-TC", "193", "mg/dL", "2093-3"),
        ("Postprandial Blood Glucose-PBG", "5", "mmol/L", "53094-9"),
        ("PostprandialBloodGlucose-PBG", "5", "mmol/L", "53094-9"),
        ("CreatineKinase-MB", "25", "U/L", "32673-6"),
        ("ApolipoproteinB-100", "80", "mg/dL", "1871-3"),
    ],
)
def test_camelcase_and_redundant_abbreviations_still_resolve(name, value, unit, code):
    assert resolve_reading(name, value, unit).loinc == code
    assert _coding(name, value, unit).code == code


@pytest.mark.parametrize(
    "name,parent",
    [
        ("CreatineKinase-BB", "Creatine Kinase"),
        ("CreatineKinase-MM", "Creatine Kinase"),
        ("LactateDehydrogenase-1", "Lactate Dehydrogenase"),
        ("ComplementC-3", "Complement C"),
        ("VitaminD-3", "Vitamin D"),
        ("ApolipoproteinB-100", "Apolipoprotein B"),
    ],
)
def test_camelcase_numbered_and_isoenzyme_suffixes_keep_their_identity(name, parent):
    resolver = get_resolver()
    hit = resolver.resolve(name)
    parent_hit = resolver.resolve(parent)
    assert not hit.loinc or hit.loinc != parent_hit.loinc

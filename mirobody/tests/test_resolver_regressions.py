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


@pytest.mark.parametrize("name", ["BloodGlucose-FPG", "血糖(FPG)", "Glucose FPG", "血糖FPG"])
def test_a_fasting_label_on_glucose_names_fasting_glucose(name):
    """#305 compares a name's two halves by LOINC analyte head, so `Glucose`
    and `Glucose^post CFst` agree. Which half answers then matters: the stem
    alone would drop the fasting state the label carries, so the label is
    the measurement, as `空腹血糖` and bare `FPG` already answer 1558-6. In
    mmol/L the unit switches it to the moles sibling, 14771-0."""
    assert get_resolver().resolve(name).loinc == "1558-6"
    assert resolve_reading(name, "5.2", "mmol/L").loinc == "14771-0"


@pytest.mark.parametrize(
    "name", ["BloodGlucose-OGTT", "血糖(OGTT)", "Glucose OGTT", "血糖OGTT", "葡萄糖OGTT", "Glucose (OGTT)"],
)
def test_a_tolerance_test_label_on_glucose_stays_unresolved(name):
    """`OGTT` answers 1518-0, the 2-hour draw of a 75 g load, by the index's
    commonness prior alone: the name says neither which draw nor which load.
    Answered with the stem instead, a post-load 9.5 mmol/L joined the plain
    glucose series (1.6.1 did that for `血糖(OGTT)`, #305 for every other
    shape). Both halves fit mmol/L, so the reading's unit cannot settle it
    either."""
    hit = get_resolver().resolve(name)
    assert not hit.resolved
    assert hit.ambiguous == ("2339-0", "1518-0")
    assert not resolve_reading(name, "9.5", "mmol/L").resolved


def test_a_label_without_a_challenge_still_only_labels_the_stem():
    """The rule `空腹血糖(GLU)` was written for is unchanged: the stem carries
    the fasting state, the label is plain glucose, and the stem answers."""
    resolver = get_resolver()
    assert resolver.resolve("空腹血糖(GLU)").loinc == "1558-6"
    assert resolver.resolve("血糖(GLU)").loinc == "2339-0"


def test_camelcase_qualifiers_that_disagree_let_the_measurement_settle_it():
    """`BloodGlucose-HbA1c` is the CamelCase/hyphen spelling of `血糖(HbA1c)`
    (#306's R2): glycated hemoglobin is a different ANALYTE than glucose, so
    the bare name still refuses with both codes named, but `resolve_reading`
    arbitrates by the unit exactly as the Chinese bracket form does -- one
    rule, every shape, not a Chinese-only carve-out."""
    resolver = get_resolver()
    bare = resolver.resolve("BloodGlucose-HbA1c")
    assert bare.method == "refused" and bare.ambiguous == ("2339-0", "4548-4")
    as_glucose = resolve_reading("BloodGlucose-HbA1c", "5", "mmol/L")
    assert as_glucose.resolved and as_glucose.loinc == "15074-8"
    as_hba1c = resolve_reading("BloodGlucose-HbA1c", "6.1", "%")
    assert as_hba1c.resolved and as_hba1c.loinc == "4548-4"


def test_specimen_vocabulary_keeps_the_catalogue_axis_spelling():
    resolver = get_resolver()
    assert "CSF" in resolver._systems()
    # `_is_specimen_token` alone would also flag `WBC` (#305's `白细胞WBC`):
    # LOINC uses leukocytes as a SPECIMEN for a few molecular tests, the same
    # three letters as the abbreviation of Leukocytes itself. The veto in
    # `_resolve_seam` only fires when nothing independently resolved the
    # label (`Total Protein` has nothing to agree with `csf` about), which
    # is what this pins: the token check alone, not the full guarded path.
    assert resolver._is_specimen_token("csf")
    assert resolver._is_specimen_token("WBC")


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

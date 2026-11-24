"""Names coded only under their section's heading (#281, item 3; #343).

An ECG page prints `HR`, `RV5` and `SV1`, and the resolver answers none of
them, so each was stored under a `local:` key named only by its abbreviation.
`standardize.panels` answers them when the reading carries an ECG heading in
`panel_text`, and only then: `HR` on a lab page is still not guessed. A urine
dipstick's `PRO`, `LEU`, `KET` … are answered the same way under a urine
heading.
"""

import pytest

from mirobody import standardize
from mirobody.standardize import panels
from mirobody.standardize.code import axes_for, loinc_units
from mirobody.standardize.parse import KIND_QUANTITY


def _code(name: str, value: str, unit: str, panel_text: str = "", alias=None) -> standardize.Coding:
    parsed = standardize.parse_value(value, unit)
    key = standardize.name_key(name)
    return standardize.code(
        name, name_key=key, local_key=f"{key}|{parsed.unit_ucum}", value_kind=parsed.value_kind,
        value_text=value, unit_text=unit, unit_ucum=parsed.unit_ucum, value_num=parsed.value_num,
        alias=alias, panel_text=panel_text,
    )


@pytest.mark.parametrize("heading,panel", [
    ("心电图", panels.PANEL_ECG),
    ("常规心电图", panels.PANEL_ECG),
    ("12导联心电图", panels.PANEL_ECG),
    ("心电图(ECG)", panels.PANEL_ECG),
    ("心電圖", panels.PANEL_ECG),
    ("心電図", panels.PANEL_ECG),
    ("ECG", panels.PANEL_ECG),
    ("12-lead ECG", panels.PANEL_ECG),
    ("ＥＫＧ", panels.PANEL_ECG),
    ("Resting Electrocardiogram", panels.PANEL_ECG),
    ("尿常规", panels.PANEL_URINE),
    ("尿液分析", panels.PANEL_URINE),
    ("尿液干化学", panels.PANEL_URINE),
    ("尿常規", panels.PANEL_URINE),
    ("Urinalysis", panels.PANEL_URINE),
    ("Urine Dipstick", panels.PANEL_URINE),
    # Microscopy counts per field, and a serum test whose name starts with 尿.
    ("尿沉渣", ""),
    ("尿液沉渣镜检", ""),
    ("尿酸", ""),
    ("血常规", ""),
    ("肝功能", ""),
    ("Lipid Panel", ""),
    # Letters inside a word are no ECG.
    ("Secgment", ""),
    ("", ""),
])
def test_a_heading_names_its_panel(heading, panel):
    assert panels.panel_of(heading) == panel


@pytest.mark.parametrize("name,value,unit,loinc", [
    ("HR", "72", "bpm", "8867-4"),
    ("HR", "72", "次/分", "8867-4"),
    ("RV5", "1.85", "mV", "9995-2"),
    ("SV1", "0.92", "mV", "10040-4"),
    ("P-R间期", "160", "ms", "8625-6"),
    ("QRS时限", "90", "ms", "8633-0"),
    ("QTc", "410", "ms", "8636-3"),
    ("电轴", "60", "°", "8632-2"),
])
def test_an_ecg_abbreviation_codes_under_an_ecg_heading(name, value, unit, loinc):
    coding = _code(name, value, unit, panel_text="心电图")
    assert coding.coded and coding.code == loinc
    assert coding.rule == "panel:ecg"
    assert coding.unit_canonical


@pytest.mark.parametrize("panel_text", ["", "血常规", "Comprehensive Metabolic Panel"])
@pytest.mark.parametrize("name,value,unit", [("HR", "72", "bpm"), ("RV5", "1.85", "mV"), ("SV1", "0.92", "mV")])
def test_the_same_abbreviation_elsewhere_is_not_guessed(panel_text, name, value, unit):
    """With no ECG heading the abbreviation goes to the resolver, which does
    not answer it: the reading keeps its local series and waits for a person."""
    coding = _code(name, value, unit, panel_text=panel_text)
    assert not coding.coded
    assert coding.rule == "engine:lexical"


@pytest.mark.parametrize("name,value,loinc", [
    ("PRO", "阴性", "2887-8"),
    ("LEU", "-", "5799-2"),
    ("KET", "+", "33903-6"),
    ("NIT", "阴性", "32710-6"),
    ("URO", "-", "13658-0"),
    ("BIL", "阴性", "1977-8"),
    ("GLU", "-", "2349-9"),
])
def test_a_dipstick_abbreviation_codes_under_a_urine_heading(name, value, loinc):
    """#343: alone, `PRO` answered proline, `LEU` leucine, `URO` free PSA and
    `GLU` a blood glucose."""
    coding = _code(name, value, "", panel_text="尿常规")
    assert coding.coded and coding.code == loinc
    assert coding.rule == "panel:urine"


@pytest.mark.parametrize("panel_text", ["", "血常规", "尿沉渣"])
@pytest.mark.parametrize("name,value", [("PRO", "阴性"), ("KET", "+"), ("URO", "-")])
def test_a_dipstick_abbreviation_elsewhere_is_not_coded_by_the_panel(panel_text, name, value):
    assert _code(name, value, "", panel_text=panel_text).rule != "panel:urine"


@pytest.mark.parametrize("spelled,abbreviated", [("尿蛋白", "PRO"), ("尿胆红素", "BIL"), ("尿糖", "GLU")])
def test_one_urine_result_is_one_series_whichever_spelling_the_panel_printed(spelled, abbreviated):
    a, b = _code(spelled, "阴性", "", "尿常规"), _code(abbreviated, "阴性", "", "尿常规")
    assert a.coded and b.coded
    assert a.series_id == b.series_id


def test_one_heart_rate_is_one_series_whichever_spelling_the_ecg_printed():
    """`心率` resolves on its own, and `HR` under the same heading lands in
    the series it does, not beside it."""
    spelled, abbreviated = _code("心率", "72", "bpm", "心电图"), _code("HR", "72", "bpm", "心电图")
    assert spelled.coded and abbreviated.coded
    assert spelled.series_id == abbreviated.series_id


def test_a_unit_no_sibling_carries_is_a_conflict_not_the_code():
    """`RV5 18 mm` is the paper's millimetres; 9995-2 is an amplitude in mV."""
    coding = _code("RV5", "18", "mm", panel_text="心电图")
    assert (coding.outcome, coding.reason) == (standardize.OUTCOME_NEEDS_INPUT, "unit:conflict")
    assert coding.code is None


def test_a_word_is_not_filed_under_a_number_code():
    """The scale gate holds for the panel's codes as for the resolver's."""
    coding = _code("HR", "正常", "", panel_text="心电图")
    assert not coding.coded
    assert coding.reason.startswith("scale:")


def test_a_persons_alias_outranks_the_panel():
    alias = standardize.Alias("user", standardize.LOINC_SYSTEM, "76282-3")
    coding = _code("HR", "72", "bpm", panel_text="心电图", alias=alias)
    assert coding.code == "76282-3"
    assert coding.rule == "alias:user"


def test_the_panel_decision_is_not_the_resolvers():
    """A decision row stands for one outcome: `HR` under the heading and
    `HR` with none must not share one."""
    assert _code("HR", "72", "bpm", "心电图").decision_id != _code("HR", "72", "bpm").decision_id


def test_every_row_names_a_shipped_code_its_term_reaches():
    """A row whose code the bundle does not hold, or whose term folds to
    something no reading's `name_key` can be, is a row that never answers."""
    rows = panels._terms()
    assert rows
    heading = {panels.PANEL_ECG: "ECG", panels.PANEL_URINE: "尿常规"}
    for (panel, key), loinc in rows.items():
        assert panel in panels._HEADINGS and panel in heading, panel
        axes = axes_for(loinc)
        assert axes is not None, loinc
        if axes[0].scale == "Ord":
            # A presence result, as a dipstick prints it.
            coding = _code(key, "阴性", "", panel_text=heading[panel])
        else:
            unit = (loinc_units(loinc) or ("deg",))[0]
            coding = standardize.code(
                key, name_key=key, local_key=f"{key}|{unit}", value_kind=KIND_QUANTITY, value_text="1",
                unit_text=unit, unit_ucum=unit, value_num=1.0, panel_text=heading[panel],
            )
        assert coding.coded and coding.code == loinc, (key, loinc, coding.reason)

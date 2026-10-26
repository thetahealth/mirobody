"""Aggregate citations retain metric, unit and date meaning.

The numbers a person reads are often computed - "my average was 6,978", "the highest was
11,846" - and a figure nobody can trace back is exactly what the record is supposed to prevent.
These tests pin the three cases that used to leave such a number uncited.
"""
from __future__ import annotations

import pytest

from mirobody.agent.chat.citations_auto import attach_citations, rows_from_result
from mirobody.kernel.query import compact

AGGREGATE = "(constants: rid=r1, indicator=steps, count=30, avg=6978.5, max=11846, unit=count)"


def test_a_stats_aggregate_is_citable() -> None:
    rows = rows_from_result(AGGREGATE)
    text, cited, uncited = attach_citations("Average steps 6978.5.", rows)
    assert "6978.5<cite>[r1]</cite>" in text
    assert (cited, uncited) == (1, [])


def test_two_figures_from_one_row_cite_that_row() -> None:
    rows = rows_from_result(AGGREGATE)
    text, cited, _ = attach_citations("Average steps 6978.5, maximum 11846.", rows)
    assert text.count("<cite>[r1]</cite>") == 2
    assert cited == 2


def test_a_unit_in_another_language_keeps_the_citation() -> None:
    rows = rows_from_result("(constants: rid=r3, indicator=静息心率, avg=57.7, unit=bpm)")
    text, cited, _ = attach_citations("静息心率 57.7 次/分。", rows)
    assert cited == 1
    assert "次/分<cite>[r3]</cite>" in text


def test_a_trend_field_is_citable() -> None:
    rows = rows_from_result("(constants: rid=r7, indicator=Sleep, unit=h, first_third_mean=7.659, last_third_mean=6.963, delta=-0.696)")
    text, cited, _ = attach_citations("Sleep fell from 7.659 to 6.963 between the thirds.", rows)
    assert cited == 2
    assert text.count("<cite>[r7]</cite>") == 2
    assert "7.659<cite>[r7]</cite> to 6.963<cite>[r7]</cite>" in text
    text, cited, _ = attach_citations("Sleep fell from 7.659 h to 6.963 h.", rows)
    assert cited == 2 and text.count("h<cite>[r7]</cite>") == 2


def test_a_number_no_row_supports_stays_uncited() -> None:
    rows = rows_from_result(AGGREGATE)
    text, cited, uncited = attach_citations("Average steps 6978.5, about 12000 on the best day.", rows)
    assert "12000<cite>" not in text
    assert (cited, uncited) == (1, ["12000"])


@pytest.mark.parametrize("answer", [
    "Calcium is 5.5 mmol/L.",
    "Glucose is 5.5 mg/dL.",
    "2026-08-30 Glucose is 5.5 mmol/L.",
])
def test_aggregate_requires_its_own_metric_unit_and_day(answer):
    row = {"rid": "r1", "indicator": "Glucose", "avg": 5.5, "min": 5, "max": 6,
           "unit": "mmol/L", "period": "2026-08-31"}
    text, cited, uncited = attach_citations(answer, rows_from_result(compact([row])))
    assert cited == 0 and text == answer
    assert "5.5" in uncited


@pytest.mark.parametrize("row, answer", [
    ({"rid": "r1", "indicator": "体重", "value": 70, "unit": "kg"}, "体重是 70 磅。"),
    ({"rid": "r1", "indicator": "血糖", "value": 5.5, "unit": "mmol/L"}, "血糖是 5.5 毫克/分升。"),
])
def test_plain_reading_rejects_incompatible_chinese_units(row, answer):
    text, cited, _ = attach_citations(answer, rows_from_result(compact([row])))
    assert cited == 0 and text == answer


def test_weight_aggregate_cannot_cite_an_age():
    row = {"rid": "r1", "indicator": "Body weight", "avg": 70, "min": 69, "max": 71, "unit": "kg"}
    assert attach_citations("I am 70 years old.", rows_from_result(compact([row])))[1] == 0


@pytest.mark.parametrize("unit", ["mystery_units", "mg/dL_unknown", "mmol/L_unknown", "未知单位"])
def test_explicit_unknown_units_do_not_establish_provenance(unit):
    row = {"rid": "r1", "indicator": "Glucose", "value": 5.5, "unit": "mmol/L"}
    assert attach_citations(f"Glucose is 5.5 {unit}.", rows_from_result(compact([row])))[1] == 0


@pytest.mark.parametrize("unit", ["公斤", "千克", "kg"])
def test_known_units_are_cited_in_full(unit):
    row = {"rid": "r1", "indicator": "体重", "value": 70, "unit": "kg"}
    text, cited, _ = attach_citations(f"体重是 70 {unit}。", rows_from_result(compact([row])))
    assert cited == 1 and f"70 {unit}<cite>[r1]</cite>" in text


def test_chinese_step_unit_matches_a_named_generic_count():
    row = {"rid": "r1", "indicator": "步数", "avg": 6978.5, "unit": "count"}
    text, cited, _ = attach_citations("步数平均 6978.5 步。", rows_from_result(compact([row])))
    assert cited == 1 and "步<cite>[r1]</cite>" in text
    assert attach_citations("步数平均 6978.5 磅。", rows_from_result(compact([row])))[1] == 0


def test_metric_named_as_a_physical_unit_cannot_reinterpret_a_count():
    row = {"rid": "r1", "indicator": "公斤", "avg": 70, "unit": "count"}
    assert attach_citations("公斤平均 70 kg。", rows_from_result(compact([row])))[1] == 0


def test_generic_aggregate_fields_share_a_rid_only_after_identity_checks():
    row = {"rid": "r1", "indicator": "Body weight", "avg": 70, "min": 70, "max": 70.01, "unit": "kg"}
    rows = rows_from_result(compact([row]))
    assert attach_citations("Body weight averaged 70 kg.", rows)[1] == 1
    assert attach_citations("Calcium averaged 70 kg.", rows)[1] == 0


@pytest.mark.parametrize("window, expected", [
    ("2026-08-01 to 2026-08-31", 1), ("2026-07-01 to 2026-08-31", 0), ("2026-08-30", 0),
])
def test_aggregate_date_window_must_match_recorded_bounds(window, expected):
    row = {"rid": "r1", "indicator": "Glucose", "avg": 5.5, "unit": "mmol/L",
           "first_date": "2026-08-01", "last_date": "2026-08-31"}
    assert attach_citations(f"{window} Glucose averaged 5.5 mmol/L.", rows_from_result(compact([row])))[1] == expected

def test_a_unit_inside_formatting_wrappers_is_still_a_claim() -> None:
    """"(mg/dL)" and "**mg/dL**" must be read as mg/dL, not as no unit at all.

    Read as no unit, the sentence was accepted and a mmol/L row was cited to it.
    """
    rows = rows_from_result("(constants: rid=r1, indicator=Glucose, value=5.5, unit=mmol/L)")
    for text in ("Glucose is 5.5 (mg/dL).", "Glucose is 5.5 **mg/dL**."):
        _, cited, uncited = attach_citations(text, rows)
        assert cited == 0, f"{text} states a different unit and must not be cited"
        assert "5.5" in uncited

def test_a_tolerance_that_scales_with_the_value_keeps_unrelated_numbers_out() -> None:
    """The old floor of 0.011 absolute made every figure of a small row "match".

    A row with min=0.002, avg=0.003, max=0.004 endorsed the sentence "Analyte is 0.01 g/L".
    """
    rows = rows_from_result("(constants: rid=r1, indicator=Analyte, min=0.002, avg=0.003, max=0.004, unit=g/L)")
    _, cited, uncited = attach_citations("Analyte is 0.01 g/L.", rows)
    assert cited == 0
    assert "0.01" in uncited

def test_a_named_statistic_binds_to_that_field_only() -> None:
    """codex's case: a row reporting min=5, avg=5.5, max=6.

    "Glucose maximum is 5" used to be cited (to the minimum), and "Glucose mean is 6" to the
    maximum, because the field was never part of the match.
    """
    rows = rows_from_result(
        "(constants: rid=r1, indicator=Glucose, min=5, avg=5.5, max=6, unit=mmol/L)")
    assert attach_citations("Glucose maximum is 5 mmol/L.", rows)[1] == 0
    assert attach_citations("Glucose mean is 6 mmol/L.", rows)[1] == 0
    assert attach_citations("Glucose maximum is 6 mmol/L.", rows)[1] == 1
    assert attach_citations("Glucose mean is 5.5 mmol/L.", rows)[1] == 1


SLEEP_TREND = "(constants: rid=r7, indicator=Sleep, unit=h, first_third_mean=8, last_third_mean=6, delta=-2)"


def test_an_inch_claim_cannot_cite_a_centimetre_reading() -> None:
    """`in` used to be dropped as a connective, so the claim looked unitless.

    "Height is 70 in." then received the citation of a record that says 70 cm.
    """
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Height", "value": 70, "unit": "cm"}]))
    text, cited, uncited = attach_citations("Height is 70 in.", rows)
    assert (cited, uncited) == (0, ["70"])
    assert text == "Height is 70 in."


def test_an_inch_reading_is_cited_in_inches() -> None:
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Height", "value": 70, "unit": "in"}]))
    text, cited, _ = attach_citations("Height is 70 in.", rows)
    assert cited == 1 and "70 in<cite>[r1]</cite>" in text


def test_in_as_a_preposition_is_not_a_unit() -> None:
    """The same letters open a phrase here, so the reading is still citable."""
    rows = rows_from_result("(constants: rid=r7, indicator=Sleep, unit=h, "
                            "first_third_mean=7.659, last_third_mean=6.963, delta=-0.696)")
    text, cited, _ = attach_citations("Sleep was 7.659 in the first third.", rows)
    assert cited == 1 and "7.659<cite>[r7]</cite>" in text


def test_a_change_claim_cites_the_change_field() -> None:
    rows = rows_from_result(SLEEP_TREND)
    text, cited, _ = attach_citations("Sleep fell by 2 h.", rows)
    assert cited == 1 and "2 h<cite>[r7]</cite>" in text


def test_a_change_claim_cannot_cite_an_endpoint_mean() -> None:
    """Daily sleep [8, 7, 6] has delta -2; 6 is the last third, not the drop."""
    rows = rows_from_result(SLEEP_TREND)
    text, cited, uncited = attach_citations("Sleep fell by 6 h.", rows)
    assert (cited, uncited) == (0, ["6"])
    assert text == "Sleep fell by 6 h."


def test_a_change_claim_must_agree_with_the_stated_direction() -> None:
    rows = rows_from_result(SLEEP_TREND)
    assert attach_citations("Sleep rose by 2 h.", rows)[1] == 0


def test_a_change_of_form_is_bound_like_a_verb() -> None:
    rows = rows_from_result(SLEEP_TREND)
    text, cited, _ = attach_citations("Sleep fell, a drop of 2 h.", rows)
    assert cited == 1 and "2 h<cite>[r7]</cite>" in text


def test_reversed_endpoints_are_not_cited() -> None:
    rows = rows_from_result(SLEEP_TREND)
    assert attach_citations("Sleep fell from 6 to 8 h.", rows)[1] == 0


def test_endpoints_cite_the_ends_they_state() -> None:
    rows = rows_from_result(SLEEP_TREND)
    text, cited, _ = attach_citations("Sleep fell from 8 to 6 h.", rows)
    assert cited == 2
    assert "8<cite>[r7]</cite>" in text and "6 h<cite>[r7]</cite>" in text


def test_endpoints_must_move_the_way_the_sentence_says() -> None:
    rows = rows_from_result(SLEEP_TREND)
    assert attach_citations("Sleep rose from 6 to 8 h.", rows)[1] == 0


@pytest.mark.parametrize("answer", [
    "Exercise fell by 2 h.",
    "Sleep was measured and Exercise fell by 2 h.",
    "Exercise fell from 8 to 6 h.",
])
def test_change_grammar_still_requires_the_claimed_metric(answer):
    assert attach_citations(answer, rows_from_result(SLEEP_TREND))[1] == 0


@pytest.mark.parametrize("delta, token", [(2, "-2"), (2, "−2"), (-2, "+2")])
def test_explicit_change_sign_cannot_match_the_opposite_delta(delta, token):
    row = {"rid": "r7", "indicator": "Sleep", "unit": "h", "delta": delta}
    assert attach_citations(f"Sleep had a change of {token} h.", rows_from_result(compact([row])))[1] == 0


@pytest.mark.parametrize("delta, token", [(2, "+2"), (-2, "-2"), (-2, "−2")])
def test_explicit_change_sign_keeps_its_correct_source(delta, token):
    row = {"rid": "r7", "indicator": "Sleep", "unit": "h", "delta": delta}
    assert attach_citations(f"Sleep had a change of {token} h.", rows_from_result(compact([row])))[1] == 1


@pytest.mark.parametrize("indicator, value, unit", [
    ("Vitamin D", 30, "ng/mL"), ("Mean corpuscular volume", 90, "fL"),
])
def test_metric_names_are_not_statistic_operators(indicator, value, unit):
    row = {"rid": "r1", "indicator": indicator, "value": value, "unit": unit}
    assert attach_citations(f"{indicator} is {value} {unit}.", rows_from_result(compact([row])))[1] == 1


def test_statistic_word_outside_a_metric_name_still_selects_the_field():
    row = {"rid": "r1", "indicator": "Vitamin D", "avg": 30, "min": 20, "max": 40, "unit": "ng/mL"}
    rows = rows_from_result(compact([row]))
    assert attach_citations("Average Vitamin D is 30 ng/mL.", rows)[1] == 1
    assert attach_citations("Vitamin D mean is 40 ng/mL.", rows)[1] == 0

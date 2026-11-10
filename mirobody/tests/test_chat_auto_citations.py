"""What a citation may and may not claim about an answer's numbers.

The properties under test are the ones a reader relies on: the rid is the row that
carried that value, an ambiguous value is not attributed at all, a machine-readable block
is copied through untouched, and a number the rows do not support is reported rather than
cited.
"""
from __future__ import annotations

import json
import pytest

from mirobody.agent.chat.citations_auto import attach_citations, collect_rows, rows_from_result
from mirobody.kernel.query import compact

TABLE = """(constants: unit=kg)
rid|indicator|name|time|value|unit
r3|Body weight|体重|2026-08-31|69.5|kg
r4|Body weight|体重|2026-08-30|69.7|kg"""

COMPACT = "(constants: rid=r1, indicator=Glucose, value=5.5, unit=mmol/L)"


def test_value_is_cited_after_its_unit() -> None:
    rows = rows_from_result(TABLE)
    text, cited, uncited = attach_citations("最新体重 69.5 kg，上次 69.7 kg。", rows)
    assert "69.5 kg<cite>[r3]</cite>" in text
    assert "69.7 kg<cite>[r4]</cite>" in text
    assert (cited, uncited) == (2, [])


def test_dates_are_not_cited() -> None:
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations("2026-08-31 的体重是 69.5 kg；上次检查是 2026-08-30。", rows)
    assert "2026-08-31<cite>" not in text
    assert cited == 1


def test_an_ambiguous_value_is_not_attributed() -> None:
    rows = rows_from_result(
        "rid|name|time|value|unit\nr1|HbA1c|2026-01-01|5.5|%\nr2|Glucose|2026-01-01|5.5|mmol/L")
    # No indicator is named, so the number cannot be attributed to either row.
    text, cited, uncited = attach_citations("It came back 5.5% this time.", rows)
    assert "<cite>" not in text
    assert (cited, uncited) == (0, ["5.5"])


def test_the_named_indicator_chooses_its_own_row() -> None:
    rows = rows_from_result(
        "rid|name|time|value|unit\nr1|HbA1c|2026-01-01|5.5|%\nr2|Glucose|2026-01-01|5.5|mmol/L")
    text, cited, _ = attach_citations("HbA1c is 5.5%.", rows)
    assert "5.5%<cite>[r1]</cite>" in text
    assert cited == 1


def test_a_named_indicator_disambiguates_the_value() -> None:
    rows = rows_from_result(
        "rid|name|time|value|unit\nr1|HbA1c|2026-01-01|5.5|%\nr2|Glucose|2026-01-01|5.5|mmol/L")
    text, cited, _ = attach_citations("Glucose 5.5 mmol/L today.", rows)
    assert "5.5 mmol/L<cite>[r2]</cite>" in text
    assert cited == 1


def test_a_fenced_chart_block_is_copied_through() -> None:
    chart = '```vis-chart\n{"type":"line","data":[{"time":"2026-08-01","value":69.5}]}\n```'
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations(f"Weight trend:\n{chart}\nLatest 69.5 kg.", rows)
    assert '"value":69.5' in text
    assert "<cite>" in text.split("```")[-1]
    assert cited == 1


def test_an_existing_citation_is_not_duplicated() -> None:
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations("体重 69.5 kg<cite>[r3]</cite>。", rows)
    assert text.count("<cite>") == 1
    assert cited == 0


def test_a_citation_after_a_space_is_not_duplicated() -> None:
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations("| 体重 | 69.5 kg <cite>[r3]</cite> |", rows)
    assert text.count("<cite>") == 1
    assert cited == 0


def test_a_row_the_claim_already_cites_is_not_cited_again() -> None:
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations("体重 69.5 kg，和上周持平 <cite>[r3]</cite>。", rows)
    assert text.count("[r3]") == 1
    assert cited == 0


def test_another_rows_cite_later_in_the_claim_does_not_stop_one() -> None:
    rows = rows_from_result(TABLE)
    text, cited, _ = attach_citations("体重 69.5 kg，上次 69.7 kg<cite>[r4]</cite>。", rows)
    assert "69.5 kg<cite>[r3]</cite>" in text
    assert text.count("[r4]") == 1
    assert cited == 1


def test_a_compact_constant_row_is_a_source() -> None:
    rows = rows_from_result(COMPACT)
    assert len(rows) == 1 and rows[0]["rid"] == "r1"
    text, cited, _ = attach_citations("Glucose 5.5 mmol/L.", rows)
    assert "5.5 mmol/L<cite>[r1]</cite>" in text
    assert cited == 1


def test_a_number_no_row_supports_is_reported() -> None:
    rows = rows_from_result(TABLE)
    text, cited, uncited = attach_citations("体重涨了 3.2 kg，最新 69.5 kg。", rows)
    assert "3.2<cite>" not in text
    assert (cited, uncited) == (1, ["3.2"])


def test_rows_come_from_every_result_of_the_turn() -> None:
    transcript = [
        {"type": "tool_result", "status": "ok", "content": TABLE},
        {"type": "text", "text": "ignored"},
    ]
    assert len(collect_rows(transcript)) == 2


def test_a_table_without_a_value_column_is_not_a_citation_source() -> None:
    assert rows_from_result("rid|indicator|time\nr1|x|2026-01-01") == []


def test_insertion_never_breaks_json() -> None:
    rows = rows_from_result(TABLE)
    payload = json.dumps([{"time": "2026-08-31", "value": 69.5}])
    text, _, _ = attach_citations(f"```vis-chart\n{payload}\n```", rows)
    body = text.split("```vis-chart", 1)[1].rsplit("```", 1)[0]
    assert json.loads(body)


@pytest.mark.parametrize("answer", ["HbA1c is 5.5%.", "Calcium is 5.5 mmol/L.", "Glucose is 5.5 mg/dL."])
def test_a_unique_value_does_not_override_the_claims_identity(answer):
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Glucose", "value": 5.5, "unit": "mmol/L"}]))
    text, cited, uncited = attach_citations(answer, rows)
    assert text == answer and cited == 0
    assert "5.5" in uncited


def test_a_unique_value_cannot_borrow_another_encounter_date():
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Glucose", "value": 5.5,
                                      "unit": "mmol/L", "time": "2026-08-31"}]))
    text, cited, _ = attach_citations("2026-08-30 Glucose is 5.5 mmol/L.", rows)
    assert "<cite>" not in text and cited == 0


def test_compact_multirow_constants_preserve_all_source_rows():
    rendered = compact([{"rid": "r3", "indicator": "Body weight", "value": 69.5, "unit": "kg", "time": "2026-08-31"},
                        {"rid": "r4", "indicator": "Body weight", "value": 69.5, "unit": "kg", "time": "2026-08-30"}])
    rows = rows_from_result(rendered)
    assert {r["rid"] for r in rows} == {"r3", "r4"}
    text, cited, _ = attach_citations("2026-08-31 Body weight is 69.5 kg.", rows)
    assert "[r3]" in text and cited == 1
    assert attach_citations("Body weight is 69.5 kg.", rows)[1] == 0


def test_compact_statistics_supply_their_actual_aggregate_values():
    rows = rows_from_result(compact([{"rid": "r5", "indicator": "Body weight", "count": 7,
                                      "avg": 69.5, "min": 69, "max": 70, "unit": "kg"}]))
    assert {r["_value"] for r in rows} == {69, 69.5, 70}
    assert attach_citations("Body weight averaged 69.5 kg.", rows)[1] == 1


def test_escaped_cells_and_separate_tables_keep_their_source_identity():
    rendered = compact([{"rid": "r1", "name": "a|b", "value": 5.5, "unit": "mmol/L", "note": "one\ntwo"},
                        {"rid": "r2", "name": "other", "value": 6.0, "unit": "mmol/L", "note": "plain"}])
    rendered += "\n\n" + compact([{"rid": "r3", "indicator": "Weight", "value": 70, "unit": "kg"}])
    rows = rows_from_result(rendered)
    assert {r["rid"] for r in rows} == {"r1", "r2", "r3"}
    assert next(r for r in rows if r["rid"] == "r1")["name"] == "a|b"


def test_repeated_reads_of_one_source_do_not_create_ambiguity():
    rows = rows_from_result(COMPACT)
    assert attach_citations("Glucose 5.5 mmol/L.", rows + rows)[1] == 1


@pytest.mark.parametrize("prefix", ["Glucose is 5.5 mmol/L. ", "Glucose was measured, ",
                                   "Glucose was measured and "])
def test_an_earlier_metric_cannot_endorse_a_later_unknown_claim(prefix):
    answer = prefix + "Calcium is 5.5 mmol/L."
    result, _, _ = attach_citations(answer, rows_from_result(COMPACT))
    assert "Calcium is 5.5 mmol/L<cite>" not in result


def test_chinese_dates_use_the_same_encounter_identity_as_iso_dates():
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Glucose", "value": 5.5,
                                      "unit": "mmol/L", "time": "2026-08-31"}]))
    assert attach_citations("2026年8月30日 Glucose is 5.5 mmol/L.", rows)[1] == 0
    assert attach_citations("2026年8月31日 Glucose is 5.5 mmol/L.", rows)[1] == 1


def test_a_specific_metric_name_wins_over_its_shared_suffix():
    rows = rows_from_result(compact([{"rid": "r1", "indicator": "Glucose", "value": 5.5, "unit": "mmol/L"},
                                     {"rid": "r2", "indicator": "Fasting glucose", "value": 5.5, "unit": "mmol/L"}]))
    result, cited, _ = attach_citations("Fasting glucose is 5.5 mmol/L.", rows)
    assert "[r2]" in result and cited == 1


def test_the_models_misspelt_markup_is_mended_before_the_answer_is_kept(monkeypatch) -> None:
    # The turn needs the agent's dependencies; a minimal install has none.
    turn = pytest.importorskip("mirobody.agent.chat.turn")

    monkeypatch.setenv("AUTO_CITE", "off")
    block = {"type": "text", "text": "<statement>均值 74.4 次/分<xcite>[r4]</cite></statement>"}
    assert turn._attach_citations(block, [])["text"] == "<statement>均值 74.4 次/分<cite>[r4]</cite></statement>"
    untouched = {"type": "text", "text": "没有标记"}
    assert turn._attach_citations(untouched, []) is untouched

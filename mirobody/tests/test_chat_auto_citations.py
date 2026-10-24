"""What a citation may and may not claim about an answer's numbers.

The properties under test are the ones a reader relies on: the rid is the row that
carried that value, an ambiguous value is not attributed at all, a machine-readable block
is copied through untouched, and a number the rows do not support is reported rather than
cited.
"""
from __future__ import annotations

import json

from mirobody.agent.chat.citations_auto import attach_citations, collect_rows, rows_from_result

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
    text, cited, _ = attach_citations("2026-08-31 与 2026-08-30 的体重是 69.5 kg。", rows)
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

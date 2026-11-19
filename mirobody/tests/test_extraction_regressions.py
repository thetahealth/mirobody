"""Synthetic regressions for extraction evidence and the answers it feeds.

These exercise the application seam, which needs optional application
dependencies; the library-only test installation skips this module.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("openai")

from mirobody.agent.tools.health_indicators_service import _envelope_for
from mirobody.collect import observations
from mirobody.collect.files.services import indicator_store, table_indicators
from mirobody.collect.files.services.indicator_extractor import IndicatorExtractor
from mirobody.collect.query import _bucket_row, _catalog_row, _stats_row
from mirobody.kernel import query


@pytest.mark.parametrize("text,value,expected", [
    ("Base excess -4.5 mmol/L", "4.5", False),
    ("Base excess +4.5 mmol/L", "-4.5", False),
    ("Base excess -4.50 mmol/L", "-4.5", True),
    ("Base excess +4.50 mmol/L", "4.5", True),
    ("Base excess −4.50 mmol/L", "-4.5", True),
    ("Base excess −4.50 mmol/L", "4.5", False),
    ("Base excess -.5 mmol/L", "-0.50", True),
    ("Base excess -.5 mmol/L", ".5", False),
    ("Concentration .5 mg/L", "0.50", True),
    ("Concentration 4.5e-3 mg/L", "0.0045", True),
    ("Concentration -4.5E-3 mg/L", "0.0045", False),
    ("Reference range 125-350", "350", True),
    ("Reference range 125-350", "-350", False),
    ("Reference range 125--350", "350", True),
    ("Reference range 125--350", "-350", False),
    ("Reference range 125－－350", "350", True),
    ("Reference range -5---1", "-1", True),
    ("Reference range -5---1", "1", False),
    ("Reference range -5--1", "1", True),
    ("Reference range -5--1", "-1", False),
    ("Reference range -5 to -1", "-1", True),
    ("Reference range -5 to -1", "1", False),
    ("Malformed exponent 1e999999999999999999999999999999", "4.5", None),
    ("Malformed exponent 1e-999999999999999999999999999999", "4.5", None),
    ("Base excess -4.5 mmol/L", "1e999999999999999999999999999999", None),
    ("Platelets 1,234,567 /L", "1234567", True),
    ("Platelets 1,234,567 /L", "1,234,567", True),
    ("Platelets １２，３４５ /L", "12345", True),
    # A grouping is one number: the text does not print its pieces.
    ("Platelets 250,000 /uL", "250", False),
    ("Platelets 250,000 /uL", "0", False),
    # A decimal comma is the decimal the parser stores, and in running text
    # or CSV it may also separate two numbers, so both readings are printed.
    ("Calcium 1,25 mmol/L", "1.25", True),
    ("Calcium 1,25 mmol/L", "12.5", False),
    ("2026-08-01,72", "72", True),
    ("Glucose,95,99", "95", True),
    ("Calcium 2,15-2,55 mmol/L", "2.55", True),
    ("Platelets 150,000-400,000 /uL", "400000", True),
    ("Platelets 150,000-400,000 /uL", "400", False),
    ("Platelets 2,50,000 /cumm", "250000", True),
    ("Readings 72,66,65", "66", True),
    ("Base excess －４．５０ mmol/L", "-4.5", True),
    ("Base excess －４．５０ mmol/L", "4.5", False),
    ("ALT 32 U/L", "77.77", False),
    ("Result: 阴性", "阴性", True),
    ("Result: 阴性", "阳性", False),
    ("Result: ++", "+ +", True),
    ("", "4.5", None),
    ("ALT 32 U/L", "", None),
    ("Descriptive findings", "A long finding a model can rephrase", None),
])
def test_document_evidence_preserves_numeric_meaning(text, value, expected):
    assert indicator_store.text_bears_out(text)(value) is expected


@pytest.mark.parametrize("text,method,expected", [
    ("Base excess -4.5 mmol/L", "laboratory", observations.MODALITY_UNVERIFIED),
    ("Base excess 4.50 mmol/L", "laboratory", observations.MODALITY_LAB),
    ("", "laboratory", observations.MODALITY_LAB),
    ("Malformed exponent 1e999999999999999999999999999999", "laboratory", observations.MODALITY_LAB),
    ("Finding -4.5", "Imaging", observations.MODALITY_LAB),
    ("Finding -4.5", "Pathological", observations.MODALITY_LAB),
])
async def test_saved_measurement_carries_evidence_verdict(monkeypatch, text, method, expected):
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-evidence-user",
        [{"original_indicator": "Base excess", "value": "4.5", "unit": "mmol/L",
          "detection_method": method}],
        datetime(2026, 10, 1), "extracted", "synthetic-evidence.txt", text=text,
    )
    ingest.assert_awaited_once()
    _, drafts, provenance = ingest.call_args.args
    saved = observations.prepare(drafts[0], provenance, "synthetic-evidence-user", "UTC",
                                 now=datetime(2026, 10, 14, tzinfo=UTC))
    assert saved["value_num"] == 4.5
    assert saved["modality"] == expected


@pytest.mark.parametrize("text,value,unit,value_num,modality", [
    # A US count: stored as 250 and 12.5 (the comma read as a decimal mark),
    # and marked as matching the report, which read the same comma as
    # thousands.
    ("Platelets 250,000 /uL", "250,000", "/uL", 250000.0, observations.MODALITY_LAB),
    ("WBC 12,500 /uL", "12,500", "/uL", 12500.0, observations.MODALITY_LAB),
    # A model that drops the thousands is caught, not credited with the 250.
    ("Platelets 250,000 /uL", "250", "/uL", 250.0, observations.MODALITY_UNVERIFIED),
    ("WBC 12,500 /uL", "12.5", "/uL", 12.5, observations.MODALITY_UNVERIFIED),
    # A European decimal comma stays a decimal, copied or rewritten.
    ("Кальций 1,25 ммоль/л", "1,25", "ммоль/л", 1.25, observations.MODALITY_LAB),
    ("Kaltsium 1,25 mmol/L", "1.25", "mmol/L", 1.25, observations.MODALITY_LAB),
    ("Kaltsium 1,25 mmol/L", "125", "mmol/L", 125.0, observations.MODALITY_UNVERIFIED),
    # Three digits after one comma: thousands (`units.numerals` says why).
    ("Vitamin B12 1,250 pg/mL", "1,250", "pg/mL", 1250.0, observations.MODALITY_LAB),
    ("Vitamin B12 1,250 pg/mL", "1.25", "pg/mL", 1.25, observations.MODALITY_UNVERIFIED),
])
async def test_a_printed_comma_is_read_one_way_for_the_value_and_its_evidence(
        monkeypatch, text, value, unit, value_num, modality):
    """The stored number and the evidence verdict read a comma by one rule.
    `250,000 /uL` was stored as 250 and found printed: a platelet count 1000
    times too small, marked as matching the report."""
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-comma-user",
        [{"original_indicator": text.split()[0], "value": value, "unit": unit, "detection_method": "laboratory"}],
        datetime(2026, 10, 1), "extracted", "synthetic-comma.txt", text=text,
    )
    _, drafts, provenance = ingest.call_args.args
    saved = observations.prepare(drafts[0], provenance, "synthetic-comma-user", "UTC",
                                 now=datetime(2026, 10, 14, tzinfo=UTC))
    assert saved["value_num"] == value_num
    assert saved["modality"] == modality


@pytest.mark.parametrize("name", ["", "  ", "：", ":", "。", " ， ", "\u200b", "\ufeff", "\u00ad", "\u200b："],
                         ids=repr)
def test_a_name_that_folds_to_nothing_visible_is_no_name(name):
    """`：` passed the empty-text check and was stored under a series key of ""
    (#235); a zero-width space, a byte-order mark or a soft hyphen survives the
    fold but prints as nothing. The gate tests the folded key every series is
    built from for a character that shows."""
    draft = observations.Draft(name_text=name, observed_start=datetime(2026, 3, 1, tzinfo=UTC),
                               value_text="420", unit_text="umol/L")
    provenance = observations.Provenance(modality=observations.MODALITY_LAB, source_kind="file",
                                         source_ref="synthetic-report.pdf")
    with pytest.raises(observations.Rejected) as rejected:
        observations.prepare(draft, provenance, "synthetic-user", "UTC", now=datetime(2026, 10, 14, tzinfo=UTC))
    assert rejected.value.reason == observations.REJECT_NO_NAME


@pytest.mark.parametrize("shape", [_catalog_row, _stats_row, _bucket_row])
@pytest.mark.parametrize("unverified", [True, False, None])
def test_aggregate_rows_preserve_evidence_verdict(shape, unverified):
    shaped = shape({"series_id": "local:alt|U/L", "display": "ALT", "unverified": unverified})
    assert shaped["unverified"] is bool(unverified)


@pytest.mark.parametrize("method,view", [
    ("catalog", "raw"), ("stats", "stats"),
    ("buckets", "minute"), ("buckets", "hour"), ("buckets", "day"),
    ("buckets", "week"), ("buckets", "month"),
    ("readings", "raw"), ("latest", "latest"),
])
@pytest.mark.parametrize("unverified", [True, False])
def test_every_answer_view_warns_about_its_unverified_inputs(method, view, unverified):
    row = {"indicator": "ALT", "series": "local:alt|U/L"}
    if method in ("readings", "latest"):
        row["modality"] = observations.MODALITY_UNVERIFIED if unverified else observations.MODALITY_LAB
    else:
        row["unverified"] = unverified
    request = query.QueryRequest(selection=query.Selection(indicators=("ALT",)), view=view)
    envelope = _envelope_for(method, request, query.Window(start_ms=0, end_ms=0, tz="UTC"), [row])
    warnings = [note for note in envelope.assumptions if "check the file" in note]
    assert bool(warnings) is unverified
    if warnings:
        assert "ALT" in warnings[0]


# The printed names #235 reports from Chinese check-up PDFs; every value is
# synthetic. `ＦＲ` and `既往史：` are the same labels as a full-width OCR or a
# model that kept the colon would print them.
_NOT_READINGS_235 = ["AO%", "D", "DR", "FR", "Frd", "Frq", "Gn", "PAT TIS", "S/A", "ＦＲ",
                     "既往史", "既往史：", "其他"]
_ANALYTES = [("K", "4.1", "mmol/L"), ("Na", "140", "mmol/L"), ("Ca", "2.31", "mmol/L"),
             ("HR", "72", "bpm"), ("RV5", "1.85", "mV"), ("SV1", "0.92", "mV"),
             ("AO", "28", "mm"), ("E/A", "1.3", ""), ("MAP", "93", "mmHg")]
_FINDINGS_235 = ["双肾", "双肾彩超", "双肾结晶", "心影", "两肺纹理", "气管及诸纵隔", "牙周", "义齿"]


def _row(name, value, unit="", method="laboratory"):
    return {"original_indicator": name, "value": value, "unit": unit, "reference_range": "",
            "detection_method": method, "status": "normal", "notes": "", "date_time": ""}


async def test_scanner_settings_and_record_headings_are_not_readings(monkeypatch):
    """Symptoms A and B of #235, through the model's reader: the scanner's
    settings and a record's history heading are dropped, the short analytes
    and the ECG amplitudes stay, and so does every organ finding."""
    rows = [_row(name, str(10 + i), "Hz") for i, name in enumerate(_NOT_READINGS_235)]
    rows += [_row(name, value, unit, "Physiological" if name in ("HR", "RV5", "SV1") else "laboratory")
             for name, value, unit in _ANALYTES]
    rows += [_row(name, f"synthetic finding {i}", method="Imaging") for i, name in enumerate(_FINDINGS_235)]
    answer = {"content_type": "medical_report", "content_info": {"date_time": "2026-03-04 08:00:00"},
              "indicators": rows}
    monkeypatch.setattr(IndicatorExtractor, "_llm_extract", AsyncMock(return_value=answer))

    reading = await IndicatorExtractor.read_indicators("A synthetic check-up page with no table in it.")

    kept = [r["original_indicator"] for r in reading.rows]
    assert kept == [name for name, _, _ in _ANALYTES] + _FINDINGS_235
    assert len(reading.answer["indicators"]) == len(rows)


async def test_the_table_rules_rows_pass_the_same_gate(monkeypatch):
    """The table rules read a scanner's `FR | 25 | Hz` as a laboratory row, so
    the gate is on what both readers found, not on the model's rows alone."""
    text = "\n".join([
        "| 项目 | 结果 | 单位 | 参考范围 |",
        "|---|---|---|---|",
        "| K | 4.1 | mmol/L | 3.5-5.3 |",
        "| Na | 140 | mmol/L | 137-147 |",
        "| FR | 25 | Hz | |",
        "| Frq | 3.5 | MHz | |",
        "| D | 16 | cm | |",
        "| Gn | 50 | | |",
    ])
    model = AsyncMock(return_value=None)
    monkeypatch.setattr(IndicatorExtractor, "_llm_extract", model)

    reading = await IndicatorExtractor.read_indicators(text)

    model.assert_not_awaited()
    assert [r["original_indicator"] for r in reading.rows] == ["K", "Na"]


@pytest.mark.parametrize("name,method,kind", [
    ("心影", "Imaging", observations.KIND_FINDING),
    ("双肾结晶", "Imaging", observations.KIND_FINDING),
    ("牙周", "Examination", observations.KIND_FINDING),
    ("HR", "Physiological", observations.KIND_MEASUREMENT),
    ("K", "laboratory", observations.KIND_MEASUREMENT),
])
async def test_an_imaging_row_is_stored_as_a_finding(monkeypatch, name, method, kind):
    """Symptom B's organ rows are findings by design: the prompt asks for one
    per organ, and an Imaging row is stored with `kind='finding'`. What lists
    them beside lab analytes is the client, which does not read `kind`."""
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-235-user", [_row(name, "synthetic", method=method)],
        datetime(2026, 3, 4), "extracted", "synthetic-235.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    assert drafts[0].kind == kind


@pytest.mark.parametrize("value,unit,kind", [
    ("牙周炎", "", observations.KIND_FINDING),
    ("义齿修复", "", observations.KIND_FINDING),
    ("缺牙（26）", "", observations.KIND_FINDING),
    ("双肾未见异常", "", observations.KIND_FINDING),
    ("170", "cm", observations.KIND_MEASUREMENT),
    ("<0.5", "", observations.KIND_MEASUREMENT),
    ("170cm", "", observations.KIND_MEASUREMENT),
    ("170 cm", "", observations.KIND_MEASUREMENT),
    ("36.5℃", "", observations.KIND_MEASUREMENT),
    ("72次/分", "", observations.KIND_MEASUREMENT),
    ("97%", "", observations.KIND_MEASUREMENT),
    # Two-character comparators: `>` alone lost the head of `>=97%`, and
    # the row was stored as a finding with no number.
    (">=97%", "", observations.KIND_MEASUREMENT),
    ("<=38", "℃", observations.KIND_MEASUREMENT),
    ("≥97 %", "", observations.KIND_MEASUREMENT),
    # A grouping under #289's rule (`units.numerals`): neither is a decimal.
    ("1,250", "", observations.KIND_MEASUREMENT),
    ("250,000", "", observations.KIND_MEASUREMENT),
    ("结石 3 mm", "", observations.KIND_FINDING),
    ("2 cm 结节", "", observations.KIND_FINDING),
    ("120/80", "", observations.KIND_FINDING),
    ("5/5", "", observations.KIND_FINDING),
    # An eye pressure is a pair in a pressure unit, not a blood pressure:
    # the two eyes, not systolic and diastolic.
    ("15/16 mmHg", "", observations.KIND_FINDING),
])
async def test_a_numeric_examination_row_stays_a_measurement(monkeypatch, value, unit, kind):
    """A finding's record carries its text, never a number: `身高 170 cm`
    tagged `Examination` keeps its value and unit as a measurement, whether
    the unit is in its own column, printed inside the value, or spaced from
    it. `120/80` under this generic name stays a finding: `身高 120/80` is
    not a blood pressure (`test_a_blood_pressure_lands_as_its_two_readings`
    covers the row that is)."""
    method = "Examination"
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-281-user", [_row("身高", value, unit, method=method)],
        datetime(2026, 3, 4), "extracted", "synthetic-281.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    assert drafts[0].kind == kind
    assert drafts[0].value_text == value


_S, _D = observations.KIND_MEASUREMENT, observations.KIND_FINDING


@pytest.mark.parametrize("rows,expected", [
    # One row holding the pair, whatever method the model gave it: two
    # readings, named in the row's script, unit as printed or mmHg.
    ([("血压", "120/80", "", "Examination")], [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("血压", "120/80", "", "laboratory")], [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("血压", "120/80", "", "Imaging")], [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("血压", "120/80", "", "Pathological")], [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("血壓", "120/80", "", "Examination")], [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("血圧", "128/82", "mmHg", "Physiological")],
     [("収縮期血圧", "128", "mmHg", _S), ("拡張期血圧", "82", "mmHg", _S)]),
    ([("Blood pressure", "111/65 mmHg", "", "Physiological")],
     [("Systolic blood pressure", "111", "mmHg", _S), ("Diastolic blood pressure", "65", "mmHg", _S)]),
    ([("BP", "15/10", "kPa", "Examination")],
     [("Systolic blood pressure", "15", "kPa", _S), ("Diastolic blood pressure", "10", "kPa", _S)]),
    # Two rows both named for the pair: renamed, or the two shared one
    # series and read back as one line alternating 120, 80.
    ([("血压", "120", "mmHg", "Physiological"), ("血压", "80", "mmHg", "Physiological")],
     [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("BP", "120", "mmHg", "Physiological"), ("BP", "80", "mmHg", "Physiological")],
     [("Systolic blood pressure", "120", "mmHg", _S), ("Diastolic blood pressure", "80", "mmHg", _S)]),
    # Two pairs in a row: each split on its own, not joined into one.
    ([("血压", "120/80", "", "Physiological"), ("血压", "118/76", "", "Physiological")],
     [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S),
      ("收缩压", "118", "mmHg", _S), ("舒张压", "76", "mmHg", _S)]),
    # Not a blood pressure, or not one pair: left as the model gave it.
    ([("收缩压", "120", "mmHg", "Physiological"), ("舒张压", "80", "mmHg", "Physiological")],
     [("收缩压", "120", "mmHg", _S), ("舒张压", "80", "mmHg", _S)]),
    ([("Systolic blood pressure", "120", "mmHg", "Physiological"),
      ("Systolic blood pressure", "118", "mmHg", "Physiological")],
     [("Systolic blood pressure", "120", "mmHg", _S), ("Systolic blood pressure", "118", "mmHg", _S)]),
    ([("K", "4.1", "mmol/L", "laboratory"), ("K", "4.3", "mmol/L", "laboratory")],
     [("K", "4.1", "mmol/L", _S), ("K", "4.3", "mmol/L", _S)]),
    ([("血压", "5/5", "", "Examination")], [("血压", "5/5", "", _D)]),
    ([("血压", "5", "", "Examination"), ("血压", "5", "", "Examination")],
     [("血压", "5", "", _S), ("血压", "5", "", _S)]),
    ([("眼压", "15/16", "mmHg", "Examination")], [("眼压", "15/16", "mmHg", _D)]),
    ([("血压", "120/80 左侧卧位", "", "Examination")], [("血压", "120/80 左侧卧位", "", _D)]),
    ([("身高", "120/80", "", "Examination")], [("身高", "120/80", "", _D)]),
])
async def test_a_blood_pressure_lands_as_its_two_readings(monkeypatch, rows, expected):
    """A blood pressure the model left as one pair, or split into two rows
    both still named for the pair, is stored as the systolic and diastolic
    readings a table row or a journal entry already makes of one. Left as
    one row it had no number in it (`standardize.parse_value` reads `120/80`
    as narrative), so a vital the model tagged `Examination` was a finding
    and one tagged anything else was a measurement nothing could chart."""
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-284-bp-user", [_row(*r) for r in rows],
        datetime(2026, 3, 4), "extracted", "synthetic-284-bp.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    assert [(d.name_text, d.value_text, d.unit_text, d.kind) for d in drafts] == expected


async def test_a_pair_split_by_rows_needs_one_printed_time(monkeypatch):
    """Two `血压` rows a home log prints on different days are two readings
    of that day's pair, not one pair: they are not renamed."""
    rows = [{**_row("血压", "120", "mmHg", "Physiological"), "date_time": "2026-03-01"},
            {**_row("血压", "80", "mmHg", "Physiological"), "date_time": "2026-03-02"}]
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-284-bp-days-user", rows, datetime(2026, 3, 4), "extracted", "synthetic-284-bp-days.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    assert [d.name_text for d in drafts] == ["血压", "血压"]


async def test_a_model_row_and_a_table_row_split_one_pair_alike(monkeypatch):
    """The printed `Blood Pressure | 123/78↑ | mmHg | 90-139/60-89` comes out
    as the same two readings, ranges and flag whether the table rules read
    it or the model did (`table_indicators.split_pressure`, one split)."""
    row = {**_row("Blood Pressure", "123/78↑", "mmHg"), "reference_range": "90-139/60-89"}
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-284-bp-table-user", [row], datetime(2026, 3, 4), "extracted", "synthetic-284-bp-table.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    model = [(d.name_text, d.value_text, d.unit_text, d.ref_text, d.flag_text) for d in drafts]
    # A second row, because a one-row table is not read as a table.
    table, _, _ = table_indicators.table_indicators(
        "<table><tr><th>项目</th><th>结果</th><th>单位</th><th>参考范围</th></tr>"
        "<tr><td>Blood Pressure</td><td>123/78↑</td><td>mmHg</td><td>90-139/60-89</td></tr>"
        "<tr><td>葡萄糖</td><td>5.6</td><td>mmol/L</td><td>3.9-6.1</td></tr></table>")
    table = table[:2]
    assert model == [("Systolic blood pressure", "123", "mmHg", "90-139", "high"),
                     ("Diastolic blood pressure", "78", "mmHg", "60-89", "high")]
    assert [(r["original_indicator"], r["value"], r["unit"], r["reference_range"]) for r in table] == \
        [(n, v, u, ref) for n, v, u, ref, _ in model]


def test_the_extraction_offers_every_finding_method():
    """A method the model cannot choose is a kind it can never store."""
    from mirobody.collect.files.services.prompts import file_indicator_extract as prompt
    schema = prompt.RESPONSE_SCHEMA_EXTRACT_INDICATORS
    offered = repr(schema)
    text = prompt.get_extract_indicators_prompt("en")
    for method in indicator_store.FINDING_METHODS:
        assert f"'{method}'" in offered
        assert f'"{method}"' in text


_ECG_TABLE = ("<table><tr><td colspan=\"4\">心电图</td></tr>"
              "<tr><th>项目</th><th>结果</th><th>单位</th><th>参考范围</th></tr>"
              "<tr><td>HR</td><td>72</td><td>bpm</td><td>60-100</td></tr>"
              "<tr><td>RV5</td><td>1.85</td><td>mV</td><td>&lt;2.5</td></tr></table>")


def test_a_table_row_carries_the_heading_it_was_printed_under():
    """#281 item 3: the section row above a table's header is each reading's
    `panel`, held over the header repeated under it."""
    rows, _, unread = table_indicators.table_indicators(_ECG_TABLE)
    assert unread == 0
    assert [(r["original_indicator"], r["panel"]) for r in rows] == [("HR", "心电图"), ("RV5", "心电图")]


def test_a_heading_holds_until_a_table_opens_a_panel_of_its_own():
    """A later table that opens on its own header, with no heading row, is
    a new panel: its `K` is not an ECG reading. A table that continues the
    page's panel with no header keeps the heading, as it keeps the header."""
    lab = ("<table><tr><th>项目</th><th>结果</th><th>单位</th><th>参考范围</th></tr>"
           "<tr><td>K</td><td>4.1</td><td>mmol/L</td><td>3.5-5.3</td></tr>"
           "<tr><td>Na</td><td>140</td><td>mmol/L</td><td>137-147</td></tr></table>")
    continued = "<table><tr><td>SV1</td><td>0.92</td><td>mV</td><td>&lt;1.5</td></tr></table>"
    rows, _, _ = table_indicators.table_indicators(_ECG_TABLE + continued + lab)
    assert [(r["original_indicator"], r["panel"]) for r in rows] == [
        ("HR", "心电图"), ("RV5", "心电图"), ("SV1", "心电图"), ("K", ""), ("Na", "")]


@pytest.mark.parametrize("cell,heading", [
    ("肝功能", "肝功能"),
    ("Comprehensive Metabolic Panel", "Comprehensive Metabolic Panel"),
    ("备注：空腹采血", ""),
    ("2026-03-04", ""),
    ("72", ""),
    ("结论", ""),
    ("检验项目", ""),
    ("审核者", ""),
])
def test_only_a_heading_is_read_as_a_section(cell, heading):
    assert table_indicators._section(cell) == heading


async def test_the_heading_reaches_the_stored_row(monkeypatch):
    """A model's `panel` and a table row's land in `Draft.panel_text`, and so
    does a blood pressure's, split into its two readings."""
    rows = [{**_row("HR", "72", "bpm", "Physiological"), "panel": "心电图"},
            {**_row("血压", "120/80", "", "Physiological"), "panel": "一般检查"},
            _row("K", "4.1", "mmol/L")]
    ingest = AsyncMock(return_value=observations.Report())
    monkeypatch.setattr(observations, "ingest", ingest)
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    await indicator_store.save_indicators_to_db(
        "synthetic-281-panel-user", rows, datetime(2026, 3, 4), "extracted", "synthetic-281-panel.pdf",
    )
    _, drafts, _ = ingest.call_args.args
    assert [(d.name_text, d.panel_text) for d in drafts] == [
        ("HR", "心电图"), ("收缩压", "一般检查"), ("舒张压", "一般检查"), ("K", "")]


def test_an_ecg_reading_is_coded_through_the_writer():
    """The writer's own path, `prepare` then `coding_for`: `HR` printed under
    `心电图` is the heart rate `心率` resolves to, and the same `HR` with no
    heading keeps its local series."""
    now = datetime(2026, 3, 5, tzinfo=UTC)
    provenance = observations.Provenance(observations.MODALITY_LAB, observations.SOURCE_FILE, "file:synthetic-281")

    def coding(panel_text):
        draft = observations.Draft(name_text="HR", observed_start=datetime(2026, 3, 4), value_text="72",
                                   unit_text="bpm", panel_text=panel_text)
        row = observations.prepare(draft, provenance, "synthetic-281-user", "UTC", now=now)
        assert row["panel_text"] == panel_text
        return observations.coding_for(row, {})

    assert coding("心电图").code == "8867-4"
    assert not coding("").coded


def test_the_extraction_asks_for_each_rows_heading():
    """A field the schema does not require is one a strict provider never
    returns, and one the prompt does not show a small model never fills."""
    from mirobody.collect.files.services.prompts import file_indicator_extract as prompt
    item = prompt.RESPONSE_SCHEMA_EXTRACT_INDICATORS["properties"]["indicators"]["items"]
    assert "panel" in item["properties"] and "panel" in item["required"]
    assert '"panel": "心电图"' in prompt.get_extract_indicators_prompt("en")

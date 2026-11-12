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
from mirobody.collect.files.services import indicator_store
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

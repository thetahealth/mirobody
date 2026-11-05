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


@pytest.mark.parametrize("name", ["", "  ", "：", ":", "。", " ， "])
def test_a_name_that_folds_to_nothing_is_no_name(name):
    """`：` passed the empty-text check and was stored under a series key of ""
    (#235); the gate tests the folded key every series is built from."""
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

"""Pure contract checks for the web record/delta adapters."""

from datetime import UTC, datetime

import pytest

from mirobody.collect.query import _record_row
from mirobody.server.routers.indicator_router import _date, _instant


def test_record_row_keeps_measurement_fields_and_source_period():
    row = _record_row({
        "id": 7, "kind": "measurement", "display": "Body weight",
        "name_text": "weight", "value_text": "70", "unit_text": "kg",
        "observed_start": datetime(2026, 9, 28, 8, tzinfo=UTC),
        "local_date": datetime(2026, 9, 28).date(), "source_kind": "file",
        "file_name": "report.pdf", "file_key": "uploads/a.pdf",
        "period_start": datetime(2026, 9, 27, 7, tzinfo=UTC),
        "elected": False,
    })
    assert row["row_id"] == 7
    assert row["indicator"] == "Body weight"
    assert row["value"] == "70" and row["unit"] == "kg"
    assert row["created_at"].startswith("2026-09-27")
    assert row["file_key"] == "uploads/a.pdf"


def test_record_row_reports_text_for_non_measurement_without_a_fake_value():
    row = _record_row({
        "id": 9, "kind": "symptom", "name_text": "headache", "value_text": "",
        "note": "after lunch", "local_date": datetime(2026, 9, 28).date(),
    })
    assert row["value"] == ""
    assert row["text"] == "after lunch"


def test_since_requires_an_explicit_timezone():
    with pytest.raises(ValueError, match="timezone"):
        _instant("2026-09-28T09:00:00")
    assert _instant("2026-09-28T09:00:00Z").tzinfo is not None
    assert _date("2026-09-28", "start_time").isoformat() == "2026-09-28"

"""Synthetic evidence for Apple hub election, printed flags and VCF citations.

The storage adapters require the ``[app]`` extra; their cases skip on a
minimal install. PostgreSQL coverage executes the same paths with real tables.
"""

import sqlite3
from datetime import datetime
from importlib import import_module
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def source_priority():
    pytest.importorskip("fastapi")
    return import_module("mirobody.standardize.aggregate.aggregators.source_id_priority")


@pytest.mark.parametrize("source,source_id,expected", [
    ("apple_health_export", "Synthetic Apple Watch", 10),
    ("apple_health_export", "synthetic apple watch", 10),
    ("apple_health_export", "Synthetic iPhone", 20),
    ("apple_health_export", "com.apple.health.synthetic", 10),
    ("apple_health_export", "com.whoop.iphone", 60),
    ("apple_health_export", "Unknown synthetic source", 90),
    ("apple_health", "com.apple.health.synthetic", 10),
    ("apple_health", "com.ouraring.oura", 50),
    ("apple_health", "com.apple.mobiletimer", 99),
    ("vital.synthetic", "Synthetic Apple Watch", 0),
])
def test_apple_export_device_names_and_existing_bundle_priorities(source_priority, source, source_id, expected):
    expression = source_priority.build_apple_priority_case("source", "source_id")
    with sqlite3.connect(":memory:") as connection:
        result = connection.execute(
            f"SELECT {expression} FROM (SELECT ? AS source, ? AS source_id)", (source, source_id),
        ).fetchone()[0]
    assert result == expected


@pytest.fixture
def point_buffer(monkeypatch):
    pytest.importorskip("fastapi")
    repository = import_module("mirobody.collect.ingest.repositories.health_data")
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.create_function("now", 0, lambda: "2026-10-01 00:00:00")
    connection.execute("""CREATE TABLE series_data (
        user_id text, indicator text, source text, time text, value text, timezone text,
        task_id text, source_id text, create_time text, update_time text,
        UNIQUE(user_id, indicator, source, time))""")

    async def execute(statement, params):
        cursor = connection.executemany(statement, params)
        return {"record_count": cursor.rowcount}

    monkeypatch.setattr(repository, "execute_query", execute)
    try:
        yield repository, connection
    finally:
        connection.close()


_EXPORT_COLLISIONS = [
    pytest.param(("Apple Watch", "100", None), ("iPhone", "80", None), ("Apple Watch", "100", None), id="watch-phone"),
    pytest.param(("Apple Watch", "100", None), ("iPhone", "100", None), ("Apple Watch", "100", None), id="equal-values-change-owner"),
    pytest.param(("Unknown Alpha", "80", None), ("Unknown Beta", "100", None), ("Unknown Alpha", "80", None), id="stable-name-tie"),
    pytest.param((None, "100", None), ("Unknown Alpha", "80", None), ("Unknown Alpha", "80", None), id="null-name-last"),
    pytest.param(("Apple Watch", "100", "filtered_out_of_range"), ("iPhone", "80", None), ("iPhone", "80", None), id="valid-device-beats-filtered-watch"),
]


@pytest.mark.parametrize("left,right,expected", _EXPORT_COLLISIONS)
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize("batch_size", [1, 10_000], ids=["split-batches", "one-batch"])
async def test_apple_export_same_instant_elects_the_device_before_upsert(
    point_buffer, monkeypatch, left, right, expected, reverse, batch_size,
):
    repository, connection = point_buffer
    monkeypatch.setattr(repository, "_BATCH_SIZE", batch_size)
    readings = [left, right][::-1] if reverse else [left, right]
    records = [{"user_id": "synthetic-subject", "indicator": "steps", "source": "apple_health_export",
                "start_time": "2026-10-01 10:00:00", "value": value, "timezone": "UTC",
                "source_id": name, "task_id": task} for name, value, task in readings]
    assert await repository.HealthDataRepository().save_health_records(records)
    [stored] = connection.execute("SELECT source_id, value, task_id FROM series_data").fetchall()
    assert tuple(stored) == expected


@pytest.mark.parametrize("source", ["apple_health_export", "apple_health", "vital.synthetic"])
async def test_updates_from_the_same_device_still_apply_value_and_repair_task(point_buffer, source):
    repository, connection = point_buffer
    record = {"user_id": "synthetic-subject", "indicator": "steps", "source": source,
              "start_time": "2026-10-01 10:00:00", "value": "100", "timezone": "UTC", "source_id": "Apple Watch"}
    writer = repository.HealthDataRepository()
    assert await writer.save_health_records([record])
    assert await writer.save_health_records([{**record, "value": "120", "task_id": "synthetic-repair"}])
    [stored] = connection.execute("SELECT source_id, value, task_id FROM series_data").fetchall()
    assert tuple(stored) == ("Apple Watch", "120", "synthetic-repair")


@pytest.mark.parametrize("old_task,new_task", [(None, "filtered_out_of_range"), ("filtered_out_of_range", None)])
async def test_the_export_device_can_correct_its_own_range_status(point_buffer, old_task, new_task):
    repository, connection = point_buffer
    record = {"user_id": "synthetic-subject", "indicator": "steps", "source": "apple_health_export",
              "start_time": "2026-10-01 10:00:00", "value": "100", "timezone": "UTC",
              "source_id": "Apple Watch", "task_id": old_task}
    writer = repository.HealthDataRepository()
    assert await writer.save_health_records([record])
    assert await writer.save_health_records([{**record, "task_id": new_task}])
    [stored] = connection.execute("SELECT source_id, value, task_id FROM series_data").fetchall()
    assert tuple(stored) == ("Apple Watch", "100", new_task)


@pytest.mark.parametrize("source", ["apple_health", "vital.synthetic"])
@pytest.mark.parametrize("second_value,expected_name", [("80", "iPhone"), ("100", "Apple Watch")])
async def test_other_sources_keep_the_existing_value_and_task_update_condition(
    point_buffer, source, second_value, expected_name,
):
    repository, connection = point_buffer
    record = {"user_id": "synthetic-subject", "indicator": "steps", "source": source,
              "start_time": "2026-10-01 10:00:00", "value": "100", "timezone": "UTC", "source_id": "Apple Watch"}
    writer = repository.HealthDataRepository()
    assert await writer.save_health_records([record])
    assert await writer.save_health_records([{**record, "source_id": "iPhone", "value": second_value}])
    [stored] = connection.execute("SELECT source_id, value FROM series_data").fetchall()
    assert tuple(stored) == (expected_name, second_value)


@pytest.fixture
def file_store():
    pytest.importorskip("fastapi")
    return import_module("mirobody.collect.files.services.indicator_store")


def test_a_model_cannot_supply_the_rule_parsers_printed_flag(file_store):
    extractor = import_module("mirobody.collect.files.services.indicator_extractor")
    row = extractor._as_printed({"original_indicator": "ALT", "value": "92", "unit": "U/L",
                                "reference_range": "", "status": "high", "_printed_flag": "H"})
    assert "_printed_flag" not in row
    assert file_store.value_and_flag(row) == ("92", "")


@pytest.mark.parametrize("name,value,unit,column_flag,expected_value,expected_flag", [
    ("ALT", "92", "U/L", "H", "92", "high"),
    ("WBC", "2.9", "10^9/L", "L", "2.9", "low"),
    ("ALT", "92↑", "U/L", "", "92", "high"),
    ("WBC", "2.9↓", "10^9/L", "", "2.9", "low"),
    ("ALT", "32", "U/L", "", "32", ""),
])
async def test_printed_flags_survive_rule_parsing_and_the_observation_writer(
    file_store, monkeypatch, name, value, unit, column_flag, expected_value, expected_flag,
):
    tables = import_module("mirobody.collect.files.services.table_indicators")
    text = ("| Test | Result | Unit | Flag |\n| --- | --- | --- | --- |\n"
            f"| {name} | {value} | {unit} | {column_flag} |\n")
    rows, _, unread = tables.table_indicators(text)
    assert len(rows) == 1 and unread == 0
    assert rows[0]["status"] == expected_flag
    ingest = AsyncMock(return_value=file_store.observations.Report())
    timezone = AsyncMock(return_value="UTC")
    monkeypatch.setattr(file_store.observations, "ingest", ingest)
    monkeypatch.setattr(file_store.observations, "user_tz", timezone)

    await file_store.save_indicators_to_db(
        "synthetic-subject", rows, datetime(2026, 10, 1), "extracted", "synthetic-report",
        extractor=tables.EXTRACTOR,
    )

    ingest.assert_awaited_once()
    timezone.assert_awaited_once_with("synthetic-subject")
    [draft] = ingest.await_args.args[1]
    assert (draft.value_text, draft.flag_text) == (expected_value, expected_flag)


@pytest.mark.parametrize("value,unit,status,reference,expected", [
    ("4.76", "mmol/L", "high", "", ("4.76", "")),
    ("1.5 L", "", "low", "", ("1.5 L", "")),
    ("92↑", "U/L", "", "", ("92", "high")),
    ("2.9", "10^9/L", "low", "4.0-10.0", ("2.9", "low")),
])
def test_model_status_requires_evidence_and_litres_remain_a_unit(
    file_store, value, unit, status, reference, expected,
):
    assert file_store.value_and_flag({
        "value": value, "unit": unit, "status": status, "reference_range": reference,
        "detection_method": "laboratory",
    }) == expected


@pytest.fixture
def citation_store():
    pytest.importorskip("fastapi")
    return import_module("mirobody.collect.citations")


@pytest.mark.parametrize("identity", ["rs1000000001", "loc:synthetic-position-hash"])
async def test_genotype_citation_keeps_the_complete_variant_identity(citation_store, monkeypatch, identity):
    execute = AsyncMock(return_value=[{
        "set_id": 7, "rsid": identity, "gene": "", "genotype_raw": "AG", "file_key": "synthetic.vcf",
        "file_name": "synthetic.vcf", "time": "2026-10-01 00:00:00",
    }])
    monkeypatch.setattr(citation_store, "execute_query", execute)
    key = citation_store.genotype_key(7, identity)

    result = await citation_store._genotypes("synthetic-subject", [key])

    assert result[key]["indicator"] == identity
    assert result[key]["value"] == "AG"
    execute.assert_awaited_once()
    assert execute.await_args.args[1] == {"uid": "synthetic-subject", "sets": [7], "rsids": [identity]}
    assert "s.user_id = :uid AND s.status = 'active'" in execute.await_args.args[0]


@pytest.mark.parametrize("key", ["g:", "g:bad:rs1000000001", "g:7:", "o:7:rs1000000001"])
async def test_malformed_genotype_citation_does_not_query(citation_store, monkeypatch, key):
    execute = AsyncMock()
    monkeypatch.setattr(citation_store, "execute_query", execute)
    assert await citation_store._genotypes("synthetic-subject", [key]) == {}
    execute.assert_not_awaited()


def test_a_rendering_cut_by_characters_says_so_before_its_rows():
    from mirobody.agent.tools import _render
    from mirobody.kernel import tools

    rows = [{"indicator": "Heart rate", "name": "HR", "time": f"2026-01-01 00:{i // 60:02d}:{i % 60:02d}",
             "value": str(60 + i % 40), "unit": "/min", "system": "http://loinc.org", "code": "8867-4",
             "file": f"synthetic-{i}.json", "total": 5000} for i in range(3000)]
    text = _render.render_compact(tools.Envelope(status="ok", data=rows, meta=tools.Meta(row_count=len(rows))))
    # The cut used to be said only after the rows, with a meta line that read complete.
    assert text.splitlines()[0].startswith("cut: only the first")
    assert "truncated" in text.splitlines()[-1]


async def test_a_character_cut_does_not_suggest_an_invalid_medication_view():
    import re
    from datetime import UTC, date, timedelta
    from types import SimpleNamespace

    # The tool's configurable clock imports the optional configuration stack.
    pytest.importorskip("dotenv")
    pytest.importorskip("ruamel.yaml")
    pytest.importorskip("cryptography")

    from mirobody.agent.tools.medications_service import MedicationsService
    from mirobody.kernel import meds, tools

    names = [f"Synthetic compounded formula {suffix}: " + "ingredient " * 32 for suffix in ("alpha", "beta")]
    assert max(map(len, names)) <= 500
    plans = [meds.MedicationPlan(
        plan_id=f"synthetic-plan-{i}", concept=meds.MedicationConcept(text=name),
        schedule=(meds.DoseInstruction(),), start=date(2026, 1, 1),
    ) for i, name in enumerate(names)]
    events = [meds.DoseEvent(
        event_id=f"synthetic-event-{i}", plan_id=plans[i % 2].plan_id, status="taken",
        taken_at_ms=int((datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=12 * i)).timestamp() * 1000),
        tz="UTC",
    ) for i in range(meds.MAX_ROWS)]
    service = MedicationsService(
        store=SimpleNamespace(list=lambda _: plans), dose_log=SimpleNamespace(list=lambda _subject, _window: events),
        tz=lambda _: "UTC", now=lambda: datetime(2026, 6, 1, tzinfo=UTC),
    )

    result = await service.query_medications(
        {"user_id": "synthetic-subject"}, view="log", start="2026-01-01", end="2026-04-30",
    )

    warning = result["result"].splitlines()[0]
    assert warning.startswith("cut: only the first")
    for view in re.findall(r"\bview=(\w+)", warning):
        assert not meds.validate_query({"view": view})
    # A display cut must not discard the complete original result used by Python.
    assert "truncated" in result["result"]
    assert (result["status"], result["row_count"], result["truncated"]) == (tools.STATUS_OK, meds.MAX_ROWS, False)


@pytest.fixture
def health_query():
    return import_module("mirobody.collect.query").PostgresHealthQuery()


async def test_a_keyword_reaches_every_series_sharing_a_name(health_query, monkeypatch):
    """A lab's fasting glucose and a CGM's share the printed name. Keeping one
    series per name hid the device series: asked for the CGM value of a day,
    the model answered that there was none (ESL-Bench same-day questions)."""
    lab, cgm = "loinc:glucose^post cfst|Ser/Plas|Pt|Qn|QCnc", "mirobody-device:FastingBloodGlucose-FBG"
    labels = [("FastingBloodGlucose-FBG", lab), ("Fasting glucose [Moles/volume] in Serum or Plasma", lab),
              ("FastingBloodGlucose-FBG", cgm), ("BodyWeight", "mirobody-device:BodyWeight")]
    monkeypatch.setattr(health_query, "_labels", AsyncMock(return_value=labels))
    monkeypatch.setattr(health_query, "_series_for", lambda keyword: set())
    found = await health_query._by_keywords("1", ("fasting glucose",), None)
    assert found == [lab, cgm]
    assert await health_query._by_keywords("1", ("FastingBloodGlucose-FBG",), None) == [lab, cgm]

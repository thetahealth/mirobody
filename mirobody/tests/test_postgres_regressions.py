"""Synthetic write/read regressions executed against PostgreSQL, including CI.

Set MIROBODY_TEST_POSTGRES_DSN to a disposable PostgreSQL database. Every test
creates and drops its own schema; no application account or document is read.
"""

from __future__ import annotations

import argparse
import importlib
import os
import statistics
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from .test_collect_regressions import _EXPORT_COLLISIONS

DSN = os.environ.get("MIROBODY_TEST_POSTGRES_DSN", "")
pytestmark = [pytest.mark.needs_db, pytest.mark.skipif(not DSN, reason="MIROBODY_TEST_POSTGRES_DSN is not set")]


async def test_catalogue_keeps_serum_sodium_dietary_sodium_and_walking_identities(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    at = datetime(2026, 1, 1, tzinfo=UTC)
    report = await observations.ingest("1", [
        observations.Draft(name_text="serumSodium", value_text="140", unit_text="mmol/L", observed_start=at),
        observations.Draft(name_text="sodium", value_text="500", unit_text="mg", observed_start=at),
        observations.Draft(name_text="walkingSpeeds", value_text="1.2", unit_text="m/s", observed_start=at),
    ], observations.Provenance("device-sensed", "device", "synthetic-catalogue"), user_tz="UTC")
    assert report.inserted == 3
    rows = await database("SELECT name_text, code, series_id, unit_ucum FROM v_observation", log_sql=False)
    by_name = {r["name_text"]: r for r in rows}
    assert by_name["serumSodium"]["code"] == "2951-2"
    assert by_name["serumSodium"]["unit_ucum"] == "mmol/L"
    assert by_name["sodium"]["code"] == "sodium"
    assert by_name["sodium"]["unit_ucum"] == "mg"
    assert by_name["walkingSpeeds"]["code"] == "walkingSpeeds"
    assert len({r["series_id"] for r in rows}) == 3
    latest = await PostgresHealthQuery().latest("1", query.Selection(indicators=("2951-2",)), query.Window(0, 0, "UTC"))
    assert len(latest) == 1 and latest[0]["code"] == "2951-2"


async def test_competing_printed_units_do_not_create_a_canonical_glucose_value(database):
    from mirobody.collect import observations

    await observations.ingest("1", [observations.Draft(
        name_text="Glucose", value_text="90", unit_text="mg/dL, mmol/L",
        observed_start=datetime(2026, 1, 1, tzinfo=UTC),
    )], observations.Provenance("lab-report", "file", "th_files:synthetic-units"), user_tz="UTC")
    [row] = await database("SELECT outcome, value_canonical FROM v_observation", log_sql=False)
    assert row["outcome"] == "needs-input"
    assert row["value_canonical"] is None


@pytest.fixture
async def database(monkeypatch):
    import psycopg
    from psycopg import sql
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine
    from mirobody.collect import observations
    from mirobody.task import ProfileRefreshTask
    from mirobody.utils import db

    schema = "regression_" + uuid4().hex
    driver_url = make_url(DSN).set(drivername="postgresql")
    connection = await psycopg.AsyncConnection.connect(driver_url.render_as_string(hide_password=False), autocommit=True)
    engine = None
    try:
        await connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        await connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        # Identity functions keep these synthetic SQL fixtures independent of
        # a deployment's encryption key. Encryption has its own security gate.
        await connection.execute("CREATE FUNCTION encrypt_content(text) RETURNS text LANGUAGE SQL IMMUTABLE AS $$SELECT nullif($1,'')$$")
        await connection.execute("CREATE FUNCTION decrypt_content(text) RETURNS text LANGUAGE SQL IMMUTABLE AS $$SELECT $1$$")
        root = Path(__file__).resolve().parents[1]
        for file in ("10_accounts.sql", "20_files.sql", "30_observations.sql", "31_medications.sql", "32_genomics.sql",
                     "40_devices.sql", "41_device_rules.sql", "62_personal_mcp.sql"):
            await connection.execute((root / "schema" / file).read_text())
        await connection.execute("INSERT INTO health_app_user(is_del,email,tz) VALUES (false,'synthetic@example.invalid','UTC')")
        chat = (root / "schema" / "50_chat.sql").read_text()
        await connection.execute(chat[chat.index("CREATE TABLE IF NOT EXISTS th_chat_citation ("):])
        engine = create_async_engine(make_url(DSN).set(drivername="postgresql+psycopg"),
                                     connect_args={"options": f"-c search_path={schema} -c timezone=UTC"})
        monkeypatch.setattr(db, "_provider", lambda _: engine)
        monkeypatch.setattr(db, "_engines", {})
        monkeypatch.setattr(observations, "_range_rules", None)
        monkeypatch.setattr(ProfileRefreshTask, "enqueue", AsyncMock())
        yield db.execute_query
    finally:
        if engine is not None:
            await engine.dispose()
        await connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await connection.close()


async def _write_glucose():
    from mirobody.collect import observations

    return await observations.ingest("1", [
        observations.Draft(name_text="FPG", value_text="4.76", note_text="synthetic printed timing context",
                           observed_start=datetime(2026, 1, 1, tzinfo=UTC)),
        observations.Draft(name_text="FPG", value_text="6.0", unit_text="mmol/L", observed_start=datetime(2026, 1, 2, tzinfo=UTC)),
    ], observations.Provenance(modality="lab-report", source_kind="file", source_ref="th_files:synthetic-glucose"), user_tz="UTC")


@pytest.mark.parametrize("zone", ["UTC", "UTC+08:00", "UTC-05:30", "Asia/Shanghai", "America/New_York"])
async def test_reading_times_retain_microseconds_across_raw_latest_and_records(database, zone):
    from datetime import timedelta, timezone
    from zoneinfo import ZoneInfo
    from mirobody.collect.observations import Draft, Provenance, ingest
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    if zone.startswith(("UTC+", "UTC-")):
        hours, minutes = map(int, zone[4:].split(":"))
        delta = timedelta(hours=hours, minutes=minutes)
        local_zone = timezone(delta if zone[3] == "+" else -delta)
    else:
        local_zone = ZoneInfo(zone)
    base = datetime(2026, 6, 1, 23, 59, 59, tzinfo=UTC)
    instants = [base + timedelta(microseconds=n) for n in (0, 1, 10, 100, 1000, 10000, 100000, 120000, 123456, 999999, 1000000, 1000001)]
    drafts = [Draft(name_text=f"Synthetic clock {i}", value_text=str(i), tz=zone, observed_start=at)
              for i, at in enumerate(instants)]
    report = await ingest("1", drafts, Provenance("device-sensed", "device", "synthetic-subsecond"), user_tz="UTC")
    assert report.inserted == len(drafts)
    expected = {}
    for row_id, instant in zip(report.ids, instants, strict=True):
        local = instant.astimezone(local_zone).replace(tzinfo=None)
        text = local.isoformat(sep=" ")
        expected[row_id] = text.rstrip("0").rstrip(".") if local.microsecond else text
    hq = PostgresHealthQuery()
    selection = query.Selection(indicators=tuple(d.name_text for d in drafts))
    window = query.Window(0, 0, "UTC")
    for rows in (await hq.readings("1", selection, window, limit=50),
                 await hq.latest("1", selection, window),
                 (await hq.records("1", limit=None))["rows"],
                 (await hq.records("1", created_since=datetime(2026, 1, 1, tzinfo=UTC), limit=None))["rows"]):
        assert {row["row_id"]: row["time"] for row in rows} == expected


async def test_confirmation_without_unit_does_not_silently_join_a_standard_series(database):
    router = importlib.import_module("mirobody.server.routers.indicator_router")
    report = await _write_glucose()
    assert report.inserted == 2
    response = await router.confirm_code(router.CodeConfirmation(id=report.ids[0], code="14771-0"), "1")
    assert response.code == 400
    rows = await database("SELECT outcome, series_id FROM v_observation WHERE id=:id", {"id": report.ids[0]}, log_sql=False)
    assert rows[0]["outcome"] == "needs-input"
    assert rows[0]["series_id"].startswith("local:")
    assert not await database("SELECT id FROM th_coding_alias", log_sql=False)


async def test_confirmation_http_does_not_write_another_persons_record(database):
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    router = importlib.import_module("mirobody.server.routers.indicator_router")
    report = await _write_glucose()
    app = FastAPI()
    app.include_router(router.router)
    app.dependency_overrides[router.verify_token] = lambda: "2"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/health-indicators/confirm-code",
                                     json={"id": report.ids[0], "code": "14771-0", "unit": "mmol/L"})
    assert response.json()["code"] == 404
    assert not await database("SELECT id FROM th_coding_alias", log_sql=False)
    count = await database("SELECT count(*) AS n FROM th_observation", log_sql=False)
    assert count[0]["n"] == 2


async def test_confirmed_unit_preserves_original_and_both_readings_enter_statistics(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    router = importlib.import_module("mirobody.server.routers.indicator_router")
    report = await _write_glucose()
    response = await router.confirm_code(router.CodeConfirmation(id=report.ids[0], code="14771-0", unit="mmol/L"), "1")
    assert response.code == 0
    assert response.data["id"] != report.ids[0]
    stored = await database("SELECT unit_text FROM th_observation WHERE id=:id", {"id": report.ids[0]}, log_sql=False)
    assert stored[0]["unit_text"] == ""
    rows = await database("SELECT outcome, value_canonical, unit_canonical, decrypt_content(note_text) AS note "
                          "FROM v_observation ORDER BY observed_start", log_sql=False)
    assert len(rows) == 2
    assert all(r["outcome"] == "coded" and r["value_canonical"] is not None for r in rows)
    assert float(rows[0]["value_canonical"]) == pytest.approx(0.00476)
    assert rows[0]["note"] == "synthetic printed timing context"
    stats = await PostgresHealthQuery().stats("1", query.Selection(indicators=("14771-0",)), query.Window(0, 0, "UTC"))
    assert stats[0]["count"] == stats[0]["numeric_count"] == 2
    assert stats[0]["avg"] == pytest.approx(5.38)
    later = await observations.ingest("1", [observations.Draft(name_text="FPG", value_text="5.1",
        observed_start=datetime(2026, 1, 3, tzinfo=UTC))],
        observations.Provenance(modality="lab-report", source_kind="file", source_ref="th_files:synthetic-later"), user_tz="UTC")
    rows = await database("SELECT outcome, series_id FROM v_observation WHERE id=:id", {"id": later.ids[0]}, log_sql=False)
    assert rows[0]["outcome"] == "needs-input"
    assert rows[0]["series_id"].startswith("local:")


async def test_a_mixed_unit_mean_keeps_its_precision_and_a_single_basis(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    drafts = [observations.Draft(name_text=name, value_text=value, unit_text=unit,
                                 observed_start=datetime(2026, 1, day, tzinfo=UTC))
              for name, value, unit, day in (("Creatinine", "80", "umol/L", 1), ("Creatinine", "0.9", "mg/dL", 2),
                                             ("Albumin", "40", "g/L", 1), ("Albumin", "600", "umol/L", 2))]
    await observations.ingest("1", drafts, observations.Provenance(modality="lab-report", source_kind="file",
                                                                   source_ref="th_files:synthetic-units"), user_tz="UTC")
    store, window = PostgresHealthQuery(), query.Window(0, 0, "UTC")
    creatinine, albumin = query.Selection(indicators=("2160-0",)), query.Selection(indicators=("1751-7",))
    # 0.9 mg/dL is 79.56 umol/L; rounded to four places in mol/L the mean read 0.0001.
    [stats] = await store.stats("1", creatinine, window)
    [month] = await store.buckets("1", creatinine, window, resolution="month", limit=10)
    for row in (stats, month):
        assert row["unit"] == "mol/L"
        assert row["avg"] == pytest.approx(79.7808e-6, rel=1e-5)
    canonical = [float(r["value_canonical"]) for r in await database(
        "SELECT value_canonical FROM v_observation WHERE series_id=:series",
        {"series": stats["series"]}, log_sql=False)]
    assert stats["median"] == pytest.approx(statistics.median(canonical), rel=1e-12)
    assert stats["stddev"] == pytest.approx(statistics.stdev(canonical), rel=1e-12)
    # No molar mass joins g/L to mol/L, so there is no mean to give.
    [stats] = await store.stats("1", albumin, window)
    [month] = await store.buckets("1", albumin, window, resolution="month", limit=10)
    assert stats["count"] == 2 and stats["mixed_units"]
    assert stats["median"] is None and stats["stddev"] is None
    for row in (stats, month):
        assert (row["min"], row["max"], row["avg"], row["unit"]) == (None, None, None, "")


@pytest.mark.parametrize("values", [("1", "2", "9"), ("1", "3"), ("0",), ("4", "4")])
async def test_median_and_sample_spread_use_every_numeric_reading(database, values):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    drafts = [observations.Draft(name_text="Synthetic spread", value_text=value, unit_text="kg",
                                observed_start=datetime(2026, 1, day, tzinfo=UTC))
              for day, value in enumerate(values, start=1)]
    report = await observations.ingest("1", drafts, observations.Provenance(
        modality="lab-report", source_kind="file", source_ref="th_files:synthetic-spread"), user_tz="UTC")
    assert report.inserted == len(values)
    [stats] = await PostgresHealthQuery().stats(
        "1", query.Selection(indicators=("Synthetic spread",)), query.Window(0, 0, "UTC"))
    numbers = [float(value) for value in values]
    assert stats["count"] == stats["numeric_count"] == len(values)
    assert stats["median"] == pytest.approx(statistics.median(numbers))
    assert stats["unit"] == "kg" and not stats["mixed_units"]
    if len(numbers) == 1:
        assert stats["stddev"] is None
    else:
        assert stats["stddev"] == pytest.approx(statistics.stdev(numbers))


async def test_a_reading_of_exactly_one_enters_the_statistics(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.engine import resolve_reading
    from mirobody.kernel import query

    drafts = [observations.Draft(name_text="Glucose", value_text=value, unit_text="mmol/L",
                                 observed_start=datetime(2026, 1, day, tzinfo=UTC))
              for value, day in (("1", 1), ("2", 2), ("3", 3))]
    await observations.ingest("1", drafts, observations.Provenance(modality="lab-report", source_kind="file",
                                                                   source_ref="th_files:synthetic-unity"), user_tz="UTC")
    selection = query.Selection(indicators=(resolve_reading("Glucose", "2", "mmol/L").loinc,))
    # `1` read as the UCUM unity unit: the count kept it and the mean, 2.5, did not.
    [stats] = await PostgresHealthQuery().stats("1", selection, query.Window(0, 0, "UTC"))
    assert (stats["count"], stats["avg"], stats["min"]) == (3, pytest.approx(2.0), pytest.approx(1.0))


async def test_confirmation_does_not_replace_an_existing_recognized_unit(database):
    from mirobody.collect import observations

    report = await observations.ingest("1", [observations.Draft(name_text="SyntheticUnlistedMarker",
        value_text="4.76", unit_text="mg/dL", observed_start=datetime(2026, 1, 1, tzinfo=UTC))],
        observations.Provenance(modality="lab-report", source_kind="file", source_ref="th_files:synthetic-unlisted"), user_tz="UTC")
    router = importlib.import_module("mirobody.server.routers.indicator_router")
    response = await router.confirm_code(router.CodeConfirmation(id=report.ids[0], code="14771-0", unit="mmol/L"), "1")
    assert response.code == 400
    rows = await database("SELECT id, unit_text FROM v_observation", log_sql=False)
    assert rows == [{"id": report.ids[0], "unit_text": "mg/dL"}]
    assert not await database("SELECT id FROM th_coding_alias", log_sql=False)


async def test_amendment_preserves_notes_unless_a_new_note_is_requested(database):
    from mirobody.collect import observations

    report = await _write_glucose()
    original = report.ids[0]
    kept = await observations.amend("1", original, unit_text="mmol/L")
    rows = await database("SELECT decrypt_content(note_text) AS note FROM v_observation WHERE id=:id",
                          {"id": kept}, log_sql=False)
    assert rows[0]["note"] == "synthetic printed timing context"
    cleared = await observations.amend("1", kept, note="")
    assert cleared != kept
    rows = await database("SELECT decrypt_content(note_text) AS note FROM v_observation WHERE id=:id",
                          {"id": cleared}, log_sql=False)
    assert rows[0]["note"] is None


async def test_a_report_with_a_row_stored_without_a_name_can_still_be_redated(database):
    """A row named a lone `：` was stored before the no-name gate read the
    folded key (#235). Re-dating its report amended it, the amendment raised
    `no-name`, and the report was left half moved with its date unsaved on
    every retry. That row now keeps its time and is counted neither moved nor
    skipped, the count the Data page words as "already had a reading that
    day"; a future date still fails."""
    from mirobody.collect import observations
    from mirobody.kernel import quality

    source = "th_files:synthetic-redate"
    first = datetime(2026, 1, 1, tzinfo=UTC)
    report = await observations.ingest("1", [
        observations.Draft(name_text="FPG", value_text="5.0", unit_text="mmol/L", observed_start=first),
        observations.Draft(name_text="UA", value_text="420", unit_text="umol/L", observed_start=first),
        observations.Draft(name_text="TC", value_text="4.5", unit_text="mmol/L", observed_start=first),
    ], observations.Provenance(modality="lab-report", source_kind="file", source_ref=source), user_tz="UTC")
    nameless = report.ids[1]
    # What the write path stored for `：` before the gate: the text, and a key of "".
    await database("UPDATE th_observation SET name_text='：', name_key='', local_key='|umol/L' WHERE id=:id",
                   {"id": nameless}, log_sql=False)

    with pytest.raises(observations.Rejected) as refused:
        await observations.redate("1", source, datetime(2100, 1, 1, tzinfo=UTC), user_tz="UTC")
    assert refused.value.reason == quality.ERR_IMPOSSIBLE_TIME_RANGE

    assert await observations.redate("1", source, datetime(2026, 2, 1, tzinfo=UTC), user_tz="UTC") == (2, 0)
    rows = await database("SELECT name_text, observed_start FROM v_observation WHERE source_ref=:source",
                          {"source": source}, log_sql=False)
    dates = {r["name_text"]: r["observed_start"].date().isoformat() for r in rows}
    assert dates == {"FPG": "2026-02-01", "TC": "2026-02-01", "：": "2026-01-01"}


@pytest.mark.parametrize("source_order", [("Apple Watch", "iPhone"), ("iPhone", "Apple Watch")])
@pytest.mark.parametrize("same_instant", [False, True], ids=["overlapping", "same-instant"])
async def test_apple_import_selects_one_device_before_cumulative_aggregation(database, monkeypatch, tmp_path, source_order, same_instant):
    from mirobody import cli
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.utils.config import Config

    records = []
    for second, source in enumerate(source_order):
        second = 0 if same_instant else second
        number = 100 if source == "Apple Watch" else 80
        records.append(f'<Record type="HKQuantityTypeIdentifierStepCount" sourceName="{source}" unit="count" '
                       f'startDate="2026-01-01 10:00:0{second} +0000" endDate="2026-01-01 10:10:0{second} +0000" value="{number}"/>')
    export = tmp_path / "synthetic-export.xml"
    export.write_text("<HealthData>" + "".join(records) + "</HealthData>")
    monkeypatch.setattr(Config, "init", AsyncMock())
    args = argparse.Namespace(file=str(export), into="1", config=[], tz="UTC")
    first = await cli._store_export(args)
    assert first["records"] == first["facts"] == 2
    assert first["added"] > 0
    from mirobody.standardize.aggregate.aggregators.sql_aggregator import SQLAggregator
    sums = await SQLAggregator()._execute_single_sql_aggregation(["1"], ["steps"], datetime(2026, 1, 1), "UTC", {"sum"})
    assert float(sums[0]["sum_value"]) == 100
    if same_instant:
        points = await database("SELECT source_id FROM series_data", log_sql=False)
        assert points == [{"source_id": "Apple Watch"}]
    second = await cli._store_export(args)
    assert second["added"] == 0
    records = await PostgresHealthQuery().catalog("1", None)
    assert any(float(r["latest_value"]) == 100 for r in records if "step" in r["indicator"].lower())


# Three local days of HRV (SDNN) in Shanghai's offset. Read in UTC, the import's
# default, each morning before 08:00 fell on the day before.
_SHANGHAI_HRV = [("2026-11-01 07:10:00", 40), ("2026-11-01 12:00:00", 52), ("2026-11-01 22:30:00", 61),
                 ("2026-11-02 06:00:00", 35), ("2026-11-02 07:30:00", 48), ("2026-11-03 07:10:00", 70)]
_SHANGHAI_DAYS = {"2026-11-01": (51, 40, 61), "2026-11-02": (41.5, 35, 48), "2026-11-03": (70, 70, 70)}


def _hrv_export(tmp_path):
    records = "".join(
        f'<Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" sourceName="Apple Watch" unit="ms" '
        f'startDate="{at} +0800" endDate="{at} +0800" value="{value}"/>' for at, value in _SHANGHAI_HRV)
    export = tmp_path / "synthetic-export.xml"
    export.write_text(f"<HealthData>{records}</HealthData>")
    return export


async def _hrv_days(database, source="apple_health_export"):
    """`{local date: (avg, min, max)}` of the visible daily HRV rows of `source`,
    and every visible row, so a day held twice shows."""
    rows = await database(
        "SELECT name_text, local_date, value_num, tz FROM v_observation WHERE source_ref = :ref "
        "AND name_text LIKE 'daily%HrvSDNN%' ORDER BY local_date, name_text", {"ref": f"aggregate:{source}"},
        log_sql=False)
    days: dict[str, dict[str, float]] = {}
    for r in rows:
        stat = next(s for s in ("Avg", "Min", "Max") if s in r["name_text"])
        days.setdefault(r["local_date"].isoformat(), {})[stat] = float(r["value_num"])
    return {d: (v["Avg"], v["Min"], v["Max"]) for d, v in days.items()}, rows


def _import_args(export, *extra):
    """The arguments `mirobody import apple <export> --into 1` parses to,
    with the parser's own defaults."""
    from mirobody import cli

    parsed = []
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(cli, "_cmd_import", parsed.append)
        cli.main(["import", "apple", str(export), "--into", "1", *extra])
    return parsed[0]


async def _import_as_before(export):
    """What `mirobody import apple --into` did before a record's offset decided
    its day: every reading in `--tz`, here its `UTC` default, and the export's
    UTC dates recalculated."""
    from mirobody.collect import StandardPulseData, StandardPulseMetaInfo, records_from_facts
    from mirobody.collect.ingest import StandardHealthService
    from mirobody.kernel import decoders
    from mirobody.kernel.decoders import apple_export
    from mirobody.standardize import AggregateIndicatorService

    records, span = [], []
    for kind, item in apple_export.iter_items(export):
        found = decoders.decode("apple", kind, item, "UTC")
        span.extend(f.effective_start_ms for f in found)
        records.extend(records_from_facts(found, slug="apple_health", tz="UTC", source_id=item["sourceName"],
                                          source="apple_health_export"))
    data = StandardPulseData(metaInfo=StandardPulseMetaInfo(userId="1", source="apple_health_export", timezone="UTC"),
                             healthData=records)
    assert await StandardHealthService().process_standard_data(data, "1")
    first, last = (datetime.fromtimestamp(ms / 1000, UTC).replace(tzinfo=None) for ms in (min(span), max(span)))
    assert (await AggregateIndicatorService().recalculate_date_range(first, last, user_id="1"))["status"] == "success"


async def test_apple_import_reads_each_day_in_the_records_own_offset(database, monkeypatch, tmp_path):
    from mirobody import cli
    from mirobody.utils.config import Config

    monkeypatch.setattr(Config, "init", AsyncMock())
    # The person has no zone of their own (the fixture's is UTC) and passes none.
    await cli._store_export(_import_args(_hrv_export(tmp_path)))
    days, rows = await _hrv_days(database)
    assert days == _SHANGHAI_DAYS
    assert {r["tz"] for r in rows} == {"UTC+08:00"}


async def test_apple_reimport_moves_readings_and_retracts_the_days_they_left(database, monkeypatch, tmp_path):
    from mirobody import cli
    from mirobody.utils.config import Config

    export = _hrv_export(tmp_path)
    await _import_as_before(export)
    days, _rows = await _hrv_days(database)
    assert sorted(days) == ["2026-10-31", "2026-11-01", "2026-11-02"]

    monkeypatch.setattr(Config, "init", AsyncMock())
    result = await cli._store_export(_import_args(export))
    points = await database("SELECT timezone, count(*) AS n FROM series_data GROUP BY timezone", log_sql=False)
    assert points == [{"timezone": "UTC+08:00", "n": len(_SHANGHAI_HRV)}]
    # One figure per statistic per day, on the readings' own days: the UTC days'
    # figures, 31 October's included, are retracted rather than left beside them.
    days, rows = await _hrv_days(database)
    assert days == _SHANGHAI_DAYS
    assert len(rows) == 3 * len(_SHANGHAI_DAYS)
    assert result["moved"] == 3
    retracted = await database("SELECT count(*) AS n FROM th_observation WHERE status = 'entered-in-error' "
                               "AND note_text = 'recalculated:apple_health_export'", log_sql=False)
    assert retracted[0]["n"] == 9
    # Importing again changes nothing.
    before = await database("SELECT count(*) AS n FROM th_observation", log_sql=False)
    await cli._store_export(_import_args(export))
    assert await database("SELECT count(*) AS n FROM th_observation", log_sql=False) == before


@pytest.mark.parametrize("method", ["_aggregate_person_day", "_execute_single_sql_aggregation"])
@pytest.mark.parametrize("failed_call", [1, 2], ids=["first-day", "after-partial-results"])
async def test_apple_reimport_preserves_daily_figures_when_calculation_fails(
    database, monkeypatch, tmp_path, caplog, method, failed_call,
):
    import psycopg
    from mirobody import cli
    from mirobody.standardize.aggregate.aggregators.sql_aggregator import SQLAggregator
    from mirobody.utils.config import Config

    export = _hrv_export(tmp_path)
    await _import_as_before(export)
    select = "SELECT id, value_text, tz FROM v_observation WHERE source_ref = 'aggregate:apple_health_export' ORDER BY id"
    before = await database(select, log_sql=False)
    assert len(before) == 9
    original = getattr(SQLAggregator, method)
    calls = 0
    message = "synthetic driver message containing bound health data"

    async def fail_one_day(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == failed_call:
            raise psycopg.OperationalError(message)
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(Config, "init", AsyncMock())
    with monkeypatch.context() as patch:
        patch.setattr(SQLAggregator, method, fail_one_day)
        with pytest.raises(SystemExit, match="Failed to calculate summary data") as error:
            await cli._store_export(_import_args(export))
    assert calls == failed_call
    assert message not in str(error.value) and message not in caplog.text
    assert await database(select, log_sql=False) == before
    # The raw points were stored before recalculation failed; retrying must
    # finish the zone repair rather than leave either day's summaries missing.
    result = await cli._store_export(_import_args(export))
    days, rows = await _hrv_days(database)
    assert days == _SHANGHAI_DAYS and len(rows) == 9
    assert result["moved"] == 3


@pytest.mark.parametrize("days_count", [29, 35, 65])
async def test_apple_import_and_reimport_cover_every_day_across_chunks(database, monkeypatch, tmp_path, days_count):
    from datetime import timedelta
    from mirobody import cli
    from mirobody.utils.config import Config

    first = datetime(2026, 1, 1)
    records = []
    for day in range(days_count):
        # Keeping the first sample's noon clock across 30-day chunks skipped
        # each next chunk's 01:00 sample, even with the sweep's padded range.
        at = (first + timedelta(days=day, hours=12 if day == 0 else 1)).strftime("%Y-%m-%d %H:%M:%S")
        records.append('<Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" sourceName="Apple Watch" '
                       f'unit="ms" startDate="{at} +0000" endDate="{at} +0000" value="50"/>')
    export = tmp_path / "synthetic-export.xml"
    export.write_text("<HealthData>" + "".join(records) + "</HealthData>")
    monkeypatch.setattr(Config, "init", AsyncMock())
    await cli._store_export(_import_args(export))
    days, rows = await _hrv_days(database)
    assert days == {(first + timedelta(days=day)).date().isoformat(): (50, 50, 50) for day in range(days_count)}
    assert len(rows) == 3 * days_count
    before = await database("SELECT count(*) AS n FROM th_observation", log_sql=False)
    result = await cli._store_export(_import_args(export))
    assert result["added"] == result["moved"] == 0
    assert await _hrv_days(database) == (days, rows)
    assert await database("SELECT count(*) AS n FROM th_observation", log_sql=False) == before


async def test_a_phone_zone_and_an_export_offset_for_one_day_write_it_once(database, monkeypatch, tmp_path):
    """A phone pushes `Asia/Shanghai`, the export reads `UTC+08:00`: one day.
    Computed once per spelling, each pass wrote each day's figure under the
    other zone, an amendment every time."""
    from mirobody import cli
    from mirobody.collect.ingest.repositories.health_data import HealthDataRepository
    from mirobody.standardize import AggregateIndicatorService
    from mirobody.utils.config import Config

    pushed = [{"user_id": "1", "indicator": "hrvSDNN", "source": "apple_health", "source_id": "Apple Watch",
               "start_time": datetime.strptime(at + " +0800", "%Y-%m-%d %H:%M:%S %z").astimezone(UTC).replace(tzinfo=None),
               "value": str(value), "timezone": "Asia/Shanghai"} for at, value in _SHANGHAI_HRV]
    assert await HealthDataRepository().save_health_records(pushed)
    monkeypatch.setattr(Config, "init", AsyncMock())
    await cli._store_export(_import_args(_hrv_export(tmp_path)))
    for source in ("apple_health", "apple_health_export"):
        days, rows = await _hrv_days(database, source)
        assert days == _SHANGHAI_DAYS
        assert len(rows) == 3 * len(_SHANGHAI_DAYS)
        assert {r["tz"] for r in rows} == {"Asia/Shanghai"}
    before = await database("SELECT count(*) AS n FROM th_observation", log_sql=False)
    await AggregateIndicatorService().recalculate_date_range(datetime(2026, 10, 31), datetime(2026, 11, 3), user_id="1")
    assert await database("SELECT count(*) AS n FROM th_observation", log_sql=False) == before


async def test_vcf_loc_and_rsid_citations_resolve_with_ownership_and_upload_lifecycle(database, tmp_path):
    from mirobody.agent.tool_loader import _envelope_wrapper
    from mirobody.agent.tools.genetic_service import GeneticService
    from mirobody.collect import citations
    from mirobody.collect.files.services import genotype_format
    from mirobody.collect.files.services.genetic_processor import GeneticDataLoader
    from mirobody.standardize.genotype import normalize

    vcf = '##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSYNTHETIC\n1\t123456\t.\tA\tG\t.\tPASS\t.\tGT\t0/1\n'
    file = tmp_path / "synthetic.vcf"
    file.write_text(vcf)
    record = next(GeneticDataLoader().parse_genetic_file(str(file), genotype_format.sniff(vcf)))
    call = normalize(rsid=record["rsid"], chrom=record["chromosome"], position=record["position"],
                     genotype=record["genotype"], strand=record["strand"], vcf_gt=record["gt"],
                     vcf_ref=record["ref"], vcf_alt=record["alt"])
    assert call.rsid.startswith("loc:")
    await database("INSERT INTO th_files(user_id,file_name,file_key,created_source) VALUES ('1','synthetic.vcf','synthetic.vcf','test')", log_sql=False)
    sets = await database("""INSERT INTO th_genotype_set(user_id,file_key,format_id,vendor,status,n_rows,n_called,
        normalizer_version,site_table_version,activated_at) VALUES ('1','synthetic.vcf','vcf','synthetic','active',2,2,'test','test',now()) RETURNING id""", log_sql=False)
    set_id = sets[0]["id"]
    for identity, position in ((call.rsid, 123456), ("rs1000000001", 123457)):
        await database("""INSERT INTO th_genotype(set_id,rsid,rsid_raw,chrom,position_raw,ref,alt,genotype_raw,gt,
            call_status,zygosity,strand_check) VALUES (:set,:id,'.',:chrom,:pos,:ref,:alt,:raw,:gt,:status,:zygosity,:strand)""",
            {"set":set_id,"id":identity,"chrom":call.chrom,"pos":position,"ref":call.ref,"alt":call.alt,
             "raw":record["genotype_raw"],"gt":call.gt,"status":call.call_status,"zygosity":call.zygosity,"strand":call.strand_check}, log_sql=False)
    service = GeneticService()
    rendered, envelope = await _envelope_wrapper(service.query_genetic_data, {"user_id":"1","session_id":"synthetic-thread"})(
        chromosome="1", start=123456, end=123457, build="raw")
    rids = [row["rid"] for row in envelope.data]
    assert len(rids) == 2 and all(rid in rendered for rid in rids)
    resolved = await citations.resolve_rids("synthetic-thread", "1", rids)
    assert all(r["status"] == "ok" and r["kind"] == "genotype" and r["file"] == "synthetic.vcf" for r in resolved)
    assert all(r["status"] == "unknown" for r in await citations.resolve_rids("synthetic-thread", "2", rids))
    await database("UPDATE th_genotype_set SET status='superseded' WHERE id=:id", {"id":set_id}, log_sql=False)
    assert all(r["status"] == "gone" for r in await citations.resolve_rids("synthetic-thread", "1", rids))


async def test_health_data_e2e_script_runs_over_synthetic_rows(database, monkeypatch, caplog):
    from mirobody.collect import observations
    from mirobody.server.demo import PHI_CANARY
    from mirobody.utils.config import Config

    report = await observations.ingest("1", [
        observations.Draft(name_text="RestingHeartRate-RHR", value_text="60", unit_text="/min",
                           note_text=PHI_CANARY, observed_start=datetime(2025, 6, 1, tzinfo=UTC)),
        observations.Draft(name_text="RestingHeartRate-RHR", value_text="62", unit_text="/min",
                           observed_start=datetime(2025, 6, 2, tzinfo=UTC)),
    ], observations.Provenance(modality="lab-report", source_kind="file", source_ref="th_files:synthetic-e2e"), user_tz="UTC")
    assert report.inserted == 2
    script = Path(__file__).resolve().parents[2] / "scripts" / "e2e_health_data.py"
    spec = importlib.util.spec_from_file_location("health_data_e2e_gate", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(Config, "init", AsyncMock())
    assert await module.run("1", None) == 0
    assert PHI_CANARY not in caplog.text


@pytest.mark.parametrize('left,right,expected', _EXPORT_COLLISIONS)
@pytest.mark.parametrize('reverse', [False,True])
@pytest.mark.parametrize('batch_size', [1,10000])
async def test_export_collisions_execute_on_postgres(database,monkeypatch,left,right,expected,reverse,batch_size):
    repository = importlib.import_module('mirobody.collect.ingest.repositories.health_data')
    monkeypatch.setattr(repository,'_BATCH_SIZE',batch_size)
    readings = [left,right][::-1] if reverse else [left,right]
    records = [{'user_id':'1','indicator':'steps','source':'apple_health_export','start_time':datetime(2026,1,1,10),'value':value,'timezone':'UTC','source_id':name,'task_id':task} for name,value,task in readings]
    assert await repository.HealthDataRepository().save_health_records(records)
    rows = await database('SELECT source_id,value,task_id FROM series_data',log_sql=False)
    assert (rows[0]['source_id'],rows[0]['value'],rows[0]['task_id']) == expected

@pytest.mark.parametrize('source', ['apple_health_export','apple_health','vital.synthetic'])
async def test_device_updates_still_apply_on_postgres(database,source):
    repository = importlib.import_module('mirobody.collect.ingest.repositories.health_data')
    record={'user_id':'1','indicator':'steps','source':source,'start_time':datetime(2026,1,1,10),'value':'100','timezone':'UTC','source_id':'Apple Watch'}
    writer=repository.HealthDataRepository()
    assert await writer.save_health_records([record])
    assert await writer.save_health_records([{**record,'value':'120','task_id':'synthetic-repair'}])
    rows=await database('SELECT source_id,value,task_id FROM series_data',log_sql=False)
    assert (rows[0]['source_id'],rows[0]['value'],rows[0]['task_id']) == ('Apple Watch','120','synthetic-repair')

@pytest.mark.parametrize('old_task,new_task', [(None,'filtered_out_of_range'),('filtered_out_of_range',None)])
async def test_same_export_device_can_correct_range_status_postgres(database,old_task,new_task):
    repository = importlib.import_module('mirobody.collect.ingest.repositories.health_data')
    record={'user_id':'1','indicator':'steps','source':'apple_health_export','start_time':datetime(2026,1,1,10),'value':'100','timezone':'UTC','source_id':'Apple Watch','task_id':old_task}
    writer=repository.HealthDataRepository()
    assert await writer.save_health_records([record])
    assert await writer.save_health_records([{**record,'task_id':new_task}])
    rows=await database('SELECT source_id,value,task_id FROM series_data',log_sql=False)
    assert (rows[0]['source_id'],rows[0]['value'],rows[0]['task_id']) == ('Apple Watch','100',new_task)


_EVIDENCE_CASES = {
    "unverified": [("2026-01-01T09:00", "llm-unverified", False)],
    "verified": [("2026-01-01T09:00", "lab-report", False)],
    "unknown": [("2026-01-01T09:00", "", False)],
    "mixed": [("2026-01-01T09:00", "llm-unverified", False), ("2026-01-02T09:00", "lab-report", False)],
    "window": [("2026-01-01T09:00", "llm-unverified", False), ("2026-01-02T09:00", "lab-report", False)],
    "elected-verified": [("2026-01-01T09:00", "lab-report", True), ("2026-01-01T10:00", "llm-unverified", False)],
    "latest-verified": [("2026-01-01T09:00", "llm-unverified", False), ("2026-01-01T10:00", "lab-report", False)],
}


@pytest.mark.parametrize("case", list(_EVIDENCE_CASES))
@pytest.mark.parametrize("view", ["catalog", "stats", "raw", "latest", "minute", "hour", "day", "week", "month"])
async def test_evidence_survives_actual_writes_windows_and_elected_answers(database, case, view):
    from mirobody.agent.tools.health_indicators_service import _envelope_for
    from mirobody.collect import observations
    from mirobody.collect.files.services.indicator_store import save_indicators_to_db
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    for index, (day, modality, elected) in enumerate(_EVIDENCE_CASES[case]):
        at = datetime.fromisoformat(day).replace(tzinfo=UTC)
        value = 33 + index
        if modality:
            text = f"ALT {value + 10 if modality == 'llm-unverified' else value} U/L"
            report = await save_indicators_to_db("1", [{"original_indicator":"ALT", "value":str(value),
                "unit":"U/L", "detection_method":"laboratory"}], at, "extracted", f"synthetic-evidence-{index}", text=text)
        else:
            report = await observations.ingest("1", [observations.Draft(name_text="ALT", value_text=str(value),
                unit_text="U/L", observed_start=at)], observations.Provenance(modality="", source_kind="file",
                source_ref=f"th_files:synthetic-evidence-{index}"), user_tz="UTC")
        assert report.inserted == 1
        stored = await database("SELECT modality, series_id, local_date FROM v_observation WHERE id=:id",
                                {"id": report.ids[0]}, log_sql=False)
        assert stored[0]["modality"] == modality
        if elected:
            await database("INSERT INTO th_day_authority(user_id,series_id,local_date,observation_id,rule,candidates) "
                           "VALUES ('1',:series,:day,:id,'synthetic-election',2)",
                           {"series":stored[0]["series_id"],"day":stored[0]["local_date"],"id":report.ids[0]}, log_sql=False)

    reader = PostgresHealthQuery()
    selection = query.Selection(indicators=("ALT",))
    window = query.resolve_window("UTC", start="2026-01-02", end="2026-01-02", hours=24,
        now=datetime(2026, 10, 14, tzinfo=UTC)) if case == "window" else query.Window(0, 0, "UTC")
    if view == "catalog":
        method, rows = "catalog", await reader.catalog("1", window)
    elif view == "raw":
        method, rows = "readings", await reader.readings("1", selection, window, limit=30)
    elif view in ("stats", "latest"):
        method, rows = view, await getattr(reader, view)("1", selection, window)
    else:
        method, rows = "buckets", await reader.buckets("1", selection, window, resolution=view, limit=30)
    assert rows
    expected = case not in ("verified", "unknown", "window")
    if case == "mixed" and view == "latest":
        expected = False
    if case == "elected-verified" and view in ("stats", "latest", "day", "week", "month"):
        expected = False
    if case == "latest-verified" and view in ("latest", "day", "week", "month"):
        expected = False
    if view not in ("raw", "latest"):
        assert any(r["unverified"] for r in rows) is expected
    request = query.QueryRequest(selection=selection, view="raw" if view == "catalog" else view,
                                 start=window.start if case == "window" else "", end=window.end if case == "window" else "")
    envelope = _envelope_for(method, request, window, rows)
    assert any("check the file" in note for note in envelope.assumptions) is expected


async def test_stats_sum_is_the_total_over_every_reading_of_an_unelected_day(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    drafts = [observations.Draft(name_text="activeDuration", value_text=str(ms), unit_text="ms",
                                 observed_start=datetime(2026, 1, 1, hour, tzinfo=UTC))
              for ms, hour in ((600000, 8), (1200000, 12), (900000, 18))]
    await observations.ingest("1", drafts, observations.Provenance("device-sensed", "device", "synthetic-sum",
                                                                   vendor="synthetic-research"), user_tz="UTC")
    [stats] = await PostgresHealthQuery().stats("1", query.Selection(indicators=("activeDuration",)), query.Window(0, 0, "UTC"))
    # Three logs on one day, none elected. A total over more rows than the raw
    # view returns could only be rebuilt as avg x count before `sum` existed.
    assert (stats["count"], stats["unit"]) == (3, "ms")
    assert stats["sum"] == pytest.approx(2_700_000)
    assert stats["avg"] == pytest.approx(900_000)


async def test_a_keyword_reaches_a_lab_and_a_device_series_under_one_name(database):
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery

    at = datetime(2026, 1, 1, 8, tzinfo=UTC)
    await observations.ingest("1", [observations.Draft(name_text="Glucose", value_text="7.4", unit_text="mmol/L",
                                                       observed_start=at)],
                              observations.Provenance("lab-report", "file", "th_files:synthetic-lab"), user_tz="UTC")
    await observations.ingest("1", [observations.Draft(name_text="Glucose", value_text="6.9", unit_text="mmol/L",
                                                       observed_start=at)],
                              observations.Provenance("device-sensed", "device", "device:synthetic-cgm",
                                                      vendor="synthetic-cgm"), user_tz="UTC")
    series = {r["series_id"] for r in await database("SELECT series_id FROM v_observation", log_sql=False)}
    assert len(series) == 2  # a device reading keeps its own namespace, not the lab's LOINC
    found = await PostgresHealthQuery()._by_keywords("1", ("glucose",), None)
    assert set(found) == series


def test_trend_citation_definition_tracks_its_actual_daily_input_without_values():
    from mirobody.collect.citations import aggregate_definition, aggregate_key

    row = {"indicator": "Synthetic trend", "series": "synthetic:trend", "first_date": "2026-01-02",
           "last_date": "2026-01-04", "first_third_mean": 12.0, "last_third_mean": 18.0, "direction": "up"}
    detail = aggregate_definition("trend", "trend", row, "2026-01-01", "2026-12-31")
    assert detail == {"view": "trend", "series": "synthetic:trend", "indicator": "Synthetic trend",
                      "basis": "day_authority", "from": "2026-01-02", "to": "2026-01-04"}
    stats = aggregate_definition("stats", "stats", row, "2026-01-02", "2026-01-04")
    assert aggregate_key(detail) != aggregate_key(stats)


@pytest.mark.parametrize("same_instant", [False, True], ids=["latest-time", "latest-id"])
async def test_trend_citations_resolve_daily_contributors_live_and_keep_ownership(database, same_instant):
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
    from mirobody.collect import citations, observations
    from mirobody.collect.query import PostgresHealthQuery

    samples = [(1, 8, "10"), (1, 8 if same_instant else 20, "14"),
               (2, 8, "20"), (2, 20, "99"), (3, 9, "30")]
    drafts = [observations.Draft(name_text="ALT", value_text=value, unit_text="U/L",
                               observed_start=datetime(2026, 1, day, hour, tzinfo=UTC),
                               source_record_id=f"synthetic-{index}")
              for index, (day, hour, value) in enumerate(samples)]
    provenance = observations.Provenance("lab-report", "file", "th_files:synthetic-trend-citations")
    report = await observations.ingest("1", drafts, provenance, user_tz="UTC")
    assert report.inserted == len(samples)
    await observations.ingest("2", [observations.Draft(name_text="ALT", value_text="77", unit_text="U/L",
        observed_start=datetime(2026, 1, 2, 22, tzinfo=UTC))], provenance, user_tz="UTC")
    [elected] = await database("SELECT series_id, local_date FROM v_observation WHERE id=:id",
                               {"id": report.ids[2]}, log_sql=False)
    await database("INSERT INTO th_day_authority(user_id,series_id,local_date,observation_id,rule,candidates) "
                   "VALUES ('1',:series,:day,:id,'synthetic-election',2)",
                   {"series": elected["series_id"], "day": elected["local_date"], "id": report.ids[2]}, log_sql=False)

    service = HealthIndicatorsService(PostgresHealthQuery(), outside_note=False)
    envelope = await service.envelope({"user_id": "1", "session_id": "synthetic-trend-thread"},
                                      indicators=["ALT"], start="2026-01-01", end="2026-01-03", view="trend")
    assert envelope.status == "ok"
    [trend] = envelope.data
    assert trend["n"] == 3
    rid = trend["rid"]
    [resolved] = await citations.resolve_rids("synthetic-trend-thread", "1", [rid])
    assert (resolved["view"], resolved["basis"], resolved["total"]) == ("trend", "day_authority", 3)
    assert [row["value"] for row in resolved["readings"]] == ["30", "20", "14"]
    assert (resolved["first_day"], resolved["last_day"]) == ("2026-01-01", "2026-01-03")
    assert await citations.resolve_rids("synthetic-trend-thread", "2", [rid]) == [{"rid": rid, "status": "unknown"}]
    assert await citations.resolve_rids("another-synthetic-thread", "1", [rid]) == [{"rid": rid, "status": "unknown"}]
    [stored] = await database("SELECT detail FROM th_chat_citation WHERE session_id=:session AND rid=:rid",
                              {"session": "synthetic-trend-thread", "rid": rid}, log_sql=False)
    assert set(stored["detail"]) == {"view", "series", "indicator", "basis", "from", "to"}

    stats_detail = citations.aggregate_definition("stats", "stats", trend, "2026-01-01", "2026-01-03")
    stats_key = citations.aggregate_key(stats_detail)
    stats_rids = await citations.mint_rids("synthetic-trend-thread", "1", [(stats_key, stats_detail)])
    assert stats_rids[stats_key] != rid
    [stats] = await citations.resolve_rids("synthetic-trend-thread", "1", [stats_rids[stats_key]])
    assert stats["total"] == len(samples)

    corrected = await observations.amend("1", report.ids[1], value_text="16")
    assert corrected and corrected != report.ids[1]
    [resolved] = await citations.resolve_rids("synthetic-trend-thread", "1", [rid])
    assert [row["value"] for row in resolved["readings"]] == ["30", "20", "16"]
    assert await observations.erase("1", ids=[report.ids[4]]) == 1
    [resolved] = await citations.resolve_rids("synthetic-trend-thread", "1", [rid])
    assert resolved["total"] == 2
    assert [row["value"] for row in resolved["readings"]] == ["20", "16"]


async def test_trend_citation_members_use_the_actual_capped_input_span(database):
    from datetime import timedelta
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
    from mirobody.collect import citations, observations
    from mirobody.collect.query import PostgresHealthQuery

    first = datetime(2024, 1, 1, tzinfo=UTC)
    drafts = [observations.Draft(name_text="ALT", value_text=str(20 + i % 10), unit_text="U/L",
                               observed_start=first + timedelta(days=i)) for i in range(401)]
    report = await observations.ingest("1", drafts,
        observations.Provenance("lab-report", "file", "th_files:synthetic-capped-trend"), user_tz="UTC")
    assert report.inserted == len(drafts)
    envelope = await HealthIndicatorsService(PostgresHealthQuery(), outside_note=False).envelope(
        {"user_id": "1", "session_id": "synthetic-capped-thread"}, indicators=["ALT"], view="trend")
    [trend] = envelope.data
    assert trend["n"] == 400
    [resolved] = await citations.resolve_rids("synthetic-capped-thread", "1", [trend["rid"]])
    assert resolved["view"] == "trend"
    assert resolved["total"] == 400
    assert resolved["from"] == resolved["first_day"] == (first + timedelta(days=1)).date().isoformat()
    assert resolved["to"] == resolved["last_day"] == (first + timedelta(days=400)).date().isoformat()
    assert len(resolved["readings"]) == citations.MEMBERS_SHOWN


async def test_a_value_band_counts_readings_and_days_on_the_rows_own_basis(database):
    """The band is compared as min/max/avg are: printed values for one unit,
    canonical values for a shared canonical unit, nothing otherwise.
    Compared on printed values, 80 umol/L counted and 0.9 mg/dL (79.56
    umol/L, the same creatinine) did not, in a row reported in mol/L."""
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query

    def at(day, hour=8):
        return datetime(2026, 1, day, hour, tzinfo=UTC)

    drafts = [observations.Draft(name_text=name, value_text=value, unit_text=unit, observed_start=when)
              for name, value, unit, when in (
                  ("ALT", "20", "U/L", at(1)), ("ALT", "45", "U/L", at(1, 20)), ("ALT", "50", "U/L", at(2)),
                  ("ALT", "10", "U/L", at(3)),
                  ("Creatinine", "80", "umol/L", at(1)), ("Creatinine", "0.9", "mg/dL", at(2)),
                  ("Albumin", "40", "g/L", at(1)), ("Albumin", "600", "umol/L", at(2)))]
    await observations.ingest("1", drafts, observations.Provenance("lab-report", "file", "th_files:synthetic-band"),
                              user_tz="UTC")
    store, window = PostgresHealthQuery(), query.Window(0, 0, "UTC")

    [alt] = await store.stats("1", query.Selection(indicators=("ALT",)), window, value_min=40)
    assert (alt["count"], alt["days"], alt["count_in_range"], alt["days_in_range"]) == (4, 3, 2, 2)
    [plain] = await store.stats("1", query.Selection(indicators=("ALT",)), window)
    assert plain["count_in_range"] is None and plain["days_in_range"] is None

    creatinine = query.Selection(indicators=("2160-0",))
    [both] = await store.stats("1", creatinine, window, value_min=79e-6)
    assert both["unit"] == "mol/L" and both["count_in_range"] == 2
    [neither] = await store.stats("1", creatinine, window, value_min=50)
    assert neither["count_in_range"] == 0

    [albumin] = await store.stats("1", query.Selection(indicators=("1751-7",)), window, value_min=1)
    assert albumin["mixed_units"] and albumin["count_in_range"] is None


async def test_a_value_band_is_not_counted_over_steps_stored_as_increments(database):
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery

    drafts = [observations.Draft(name_text="steps", value_text="4000", unit_text="{#}",
                                 observed_start=datetime(2026, 1, 1, hour, tzinfo=UTC)) for hour in (8, 12, 18)]
    await observations.ingest("1", drafts, observations.Provenance("device-sensed", "device", "synthetic-steps",
                                                                   vendor="synthetic-research"), user_tz="UTC")
    envelope = await HealthIndicatorsService(PostgresHealthQuery(), outside_note=False).envelope(
        {"user_id": "1"}, indicators=["steps"], view="stats", value_min=10000)
    [row] = envelope.data
    assert row["code"] == "55423-8"
    assert row["count_in_range"] is None and row["days_in_range"] is None
    assert any(a.startswith("no band was counted for") for a in envelope.assumptions)


async def test_an_empty_window_names_the_readings_the_record_holds_elsewhere(database):
    """A window holding none of a series' readings is zero there, not a
    record without the series; the whole-record stats say where it is."""
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
    from mirobody.collect import observations
    from mirobody.collect.query import PostgresHealthQuery

    drafts = [observations.Draft(name_text="ALT", value_text=str(20 + day), unit_text="U/L",
                                 observed_start=datetime(2026, 3, day, 8, tzinfo=UTC)) for day in (2, 9, 16)]
    await observations.ingest("1", drafts, observations.Provenance("lab-report", "file", "th_files:synthetic-elsewhere"),
                              user_tz="UTC")
    service = HealthIndicatorsService(PostgresHealthQuery(), outside_note=False)
    envelope = await service.envelope({"user_id": "1"}, indicators=["ALT"], start="2026-10-01", end="2026-10-07",
                                      view="day")
    assert envelope.meta.row_count == 0
    note = next(a for a in envelope.assumptions if a.startswith("0 readings"))
    assert "between 2026-10-01 and 2026-10-07" in note
    assert "has 3 from 2026-03-02 to 2026-03-16" in note
    assert not any("no indicator matched" in a for a in envelope.assumptions)

    missing = await service.envelope({"user_id": "1"}, indicators=["Ferritin"], start="2026-10-01",
                                     end="2026-10-07", view="day")
    assert any("no indicator matched" in a for a in missing.assumptions)


async def test_choosing_not_to_share_at_activation_revokes_the_creators_link(database):
    """A person who takes over the account a family member managed, and
    chooses not to share, ends that member's personal MCP link for good:
    raising the switch later must not bring the old URL back, the rule
    unsharing already keeps (#232). Before, activation only refused it."""
    from mirobody.user import activation, personal_mcp
    from mirobody.user import care_circle as cc

    # `hand_over` sets the password through pgcrypto, which this schema does not
    # see; identity stand-ins, like the encryption ones the fixture creates.
    await database("CREATE FUNCTION gen_salt(text, integer) RETURNS text LANGUAGE SQL AS $$SELECT $1$$")
    await database("CREATE FUNCTION crypt(text, text) RETURNS text LANGUAGE SQL AS $$SELECT $1$$")
    [creator] = await database("INSERT INTO health_app_user(is_del,email,tz) VALUES (false,'creator@example.invalid','UTC')"
                               " RETURNING id", log_sql=False)
    [member] = await database("INSERT INTO health_app_user(is_del,email,tz,managed_by)"
                              " VALUES (false,'managed@example.invalid','UTC',:c) RETURNING id",
                              {"c": creator["id"]}, log_sql=False)
    creator_id, member_id = int(creator["id"]), int(member["id"])
    circle = await cc.create_circle(creator_id)
    await cc.force_accept_managed_member(circle, member_id)
    secret, _ = await personal_mcp.mint(str(creator_id), str(member_id))
    assert await personal_mcp.authorize(secret) == str(member_id)

    taken = activation.Activation(id=1, member_id=member_id, email="member@example.invalid", created_by=creator_id,
                                  member_name="", creator_name="", expires_at=datetime.now(UTC))
    assert await activation.hand_over(None, taken, into=None, password="", access=cc.ACCESS_NONE) == member_id
    assert await personal_mcp.authorize(secret) == ""
    [link] = await database("SELECT revoked_by FROM th_personal_mcp_url", log_sql=False)
    assert link["revoked_by"] == "care_circle"

    assert await cc.set_health_access(member_id, circle, cc.ACCESS_VIEW)
    assert await personal_mcp.authorize(secret) == ""


@asynccontextmanager
async def _merge_pool(database):
    """A psycopg pool on the fixture's schema: `merge_accounts` takes a pool,
    not the engine `execute_query` runs on."""
    from psycopg_pool import AsyncConnectionPool
    from sqlalchemy.engine import make_url

    [row] = await database("SELECT current_schema() AS s", log_sql=False)
    pool = AsyncConnectionPool(make_url(DSN).set(drivername="postgresql").render_as_string(hide_password=False),
                               kwargs={"options": f"-c search_path={row['s']}"}, min_size=1, open=False)
    await pool.open()
    try:
        yield pool
    finally:
        await pool.close()


async def _account(database, email: str, managed_by: int | None = None) -> int:
    [row] = await database("INSERT INTO health_app_user(is_del,email,tz,managed_by) VALUES (false,:e,'UTC',:m)"
                           " RETURNING id", {"e": email, "m": managed_by}, log_sql=False)
    return int(row["id"])


@pytest.mark.parametrize("access", ["view", "none"])
async def test_activation_into_an_existing_account_carries_the_creators_link(database, access):
    """#232: a person who takes over a managed account by signing in to one
    they already had merges the two, and the creator's link went on naming the
    deleted account. It was refused even when they kept sharing, still listed
    as live, and missing from their own list of links that read their record.
    The link now follows the record, and their choice applies to it as it does
    when there is no account to merge into."""
    from mirobody.user import activation, personal_mcp
    from mirobody.user import care_circle as cc

    await database("CREATE FUNCTION gen_salt(text, integer) RETURNS text LANGUAGE SQL AS $$SELECT $1$$")
    await database("CREATE FUNCTION crypt(text, text) RETURNS text LANGUAGE SQL AS $$SELECT $1$$")
    creator_id = await _account(database, "creator@example.invalid")
    member_id = await _account(database, "managed@example.invalid", managed_by=creator_id)
    mom_id = await _account(database, "mom@example.invalid")
    circle = await cc.create_circle(creator_id)
    await cc.force_accept_managed_member(circle, member_id)
    secret, _ = await personal_mcp.mint(str(creator_id), str(member_id))

    taken = activation.Activation(id=1, member_id=member_id, email="mom@example.invalid", created_by=creator_id,
                                  member_name="", creator_name="", expires_at=datetime.now(UTC))
    choice = cc.ACCESS_VIEW if access == "view" else cc.ACCESS_NONE
    async with _merge_pool(database) as pool:
        assert await activation.hand_over(pool, taken, into=mom_id, password="", access=choice) == mom_id

    [link] = await database("SELECT subject_id, revoked_by FROM th_personal_mcp_url", log_sql=False)
    assert link["subject_id"] == str(mom_id)
    if access == "view":
        assert link["revoked_by"] is None
        assert await personal_mcp.authorize(secret) == str(mom_id)
        assert [r["subject_id"] for r in (await personal_mcp.listed(str(creator_id)))["made"]] == [str(mom_id)]
        assert [r["creator_id"] for r in (await personal_mcp.listed(str(mom_id)))["holders"]] == [str(creator_id)]
    else:
        assert link["revoked_by"] == "care_circle"
        assert await personal_mcp.authorize(secret) == ""
        assert await cc.set_health_access(mom_id, circle, cc.ACCESS_VIEW)
        assert await personal_mcp.authorize(secret) == ""


async def test_account_merge_keeps_one_unrevoked_link_per_creator_and_record(database):
    """`th_personal_mcp_url` allows one unrevoked link per (creator, subject),
    expired or not. A creator holding links to both records keeps the one that
    expires later; links the closed account made, and the surviving account's
    link to the closed one, are revoked and stay where they were."""
    from mirobody.user import personal_mcp
    from mirobody.user import care_circle as cc
    from mirobody.user.account_merge import merge_accounts

    creator_id = await _account(database, "creator@example.invalid")
    member_id = await _account(database, "managed@example.invalid", managed_by=creator_id)
    mom_id = await _account(database, "mom@example.invalid")
    circle = await cc.create_circle(creator_id)
    await cc.force_accept_managed_member(circle, member_id)
    await database("INSERT INTO care_circle_members (care_circle_id, user_id, role, status, health_access)"
                   " VALUES (:c, :u, :role, :status, :access)",
                   {"c": circle, "u": mom_id, "role": cc.ROLE_MEMBER, "status": cc.STATUS_ACCEPTED,
                    "access": cc.ACCESS_VIEW}, log_sql=False)
    kept, _ = await personal_mcp.mint(str(creator_id), str(member_id))
    await personal_mcp.mint(str(creator_id), str(mom_id))
    await database("UPDATE th_personal_mcp_url SET expires_at = now() - interval '1 day' WHERE subject_id = :s",
                   {"s": str(mom_id)}, log_sql=False)
    await personal_mcp.mint(str(member_id), str(member_id))
    await personal_mcp.mint(str(member_id), str(creator_id))
    await personal_mcp.mint(str(mom_id), str(member_id))

    async with _merge_pool(database) as pool:
        affected, err = await merge_accounts(pool, losing_user_id=member_id, winning_user_id=mom_id,
                                             reason="activation")
    assert err is None
    assert affected["th_personal_mcp_url"] == 5

    rows = await database("SELECT creator_id, subject_id, revoked_by FROM th_personal_mcp_url ORDER BY id",
                          log_sql=False)
    c, m, w = str(creator_id), str(member_id), str(mom_id)
    assert [(r["creator_id"], r["subject_id"], r["revoked_by"]) for r in rows] == [
        (c, w, None),
        (c, w, "account_merge"),
        (m, m, "account_merge"),
        (m, c, "account_merge"),
        (w, m, "account_merge"),
    ]
    assert await personal_mcp.authorize(kept) == w


async def test_a_chinese_request_names_coded_readings_in_chinese(database):
    """#231: a Chinese report's readings were coded, and the indicator list then
    named them by LOINC's long name only. The list's rows keep `indicator` (what
    callers select by) and gain `label` and `facets` in the request's language."""
    pytest.importorskip("fastapi")
    from mirobody.agent.tools._render import render_rest
    from mirobody.collect import observations
    from mirobody.server.routers.indicator_router import _service
    from mirobody.utils.req_ctx import set_req_ctx

    at = datetime(2026, 7, 23, tzinfo=UTC)
    report = await observations.ingest("1", [
        observations.Draft(name_text="低密度脂蛋白胆固醇", value_text="2.85", unit_text="mmol/L", observed_start=at),
        observations.Draft(name_text="糖化血红蛋白", value_text="5.6", unit_text="%", observed_start=at),
        observations.Draft(name_text="某自定义指标", value_text="1", unit_text="", observed_start=at),
    ], observations.Provenance("lab-report", "file", "th_files:synthetic-zh-names"), user_tz="UTC")
    assert report.inserted == 3

    with set_req_ctx({"language": "zh-CN"}):
        chinese = render_rest(await _service.envelope({"user_id": "1"}))["rows"]
    english = render_rest(await _service.envelope({"user_id": "1"}))["rows"]

    by_code = {r["code"]: r for r in chinese if r.get("code")}
    assert by_code["22748-8"]["label"] == "低密度脂蛋白胆固醇"
    assert by_code["4548-4"]["label"] == "糖化血红蛋白（HbA1c）"
    assert by_code["22748-8"]["indicator"] == "Cholesterol in LDL [Moles/volume] in Serum or Plasma"
    assert by_code["22748-8"]["facets"] == {"specimen": "血清/血浆", "property": "物质的量浓度"}
    assert [r["indicator"] for r in chinese] == [r["indicator"] for r in english]
    english_by_code = {r["code"]: r for r in english if r.get("code")}
    assert english_by_code["22748-8"]["label"] == "LDL cholesterol, Blood"
    assert english_by_code["22748-8"]["facets"]["specimen"] == "Serum or Plasma"
    # An uncoded reading already reads as it was printed; it gets no label.
    assert not any("label" in r or "facets" in r for r in chinese if not r.get("code"))


async def test_a_cite_names_its_reading_in_the_readers_language(database):
    """#231: the indicator list named a coded reading in Chinese and the cite on
    the same number, in the answer beside it, still said LOINC's English. A
    reading cite and an aggregate cite both carry `label` for a Chinese request.
    An English one carries LOINC's consumer name, which 8480-6 does not have, so
    there it keeps the long name."""
    from mirobody.collect import citations, observations
    from mirobody.utils.req_ctx import set_req_ctx

    at = datetime(2026, 7, 23, tzinfo=UTC)
    report = await observations.ingest("1", [
        observations.Draft(name_text="Systolic Blood Pressure", value_text="118", unit_text="mm[Hg]", observed_start=at),
    ], observations.Provenance("lab-report", "file", "th_files:synthetic-cite-names"), user_tz="UTC")
    [row] = await database("SELECT id, series_id, display FROM v_observation", log_sql=False)
    detail = {"view": "stats", "series": row["series_id"], "indicator": row["display"],
              "from": "2026-07-23", "to": "2026-07-23"}
    keys = [citations.reading_key(report.ids[0]), citations.aggregate_key(detail)]
    rids = await citations.mint_rids("synthetic-cite-names", "1", [(keys[0], None), (keys[1], detail)])

    with set_req_ctx({"language": "zh-CN"}):
        reading, aggregate = await citations.resolve_rids("synthetic-cite-names", "1", [rids[k] for k in keys])
    assert reading["label"] == "收缩压" and reading["code"] == "8480-6"
    assert aggregate["label"] == "收缩压" and aggregate["indicator"] == row["display"]
    for cited in await citations.resolve_rids("synthetic-cite-names", "1", [rids[k] for k in keys]):
        assert "label" not in cited

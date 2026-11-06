"""An Apple reading's day is read in the offset its own record carries.

`mirobody import apple export.zip --into <id>` read every day in `--tz`,
`UTC` unless given, while its help said Apple records carry their own offset.
An HRV sample written `2026-11-01 07:10:00 +0800` was filed under 31 October
for anyone who did not pass `--tz`. The phone push had the same shape: the
record model defaulted its `timezone` to `UTC`, so `metaInfo.timezone` was
never read. The database half (the day the aggregator stores, a re-import
moving rows it filed before) is in `test_postgres_regressions.py`.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from mirobody.kernel.decoders import apple
from mirobody.standardize import local_day

SHANGHAI_MORNING = "2026-11-01 07:10:00 +0800"


@pytest.mark.parametrize("stamp,fallback,expected", [
    # The import's default: an unset zone no longer moves the day.
    (SHANGHAI_MORNING, "UTC", "UTC+08:00"),
    (SHANGHAI_MORNING, "", "UTC+08:00"),
    # A zone that agrees is kept: its name knows daylight saving.
    (SHANGHAI_MORNING, "Asia/Shanghai", "Asia/Shanghai"),
    ("2026-07-01 07:10:00 -0400", "America/New_York", "America/New_York"),
    ("2026-12-01 07:10:00 -0500", "America/New_York", "America/New_York"),
    # A record taken elsewhere keeps its own offset, half hours included.
    ("2026-12-01 07:10:00 +0530", "America/New_York", "UTC+05:30"),
    (SHANGHAI_MORNING, "Not/AZone", "UTC+08:00"),
    # No offset to read: epoch milliseconds, an unreadable string.
    (1793488200000, "Asia/Shanghai", "Asia/Shanghai"),
    (1793488200000, "+08:00", "UTC+08:00"),
    ("not a date", "UTC", "UTC"),
])
def test_record_zone(stamp, fallback, expected):
    assert apple.record_zone(stamp, fallback) == expected


def test_the_record_zone_places_the_morning_on_its_own_day():
    at = datetime(2026, 10, 31, 23, 10, tzinfo=UTC)
    assert local_day(at, apple.record_zone(SHANGHAI_MORNING, "UTC")).isoformat() == "2026-11-01"


_EXPORT = """<HealthData>
<Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" sourceName="Apple Watch" unit="ms"
 startDate="2026-11-01 07:10:00 +0800" endDate="2026-11-01 07:10:00 +0800" value="40"/>
<Record type="HKQuantityTypeIdentifierHeartRateVariabilitySDNN" sourceName="Apple Watch" unit="ms"
 startDate="2026-12-01 07:10:00 +0530" endDate="2026-12-01 07:10:00 +0530" value="52"/>
</HealthData>"""


@pytest.fixture
def stored(monkeypatch):
    """`_store_export` with its three database calls replaced: what it hands
    the writer, and what it asks the aggregation to recalculate."""
    pytest.importorskip("sqlalchemy")
    from mirobody import cli
    from mirobody.collect import observations
    from mirobody.collect.ingest import StandardHealthService
    from mirobody.standardize import AggregateIndicatorService
    from mirobody.utils.config import Config

    batches, ranges = [], []

    async def write(self, data, user):
        batches.append(data)
        return True

    async def recalculate(self, start, end, user_id=None, **kw):
        ranges.append((start, end, user_id, kw))
        return {"status": "success"}

    monkeypatch.setattr(Config, "init", AsyncMock())
    monkeypatch.setattr(observations, "user_tz", AsyncMock(return_value="UTC"))
    monkeypatch.setattr(StandardHealthService, "process_standard_data", write)
    monkeypatch.setattr(AggregateIndicatorService, "recalculate_date_range", recalculate)
    monkeypatch.setattr("mirobody.utils.execute_query", AsyncMock(return_value=[{"n": 0}]))
    return cli, batches, ranges


def test_import_without_tz_reads_each_record_in_its_own_offset(stored, tmp_path):
    cli, batches, ranges = stored
    export = tmp_path / "export.xml"
    export.write_text(_EXPORT)
    cli.main(["import", "apple", str(export), "--into", "1"])
    records = [r for batch in batches for r in batch.healthData]
    assert [r.timezone for r in records] == ["UTC+08:00", "UTC+05:30"]
    assert [local_day(datetime.fromtimestamp(r.timestamp / 1000, UTC), r.timezone).isoformat()
            for r in records] == ["2026-11-01", "2026-12-01"]
    # The days either side are read again, and the figures left on the days
    # an earlier import filed these readings under are retracted.
    [(start, end, user, kw)] = ranges
    assert (start.date().isoformat(), end.date().isoformat(), user) == ("2026-10-30", "2026-12-02", "1")
    assert kw == {"sweep_source": "apple_health_export"}


def test_import_keeps_a_named_zone_that_agrees(stored, tmp_path):
    cli, batches, _ranges = stored
    export = tmp_path / "export.xml"
    export.write_text(_EXPORT)
    cli.main(["import", "apple", str(export), "--into", "1", "--tz", "Asia/Shanghai"])
    assert [r.timezone for b in batches for r in b.healthData] == ["Asia/Shanghai", "UTC+05:30"]


def _pushed(records, meta=None):
    pytest.importorskip("fastapi")
    from mirobody.collect.ingest import FormatDataInput
    from mirobody.collect.ingest.models.requests import FormatDataContext
    from mirobody.collect.providers.apple.models import AppleHealthRequest

    request = AppleHealthRequest.model_validate({"metaInfo": meta or {}, "healthData": records})
    return FormatDataInput(context=FormatDataContext(theta_user_id="1"),
                           payload={"user_id": "1", "meta_info": request.metaInfo, "health_data": request.healthData})


_HRV = {"type": "HKQuantityTypeIdentifierHeartRateVariabilitySDNN", "value": 40, "unit": "ms", "sourceName": "Apple Watch"}


@pytest.mark.parametrize("record,meta,expected", [
    # The documented payload: epoch milliseconds, the zone in `metaInfo` only.
    ({**_HRV, "startDate": 1793488200000}, {"timezone": "Asia/Shanghai"}, "Asia/Shanghai"),
    # A record's own zone still wins over `metaInfo`.
    ({**_HRV, "startDate": 1793488200000, "timezone": "Asia/Tokyo"}, {"timezone": "Asia/Shanghai"}, "Asia/Tokyo"),
    # Apple's date string read by the rule the import uses, so one record
    # pushed and exported lands on one day.
    ({**_HRV, "startDate": SHANGHAI_MORNING}, {}, "UTC+08:00"),
    ({**_HRV, "startDate": SHANGHAI_MORNING}, {"timezone": "Asia/Shanghai"}, "Asia/Shanghai"),
])
async def test_push_reads_the_zone_the_payload_gives(record, meta, expected):
    from mirobody.collect.providers.apple.provider import AppleHealthProvider

    data = await AppleHealthProvider(platform=None).format_data(_pushed([record], meta))
    assert [r.timezone for r in data.healthData] == [expected]


def test_a_summary_day_in_a_fixed_offset_is_that_offsets_day():
    pytest.importorskip("sqlalchemy")
    from mirobody.standardize.aggregate.aggregators.sql_aggregator import to_local_day_range

    # 16:00 UTC is midnight at +08:00. `ZoneInfo` raised on the offset and the
    # day was stored as 31 October 16:00.
    start, end = to_local_day_range(datetime(2026, 10, 31, 16), "UTC+08:00")
    assert (start, end) == (datetime(2026, 11, 1), datetime(2026, 11, 1, 23, 59, 59))


async def test_two_spellings_of_one_zone_are_one_day(monkeypatch):
    pytest.importorskip("sqlalchemy")
    from mirobody.standardize.aggregate.aggregators.sql_aggregator import SQLAggregator
    from mirobody.standardize.aggregate.models import CalculationTask

    calls = []

    async def aggregate(self, user_id, timezone, tasks, data_begin_utc):
        calls.append((user_id, timezone, sorted({t.timezone for t in tasks}), len(tasks)))
        return []

    monkeypatch.setattr(SQLAggregator, "_aggregate_person_day", aggregate)
    begin = datetime(2026, 10, 31, 16)
    tasks = [CalculationTask(user_id="1", source_indicator="hrvSDNN", target_indicator="dailyAvgHrvSDNN",
                             aggregation_type="avg", data_begin_utc=begin, timezone=tz, update_time=None)
             for tz in ("UTC+08:00", "Asia/Shanghai", "Asia/Tokyo")]
    await SQLAggregator()._process_data_begin_aggregations(tasks)
    # Shanghai and +08:00 end the day at the same instant: one computation,
    # under the IANA name. Tokyo's day ends an hour earlier: its own.
    assert sorted(calls) == [("1", "Asia/Shanghai", ["Asia/Shanghai"], 1), ("1", "Asia/Tokyo", ["Asia/Tokyo"], 1)]

"""Apple transports publish one stream, using retained measurement instants."""

from datetime import UTC, date, datetime, timedelta
import os
from unittest.mock import AsyncMock

import pytest

from mirobody.standardize.aggregate import election


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("newer", ["apple_health", "apple_health_export", "apple_health_watch"])
@pytest.mark.parametrize("name", ["dailyTotalSteps", "dailyAvgHeartRates"])
@pytest.mark.parametrize("competitor", [None, "measurer", "profile_echo"])
async def test_transport_freshness_precedes_coverage(monkeypatch, reverse, newer, name, competitor):
    day = date(2026, 11, 1)
    start = datetime(2026, 11, 1, tzinfo=UTC)
    older = "apple_health_export" if newer != "apple_health_export" else "apple_health"
    rows = [{"id": i, "stream_key": f"stream:{vendor}", "source_class": "aggregator", "vendor": vendor,
                 "name_text": f"{name}.{vendor}", "value_num": value, "value_canonical": None,
                 "observed_start": start, "observed_end": start + timedelta(hours=hours), "tz": "America/New_York"}
            for i, vendor, value, hours in [(1, older, 100, 24), (2, newer, 120, 20)]]
    if competitor:
        rows.append({**rows[0], "id": 3, "stream_key": "vendor:other", "vendor": "other",
                     "source_class": competitor, "observed_end": start + timedelta(hours=1)})
    if reverse:
        rows.reverse()
    execute = AsyncMock(side_effect=[rows, [
        {"source": older, "measured_at": datetime(2026, 11, 1, 12)},
        {"source": newer, "measured_at": datetime(2026, 11, 1, 20)},
    ], None, None] * 2)
    monkeypatch.setattr(election, "execute_query", execute)
    first = await election.elect_day("1", "synthetic", day, priorities=[older, newer])
    second = await election.elect_day("1", "synthetic", day, priorities=[older, newer])
    expected = "vendor:other" if competitor == "measurer" else f"stream:{newer}"
    assert first.elected.identity == expected
    assert first.elected.values == {"value": 100 if competitor == "measurer" else 120}
    assert first.fingerprint == second.fingerprint
    assert len(first.ranked) == len(rows)
    mark = execute.call_args_list[3].args[1]
    assert mark["observation_id"] == (3 if competitor == "measurer" else 2)
    assert mark["candidates"] == len(rows)
    assert mark["rule"] == election.APPLE_RULE
    bounds = execute.call_args_list[1].args[1]
    assert bounds["start"] == datetime(2026, 11, 1, 4)
    assert bounds["end"] == datetime(2026, 11, 2, 5)  # 25-hour DST day


@pytest.mark.parametrize("evidence", [[], [{"source": "apple_health", "measured_at": datetime(2026, 1, 1, 9)},
                                         {"source": "apple_health_export", "measured_at": datetime(2026, 1, 1, 9)}]])
async def test_missing_or_equal_evidence_keeps_coverage_order(monkeypatch, evidence):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [{"id": i, "stream_key": vendor, "source_class": "aggregator", "vendor": vendor,
                 "name_text": "dailyTotalSteps", "value_num": i * 100, "value_canonical": None,
                 "observed_start": start, "observed_end": start + timedelta(hours=i), "tz": "UTC"}
            for i, vendor in enumerate(("apple_health", "apple_health_export"), 1)]
    execute = AsyncMock(side_effect=[rows, evidence, None, None])
    monkeypatch.setattr(election, "execute_query", execute)
    result = await election.elect_day("1", "synthetic", start.date(), priorities=[])
    assert result.elected.identity == "apple_health_export"


@pytest.mark.parametrize("values", [(180_000_000, 24_000_000), (180_000_000, 240_000_000)])
async def test_invalid_transport_preserves_valid_alternative_or_authority(monkeypatch, values):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [{"id": i, "stream_key": vendor, "source_class": "aggregator", "vendor": vendor,
                 "name_text": "dailyTotalSleepAnalysis_Asleep(Total)", "value_num": value, "value_canonical": None,
                 "observed_start": start, "observed_end": start + timedelta(days=1), "tz": "UTC"}
            for i, vendor, value in [(1, "apple_health", values[0]), (2, "apple_health_export", values[1])]]
    execute = AsyncMock(side_effect=[rows, [
        {"source": "apple_health", "measured_at": datetime(2026, 1, 2, 16)},
        {"source": "apple_health_export", "measured_at": datetime(2026, 1, 2, 9)},
    ], None, None])
    monkeypatch.setattr(election, "execute_query", execute)
    result = await election.elect_day("1", "synthetic", start.date(), priorities=["apple_health"])
    assert "apple_health" in result.blocked
    if values[1] == 24_000_000:
        assert result.elected.identity == "apple_health_export"
    else:
        assert result.elected is None
        assert execute.call_args.args[0] == election._CHECK_FAIL


@pytest.mark.parametrize("name,vendors", [
    ("dailyTotalSteps", ["apple_health"]),
    ("dailySteps", ["apple_health", "apple_health_export"]),
])
async def test_single_transport_and_direct_daily_statistics_need_no_lookup(monkeypatch, name, vendors):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [{"id": i, "stream_key": vendor, "source_class": "measurer", "vendor": vendor,
             "name_text": name, "value_num": 100, "value_canonical": None,
             "observed_start": start, "observed_end": start, "tz": "UTC"}
            for i, vendor in enumerate(vendors, 1)]
    execute = AsyncMock(side_effect=[rows, None, None])
    monkeypatch.setattr(election, "execute_query", execute)
    result = await election.elect_day("1", "synthetic", start.date(), priorities=[])
    assert result.elected.identity == "apple_health"
    assert execute.call_count == 3


@pytest.mark.needs_db
@pytest.mark.skipif(not os.environ.get("MIROBODY_TEST_POSTGRES_DSN"), reason="MIROBODY_TEST_POSTGRES_DSN is not set")
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("newer", ["apple_health", "apple_health_export"])
@pytest.mark.parametrize("indicator,target,tz,day", [
    ("steps", "dailyTotalSteps", "America/New_York", date(2026, 11, 1)),
    ("heartRates", "dailyAvgHeartRates", "UTC", date(2026, 1, 1)),
    ("sleepAnalysis_Asleep(Total)", "dailyTotalSleepAnalysis_Asleep(Total)", "UTC", date(2026, 1, 1)),
])
async def test_aggregation_publishes_one_transport(database, reverse, newer, indicator, target, tz, day):  # noqa: F811
    from mirobody.collect.ingest.repositories.health_data import HealthDataRepository
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.kernel import query
    from mirobody.standardize.aggregate.service import AggregateIndicatorService

    metric = election.metrics.METRICS[indicator]
    lo, hi = election.series.day_bounds_ms(day, tz, metric.window)
    begin = datetime.fromtimestamp(lo / 1000, UTC).replace(tzinfo=None)
    end = datetime.fromtimestamp(hi / 1000, UTC).replace(tzinfo=None)
    older = "apple_health_export" if newer == "apple_health" else "apple_health"
    # Unknown source IDs are chosen deterministically without assuming that
    # an export's display name identifies the push's physical device.
    ids = {"apple_health": "unknown.bundle.a", "apple_health_export": "Unknown device A"}
    rows = [{"user_id": "1", "indicator": indicator, "source": source,
             "start_time": begin + timedelta(hours=hour), "value": value,
             "timezone": tz, "source_id": ids[source]}
            for source, hour, value in [(older, 9, "100"), (newer, 16, "120")]]
    writer = HealthDataRepository()
    for row in rows[::-1] if reverse else rows:
        assert await writer.save_health_records([row])
        result = await AggregateIndicatorService().recalculate_date_range(begin, end, "1")
        assert result["status"] == "success"
    # Later losing-device, rejected and invalid samples cannot refresh the
    # older stream; an adjacent day's chosen-device sample cannot either.
    old = next(row for row in rows if row["source"] == older)
    for hour, changes in [
        (17, {"source_id": "zzz-losing-device"}),
        (18, {"task_id": "filtered_out_of_range"}),
        (19, {"value": "--"}),
    ]:
        assert await writer.save_health_records([{**old, "start_time": begin + timedelta(hours=hour), **changes}])
    assert await writer.save_health_records([{**old, "start_time": end}])
    assert await writer.save_health_records([old])  # reimport after newer measurements
    [cell] = await database(
        "SELECT a.series_id FROM th_day_authority a JOIN v_observation o ON o.id=a.observation_id "
        "WHERE o.name_text = :name AND a.local_date = :day",
        {"name": f"{target}.{newer}", "day": day}, log_sql=False,
    )
    for _ in range(2):
        decision = await election.elect_day("1", cell["series_id"], day, priorities=[older, newer])
        assert decision.elected.values == {"value": 120}
    [authority] = await database(
        "SELECT o.vendor, o.value_num FROM th_day_authority a JOIN v_observation o ON o.id=a.observation_id "
        "WHERE a.series_id=:sid AND a.local_date=:day",
        {"sid": cell["series_id"], "day": day}, log_sql=False,
    )
    assert authority["vendor"] == newer and float(authority["value_num"]) == 120
    readings = await PostgresHealthQuery().stats(
        "1", query.Selection(indicators=(target,)), query.Window(0, 0, tz),
    )
    assert readings and float(readings[0]["avg"]) == 120


# Reuse the existing isolated PostgreSQL schema seam.
from .test_postgres_regressions import database  # noqa: F401

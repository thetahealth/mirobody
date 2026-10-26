"""Trend contracts through the product service, using synthetic mapped rows."""
from datetime import date, timedelta
from unittest.mock import Mock

import pytest

from mirobody.agent.tools._render import render_compact, render_rest
from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
from mirobody.collect.query import _bucket_row
from mirobody.kernel import tools


def daily(values, *, total=None, reported=False):
    return [_bucket_row({"display": "Synthetic metric", "series_id": "synthetic-series",
                         "code_system": "LOINC", "code": "29463-7", "unit": "kg",
                         "period": (date(2025, 1, 1) + timedelta(days=i)).isoformat(),
                         "avg": value, "n": 1, "total": total or len(values), "reported": reported})
            for i, value in enumerate(values)]


async def envelope(rows):
    hq = Mock()
    hq.tz.return_value = "UTC"
    hq.on_read.return_value = None
    hq.buckets.return_value = rows
    result = await HealthIndicatorsService(hq, outside_note=False).envelope(
        {"user_id": "1"}, indicators=["Synthetic metric"], view="trend")
    hq.catalog.assert_not_called()
    assert hq.buckets.call_args.kwargs == {"resolution": "day", "limit": 400}
    return result


async def test_complete_trend_keeps_compact_and_rest_numeric_contract():
    result = await envelope(daily([1, 2, 3, 2, 1]))
    assert result.status == "ok" and result.meta.truncated is False
    compact = render_compact(result)
    for expected in ("first_third_mean", "delta_pct", "slope_per_day", "unit=kg", "shape=hump", "source_days=5"):
        assert expected in compact
    assert "shape=hump" in " ".join(result.assumptions)
    assert render_rest(result)["rows"] == result.data


async def test_capped_input_is_partial_even_though_there_is_only_one_summary():
    result = await envelope(daily([70] * 400, total=401))
    assert result.status == "partial" and result.meta.truncated
    assert "400 of 401 recorded days" in result.meta.cut
    assert "not missing" in result.meta.cut
    assert result.data[0]["source_days"] == 400


async def test_source_flags_survive_non_numeric_days():
    rows = daily([1, None, 2, 3])
    rows[1].update(unverified=True, day_known=False)
    result = await envelope(rows)
    assert result.status == "partial" and not result.meta.truncated
    assert result.data[0]["unverified"] and not result.data[0]["day_known"]
    assert "not verified" in " ".join(result.assumptions)


@pytest.mark.parametrize("reported", [False, True])
async def test_unsupported_results_preserve_identity_and_do_not_fall_back(reported):
    result = await envelope(daily([None] * 3, reported=reported))
    assert result.status == "partial" and result.meta.view == "trend"
    assert result.data[0]["direction"] == "unsupported"
    assert result.data[0]["series"] == "synthetic-series"
    assert tools.NEXT_VIEW_STATS in result.next_steps
    assert "no indicator matched" not in " ".join(result.assumptions)
    if reported:
        assert "reported by the person" in render_compact(result)


async def test_insufficient_points_do_not_print_a_fabricated_percentage():
    result = await envelope(daily([1, 2]))
    assert result.status == "partial" and result.data[0]["direction"] == "insufficient"
    assert "None%" not in render_compact(result)

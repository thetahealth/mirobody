"""Statistics must survive the public tool's compact rendering for a model."""

from decimal import Decimal
from unittest.mock import Mock

import pytest

from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
from mirobody.collect.query import _stats_row
from mirobody.kernel import query


@pytest.mark.parametrize(
    ("median", "stddev", "count"),
    [(Decimal("2"), Decimal("4.358898943540674"), 3),
     (Decimal("0"), None, 1),
     (Decimal("4"), Decimal("0"), 2)],
)
async def test_public_stats_tool_exposes_median_and_available_sample_spread(median, stddev, count):
    row = _stats_row({
        "display": "Synthetic metric", "series_id": "synthetic-series",
        "count": count, "numeric_count": count, "avg": median if count < 3 else Decimal("4"),
        "median": median, "stddev": stddev, "unit": "kg",
    })
    hq = Mock(spec=query.HealthQuery)
    hq.tz.return_value = "UTC"
    hq.on_read.return_value = None
    hq.stats.return_value = [row]

    answer = await HealthIndicatorsService(hq, outside_note=False).query_health_indicators(
        {"user_id": "synthetic"}, indicators=["Synthetic metric"], view="stats")

    assert answer["status"] == "ok" and answer["row_count"] == 1
    assert f"median={float(median)}" in answer["result"]
    if stddev is None:
        assert "stddev=" not in answer["result"]
    else:
        assert f"stddev={float(stddev)}" in answer["result"]
    assert "unit=kg" in answer["result"]
    subject, selection, _window = hq.stats.call_args.args
    assert subject == "synthetic" and selection.indicators == ("Synthetic metric",)
    hq.stats.assert_called_once()
    hq.catalog.assert_not_called()

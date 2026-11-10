"""A window with no readings has to say so, and say which cause it is.

An empty answer to a named window has two causes: the selection matched no
series, or it matched series with nothing in the window. Both fell back to the
catalogue under "no indicator matched those terms", and a local 2B answering
PHIA's objective set concluded the record lacked a metric it held (11% of its
failures). These drive the tool's real entry point, `envelope()`: the
envelope builder alone never sees an empty non-catalogue result, because the
fallback runs first.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

# The engine tests also run on a minimal install that has no optional chat
# dependencies; the tool module imports them, so skip rather than fail there.
pytest.importorskip("dotenv")

from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
from mirobody.kernel import tools

NOW = datetime(2026, 10, 14, tzinfo=UTC)
HELD = {"indicator": "Deep sleep duration", "series": "loinc:deep sleep", "count": 30,
        "first_date": "2026-03-01", "last_date": "2026-03-30"}
CATALOG = [{"indicator": "Deep sleep duration", "series": "loinc:deep sleep", "first_date": "2026-03-01",
            "last_date": "2026-03-30", "total": 1}]


def _store(*, buckets=(), whole_record=(), catalog=CATALOG):
    hq = Mock()
    hq.tz.return_value = "UTC"
    hq.on_read.return_value = None
    hq.buckets.return_value = list(buckets)
    hq.stats.return_value = list(whole_record)
    hq.catalog.return_value = list(catalog)
    return hq


async def _ask(hq, **args):
    return await HealthIndicatorsService(hq, now=lambda: NOW, outside_note=False).envelope(
        {"user_id": "1"}, indicators=["dailyDeepSleep"], view="day", **args)


async def test_a_series_held_outside_the_window_is_zero_here_not_missing():
    hq = _store(whole_record=[HELD])
    envelope = await _ask(hq, start="2026-10-01", end="2026-10-07")
    note = next(a for a in envelope.assumptions if a.startswith("0 readings"))
    assert "2026-10-01 and 2026-10-07" in note
    assert "Deep sleep duration has 30 from 2026-03-01 to 2026-03-30" in note
    assert not any("no indicator matched" in a for a in envelope.assumptions)
    assert not any("never recorded" in a for a in envelope.assumptions)
    assert tools.NEXT_PICK_FROM_CATALOG not in envelope.next_steps
    assert envelope.meta.row_count == 0
    hq.catalog.assert_not_called()


async def test_a_selection_that_matches_nothing_still_falls_back_to_the_catalogue():
    hq = _store(whole_record=[])
    envelope = await _ask(hq, start="2026-10-01", end="2026-10-07")
    assert any("no indicator matched" in a for a in envelope.assumptions)
    assert not any(a.startswith("0 readings") for a in envelope.assumptions)
    assert envelope.meta.row_count == len(CATALOG)


async def test_an_undated_empty_answer_needs_no_second_look():
    hq = _store()
    envelope = await _ask(hq)
    hq.stats.assert_not_called()
    assert any("no indicator matched" in a for a in envelope.assumptions)


async def test_a_window_with_readings_makes_no_zero_claim():
    day = {"indicator": "Deep sleep duration", "series": "loinc:deep sleep", "period": "2026-10-02",
           "avg": 3_000_000.0, "n": 1, "unit": "ms", "total": 1}
    hq = _store(buckets=[day], whole_record=[HELD])
    envelope = await _ask(hq, start="2026-10-01", end="2026-10-07")
    hq.stats.assert_not_called()
    assert not any(a.startswith("0 readings") for a in envelope.assumptions)

"""Helpers every vendor decoder shares. Pure; stdlib only.

Timestamps are the part vendors get wrong in the most ways: Garmin sends a
calendar date (``"2026-06-01"``) that means the user's local day, Oura sends
ISO strings with the ring's offset, Whoop sends UTC with a ``Z``. One parser
with three rules, tested once.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .. import metrics
from ..series import Fact, day_bounds_ms, zone

MS = 1000


def dig(obj: Any, path: str) -> Any:
    """``dig({"a": {"b": 1}}, "a.b") == 1``; ``None`` when any step is missing."""
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def number(value: Any) -> float | None:
    """A float, or ``None`` for anything that is not a number (booleans are
    not numbers here: a ``True`` steps count is a schema drift, not a 1)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_ts_smart(text: str | None, tz: str) -> int:
    """A vendor time string → unix ms, or ``0`` when it cannot be parsed
    (never "now": a fabricated time files a reading under the wrong day).

    1. An explicit non-UTC offset is taken as is.
    2. A date-only string, or a midnight with no offset, is the user's local
       day start — that is what ``"day": "2026-06-01"`` means.
    3. Anything else is UTC.
    """
    if not text:
        return 0
    try:
        if "T" in text:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            dt = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return 0
    if dt.tzinfo is not None and dt.utcoffset() and int(dt.utcoffset().total_seconds()) != 0:
        return int(dt.timestamp() * MS)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return int(dt.replace(tzinfo=zone(tz)).timestamp() * MS)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * MS)


def epoch_to_ms(value: Any) -> int:
    """Seconds or milliseconds since the epoch → ms; ``0`` when not a number."""
    n = number(value)
    if n is None or n <= 0:
        return 0
    return int(n) if n > 1e11 else int(n * MS)


def local_day_window(ts_ms: int, tz: str) -> tuple[int, int]:
    """``(start, end)`` of the local calendar day containing ``ts_ms``; the
    end is the last millisecond of that day, so a daily summary's span lies
    inside the day it summarises."""
    d = datetime.fromtimestamp(ts_ms / MS, zone(tz)).date()
    start, end = day_bounds_ms(d, tz)
    return start, end - 1


def fact(
    metric: str,
    value: float | None,
    start_ms: int,
    end_ms: int = 0,
    *,
    text: str = "",
    ingested_at_ms: int = 0,
    source_record_id: str = "",
    panel_id: str = "",
) -> Fact:
    """A fact in the catalogue's unit for ``metric`` — the tables only say
    how to convert *into* it, so no decoder can disagree with the aggregator
    about what unit a value is in. Unknown metrics raise: a typo in a mapping
    table is a programming error, not a data-quality event."""
    return Fact(
        metric_key=metric,
        value_num=value,
        effective_start_ms=start_ms,
        effective_end_ms=end_ms if end_ms and end_ms != start_ms else 0,
        value_text=text,
        unit=metrics.METRICS[metric].standard_unit,
        ingested_at_ms=ingested_at_ms,
        source_record_id=source_record_id,
        panel_id=panel_id,
    )

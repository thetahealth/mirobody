"""Which local day an instant belongs to: one implementation.

Two production bugs share one shape: a reading filed under the wrong day
because one code path computed the day in the server's zone, another in the
user's, and a third by casting a naive timestamp. The fix is not a fourth
careful implementation but exactly one, and a column (`local_date`) that
every reader trusts because every writer got it from here.

The day boundary is `mirobody.kernel.series.local_date`: the sleep family
opens its day at 18:00, and a DST day is 23 or 25 hours long. This module
adds what the kernel leaves to the consumer: which zone, and how trustworthy
that zone is.

A zone is one of two shapes. An IANA name ("Asia/Shanghai") knows about DST
and is what a person's profile stores. A fixed offset ("UTC+08:00") is what a
device or a file often carries, and a reading stamped with one is placed
correctly on its own day but cannot be projected across a DST change. The
`tz_source` column says which shape a row was placed with, so a reader can
tell an exact day from a best-effort one.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mirobody.kernel import metrics
from mirobody.kernel.series import local_date

TZ_IANA = "iana"
TZ_OFFSET = "offset-only"
TZ_FLOATING = "floating"
TZ_USER_DEFAULT = "user-default"

_OFFSET = re.compile(r"^(?:UTC|GMT)?\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)


def zone_for(tz: str) -> tzinfo:
    """A `tzinfo` for an IANA name or a `UTC±HH:MM` offset. Raises on
    anything else: a zone this cannot place must be resolved through
    `resolve_tz` first, never defaulted here."""
    text = (tz or "").strip()
    if not text or text.upper() in ("UTC", "Z", "GMT"):
        return UTC
    m = _OFFSET.match(text)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        delta = timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0))
        return timezone(sign * delta)
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"not a time zone: {text!r}") from e


def resolve_tz(text: str | None, user_tz: str) -> tuple[str, str]:
    """`(tz, tz_source)` for what a source said about its zone.

    An IANA name is kept. An offset ("+08:00", "GMT-0700") is spelled
    `UTC±HH:MM` and marked `offset-only`. Nothing usable falls back to the
    person's own zone, marked `user-default`, and to UTC with the same mark
    when the person has none: the fallback is recorded, never silent.
    """
    candidate = (text or "").strip()
    if candidate:
        m = _OFFSET.match(candidate)
        if m:
            hours, minutes = int(m.group(2)), int(m.group(3) or 0)
            return f"UTC{m.group(1)}{hours:02d}:{minutes:02d}", TZ_OFFSET
        try:
            ZoneInfo(candidate)
            return candidate, TZ_IANA
        except (ZoneInfoNotFoundError, ValueError):
            pass
    fallback = (user_tz or "").strip()
    try:
        ZoneInfo(fallback or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        fallback = "UTC"
    return fallback or "UTC", TZ_USER_DEFAULT


def window_for(name: str) -> str:
    """The local-day window of a catalogue metric, `"00:00"` for anything
    else. The aggregate writers suffix the source (`totalSleepTime.apple`),
    so the head before the first dot is tried too."""
    metric = metrics.METRICS.get(name)
    if metric is None and "." in name:
        metric = metrics.METRICS.get(name.split(".", 1)[0])
    return metric.window if metric else "00:00"


def local_day(instant: datetime, tz: str, window: str = "00:00") -> date:
    """The local day of `instant` in `tz`, through `window`.

    A naive `instant` is taken to be wall clock IN `tz`, which is what a lab
    report's "2026-03-04 08:15" means; an aware one is converted. Either way
    the day comes from the kernel's boundary rule, so a sleep stage at 02:00
    lands on the night that opened at 18:00 the day before.
    """
    zone = zone_for(tz)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=zone)
    if isinstance(zone, ZoneInfo):
        return local_date(int(instant.timestamp() * 1000), tz, window)
    # `series.local_date` takes an IANA name; a fixed offset is placed here
    # with the same window rule.
    local = instant.astimezone(zone)
    hh, mm = (int(p) for p in window.split(":"))
    day = local.date()
    if (local.hour, local.minute) < (hh, mm):
        day = day - timedelta(days=1)
    return day


__all__ = [
    "TZ_FLOATING",
    "TZ_IANA",
    "TZ_OFFSET",
    "TZ_USER_DEFAULT",
    "local_day",
    "resolve_tz",
    "window_for",
    "zone_for",
]

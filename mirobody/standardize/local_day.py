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

from datetime import date, datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mirobody.kernel import metrics
from mirobody.kernel.series import local_date, offset_name, zone

TZ_IANA = "iana"
TZ_OFFSET = "offset-only"
TZ_FLOATING = "floating"
TZ_USER_DEFAULT = "user-default"


def zone_for(tz: str) -> tzinfo:
    """A `tzinfo` for an IANA name or a `UTC±HH:MM` offset (the kernel's
    `series.zone`, strict). Raises on anything else: a zone this cannot
    place must be resolved through `resolve_tz` first, never defaulted here."""
    return zone(tz, strict=True)


def resolve_tz(text: str | None, user_tz: str) -> tuple[str, str]:
    """`(tz, tz_source)` for what a source said about its zone.

    An IANA name is kept. An offset ("+08:00", "GMT-0700") is spelled
    `UTC±HH:MM` and marked `offset-only`. Nothing usable falls back to the
    person's own zone, marked `user-default`, and to UTC with the same mark
    when the person has none: the fallback is recorded, never silent.
    """
    candidate = (text or "").strip()
    if candidate:
        offset = offset_name(candidate)
        if offset:
            return offset, TZ_OFFSET
        try:
            ZoneInfo(candidate)
            return candidate, TZ_IANA
        except (ZoneInfoNotFoundError, ValueError):
            pass
    # The person's zone may itself be an offset; it read as no zone at all.
    fallback = (user_tz or "").strip()
    try:
        zone(fallback, strict=True)
    except ValueError:
        fallback = "UTC"
    return offset_name(fallback) or fallback or "UTC", TZ_USER_DEFAULT


def window_for(name: str) -> str:
    """The local-day window of a catalogue metric, `"00:00"` for anything
    else."""
    metric = metrics.METRICS.get(name)
    return metric.window if metric else "00:00"


def local_day(instant: datetime, tz: str, window: str = "00:00") -> date:
    """The local day of `instant` in `tz`, through `window`.

    A naive `instant` is taken to be wall clock IN `tz`, which is what a lab
    report's "2026-03-04 08:15" means; an aware one is converted. Either way
    the day comes from the kernel's boundary rule, so a sleep stage at 02:00
    lands on the night that opened at 18:00 the day before.
    """
    tzone = zone_for(tz)  # raises on a zone it cannot place, before the kernel would read it as UTC
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=tzone)
    return local_date(int(instant.timestamp() * 1000), tz, window)


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

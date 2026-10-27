"""Which local day an instant belongs to.

The module docstring records two production bugs of the same shape: a reading
filed under the wrong day because one path computed the day in the server's
zone, another in the user's, and a third by casting a naive timestamp. The fix
was one implementation, so the tests pin the three behaviours that a fourth
implementation would get wrong:

  the sleep family opens its day at 18:00, so 01:30 belongs to the PREVIOUS date
  the same instant is a different date in a different zone
  a zone it cannot place raises, it is never silently defaulted
"""

from __future__ import annotations

from datetime import datetime, UTC
from zoneinfo import ZoneInfo

from mirobody.standardize.local_day import local_day, resolve_tz, zone_for

NEW_YORK = "America/New_York"
SHANGHAI = "Asia/Shanghai"


def at(y, mo, d, h, mi, tz=NEW_YORK):
    """An instant written in the local wall clock, returned as UTC."""
    return datetime(y, mo, d, h, mi, tzinfo=ZoneInfo(tz)).astimezone(UTC)


def test_calendar_day_is_the_local_date():
    """With the default window a day is simply the local calendar date."""
    assert local_day(at(2024, 3, 11, 0, 30), NEW_YORK) == datetime(2024, 3, 11).date()
    assert local_day(at(2024, 3, 11, 23, 59), NEW_YORK) == datetime(2024, 3, 11).date()


def test_sleep_window_opens_at_eighteen():
    """A sleep day runs 18:00 to 18:00 and is named for the evening it began."""
    assert local_day(at(2024, 3, 11, 17, 59), NEW_YORK, window="18:00") == datetime(2024, 3, 10).date()
    assert local_day(at(2024, 3, 11, 18, 0), NEW_YORK, window="18:00") == datetime(2024, 3, 11).date()
    # 01:30 is the middle of the night that began on the 11th, not the 12th
    assert local_day(at(2024, 3, 12, 1, 30), NEW_YORK, window="18:00") == datetime(2024, 3, 11).date()


def test_the_same_instant_is_a_different_date_in_another_zone():
    """23:30 UTC is still the 10th in New York and already the 11th in Shanghai."""
    instant = datetime(2024, 3, 10, 23, 30, tzinfo=UTC)
    assert local_day(instant, NEW_YORK) == datetime(2024, 3, 10).date()
    assert local_day(instant, SHANGHAI) == datetime(2024, 3, 11).date()


def test_a_fixed_offset_places_the_day_but_is_tagged_as_such():
    """A device that ships `UTC+08:00` is placed correctly, and the caller can
    tell it was an offset rather than an IANA zone."""
    tz, source = resolve_tz("UTC+08:00", "UTC")
    assert tz == "UTC+08:00"
    assert source == "offset-only"
    instant = datetime(2024, 3, 10, 23, 30, tzinfo=UTC)
    assert local_day(instant, tz) == datetime(2024, 3, 11).date()


def test_missing_zone_falls_back_to_the_user_default_and_says_so():
    tz, source = resolve_tz(None, "Europe/Berlin")
    assert tz == "Europe/Berlin"
    assert source == "user-default"


def test_an_unplaceable_zone_raises_instead_of_defaulting():
    """The dangerous failure is a silent UTC default: it files readings under
    the wrong day without anyone noticing."""
    for bad in ("Mars/Base", "GMT+25:00", "not-a-zone"):
        try:
            zone_for(bad)
        except Exception:
            continue
        raise AssertionError(f"{bad!r} was accepted; it must raise")


def test_dst_day_still_assigns_sleep_nights_correctly():
    """America/New_York loses an hour on 2024-03-10; the night that began on
    the 9th and the one that began on the 10th must not collapse into one."""
    nightly = [local_day(at(2024, 3, d, 22, 0), NEW_YORK, window="18:00") for d in (9, 10, 11)]
    assert nightly == [datetime(2024, 3, 9).date(), datetime(2024, 3, 10).date(),
                       datetime(2024, 3, 11).date()], nightly


if __name__ == "__main__":  # runs without pytest
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  PASS {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL {name}: {str(exc)[:180]}")
        except Exception as exc:
            failures += 1
            print(f"  ERROR {name}: {exc!r}")
    raise SystemExit(1 if failures else 0)

"""Turning vendor-shaped values into ours.

Two conversions, shared by every provider because every vendor gets
them differently:

* timestamps — epoch seconds/millis, ISO-8601 with and without offset, and
  local wall-clock with a separate offset field, all reduced to one form;
* source names — the free-text device/app label a vendor reports, normalized
  so `Garmin Connect`, `garmin_connect` and `GarminConnect` are one source.

Was a 369-line module called `utils` holding five helper classes (an
encryption class, an HTTP-helper class, and a validation class among them), of
which three had no callers anywhere and five further methods were
unreachable. Removing them also removed this file's `Crypto` import, which
was the only thing pulling pycryptodome into the provider import path.
"""

import logging
import time
from collections.abc import Iterable
from datetime import datetime, UTC
from zoneinfo import ZoneInfo

from mirobody.pulse.ingest.models.requests import StandardPulseRecord
from mirobody.kernel.series import Fact

logger = logging.getLogger(__name__)


def records_from_facts(facts: Iterable[Fact], *, slug: str, tz: str, source_id: str = "") -> list[StandardPulseRecord]:
    """``mirobody.kernel.vendors`` facts → the ingest records this platform stores.

    A fact's ``effective_start_ms`` is the record timestamp; an interval fact
    (sleep stage, daily summary, workout) also carries ``startTime``/``endTime``
    so the aggregator can attribute it to the right local day.
    """
    source = DataFormatter.format_source_name(slug)
    out: list[StandardPulseRecord] = []
    for f in facts:
        value: float | str = f.value_num if f.value_num is not None else f.value_text
        out.append(StandardPulseRecord(
            source=source,
            type=f.metric_key,
            timestamp=f.effective_start_ms,
            unit=f.unit,
            value=value,
            timezone=tz,
            source_id=source_id,
            startTime=f.effective_start_ms if f.is_interval else None,
            endTime=f.effective_end_ms if f.is_interval else None,
        ))
    return out


class TimeUtils:

    @staticmethod
    def parse_time_to_timestamp(time_str: str | None) -> int:
        try:
            if not time_str:
                return int(datetime.now(UTC).timestamp() * 1000)
            try:
                num = float(time_str)
                return int(num * 1000) if num <= 1e10 else int(num)
            except (ValueError, TypeError):
                pass

            if isinstance(time_str, (int, float)):
                if time_str > 1e10:
                    return int(time_str)
                return int(time_str * 1000)

            if isinstance(time_str, str):
                if time_str.endswith("Z"):
                    time_str = time_str[:-1] + "+00:00"

                dt = datetime.fromisoformat(time_str)
                return int(dt.timestamp() * 1000)

            return int(datetime.now(UTC).timestamp() * 1000)

        except Exception as e:
            logger.error(f"Error parsing time {time_str}: {str(e)}")
            return int(datetime.now(UTC).timestamp() * 1000)


    @staticmethod
    def parse_timestamp_with_smart_timezone(
        timestamp_str: str,
        effective_timezone: str
    ) -> int:
        """
        Smart parse timestamp string to UTC millisecond timestamp.

        Three core logic branches:
        1. Non-UTC timezone in timestamp → use directly
        2. Virtual date boundary (00:00:00) or pure date → use effective_timezone
        3. Real time point with UTC/no timezone → treat as UTC

        This solves the problem where vendor APIs return date-only strings
        (e.g., Oura "day": "2026-03-06") that should be interpreted in the
        user's local timezone, not the server's local timezone.
        """
        try:
            if "T" in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d")

            if dt.tzinfo:
                offset_seconds = int(dt.tzinfo.utcoffset(dt).total_seconds())
            else:
                offset_seconds = 0

            # Logic 1: Non-UTC timezone → use directly
            if offset_seconds != 0:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
                return int(dt_utc.timestamp() * 1000)

            # Logic 2: Virtual date boundary (00:00:00) or pure date → use effective_timezone
            is_virtual_date_boundary = (dt.hour == 0 and dt.minute == 0 and dt.second == 0)
            if is_virtual_date_boundary:
                dt_naive = dt.replace(tzinfo=None)
                dt_with_tz = dt_naive.replace(tzinfo=ZoneInfo(effective_timezone))
                dt_utc = dt_with_tz.astimezone(ZoneInfo("UTC"))
                return int(dt_utc.timestamp() * 1000)

            # Logic 3: Real time point with UTC/no timezone → treat as UTC
            if dt.tzinfo:
                dt_utc = dt.astimezone(ZoneInfo("UTC"))
            else:
                dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
            return int(dt_utc.timestamp() * 1000)

        except Exception as e:
            logger.error(
                f"Error parsing timestamp {timestamp_str} with timezone {effective_timezone}: {e}"
            )
            return int(time.time() * 1000)


class DataFormatter:


    @staticmethod
    def format_source_name(provider_slug: str, device_info: str | None = None) -> str:
        base_source = f"theta.{provider_slug}"
        if device_info:
            return f"{base_source}.{device_info}"
        return base_source


__all__ = [
    "TimeUtils",
    "DataFormatter",
    "records_from_facts",
]

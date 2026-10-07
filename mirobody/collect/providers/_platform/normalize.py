"""Decoded facts as the records this platform stores, under a source name.

Every provider turns its vendor's payload into `series.Fact`s with
`mirobody.kernel.decoders` and hands them to `records_from_facts`; the source
name each record carries is `DataFormatter.format_source_name`'s.
"""

from collections.abc import Iterable

from mirobody.collect.ingest import StandardPulseRecord
from mirobody.kernel.series import Fact


def records_from_facts(
    facts: Iterable[Fact], *, slug: str, tz: str, source_id: str = "", source: str = ""
) -> list[StandardPulseRecord]:
    """``mirobody.kernel.decoders`` facts → the ingest records this platform stores.

    A fact's ``effective_start_ms`` is the record timestamp; an interval fact
    (sleep stage, daily summary, workout) also carries ``startTime``/``endTime``
    so the aggregator can attribute it to the right local day.

    ``source`` overrides the slug-derived name. Apple needs it: its rows say
    ``apple_health`` or ``apple_health_watch`` depending on where the sample
    came from, and `th_series_data` has been written that way all along.
    """
    source = source or DataFormatter.format_source_name(slug)
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


class DataFormatter:

    @staticmethod
    def format_source_name(provider_slug: str, device_info: str | None = None) -> str:
        """`theta.<slug>`, or `theta.<slug>.<device>`: the `source` a pulled
        record is stored under. `theta` is the platform's persisted name (see
        `ProviderPlatform.name`), so stored rows and this string must agree."""
        base_source = f"theta.{provider_slug}"
        if device_info:
            return f"{base_source}.{device_info}"
        return base_source


__all__ = [
    "DataFormatter",
    "records_from_facts",
]

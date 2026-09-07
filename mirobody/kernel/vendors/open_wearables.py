"""open-wearables API responses → facts. Pure; field paths are theirs.

[open-wearables](https://github.com/the-momentum/open-wearables) (MIT) is a
device ACCESS platform: twelve provider strategies, four mobile SDKs,
webhooks, multi-account sync, a developer portal. It is very good at the half
of the problem this project deliberately does not solve, and its own
standardization document is honest about the half it does not solve — no
terminology system, unit labels rather than a unit engine, an offset rather
than a time zone.

So this decoder is the seam between the two: **run open-wearables to connect
devices, run mirobody to make the data mean one thing and answer questions
about it.** A deployment that needs eight vendors and a phone SDK gets them
there, and gets UCUM units, LOINC where it is public, day windows that survive
daylight saving, quality gates, medications and the query tools here.

## The four things that need care

* **`type` is a name, not a code.** `res/crosswalks/open_wearables.tsv` is the
  93-row table; 44 rows map, 49 decline WITH A REASON. An unmapped type is
  quarantined, never guessed into a neighbouring metric.
* **`unit` is a label, not a unit.** The engine knows seventeen of their
  twenty-six spellings; the crosswalk carries the rest. The one that bites is
  VO2 max: open-wearables publishes mL/kg/min and this catalogue carries
  L/min/kg, so a value passed through unconverted is out by a factor of a
  thousand — which looks like a plausible number, not like an error.
* **`zone_offset` is an offset, not a zone.** `+08:00` cannot say whether a
  day was 23, 24 or 25 hours long, so the caller supplies the IANA zone name
  and the offset is kept only to place the instant.
* **`is_daily_total`** marks a vendor's own daily figure. Those are
  `provider_daily`: projected, never averaged again and never summed with the
  detail rows they summarise.

Adding this decoder does not make mirobody an access platform, and it is not
meant to: the connectors, the Celery orchestration, the portal and the mobile
SDKs stay theirs.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from importlib import resources

from .. import metrics
from ... import units
from ..series import Fact
from ._common import number, parse_ts_smart

#: The API shapes this module reads. `timeseries` is the paged sample feed;
#: `sleep` and `workouts` are the event endpoints.
DATA_TYPES: tuple[str, ...] = ("timeseries", "sleep", "workouts")

#: Sources that AGGREGATE other devices rather than measure. Their rows are
#: still real, but `series.elect` must not rank a phone's re-export of a watch
#: above the watch.
AGGREGATOR_SOURCES = frozenset({"apple", "apple_health", "google", "google_fit", "health_connect", "samsung"})

#: `sleep_stages` labels → the catalogue's interval metrics.
SLEEP_STAGES: dict[str, str] = {
    "awake": "sleepAnalysis_Awake",
    "light": "sleepAnalysis_Asleep(Core)",
    "deep": "sleepAnalysis_Asleep(Deep)",
    "rem": "sleepAnalysis_Asleep(REM)",
    "in_bed": "sleepAnalysis_InBed",
    "asleep": "sleepAnalysis_Asleep(Total)",
    "unspecified": "sleepAnalysis_Asleep(Unspecified)",
}


@dataclass(frozen=True)
class Crosswalk:
    """One row of the type table."""

    ow_type: str
    ow_unit: str
    metric: str
    note: str

    @property
    def declined(self) -> bool:
        return not self.metric


def _load_crosswalk() -> dict[str, Crosswalk]:
    text = resources.files("mirobody").joinpath("res", "crosswalks", "open_wearables.tsv").read_text(encoding="utf-8")
    body = "".join(line for line in io.StringIO(text) if not line.startswith("#"))
    out: dict[str, Crosswalk] = {}
    for row in csv.DictReader(io.StringIO(body), delimiter="\t"):
        out[row["ow_type"]] = Crosswalk(
            row["ow_type"], row.get("ow_unit", ""), row.get("metric", "") or "", row.get("note", "") or ""
        )
    return out


CROSSWALK: dict[str, Crosswalk] = _load_crosswalk()

#: Their unit spellings that the UCUM engine does not recognise, and the UCUM
#: unit each one means. The engine is not taught these globally: they are one
#: platform's labels, not units, and a spelling table that leaks into the
#: engine is how `count` comes to mean something.
UNIT_ALIASES: dict[str, str] = {
    "percent": "%",
    "bpm": "/min",
    "brpm": "/min",
    "mg_dl": "mg/dL",
    "kg_m2": "kg/m2",
    "ml_kg_min": "mL/kg/min",
    "m_per_s": "m/s",
    "liters": "L",
    "meters": "m",
    "minutes": "min",
    "celsius": "Cel",
    "years": "a",
    "rpm": "/min",
    "met": "",       # a rate expressed as a multiple; not a unit
    "score": "",     # a vendor score; not a unit
    "count": "",     # dimensionless
    "degrees": "deg",
}

#: Every catalogue metric this decoder can emit — from the crosswalk plus the
#: sleep stages, so `connect.Coverage` cannot claim one it does not produce.
METRICS: frozenset[str] = frozenset(c.metric for c in CROSSWALK.values() if c.metric) | frozenset(
    SLEEP_STAGES.values()
)


def resolve_unit(ow_unit: str) -> str:
    """Their label → a UCUM unit, or `""` when the label is not a unit.

    Normalised on the way out, both for the aliases and for the spellings the
    engine already knows, so `mL/kg/min` and `mL/min/kg` are ONE unit here
    rather than two strings that happen to mean the same thing — which is the
    difference between a conversion happening and a value being relabelled.
    """
    alias = UNIT_ALIASES.get(ow_unit)
    raw = alias if alias is not None else ow_unit
    if not raw:
        return ""
    return units.normalize_unit(raw) or raw


def to_catalogue_unit(value: float, ow_unit: str, metric: str) -> tuple[float, str]:
    """`(value, unit)` in the unit the catalogue declares for `metric`.

    This is the VO2 max trap, generalised: the conversion comes from the unit
    engine and the target comes from the catalogue, so the decoder cannot hold
    an opinion about either and cannot disagree with the aggregator.
    """
    row = metrics.METRICS.get(metric)
    target = (row.unit_ucum if row else "") or ""
    source = resolve_unit(ow_unit)
    if not target or not source or source == target:
        return value, target or source
    if units.convertible(source, target):
        converted = units.convert_value(value, source, target)
        if converted is not None:
            return converted, target
    # An unconvertible pair is left ALONE and reported in its own unit rather
    # than silently relabelled: a wrong unit on a right number is the failure
    # mode this whole path exists to prevent.
    return value, source


def series_key(source: dict) -> str:
    """The physical stream: `ow:{provider}:{source}:{device}`.

    Their `DataSource` identity is user + provider + device model + source, and
    keeping all of it is what stops two devices' curves being averaged
    together.
    """
    parts = [str(source.get(k) or "") for k in ("provider", "source", "device")]
    return "ow:" + ":".join(parts)


def source_is_aggregator(source: dict) -> bool:
    return str(source.get("provider") or source.get("source") or "").lower() in AGGREGATOR_SOURCES


def decode(
    data_type: str,
    item: dict,
    tz: str,
    *,
    pulled_at_ms: int = 0,
    source_record_id: str = "",
    ingested_at_ms: int = 0,
) -> list[Fact]:
    """One open-wearables record → facts. `[]` for a shape this does not read
    and for a type the crosswalk declines."""
    if data_type == "timeseries":
        return _timeseries(item, tz, source_record_id, ingested_at_ms or pulled_at_ms)
    if data_type == "sleep":
        return _sleep(item, tz, source_record_id, ingested_at_ms or pulled_at_ms)
    if data_type == "workouts":
        return _workout(item, tz, source_record_id, ingested_at_ms or pulled_at_ms)
    return []


def _instant_ms(item: dict, tz: str) -> int:
    """The sample's instant. `zone_offset` places it; the ZONE is the caller's,
    because an offset cannot say how long a day was.

    A record with no timestamp decodes to nothing. It is never stamped with
    `now()` — a synthetic time is indistinguishable from a measured one
    afterwards, and one of the platforms this reads from does exactly that.
    """
    raw = item.get("timestamp") or item.get("recorded_at") or ""
    offset = str(item.get("zone_offset") or "")
    if isinstance(raw, str) and offset and len(raw) >= 19 and raw[-1] not in "Zz" and "+" not in raw[10:]:
        raw = raw + offset
    return parse_ts_smart(raw, tz)


def _timeseries(item: dict, tz: str, record_id: str, ingested_at_ms: int) -> list[Fact]:
    row = CROSSWALK.get(str(item.get("type") or ""))
    if row is None or row.declined:
        return []
    value = number(item.get("value"))
    at = _instant_ms(item, tz)
    if value is None or not at:
        return []
    value, unit = to_catalogue_unit(value, str(item.get("unit") or row.ow_unit), row.metric)
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    return [
        Fact(
            metric_key=row.metric,
            value_num=value,
            effective_start_ms=at,
            unit=unit,
            series_key=series_key(source),
            device_id=str(source.get("device") or ""),
            # Their `is_daily_total` is the same claim the catalogue spells
            # `provider_daily`: the vendor's own figure for the day, to be
            # projected and never averaged again.
            statistic="provider_daily" if item.get("is_daily_total") else "",
            modality="sensed",
            source_record_id=record_id or str(item.get("id") or ""),
            ingested_at_ms=ingested_at_ms,
        )
    ]


def _sleep(item: dict, tz: str, record_id: str, ingested_at_ms: int) -> list[Fact]:
    """A sleep event's stages → interval facts, one per span.

    Intervals rather than one total, because the union of the spans is what a
    night IS: two syncs of the same night overlap, and adding their totals
    counts it twice. `series.flatten_last_writer_wins` needs the spans.
    """
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    key = series_key(source)
    out: list[Fact] = []
    for stage in item.get("sleep_stages") or []:
        if not isinstance(stage, dict):
            continue
        metric = SLEEP_STAGES.get(str(stage.get("stage") or "").lower())
        start = parse_ts_smart(str(stage.get("start") or stage.get("start_time") or ""), tz)
        end = parse_ts_smart(str(stage.get("end") or stage.get("end_time") or ""), tz)
        if not metric or not start or end <= start:
            continue
        out.append(
            Fact(
                metric_key=metric,
                value_num=float(end - start),
                effective_start_ms=start,
                effective_end_ms=end,
                unit="ms",
                series_key=key,
                device_id=str(source.get("device") or ""),
                source_record_id=record_id or str(item.get("id") or ""),
                ingested_at_ms=ingested_at_ms,
            )
        )
    return out


def _workout(item: dict, tz: str, record_id: str, ingested_at_ms: int) -> list[Fact]:
    """A workout's own numbers. The workout ITSELF is an event with a type and
    a name, and this catalogue has no vocabulary for that — so the numbers
    come through and the event does not, rather than being flattened into a
    metric called `workout`."""
    source = item.get("source") if isinstance(item.get("source"), dict) else {}
    key = series_key(source)
    start = parse_ts_smart(str(item.get("start") or item.get("start_time") or ""), tz)
    end = parse_ts_smart(str(item.get("end") or item.get("end_time") or ""), tz)
    if not start:
        return []
    out: list[Fact] = []
    for field, metric in (("energy", "activeCalories"), ("distance", "dailyDistance"), ("average_heart_rate", "heartRates")):
        value = number(item.get(field))
        if value is None:
            continue
        out.append(
            Fact(
                metric_key=metric,
                value_num=value,
                effective_start_ms=start,
                effective_end_ms=end if end > start else 0,
                unit=(metrics.METRICS[metric].unit_ucum if metric in metrics.METRICS else ""),
                series_key=key,
                device_id=str(source.get("device") or ""),
                source_record_id=record_id or str(item.get("id") or ""),
                ingested_at_ms=ingested_at_ms,
            )
        )
    return out


def declined_types() -> dict[str, str]:
    """The types this decoder does NOT map, and why. Published so a consumer
    can see the gaps rather than discover them as silence."""
    return {c.ow_type: c.note for c in CROSSWALK.values() if c.declined}


__all__ = [
    "AGGREGATOR_SOURCES",
    "CROSSWALK",
    "Crosswalk",
    "DATA_TYPES",
    "METRICS",
    "SLEEP_STAGES",
    "UNIT_ALIASES",
    "decode",
    "declined_types",
    "resolve_unit",
    "series_key",
    "source_is_aggregator",
    "to_catalogue_unit",
]

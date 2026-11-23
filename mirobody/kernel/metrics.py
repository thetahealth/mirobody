"""The device-metric catalogue: what a reading's *name* means.

``mirobody/res/catalog/metrics.tsv`` is the one table behind every "which indicator is
this, what unit does it carry, and how does a day of it summarise" question.
It used to be a 3,131-line Python enum in ``pulse/standardize/indicators_info``
(291 names, hand-written, with the aggregation *method* but never the *shape*),
duplicated as a 94-entry projection map plus a 495-row mapping seed in one
downstream repo and a third copy in another. The shape is what those copies
added and what this catalogue now carries:

- ``state_class``: the Home Assistant idea, extended for health data. It
  says what a day of raw points *is*, and therefore how it may be summarised:
  ``instant`` (heart rate, a weight: mean/min/max or the last reading),
  ``cumulative`` (steps as deltas: sum, never the mean of running totals),
  ``interval`` / ``session`` (sleep stages, a workout: the union of the
  spans, so overlapping segments are not counted twice), ``provider_daily``
  (a vendor's own daily figure: project it, never average it again).
- ``aggregation_policy``: the one policy ``series.aggregate`` applies to
  that class. The two columns are kept separate because a consumer may
  legitimately override the policy for one vendor whose stream has a
  different shape from everyone else's (a continuous SpO2 stream is
  ``instant``; a single nightly pulse-ox reading is ``last``).
- ``loinc`` and ``confidence``: a code is carried with the confidence of the
  judgement behind it, ``confident`` (the six LOINC axes agree with the
  vendor's definition) or ``unverified`` (close, and a person must confirm).
  Only a confident code is the row's identity; an unverified one stays
  visible here and in ``res/crosswalks/`` while the metric keeps its own
  namespace, ``("mirobody-device", name)``. A confident wrong code is worse
  than an honest namespace. The evidence per vendor field is in
  ``mirobody.standardize.devices``.
- ``window``: the local-day boundary. ``"18:00"`` for the sleep family, so
  a night is one day; the old aggregator found sleep with ``LIKE '%sleep%'``.

This module is part of the *library layer* (see the import-linter contracts
in ``pyproject.toml``): stdlib only, importable on a bare ``pip install
mirobody``. Display labels in other languages are NOT here: an
international project ships English names and lets each deployment inject
its own (``register_labels``). Per-vendor overrides of a metric's shape are
injected the same way (``register_overrides``); the catalogue never learns a
vendor's field paths.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, replace
from functools import lru_cache
from importlib import resources
from collections.abc import Iterator

#: Bumps whenever a row's identity fields change (name, unit_ucum, state_class,
#: aggregation_policy, loinc, window). Consumers that persist standardised
#: facts store it next to the row, so a later catalogue change is visible.
TERMINOLOGY_VERSION = "1.5.10"

#: The two confidences a code may carry; empty when the row has no code.
CONFIDENT = "confident"
UNVERIFIED = "unverified"
CONFIDENCES = frozenset({CONFIDENT, UNVERIFIED})

#: What a day of raw points is. The vocabulary ``series.aggregate`` is written
#: against; a consumer's stored rows carry these literal strings.
STATE_INSTANT = "instant"
STATE_CUMULATIVE = "cumulative"
STATE_INTERVAL = "interval"
STATE_SESSION = "session"
STATE_PROVIDER_DAILY = "provider_daily"
STATE_CLASSES = frozenset({STATE_INSTANT, STATE_CUMULATIVE, STATE_INTERVAL, STATE_SESSION, STATE_PROVIDER_DAILY})

#: How that day is summarised.
POLICY_MEAN_MIN_MAX = "mean_min_max"
POLICY_LAST = "last"
POLICY_SUM_DELTA = "sum_delta"
POLICY_DURATION_UNION = "duration_union"
POLICY_PROVIDER_VALUE = "provider_value"
POLICIES = frozenset({POLICY_MEAN_MIN_MAX, POLICY_LAST, POLICY_SUM_DELTA, POLICY_DURATION_UNION, POLICY_PROVIDER_VALUE})

#: The only policies a state class admits. Anything else is a typo, and a typo
#: here silently turns a step count into "the last delta of the day".
LEGAL_POLICIES: dict[str, frozenset[str]] = {
    STATE_INSTANT: frozenset({POLICY_MEAN_MIN_MAX, POLICY_LAST}),
    STATE_CUMULATIVE: frozenset({POLICY_SUM_DELTA}),
    STATE_INTERVAL: frozenset({POLICY_DURATION_UNION}),
    STATE_SESSION: frozenset({POLICY_DURATION_UNION}),
    STATE_PROVIDER_DAILY: frozenset({POLICY_PROVIDER_VALUE}),
}

#: Terminology systems a metric's identity can live in.
SYSTEM_LOINC = "loinc"
SYSTEM_DEVICE = "mirobody-device"

#: The three classes ``metrics.tsv`` carries for how a row's value is typed:
#: kept from the enum this replaces, because the aggregator and the Apple
#: upload path branch on them.
DATA_SUMMARY = "summary"
DATA_SERIES = "series"
DATA_MIX = "mix"


@dataclass(frozen=True)
class Metric:
    """One row of the catalogue. ``name`` is the stable identity every stored
    reading carries (``"heartRates"``); ``member`` is the enum spelling the
    reference implementation still exposes (``"HEART_RATE"``)."""

    member: str
    name: str
    display: str
    category: str
    standard_unit: str
    unit_ucum: str
    data_type: str
    state_class: str
    aggregation_policy: str
    loinc: str = ""
    confidence: str = ""
    window: str = "00:00"
    panel: str = ""
    aggregation_methods: tuple[str, ...] = ()
    derived: tuple[str, ...] = ()
    description: str = ""
    since: str = TERMINOLOGY_VERSION

    @property
    def canonical(self) -> tuple[str, str]:
        """``(system, code)``: the LOINC code when one is known with confidence,
        otherwise this catalogue's own namespace, so two writers that both
        lack a code still agree on the identity. An unverified code is not
        an identity: a reading filed under it could not be told from a
        correct one."""
        if self.loinc and self.confidence == CONFIDENT:
            return (SYSTEM_LOINC, self.loinc)
        return (SYSTEM_DEVICE, self.name)


@dataclass(frozen=True)
class Mapping:
    """A vendor's metric key resolved to a catalogue row, with the shape that
    applies to *that vendor's* stream."""

    system: str
    metric_key: str
    metric: Metric
    state_class: str
    aggregation_policy: str
    unit_ucum: str


def _read_tsv(name: str) -> list[dict[str, str]]:
    text = resources.files("mirobody").joinpath("res", name).read_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(text), delimiter="\t"))


def _split(cell: str) -> tuple[str, ...]:
    return tuple(part for part in cell.split(",") if part) if cell else ()


def _load() -> tuple[list[Metric], dict[str, Metric], dict[str, Metric]]:
    rows = [
        Metric(
            member=r["member"],
            name=r["name"],
            display=r["display"],
            category=r["category"],
            standard_unit=r["standard_unit"],
            unit_ucum=r["unit_ucum"],
            data_type=r["data_type"],
            state_class=r["state_class"],
            aggregation_policy=r["aggregation_policy"],
            loinc=r.get("loinc", ""),
            confidence=r.get("confidence", ""),
            window=r.get("window") or "00:00",
            panel=r.get("panel", ""),
            aggregation_methods=_split(r.get("aggregation_methods", "")),
            derived=_split(r.get("derived", "")),
            description=r.get("description", ""),
            since=r.get("since") or TERMINOLOGY_VERSION,
        )
        for r in _read_tsv("catalog/metrics.tsv")
    ]
    by_member: dict[str, Metric] = {}
    by_name: dict[str, Metric] = {}
    for m in rows:
        if m.state_class not in STATE_CLASSES:
            raise ValueError(f"metrics.tsv: {m.member}: unknown state_class {m.state_class!r}")
        if m.aggregation_policy not in LEGAL_POLICIES[m.state_class]:
            raise ValueError(f"metrics.tsv: {m.member}: {m.aggregation_policy!r} is not a policy for {m.state_class}")
        if m.loinc and m.confidence not in CONFIDENCES:
            raise ValueError(f"metrics.tsv: {m.member}: a code needs a confidence, {m.confidence!r} is not one")
        if m.confidence and not m.loinc:
            raise ValueError(f"metrics.tsv: {m.member}: confidence {m.confidence!r} without a code")
        if m.member in by_member:
            raise ValueError(f"metrics.tsv: duplicate member {m.member}")
        by_member[m.member] = m
        # A name may be declared under several members (five sleep stages are:
        # the same Apple stage arrived under three historical spellings). The
        # first declaration is the identity; the others stay reachable by member.
        previous = by_name.setdefault(m.name, m)
        decisive = ("standard_unit", "unit_ucum", "data_type", "state_class",
                    "aggregation_policy", "loinc", "confidence", "window")
        if previous is not m and any(getattr(previous, field) != getattr(m, field) for field in decisive):
            raise ValueError(f"metrics.tsv: conflicting identity for name {m.name!r}")
    return rows, by_member, by_name


ROWS, MEMBERS, METRICS = _load()

#: Category display names (English), in catalogue order.
CATEGORIES: tuple[str, ...] = tuple(dict.fromkeys(m.category for m in ROWS))

_overrides: dict[tuple[str, str], Mapping] = {}
_labels: dict[str, dict[str, tuple[str, str]]] = {}


def canonical(name: str) -> tuple[str, str]:
    """``(system, code)`` for a catalogue name; an unknown name lands in the
    device namespace under its own spelling rather than raising: a reading
    with an unknown name is still a reading."""
    m = METRICS.get(name)
    return m.canonical if m else (SYSTEM_DEVICE, name)


#: ``grouping`` -> the prefix a generated summary name begins with. Read by
#: ``build_indicator_name`` below and by the Apple statistics upload path
#: (``collect/providers/apple/statistics_service.py``), which receives a
#: phone's own ``hour``/``day``/``week``/``month`` pre-aggregated figure and
#: must name it the same way this catalogue's own ``day`` rollup does.
GROUPING_PREFIX: dict[str, str] = {
    "hour": "hourly",
    "day": "daily",
    "week": "weekly",
    "month": "monthly",
}

#: A method's name is its CamelCase (``pct_below_70`` -> ``PctBelow70``),
#: except ``sum``, which names the same figure as ``total`` (Apple's
#: statistics upload sends ``sum``, the catalogue declares ``total``).
_METHOD_NAME_MAP: dict[str, str] = {"sum": "Total"}


def _snake_to_camel(s: str) -> str:
    return "".join(part.capitalize() for part in s.split("_"))


def build_indicator_name(grouping: str, method: str, source_indicator: str) -> str:
    """A generated summary name from a time grouping, an aggregation method
    and the series it summarises: ``("day", "avg", "heartRates")`` ->
    ``"dailyAvgHeartRates"``.

    This is the ONE naming rule both ends of the aggregator use:
    ``standardize.aggregate.rule_generator`` writes a stored row's name with
    it, and ``daily_aggregate_metric`` below reads a stored name back with
    it. Two copies of this function is exactly how #333 happened (the read
    side had no way back to a generated name's source at all); it lives here,
    in the library layer, because both the write side (``[app]``) and
    anything reading a device-coded name (``standardize.names``, pure) need
    to call the same code.

    Raises ``ValueError`` on an unknown grouping.
    """
    prefix = GROUPING_PREFIX.get(grouping)
    if prefix is None:
        raise ValueError(f"Unknown grouping: {grouping!r}, expected one of {list(GROUPING_PREFIX)}")
    method_capitalized = _METHOD_NAME_MAP.get(method) or _snake_to_camel(method)
    source_capitalized = source_indicator[0].upper() + source_indicator[1:] if source_indicator else ""
    return f"{prefix}{method_capitalized}{source_capitalized}"


#: English/Chinese words for every aggregation method a SERIES row's
#: ``aggregation_methods`` column (``res/catalog/metrics.tsv``) declares, used
#: only to NAME a generated ``daily{Method}{Indicator}`` row: the catalogue
#: carries no row for one (it is ``rule_generator``'s output, never a
#: curator's), so its label is composed from the source metric's own label
#: plus the statistic it names. Kept exhaustive (20 methods in the TSV today)
#: by ``tests/test_daily_aggregate_catalog.py``, which resolves every
#: generated name the catalogue can produce; a method added to the TSV
#: without a word here fails that test, not silently.
_METHOD_WORD: dict[str, tuple[str, str]] = {
    "avg": ("Average", "平均"),
    "max": ("Max", "最高"),
    "min": ("Min", "最低"),
    "last": ("Latest", "最新"),
    "total": ("Total", "总"),
    "sum": ("Total", "总"),
    "stddev": ("Std Dev", "标准差"),
    "time_of_max": ("Time of Max", "最高值时刻"),
    "time_of_min": ("Time of Min", "最低值时刻"),
    "sleep_onset_latency": ("Sleep Onset Latency", "入睡潜伏期"),
    "hypo_event_count": ("Hypo Event Count", "低血糖事件次数"),
    "hypo_event_times": ("Hypo Event Times", "低血糖事件时间"),
    "hypo_event_details": ("Hypo Event Details", "低血糖事件详情"),
    "pct_above_140": ("% Above 140", "高于140占比"),
    "pct_above_180": ("% Above 180", "高于180占比"),
    "pct_below_70": ("% Below 70", "低于70占比"),
    "tir_70_140": ("Time in Range 70-140", "70-140范围内时间占比"),
    "tir_70_180": ("Time in Range 70-180", "70-180范围内时间占比"),
    "gmi_14d": ("14-Day GMI", "14天GMI"),
    "morning_hr_jump": ("Morning Heart Rate Jump", "晨起心率跃升"),
    "nighttime_resting_hr": ("Nighttime Resting Heart Rate", "夜间静息心率"),
}


@lru_cache(maxsize=1)
def _daily_aggregate_sources() -> dict[str, tuple[Metric, str]]:
    """``daily{Method}{Indicator}`` -> ``(source metric, method)``, for every
    rule ``standardize.aggregate.rule_generator`` can generate: one entry per
    aggregation method a SERIES row declares (the same ``data_type`` filter
    `rule_generator` applies, so a ``mix`` row's methods, which it skips, are
    skipped here too). This is the read side's only map back to a generated
    name's source, built from the catalogue over the same naming rule the
    write side uses, so the two cannot drift apart."""
    out: dict[str, tuple[Metric, str]] = {}
    for m in ROWS:
        if m.data_type != DATA_SERIES or not m.aggregation_methods:
            continue
        for method in m.aggregation_methods:
            out[build_indicator_name("day", method, m.name)] = (m, method)
    return out


def daily_aggregate_metric(name: str) -> tuple[Metric, str] | None:
    """The source metric and aggregation method behind a generated
    ``daily{Method}{Indicator}`` name (a ``.source`` suffix the aggregator
    appended is stripped first). ``None`` for a name no SERIES row's
    ``aggregation_methods`` produces -- including a hand-curated ``summary``
    row such as ``dailyTotalSleepTime``, which is already its own catalogue
    entry under ``METRICS`` and needs no mapping back."""
    return _daily_aggregate_sources().get(name.split(".", 1)[0])


def daily_aggregate_label(name: str, *, chinese: bool) -> str:
    """The label a reader sees for a generated daily name: the source
    metric's own registered label with the statistic it names prefixed --
    ``每日最低血氧饱和度`` / ``Daily Min Blood Oxygen`` for
    ``dailyMinOxygenSaturations``, read off ``oxygenSaturations``. ``""`` when
    ``name`` is not one ``daily_aggregate_metric`` resolves, or its method has
    no registered word (``_METHOD_WORD``).

    Composed on every call rather than registered in ``labels/<locale>.tsv``:
    a generated name has no ``member`` of its own to register one under, and
    there is one of these for every (metric, method) pair the catalogue can
    produce, not one a curator wrote by hand."""
    found = daily_aggregate_metric(name)
    if found is None:
        return ""
    source, method = found
    words = _METHOD_WORD.get(method)
    if words is None:
        return ""
    en_word, zh_word = words
    if chinese:
        return f"每日{zh_word}{label(source.member, 'zh') or source.display}"
    return f"Daily {en_word} {source.display}"


def register_overrides(system: str, table: dict[str, tuple[str, str, str]]) -> None:
    """Teach the catalogue how one vendor's metric keys map, when the default
    (``metric_key == name``, shape from the catalogue) is wrong for it.

    ``table`` maps the vendor's own key (a field path such as
    ``"spo2.saturation"``) to ``(name, state_class, aggregation_policy)``.
    A vendor whose continuous SpO2 stream must not be summarised like another
    vendor's single nightly reading registers exactly that here. The table is
    the consumer's; the catalogue never ships a vendor's field paths.
    """
    for key, (name, state_class, policy) in table.items():
        metric = METRICS.get(name)
        if metric is None:
            raise KeyError(f"register_overrides({system!r}): {name!r} is not a catalogue name")
        if state_class not in STATE_CLASSES or policy not in LEGAL_POLICIES[state_class]:
            raise ValueError(f"register_overrides({system!r}): {key!r}: illegal shape {state_class}/{policy}")
        _overrides[(system, key)] = Mapping(system, key, metric, state_class, policy, metric.unit_ucum)


def mapping_for(system: str, metric_key: str) -> Mapping | None:
    """The catalogue row and shape for one vendor's metric key.

    Overrides registered for ``system`` win; otherwise the key is taken to be
    a catalogue name (which is what Garmin, Whoop and Oura emit). ``None``
    means "not a known metric": the caller decides whether that is a
    quarantine, a wait-for-mapping, or a drop. It is never a guess.
    """
    hit = _overrides.get((system, metric_key))
    if hit is not None:
        return hit
    metric = METRICS.get(metric_key)
    if metric is None:
        return None
    return Mapping(system, metric_key, metric, metric.state_class, metric.aggregation_policy, metric.unit_ucum)


def register_labels(locale: str, table: dict[str, tuple[str, str]]) -> None:
    """Attach display labels in one language: ``{name_or_member_or_category:
    (label, description)}``. The catalogue is English; a deployment that
    serves another language registers its own file (the reference
    application ships ``res/catalog/labels/zh.tsv`` as an example)."""
    _labels.setdefault(locale, {}).update(table)


def label(key: str, locale: str, default: str = "") -> str:
    """The registered label for a name/member/category, or ``default``."""
    hit = _labels.get(locale, {}).get(key)
    return hit[0] if hit and hit[0] else default


def description(key: str, locale: str, default: str = "") -> str:
    hit = _labels.get(locale, {}).get(key)
    return hit[1] if hit and hit[1] else default


def load_labels_resource(locale: str) -> None:
    """Register the labels shipped under ``res/catalog/labels/<locale>.tsv``
    (``kind, key, label, description`` rows). Called by the reference
    application, not by the library."""
    table: dict[str, tuple[str, str]] = {}
    for r in _read_tsv(f"catalog/labels/{locale}.tsv"):
        table[r["key"]] = (r.get("label", ""), r.get("description", ""))
    register_labels(locale, table)


def with_shape(metric: Metric, state_class: str, policy: str) -> Metric:
    """A copy of ``metric`` with another shape: for a consumer building its
    own override table from catalogue rows."""
    if policy not in LEGAL_POLICIES[state_class]:
        raise ValueError(f"{policy!r} is not a policy for {state_class}")
    return replace(metric, state_class=state_class, aggregation_policy=policy)


def iter_names() -> Iterator[str]:
    return iter(METRICS)


__all__ = [
    "TERMINOLOGY_VERSION",
    "STATE_INSTANT", "STATE_CUMULATIVE", "STATE_INTERVAL", "STATE_SESSION", "STATE_PROVIDER_DAILY", "STATE_CLASSES",
    "POLICY_MEAN_MIN_MAX", "POLICY_LAST", "POLICY_SUM_DELTA", "POLICY_DURATION_UNION", "POLICY_PROVIDER_VALUE", "POLICIES",
    "LEGAL_POLICIES", "SYSTEM_LOINC", "SYSTEM_DEVICE", "DATA_SUMMARY", "DATA_SERIES", "DATA_MIX",
    "Metric", "Mapping", "ROWS", "MEMBERS", "METRICS", "CATEGORIES",
    "canonical", "mapping_for", "register_overrides", "register_labels", "label", "description",
    "load_labels_resource", "with_shape", "iter_names",
    "GROUPING_PREFIX", "build_indicator_name", "daily_aggregate_metric", "daily_aggregate_label",
]

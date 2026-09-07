"""The device-metric catalogue — what a reading's *name* means.

``mirobody/res/metrics.tsv`` is the one table behind every "which indicator is
this, what unit does it carry, and how does a day of it summarise" question.
It used to be a 3,131-line Python enum in ``pulse/standardize/indicators_info``
(291 names, hand-written, with the aggregation *method* but never the *shape*),
duplicated as a 94-entry projection map plus a 495-row mapping seed in one
downstream repo and a third copy in another. The shape is what those copies
added and what this catalogue now carries:

- ``state_class`` — the Home Assistant idea, extended for health data. It
  says what a day of raw points *is*, and therefore how it may be summarised:
  ``instant`` (heart rate, a weight: mean/min/max or the last reading),
  ``cumulative`` (steps as deltas: sum, never the mean of running totals),
  ``interval`` / ``session`` (sleep stages, a workout: the union of the
  spans, so overlapping segments are not counted twice), ``provider_daily``
  (a vendor's own daily figure: project it, never average it again).
- ``aggregation_policy`` — the one policy ``series.aggregate`` applies to
  that class. The two columns are kept separate because a consumer may
  legitimately override the policy for one vendor whose stream has a
  different shape from everyone else's (a continuous SpO2 stream is
  ``instant``; a single nightly pulse-ox reading is ``last``).
- ``loinc`` — only where the code is public and undisputed; otherwise the
  metric's identity is ``("mirobody-device", name)``. A confident wrong code
  is worse than an honest namespace.
- ``window`` — the local-day boundary. ``"18:00"`` for the sleep family, so
  a night is one day; the old aggregator found sleep with ``LIKE '%sleep%'``.

This module is part of the *library layer* (see the import-linter contracts
in ``pyproject.toml``): stdlib only, importable on a bare ``pip install
mirobody``. Display labels in other languages are NOT here — an
international project ships English names and lets each deployment inject
its own (``register_labels``). Per-vendor overrides of a metric's shape are
injected the same way (``register_overrides``); the catalogue never learns a
vendor's field paths.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, replace
from importlib import resources
from collections.abc import Iterator

#: Bumps whenever a row's identity fields change (name, unit_ucum, state_class,
#: aggregation_policy, loinc, window). Consumers that persist standardised
#: facts store it next to the row, so a later catalogue change is visible.
TERMINOLOGY_VERSION = "1.4.0"

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

#: The two classes ``metrics.tsv`` carries for how a row's value is typed —
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
    window: str = "00:00"
    panel: str = ""
    aggregation_methods: tuple[str, ...] = ()
    derived: tuple[str, ...] = ()
    description: str = ""
    since: str = TERMINOLOGY_VERSION

    @property
    def canonical(self) -> tuple[str, str]:
        """``(system, code)``: the LOINC code when one is known, otherwise this
        catalogue's own namespace — so two writers that both lack a code still
        agree on the identity."""
        return (SYSTEM_LOINC, self.loinc) if self.loinc else (SYSTEM_DEVICE, self.name)


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
            window=r.get("window") or "00:00",
            panel=r.get("panel", ""),
            aggregation_methods=_split(r.get("aggregation_methods", "")),
            derived=_split(r.get("derived", "")),
            description=r.get("description", ""),
            since=r.get("since") or TERMINOLOGY_VERSION,
        )
        for r in _read_tsv("metrics.tsv")
    ]
    by_member: dict[str, Metric] = {}
    by_name: dict[str, Metric] = {}
    for m in rows:
        if m.state_class not in STATE_CLASSES:
            raise ValueError(f"metrics.tsv: {m.member}: unknown state_class {m.state_class!r}")
        if m.aggregation_policy not in LEGAL_POLICIES[m.state_class]:
            raise ValueError(f"metrics.tsv: {m.member}: {m.aggregation_policy!r} is not a policy for {m.state_class}")
        if m.member in by_member:
            raise ValueError(f"metrics.tsv: duplicate member {m.member}")
        by_member[m.member] = m
        # A name may be declared under several members (five sleep stages are:
        # the same Apple stage arrived under three historical spellings). The
        # first declaration is the identity; the others stay reachable by member.
        by_name.setdefault(m.name, m)
    return rows, by_member, by_name


ROWS, MEMBERS, METRICS = _load()

#: Category display names (English), in catalogue order.
CATEGORIES: tuple[str, ...] = tuple(dict.fromkeys(m.category for m in ROWS))

_overrides: dict[tuple[str, str], Mapping] = {}
_labels: dict[str, dict[str, tuple[str, str]]] = {}


def canonical(name: str) -> tuple[str, str]:
    """``(system, code)`` for a catalogue name; an unknown name lands in the
    device namespace under its own spelling rather than raising — a reading
    with an unknown name is still a reading."""
    m = METRICS.get(name)
    return m.canonical if m else (SYSTEM_DEVICE, name)


def register_overrides(system: str, table: dict[str, tuple[str, str, str]]) -> None:
    """Teach the catalogue how one vendor's metric keys map, when the default
    (``metric_key == name``, shape from the catalogue) is wrong for it.

    ``table`` maps the vendor's own key — a field path such as
    ``"spo2.saturation"`` — to ``(name, state_class, aggregation_policy)``.
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
    means "not a known metric" — the caller decides whether that is a
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
    application ships ``res/labels/zh.tsv`` as an example)."""
    _labels.setdefault(locale, {}).update(table)


def label(key: str, locale: str, default: str = "") -> str:
    """The registered label for a name/member/category, or ``default``."""
    hit = _labels.get(locale, {}).get(key)
    return hit[0] if hit and hit[0] else default


def description(key: str, locale: str, default: str = "") -> str:
    hit = _labels.get(locale, {}).get(key)
    return hit[1] if hit and hit[1] else default


def load_labels_resource(locale: str) -> None:
    """Register the labels shipped under ``res/labels/<locale>.tsv``
    (``kind, key, label, description`` rows). Called by the reference
    application, not by the library."""
    table: dict[str, tuple[str, str]] = {}
    for r in _read_tsv(f"labels/{locale}.tsv"):
        table[r["key"]] = (r.get("label", ""), r.get("description", ""))
    register_labels(locale, table)


def with_shape(metric: Metric, state_class: str, policy: str) -> Metric:
    """A copy of ``metric`` with another shape — for a consumer building its
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
]

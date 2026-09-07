"""Time-series kernel: what a day of readings is, and how it summarises.

Every consumer of this library ends up writing the same four things around
its own tables — where a local day starts and ends (with the sleep family
starting at 18:00 and a DST day lasting 23 or 25 hours), how a day of points
collapses to one number (which depends on what the points *are*, see
``metrics.state_class``), how two devices measuring the same thing are
reconciled, and how overlapping spans are counted without counting a minute
twice. Three repositories wrote them independently; two of them shipped the
same bug (a duplicate sync doubling a night's sleep). This module is those
four things once, as pure functions over plain values: no database, no
clock, no framework — so the same input gives the same output forever, and
a golden vector can pin every rule.

What is deliberately NOT here: tables, queues, cursors, which source a
product trusts, or any threshold. Those are the consumer's; this module takes
them as arguments.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from . import metrics
#: Bump when a formula or the day-boundary rule changes. Rows a consumer stores
#: carry it, so a replay after a change is visible instead of silent.
AGGREGATION_VERSION = 1
#: Bump when the ranking criteria or their order semantics change.
ARBITRATION_VERSION = 1

#: The statistic a daily projection carries. Kept as literal strings because
#: consumers persist them; a reader must be able to tell "this is the min of
#: heart rate" without decoding a free-text indicator name.
AGG_TYPE_MEAN = "mean"
AGG_TYPE_MIN = "min"
AGG_TYPE_MAX = "max"
AGG_TYPE_SUM = "sum"
AGG_TYPE_LAST = "last"
AGG_TYPE_DURATION = "duration"
AGG_TYPE_PROVIDER = "provider_value"

_MS_PER_DAY = 86_400_000
_MS_PER_MINUTE = 60_000


# ---------------------------------------------------------------------------
# The fact
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Fact:
    """One standardised reading, before or after storage. Field names follow
    Open mHealth / IEEE 1752 (``effective_*``, ``modality``) so a consumer
    exporting to that vocabulary maps fields by name.

    ``effective_end_ms == 0`` or ``== effective_start_ms`` means an instant.
    ``series_key`` names the physical stream (provider/device/account): two
    devices' steps must never be summed together, so the key is part of
    every window. ``panel_id`` ties readings measured together (a blood
    pressure's two numbers, a body-composition scale's thirteen).
    ``ingested_at_ms`` orders revisions of the same span — the later sync
    wins when they disagree.
    """

    metric_key: str
    value_num: float | None
    effective_start_ms: int
    effective_end_ms: int = 0
    value_text: str = ""
    unit: str = ""
    series_key: str = ""
    device_id: str = ""
    panel_id: str = ""
    statistic: str = ""
    modality: str = "sensed"
    source_record_id: str = ""
    ingested_at_ms: int = 0

    @property
    def is_interval(self) -> bool:
        return self.effective_end_ms > self.effective_start_ms

    @property
    def end_ms(self) -> int:
        return self.effective_end_ms if self.is_interval else self.effective_start_ms


# ---------------------------------------------------------------------------
# Time: the local day
# ---------------------------------------------------------------------------


def zone(name: str) -> ZoneInfo:
    """A ``ZoneInfo`` for ``name``; UTC for an empty or unknown name.

    The fallback is deliberate but the caller should log it: silently
    landing in UTC shifts every day boundary of that user, which is how a
    "today's sleep is always empty" report starts.
    """
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def _window_offset(window: str) -> tuple[int, int]:
    hh, mm = window.split(":")
    return int(hh), int(mm)


def day_bounds_ms(day: date, tz: str, window: str = "00:00") -> tuple[int, int]:
    """``[start, end)`` in unix ms of the local day ``day`` — the day that
    starts at ``window`` (``"18:00"`` for the sleep family) in ``tz``.

    Both ends are computed on the wall clock and converted separately. On a
    DST transition day that gives 23 or 25 hours; adding 86,400,000 ms to the
    start would give 24 and misplace every reading after the shift.
    """
    z = zone(tz)
    hh, mm = _window_offset(window)
    start = datetime(day.year, day.month, day.day, hh, mm, tzinfo=z)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def local_date(ms: int, tz: str, window: str = "00:00") -> date:
    """The local day (per ``day_bounds_ms``) that contains the instant ``ms``.

    With ``window="18:00"`` an instant at 02:00 belongs to the *previous*
    calendar date — the night is one day. This is the BEDTIME-day convention
    (the day the window opened); see ``display_day`` for the wake-day view
    products usually show.
    """
    dt = datetime.fromtimestamp(ms / 1000, zone(tz))
    hh, mm = _window_offset(window)
    d = dt.date()
    if (dt.hour, dt.minute) < (hh, mm):
        d = d - timedelta(days=1)
    return d


def display_day(day: date, window: str = "00:00") -> date:
    """The calendar date a product shows a windowed day under. A sleep day
    that opens at 18:00 on D is the night the user *wakes from* on D+1, and
    that is the date every wearable app labels it with. One implementation,
    so no reader adds its own ``+1`` — or forgets to."""
    return day + timedelta(days=1) if window != "00:00" else day


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


def round_meaningful(value: float, significant: int = 4) -> float:
    """Round to ``significant`` significant figures: ``71.47413793103448``
    becomes ``71.47``, ``12345.0`` stays ``12345.0``, ``0.0034`` stays.

    Significant figures, not fixed decimals: two decimals would erase a
    0.0034 as 0.00. Not ``%.4g``: that writes 12345 steps as ``1.234e+04``.
    The seventeen trailing digits of a mean are IEEE 754, not the device.
    """
    if value == 0 or value != value or value in (math.inf, -math.inf):
        return value
    decimals = max(0, min(6, significant - int(math.floor(math.log10(abs(value)))) - 1))
    return round(value, decimals)


def union_spans(spans: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or touching ``(start, end)`` spans into a disjoint,
    sorted list. Empty and inverted spans are dropped."""
    ordered = sorted((s, e) for s, e in spans if e > s)
    out: list[tuple[int, int]] = []
    for s, e in ordered:
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


def merge_intervals(spans: Iterable[tuple[int, int]]) -> int:
    """Total covered duration of the union of ``spans`` — never more than the
    plain sum, never less than the longest single span. Sleep stages from two
    syncs of the same night overlap; summing them counts the night twice."""
    return sum(e - s for s, e in union_spans(spans))


def stable_hash(*parts: object) -> str:
    """32 hex chars of SHA-256 over the parts, joined so that ``("a", "bc")``
    and ``("ab", "c")`` differ. No timestamps, no dict order — the same
    inputs give the same fingerprint on every machine, forever."""
    joined = "\x1f".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# A day of facts → one projection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Projection:
    """One summarised day of one metric from one stream.

    ``fingerprint`` describes the value actually published (after rounding),
    so two runs that round to the same number agree and a downstream
    consumer can skip work — and wake nothing — when it has not changed.
    """

    aggregation_type: str
    value: float
    unit: str
    start_ms: int
    end_ms: int
    sample_count: int
    fingerprint: str
    inputs: dict[str, float | int] = field(default_factory=dict)


def aggregate(
    policy: str,
    facts: Sequence[Fact],
    *,
    day: date,
    tz: str,
    window: str = "00:00",
    unit: str = "",
    versions: tuple[int, ...] = (AGGREGATION_VERSION,),
) -> Projection | None:
    """Collapse the facts of one window into one projection under ``policy``
    (a ``metrics.POLICY_*`` value). ``None`` means the window holds nothing
    to publish — the caller retracts whatever it had.

    The policy is passed in, not inferred: the shape of a stream is a
    catalogue decision (``metrics.mapping_for``), and a vendor override may
    legitimately differ from the metric's default.

    - ``mean_min_max``: mean is the value; min/max/count ride in ``inputs``.
    - ``sum_delta``: the day's total of deltas. Never hand this a running
      total — eight thousand steps become thirty thousand.
    - ``duration_union``: minutes covered by the union of the spans.
    - ``provider_value``: the vendor's own daily figure — the last revision
      received (by ``ingested_at_ms``, then by time), never a mean of them.
    - ``last``: the latest measurement of the day.
    """
    nums = [f for f in facts if f.value_num is not None]
    if not nums and policy != metrics.POLICY_DURATION_UNION:
        return None
    start_ms, end_ms = day_bounds_ms(day, tz, window)
    unit = unit or next((f.unit for f in nums if f.unit), "")
    inputs: dict[str, float | int] = {}

    if policy == metrics.POLICY_MEAN_MIN_MAX:
        values = [f.value_num for f in nums]  # type: ignore[misc]
        value = sum(values) / len(values)
        agg_type = AGG_TYPE_MEAN
        inputs = {"min": min(values), "max": max(values), "count": len(values)}
    elif policy == metrics.POLICY_SUM_DELTA:
        value = sum(f.value_num for f in nums)  # type: ignore[misc]
        agg_type = AGG_TYPE_SUM
        inputs = {"count": len(nums)}
    elif policy == metrics.POLICY_DURATION_UNION:
        spans = [(f.effective_start_ms, f.end_ms) for f in facts if f.is_interval]
        if not spans:
            return None
        value = merge_intervals(spans) / _MS_PER_MINUTE
        unit = "min"
        agg_type = AGG_TYPE_DURATION
        inputs = {"count": len(spans)}
    elif policy == metrics.POLICY_PROVIDER_VALUE:
        latest = max(nums, key=lambda f: (f.ingested_at_ms, f.effective_start_ms))
        value = latest.value_num  # type: ignore[assignment]
        agg_type = AGG_TYPE_PROVIDER
        inputs = {"count": len(nums)}
    elif policy == metrics.POLICY_LAST:
        latest = max(nums, key=lambda f: (f.effective_start_ms, f.ingested_at_ms))
        value = latest.value_num  # type: ignore[assignment]
        agg_type = AGG_TYPE_LAST
        inputs = {"count": len(nums)}
    else:
        raise ValueError(f"unknown aggregation policy {policy!r}")

    value = round_meaningful(float(value))
    inputs = {k: (round_meaningful(v) if isinstance(v, float) else v) for k, v in inputs.items()}
    fingerprint = stable_hash(agg_type, unit, round(value, 6), inputs.get("count", 0), *versions)
    return Projection(agg_type, value, unit, start_ms, end_ms, int(inputs.get("count", 0)), fingerprint, inputs)


@dataclass(frozen=True)
class Bucket:
    start_ms: int
    value: float
    count: int


def downsample(points: Iterable[tuple[int, float]], bucket_ms: int, method: str = AGG_TYPE_MEAN) -> list[Bucket]:
    """Regular buckets over ``(ms, value)`` points — for an intraday view
    that must not ship 86,400 heart-rate samples, and for the archive tier
    that keeps a month of them at minute resolution. ``method`` is
    ``mean``/``min``/``max``/``sum``/``last``."""
    if bucket_ms <= 0:
        raise ValueError("bucket_ms must be positive")
    groups: dict[int, list[float]] = {}
    for ms, v in points:
        groups.setdefault((ms // bucket_ms) * bucket_ms, []).append(v)
    out: list[Bucket] = []
    for start in sorted(groups):
        vs = groups[start]
        if method == AGG_TYPE_MEAN:
            val = sum(vs) / len(vs)
        elif method == AGG_TYPE_MIN:
            val = min(vs)
        elif method == AGG_TYPE_MAX:
            val = max(vs)
        elif method == AGG_TYPE_SUM:
            val = sum(vs)
        elif method == AGG_TYPE_LAST:
            val = vs[-1]
        else:
            raise ValueError(f"unknown downsample method {method!r}")
        out.append(Bucket(start, round_meaningful(val), len(vs)))
    return out


# ---------------------------------------------------------------------------
# Overlapping spans from several syncs → one timeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Segment:
    """A labelled span from one ingestion batch: a sleep stage, a workout
    zone. ``ingested_at_ms`` is the recency signal — a later batch that
    re-classifies the same minutes is presumed to supersede the earlier one."""

    start_ms: int
    end_ms: int
    label: str
    ingested_at_ms: int = 0
    batch_id: str = ""


def flatten_last_writer_wins(segments: Sequence[Segment]) -> dict[str, int]:
    """Total milliseconds per label after flattening every segment onto one
    timeline, where each elementary sub-interval belongs to the most recently
    ingested segment covering it.

    Why not union per label: a later sync often splits one earlier "light
    sleep" block into light/awake/light; unioning each label on its own
    still counts the awake minutes twice. Flattening assigns every instant to
    exactly one label, so the per-label totals sum to at most the covered
    span. A gap covered by no segment contributes to nothing.
    """
    if not segments:
        return {}
    bounds = sorted({s.start_ms for s in segments} | {s.end_ms for s in segments})
    totals: dict[str, int] = {}
    for a, b in zip(bounds, bounds[1:], strict=False):
        if b <= a:
            continue
        covering = [s for s in segments if s.start_ms <= a and s.end_ms >= b]
        if not covering:
            continue
        winner = max(covering, key=lambda s: (s.ingested_at_ms, s.batch_id))
        totals[winner.label] = totals.get(winner.label, 0) + (b - a)
    return totals


# ---------------------------------------------------------------------------
# Several sources for one day → one answer
# ---------------------------------------------------------------------------

#: What kind of thing a source is. A scale *measures* weight; a wearable's
#: profile *echoes* the weight the user typed in months ago, on every sync,
#: with today's timestamp. Both look like "a weight reading today".
SOURCE_MEASURER = "measurer"
SOURCE_PROFILE_ECHO = "profile_echo"
SOURCE_AGGREGATOR = "aggregator"
SOURCE_MANUAL = "manual"


@dataclass
class Candidate:
    """One source's offer for one cell (subject, group of members, day)."""

    identity: str
    source_class: str
    values: dict[str, float | str] = field(default_factory=dict)
    measured_at_ms: int | None = None
    coverage_ms: float = 0.0
    echo_changed: bool | None = None

    @property
    def is_information(self) -> bool:
        """An unchanged profile echo carries no information about *today*."""
        return not (self.source_class == SOURCE_PROFILE_ECHO and self.echo_changed is False)


Criterion = Callable[[Candidate, Sequence[str]], tuple]


def _key_source_class(c: Candidate, _: Sequence[str]) -> tuple:
    if c.source_class == SOURCE_MEASURER:
        return (0,)
    if c.source_class == SOURCE_MANUAL:
        return (1,)
    if c.echo_changed:
        return (2,)
    return (3,)


def _key_coverage(c: Candidate, _: Sequence[str]) -> tuple:
    return (-(c.coverage_ms or 0.0),)


def _key_freshness(c: Candidate, _: Sequence[str]) -> tuple:
    # The measurement instant, never the row's update time: an echo is
    # re-written daily and looks fresh by update time while being stale.
    return (-(c.measured_at_ms or 0),)


def _key_static_priority(c: Candidate, priorities: Sequence[str]) -> tuple:
    try:
        rank = list(priorities).index(c.identity)
    except ValueError:
        rank = len(priorities)
    return (rank, c.identity)


CRITERIA: dict[str, Criterion] = {
    "source_class": _key_source_class,
    "coverage": _key_coverage,
    "measurement_freshness": _key_freshness,
    "static_priority": _key_static_priority,
}

DEFAULT_RANKING: tuple[str, ...] = ("source_class", "coverage", "measurement_freshness", "static_priority")


def annotate_echo(candidates: Iterable[Candidate], previous: dict[str, dict[str, float | str]]) -> None:
    """Mark each profile-echo candidate as changed or unchanged against the
    values the same identity offered last time. Only a *changed* echo is an
    event; an unchanged one is the same fact wearing today's date."""
    for c in candidates:
        if c.source_class != SOURCE_PROFILE_ECHO:
            continue
        prev = previous.get(c.identity)
        c.echo_changed = prev is None or any(_differs(prev.get(k), v) for k, v in c.values.items())


def _differs(a: object, b: object) -> bool:
    try:
        return not math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return a != b


def rank_candidates(
    candidates: Sequence[Candidate],
    ranking: Sequence[str] = DEFAULT_RANKING,
    priorities: Sequence[str] = (),
) -> list[Candidate]:
    """Sort candidates best-first by the named criteria in order. Unknown
    criterion names raise — a misspelled ranking must not silently become
    "alphabetical by identity"."""
    for name in ranking:
        if name not in CRITERIA:
            raise ValueError(f"unknown ranking criterion {name!r}")
    return sorted(candidates, key=lambda c: tuple(k for name in ranking for k in CRITERIA[name](c, priorities)))


Validator = Callable[[Candidate], list[str]]


def coverage_bound(member: str, span_ms: float, *, per_unit_ms: float = _MS_PER_MINUTE, tolerance: float = 1.0) -> Validator:
    """A validator that rejects a candidate whose ``member`` total exceeds
    the span it could possibly cover (times ``tolerance``). A reported total
    sleep time longer than the night itself is arithmetic, not opinion."""

    def check(c: Candidate) -> list[str]:
        v = c.values.get(member)
        try:
            total_ms = float(v) * per_unit_ms  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return []
        return [f"{member} exceeds covered span"] if total_ms > span_ms * tolerance else []

    return check


@dataclass(frozen=True)
class Decision:
    """The outcome for one cell. ``elected is None`` with an empty
    ``blocked`` means the cell carries no information (nothing to publish);
    with a non-empty ``blocked`` every candidate failed validation and the
    caller keeps whatever it published before and raises an alert."""

    elected: Candidate | None
    ranked: list[Candidate]
    blocked: dict[str, list[str]]
    fingerprint: str


def elect(
    candidates: Sequence[Candidate],
    *,
    ranking: Sequence[str] = DEFAULT_RANKING,
    priorities: Sequence[str] = (),
    validators: Sequence[Validator] = (),
    previous: dict[str, dict[str, float | str]] | None = None,
    versions: tuple[int, ...] = (ARBITRATION_VERSION,),
) -> Decision:
    """Pick the source a cell publishes from.

    Rank by ``ranking`` (the group's policy — *what* to prefer is the
    consumer's decision, *how* to prefer it is this function), then take the
    best candidate that passes every validator and carries information. The
    fingerprint covers the ranked identities and their values, so a rerun
    with the same inputs is a no-op for the caller.
    """
    cands = list(candidates)
    if previous is not None:
        annotate_echo(cands, previous)
    ranked = rank_candidates(cands, ranking, priorities)
    blocked: dict[str, list[str]] = {}
    elected: Candidate | None = None
    for c in ranked:
        if not c.is_information:
            continue
        reasons = [r for v in validators for r in v(c)]
        if reasons:
            blocked[c.identity] = reasons
            continue
        elected = c
        break
    fp = stable_hash(*versions, *(f"{c.identity}={sorted(c.values.items())}" for c in ranked))
    return Decision(elected, ranked, blocked, fp)


__all__ = [
    "AGGREGATION_VERSION", "ARBITRATION_VERSION",
    "AGG_TYPE_MEAN", "AGG_TYPE_MIN", "AGG_TYPE_MAX", "AGG_TYPE_SUM", "AGG_TYPE_LAST", "AGG_TYPE_DURATION", "AGG_TYPE_PROVIDER",
    "Fact", "zone", "day_bounds_ms", "local_date", "display_day",
    "round_meaningful", "union_spans", "merge_intervals", "stable_hash",
    "Projection", "aggregate", "Bucket", "downsample",
    "Segment", "flatten_last_writer_wins",
    "SOURCE_MEASURER", "SOURCE_PROFILE_ECHO", "SOURCE_AGGREGATOR", "SOURCE_MANUAL",
    "Candidate", "CRITERIA", "DEFAULT_RANKING", "annotate_echo", "rank_candidates", "coverage_bound", "Decision", "elect",
]

"""Statistics produce the evidence; a model only narrates it.

Everything a summary or an insight may *claim* — "resting heart rate is 2σ
above its baseline", "no data for six of the last fourteen days" — is
computed here by pure functions and handed to the model as facts. The model
never sees raw rows and decides for itself what is remarkable, and its
narration is checked afterwards against rules it cannot argue with.

The kernel carries **no thresholds and no word lists**: every function takes
a policy or a rule set the consumer builds. A "2σ" or a banned word is a
product decision that differs between a consumer wellness app and a clinical
tool, and a default baked in here would be the wrong one for one of them.
Pure; stdlib only.
"""

from __future__ import annotations

import re
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

# --- detections --------------------------------------------------------------------

KIND_DEVIATION = "deviation"  # the latest value is far from its baseline
KIND_TREND = "trend"  # the recent mean drifted from the baseline mean
KIND_RECOVERY = "recovery"  # an excursion that has come back
KIND_GAP = "gap"  # no data for a stretch

SEVERITY_MILD = "mild"
SEVERITY_MODERATE = "moderate"
SEVERITY_SEVERE = "severe"


@dataclass(frozen=True)
class DayValue:
    day: date
    value: float


@dataclass(frozen=True)
class DetectPolicy:
    """Every number :func:`detect` uses. No defaults: the consumer decides.

    ``baseline_days``: minimum number of baseline days before anything is
    said. ``recent_days``: the tail compared against the baseline.
    ``sigma_moderate``/``sigma_severe``: deviation thresholds in baseline
    standard deviations. ``trend_pct``: relative drift of the recent mean
    that counts as a trend (0.15 = 15%). ``recovery_days``/``recovery_sigma``:
    how many closing days within how many σ close an excursion.
    ``gap_days``: a run of missing days at least this long is a gap.
    """

    baseline_days: int
    recent_days: int
    sigma_moderate: float
    sigma_severe: float
    trend_pct: float
    recovery_days: int
    recovery_sigma: float
    gap_days: int

    def __post_init__(self) -> None:
        if self.baseline_days < 2 or self.recent_days < 1 or self.recovery_days < 1 or self.gap_days < 1:
            raise ValueError("detect policy counts must be positive (baseline_days >= 2)")
        if not 0 < self.sigma_moderate <= self.sigma_severe:
            raise ValueError("0 < sigma_moderate <= sigma_severe")
        if not 0 < self.trend_pct < 1 or not 0 < self.recovery_sigma <= self.sigma_moderate:
            raise ValueError("trend_pct in (0,1); 0 < recovery_sigma <= sigma_moderate")


@dataclass(frozen=True)
class Detection:
    """One thing worth saying, with the numbers that back it. ``evidence``
    holds only numbers and dates — never text a model wrote."""

    indicator: str
    kind: str
    severity: str
    day: date
    evidence: Mapping[str, float | str]


def _split(daily: Sequence[DayValue], today: date, recent_days: int) -> tuple[list[float], list[DayValue]]:
    cutoff = today - timedelta(days=recent_days - 1)  # the last `recent_days` days, today included
    base = [p.value for p in daily if p.day < cutoff]
    recent = sorted((p for p in daily if cutoff <= p.day <= today), key=lambda p: p.day)
    return base, recent


def detect(indicator: str, daily: Sequence[DayValue], today: date, policy: DetectPolicy) -> tuple[Detection, ...]:
    """Deterministic detections over one indicator's daily values.

    A flat baseline (σ = 0) uses a 1% band of the mean so a single different
    value is a deviation rather than a division by zero. Nothing is reported
    when the baseline is shorter than ``policy.baseline_days`` — a detection
    against three days of history is noise dressed as evidence.
    """
    base, recent = _split(daily, today, policy.recent_days)
    if len(base) < policy.baseline_days or not recent:
        return ()
    mean = statistics.fmean(base)
    std = statistics.pstdev(base) or (abs(mean) * 0.01 or 1.0)
    latest = recent[-1]
    z = (latest.value - mean) / std
    ev: dict[str, float | str] = {
        "baseline_mean": round(mean, 4),
        "baseline_std": round(std, 4),
        "baseline_days": len(base),
        "latest": latest.value,
        "z": round(z, 2),
    }
    out: list[Detection] = []
    if abs(z) >= policy.sigma_moderate:
        sev = SEVERITY_SEVERE if abs(z) >= policy.sigma_severe else SEVERITY_MODERATE
        out.append(Detection(indicator, KIND_DEVIATION, sev, latest.day, ev))
    recent_mean = statistics.fmean(p.value for p in recent)
    if mean and abs(recent_mean - mean) / abs(mean) > policy.trend_pct and abs(z) < policy.sigma_moderate:
        out.append(
            Detection(
                indicator, KIND_TREND, SEVERITY_MODERATE, latest.day, {**ev, "recent_mean": round(recent_mean, 4)}
            )
        )
    n = policy.recovery_days
    if len(recent) > n:
        head, tail = recent[:-n], recent[-n:]
        excursions = [p.value for p in head if abs((p.value - mean) / std) >= policy.sigma_moderate]
        if excursions and all(abs((p.value - mean) / std) <= policy.recovery_sigma for p in tail):
            peak = max(excursions, key=lambda v: abs(v - mean))
            out.append(Detection(indicator, KIND_RECOVERY, SEVERITY_MILD, latest.day, {**ev, "excursion_peak": peak}))
    return tuple(out)


@dataclass(frozen=True)
class Density:
    """How much of a window has data. ``coverage`` is days-with-data over
    days-in-window; ``longest_gap_days`` is the longest run without."""

    days_in_window: int
    days_with_data: int
    coverage: float
    longest_gap_days: int


def density(days_with_data: Sequence[date], window: tuple[date, date]) -> Density:
    lo, hi = window
    if hi < lo:
        raise ValueError("window end precedes start")
    total = (hi - lo).days + 1
    have = {d for d in days_with_data if lo <= d <= hi}
    longest = run = 0
    d = lo
    while d <= hi:
        run = 0 if d in have else run + 1
        longest = max(longest, run)
        d += timedelta(days=1)
    return Density(total, len(have), round(len(have) / total, 4), longest)


def gaps(
    indicator: str, days_with_data: Sequence[date], window: tuple[date, date], policy: DetectPolicy
) -> tuple[Detection, ...]:
    """Runs of missing days at least ``policy.gap_days`` long, as detections
    dated on the last missing day."""
    lo, hi = window
    have = set(days_with_data)
    out: list[Detection] = []
    run_start: date | None = None
    d = lo
    while d <= hi + timedelta(days=1):
        missing = d <= hi and d not in have
        if missing and run_start is None:
            run_start = d
        elif not missing and run_start is not None:
            length = (d - run_start).days
            if length >= policy.gap_days:
                out.append(
                    Detection(
                        indicator,
                        KIND_GAP,
                        SEVERITY_MILD,
                        d - timedelta(days=1),
                        {"gap_days": length, "gap_start": run_start.isoformat()},
                    )
                )
            run_start = None
        d += timedelta(days=1)
    return tuple(out)


@dataclass(frozen=True)
class Annotation:
    """A span laid next to readings on a timeline — a medication course, a
    trip, an illness. The kernel only carries it; what it means is the
    consumer's."""

    start: date
    end: date | None
    kind: str
    ref: str = ""


# --- narrative rules -------------------------------------------------------------

_SENTENCE_END = re.compile(r"[.!?。！？]+")


@dataclass(frozen=True)
class NarrativeRules:
    """What a model's narration may not do. ``banned`` and ``required_any``
    are the consumer's own word lists (any language); ``forbidden_numbers``
    are literal spellings of values already shown elsewhere, so the text
    may not merely echo them (see :func:`number_forms`)."""

    max_chars: int
    max_sentences: int
    banned: frozenset[str]
    forbidden_numbers: frozenset[str]
    required_any: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.max_chars < 1 or self.max_sentences < 1:
            raise ValueError("limits must be positive")


@dataclass(frozen=True)
class Violation:
    code: str  # empty | too_long | too_many_sentences | banned | echoes_value | missing_required
    detail: str = ""


def validate_narrative(text: str, rules: NarrativeRules) -> tuple[Violation, ...]:
    """Every rule the text breaks, in a stable order; ``()`` when it passes.
    Number matching is digit-bounded: a forbidden ``"2"`` does not fire on
    ``"2026"``."""
    body = (text or "").strip()
    if not body:
        return (Violation("empty"),)
    out: list[Violation] = []
    if len(body) > rules.max_chars:
        out.append(Violation("too_long", str(len(body))))
    sentences = [s for s in _SENTENCE_END.split(body) if s.strip()]
    if len(sentences) > rules.max_sentences:
        out.append(Violation("too_many_sentences", str(len(sentences))))
    for word in sorted(rules.banned):
        if word and word in body:
            out.append(Violation("banned", word))
    for number in sorted(rules.forbidden_numbers):
        if number and re.search(rf"(?<![\d.]){re.escape(number)}(?![\d.])", body):
            out.append(Violation("echoes_value", number))
    if rules.required_any and not any(w in body for w in rules.required_any):
        out.append(Violation("missing_required"))
    return tuple(out)


def number_forms(value: float) -> frozenset[str]:
    """The literal spellings a model is likely to use for ``value`` — integer,
    one decimal, thousands-separated — so :func:`validate_narrative` can
    forbid echoing a number without forbidding numbers."""
    out = set()
    if float(value).is_integer():
        n = int(value)
        out.update({str(n), f"{n:,}", f"{n}.0"})
    else:
        out.update({f"{value:g}", f"{value:.1f}", f"{value:,.1f}"})
    return frozenset(out)


__all__ = [
    "Annotation",
    "DayValue",
    "Density",
    "DetectPolicy",
    "Detection",
    "NarrativeRules",
    "Violation",
    "density",
    "detect",
    "gaps",
    "number_forms",
    "validate_narrative",
]

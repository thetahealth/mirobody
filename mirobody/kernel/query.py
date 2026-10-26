"""One read authority, one tool for readings.

Every surface that shows a person their own readings (the chat agent's tool,
an MCP client, a dashboard, a daily summary) reads through
:class:`HealthQuery`. The tool the model sees, ``query_health_indicators``, has
one JSON schema (:data:`TOOL_SCHEMA`) shared by the chat and MCP surfaces and
a dispatch table from ``view`` to the one ``HealthQuery`` method that answers
it. What the person reported (a symptom, a diagnosis) is
read through the same tool: same table, same series, coded on ICPC-3 instead
of LOINC. Medications are a different data class with a different grammar and
their own tool: :mod:`mirobody.kernel.meds`.

Five parameters, each one a decision only the question can make: what to
read (``keywords`` or ``indicators``), when (``start``/``end``), and in what
shape (``view``). The rest is decided by the system, not the model. Whose
record is the call's authentication, one person per call. How many rows is
this tool's budget (:data:`ROW_CAP`). Which values a statistic counts is the
write side's election (see ``collect/query.py::stats``). A baseline is
``view=stats`` (``first``/``first_date``) rather than an ``order`` switch, a
panel is its member names, one device's curve is a row filter the answer
already carries.

The pure parts live here: window resolution with an explicit time zone and an
explicit *now*; the selection rule (keywords or indicator names: at most
one); catalogue ranking (a zero-API lexical recall with a small zh↔en synonym
seed); the token-cheap table renderer; the request validator. The IO: SQL,
authorization, the read-time refresh: is the consumer's ``HealthQuery``
implementation.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import math
import re
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from importlib import resources
from typing import Protocol

from mirobody import lexical, units
from .series import zone

logger = logging.getLogger(__name__)

# --- windows -------------------------------------------------------------------------

SEMANTICS_TZ_EXACT = "tz_exact"
SEMANTICS_DATE_PADDED = "date_padded_naive"  # a store whose time column has no zone: bounds padded a day each way


@dataclass(frozen=True)
class Window:
    """An absolute window. ``start``/``end`` are the local dates the caller
    asked for (inclusive), in the order the window uses them;
    ``start_ms``/``end_ms`` the instants they resolve to in ``tz`` (end
    exclusive). ``note`` says when the pair was swapped or the window
    clamped."""

    start_ms: int
    end_ms: int
    tz: str
    start: str = ""
    end: str = ""
    note: str = ""


def parse_bound(text: str, tz: str, *, end_of_day: bool = False) -> datetime | None:
    """One time bound as the model wrote it → an aware instant, or ``None``.
    A bare ``YYYY-MM-DD`` is that day's 00:00 in ``tz`` (``end_of_day``: the
    next day's 00:00, the right edge of an inclusive day); an ISO string with
    an offset keeps it; one without is read in ``tz``. This is the ONE answer
    to "which instant does this date mean": a database's implicit cast
    answers it with the session zone instead."""
    if not text:
        return None
    z = zone(tz)
    try:
        if len(text) == 10:
            d = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=z)
            return d + timedelta(days=1) if end_of_day else d
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=z)
    except ValueError:
        return None


def resolve_window(
    tz: str, *, start: str = "", end: str = "", hours: int, now: datetime, max_hours: int | None = None
) -> Window:
    """``start``/``end`` (either may be empty) → an absolute window in ``tz``.
    A missing end is ``now``; a missing start is ``hours`` before the end. A
    pair given the wrong way round is swapped, as the medications tool does:
    it means the span between the two dates, and the old repair (``hours``
    after the start) answered about a day nobody asked for while echoing the
    dates as asked. A window longer than ``max_hours`` is clamped to its most
    recent part. ``note`` says which of the two happened. ``now`` is a
    parameter so the same call gives the same answer in a test and at 23:59."""
    if hours < 1:
        raise ValueError("hours must be >= 1")
    z = zone(tz)
    now_local = now.astimezone(z)
    end_dt = parse_bound(end, tz, end_of_day=True) or now_local
    start_dt = parse_bound(start, tz) or (end_dt - timedelta(hours=hours))
    notes = []
    if end_dt <= start_dt:
        # Only a given start can pass the end (a derived one is `hours`
        # before it), so the swapped end always parses.
        start, end = end, start
        start_dt = parse_bound(start, tz) or now_local
        end_dt = parse_bound(end, tz, end_of_day=True) or now_local
        notes.append("start and end swapped")
    if max_hours is not None and (end_dt - start_dt) > timedelta(hours=max_hours):
        start_dt = end_dt - timedelta(hours=max_hours)
        notes.append(f"window clamped to {max_hours}h")
    return Window(int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000), tz, start, end, "; ".join(notes))


# --- selection ------------------------------------------------------------------------


@dataclass(frozen=True)
class Selection:
    """What to read: free-text ``keywords`` or exact ``indicators``, at most
    one. Neither means the catalogue."""

    keywords: tuple[str, ...] = ()
    indicators: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.keywords and self.indicators:
            raise ValueError("give keywords or indicators — not both")

    @property
    def kind(self) -> str:
        if self.keywords:
            return "keywords"
        if self.indicators:
            return "indicators"
        return "catalog"


def normalize_list_arg(value: object) -> tuple[str, ...]:
    """Whatever the model sent for a list parameter → clean names. Seen in
    the wild: a JSON-stringified list, a list of JSON strings, a comma-joined
    string. Untreated, the store compares against the literal text
    ``["Heart Rate"]`` and matches nothing: a silent empty result.

    **A comma splits a bare string, never an element of a list.** Clinical
    names contain commas (``1,25-Dihydroxyvitamin D`` is one) and splitting
    a list element made every such indicator unrequestable: three fragments,
    none of them a name, and an empty answer with nothing to attribute it to.
    A model that sends ``"a,b"`` as one string still gets two names, because
    that is a string it composed; a model that sends ``["a,b"]`` gets one,
    because a list element is a name it copied from a catalogue this tool
    gave it.
    """

    def expand(item: object, *, split_commas: bool) -> list[str]:
        if not isinstance(item, str) or not item.strip():
            return []
        s = item.strip()
        if s[0] == "[" and s[-1] == "]":
            try:
                parsed = json.loads(s)
            except (json.JSONDecodeError, ValueError):
                return [s]
            if isinstance(parsed, list):
                return [n for el in parsed for n in expand(el, split_commas=False)]
            return expand(parsed, split_commas=False) if isinstance(parsed, str) else []
        if not split_commas:
            return [s]
        return [part.strip() for part in s.split(",") if part.strip()]

    if isinstance(value, str):
        names = expand(value, split_commas=True)
    elif isinstance(value, list | tuple):
        names = [n for item in value for n in expand(item, split_commas=False)]
    else:
        return ()
    out: list[str] = []
    for name in names:
        if name not in out:
            out.append(name)
    return tuple(out)


# --- catalogue ranking -----------------------------------------------------------------

_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|[^A-Za-z0-9一-鿿]+")


def _load_synonyms() -> dict[str, tuple[str, ...]]:
    try:
        text = resources.files("mirobody").joinpath("res", "loinc", "recall_synonyms.tsv").read_text(encoding="utf-8")
    except OSError:
        logger.warning("recall_synonyms.tsv missing: catalogue recall has no zh-en bridge")
        return {}
    out: dict[str, tuple[str, ...]] = {}
    for row in csv.DictReader(io.StringIO(text), delimiter="\t"):
        out[row["surface"].casefold()] = tuple(t for t in row["tokens"].split("|") if t)
    return out


#: A small, fixed zh↔en bridge of high-frequency clinical surfaces. A cost
#: optimisation, not a knowledge base: what it cannot bridge falls through
#: to whatever richer recall the consumer runs.
SYNONYMS: dict[str, tuple[str, ...]] = _load_synonyms()


def _tokens(text: str) -> set[str]:
    """A name's word tokens, plural-folded (`lexical.fold_plural`) like the
    query's, so `heartRates` holds `rate` for "heart rate"."""
    base = {t.lower() for t in _CAMEL.split(text or "") if t}
    return {lexical.fold_plural(t) for t in base | {t.lower() for t in lexical.word_tokens(text or "")}}


def _expand(query: str, synonyms: Mapping[str, tuple[str, ...]]) -> set[str]:
    ql = (query or "").casefold()
    out: set[str] = set()
    for surface, toks in synonyms.items():
        if surface in ql:
            out.update(toks)
    return out


def rank_catalog(
    query: str, catalog: Sequence[str], *, limit: int = 10, synonyms: Mapping[str, tuple[str, ...]] | None = None
) -> tuple[str, ...]:
    """Catalogue names ranked against a free-text query, most relevant
    first, with no network call. Tiers: exact normalised surface (3.0) >
    query tokens all inside the name's tokens (2.0) > normalised substring
    (1.5) > per-token hits (1.0 + 0.2 per hit). Nothing scoring under 1.0 is
    returned; an empty result means "use a richer recall".

    A query word matches its plural and its singular alike
    (`lexical.fold_plural` has the miss that made this necessary), synonym
    words included: the seed maps 甘油三酯 to `triglycerides`, a display
    says `Triglyceride`. The substring tier tries a word both as written and
    folded, so folding only ever adds a match: `calories` folds to `calory`,
    which is not a substring of `activecalories`."""
    q = (query or "").strip()
    if not q or not catalog:
        return ()
    syn = SYNONYMS if synonyms is None else synonyms
    qn = lexical.normalize(q)
    words = {t.lower() for t in lexical.word_tokens(q)} | _expand(q, syn)
    qtok = {lexical.fold_plural(t) for t in words}
    scored: list[tuple[float, str]] = []
    for name in catalog:
        nl = name.lower()
        s = 0.0
        if lexical.normalize(name) == qn:
            s = 3.0
        elif qtok and qtok <= _tokens(name):
            s = 2.0
        else:
            if len(qn) >= 2 and qn in nl:
                s = 1.5
            hits = sum(
                1 for t in words
                if any(len(form) >= 2 and form in nl for form in (t, lexical.fold_plural(t)))
            )
            if hits:
                s = max(s, 1.0 + 0.2 * hits)
        if s >= 1.0:
            scored.append((s, name))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return tuple(n for _s, n in scored[:limit])


# --- rendering ------------------------------------------------------------------------


_MAX_CELL = 500


def _has_value(value: object) -> bool:
    return value is not None and not (isinstance(value, str) and not value.strip())


def _cell(value: object, max_cell: int | None) -> str:
    """One value → one cell. Containers are summarised past three items and
    a long cell is cut: the reader pays per token, and one 10 KB blob in one
    cell is the whole context budget."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        if len(value) <= 3:
            text = json.dumps(value, ensure_ascii=False, default=str)
            if max_cell is None or len(text) <= max_cell:
                return text
        if isinstance(value[0], Mapping):
            return f"<{len(value)} items, keys={list(value[0].keys())[:6]}>"
        head = ", ".join(str(x) for x in value[:3])
        return f"<{len(value)} items: [{head}, ...]>"
    if isinstance(value, Mapping):
        text = json.dumps(value, ensure_ascii=False, default=str)
        return text if max_cell is None or len(text) <= max_cell else f"<dict, keys={list(value.keys())[:6]}>"
    text = str(value)
    if max_cell is not None and len(text) > max_cell:
        return text[: max_cell - 3] + "..."
    return text


def compact(
    rows: Sequence[Mapping[str, object]],
    columns: Sequence[str] | None = None,
    *,
    empty: str = "",
    max_cell: int | None = _MAX_CELL,
) -> str:
    """Rows → a pipe-delimited table with constant columns factored out into
    one ``(constants: …)`` line and empty columns dropped. Repeating
    ``unit=mmol/L`` on 200 rows is pure token cost for whoever pays for the
    context.

    ``columns`` defaults to every key in first-seen order. ``empty`` is what
    no rows read as: ``""`` by default; a caller that puts the table in a
    JSON field passes a word a model cannot mistake for "the field was
    blank". Cells longer than ``max_cell`` are cut and nested containers
    summarised (see `_cell`); ``max_cell=None`` renders everything in full.
    """
    if not rows:
        return empty
    if columns is None:
        seen: dict[str, None] = {}
        for row in rows:
            for key in row:
                seen.setdefault(key, None)
        columns = list(seen)
    present = [c for c in columns if any(_has_value(r.get(c)) for r in rows)]
    if not present:
        return empty
    rendered = {c: [_cell(r.get(c), max_cell) for r in rows] for c in present}
    constants = {c: cells[0] for c, cells in rendered.items() if len(set(cells)) == 1}
    varying = [c for c in present if c not in constants]
    out: list[str] = []
    if constants:
        out.append("(constants: " + ", ".join(f"{k}={v}" for k, v in constants.items()) + ")")
    if not varying:
        return "\n".join(out)
    out.append("|".join(varying))
    # VCF phased GTs contain a literal pipe; leave one delimiter per column.
    out.extend("|".join(rendered[c][i].replace("\\", "\\\\").replace("|", "\\|").replace("\n", "\\n")
                        for c in varying) for i in range(len(rows)))
    return "\n".join(out)


def _significant(value: float, digits: int = 6) -> float:
    """A float kept to `digits` significant figures, so small scales survive rounding."""
    if value == 0 or value != value:
        return value
    from math import floor, log10

    exponent = floor(log10(abs(value)))
    return round(value, digits - 1 - exponent)


def _below(value: float, threshold: float) -> bool:
    """Whether `value` is under `threshold`, ignoring floating point noise at the boundary."""
    return value < threshold and not math.isclose(value, threshold, rel_tol=1e-12, abs_tol=0.0)


def _trend_shape(values: Sequence[float], magnitude: float, direction: str) -> str:
    span = max(values) - min(values)
    # A relative tolerance absorbs boundary noise without changing the verdict
    # when a small concentration is expressed in another unit.
    if not span or _below(span, 0.02 * magnitude):
        return "flat"
    # A reversal must travel a quarter of the range; tiny daily jitter must not
    # turn a smooth hump into several apparent peaks.
    # Prominence from the span alone. The old floor `0.02 * magnitude` used the level of the
    # series, not its movement, so the same readings in mmol/L and in umol/L sat on opposite
    # sides of it - one shape came out flat and the other hump.
    prominence = 0.25 * span
    movement, extreme = 0, values[0]
    # While no direction is established the running high and low both matter: keeping only
    # one of them (the latest sample) hid the first reversal when a series opened on its
    # midpoint - [100, 101.5, 98.5, ...] read as flat, yet the same series without its first
    # value read as swing.
    high, low = values[0], values[0]
    turns: list[str] = []
    for value in values[1:]:
        if movement == 0:
            high, low = max(high, value), min(low, value)
            if value - low >= prominence and high - value < prominence:
                movement, extreme = 1, value
            elif high - value >= prominence and value - low < prominence:
                movement, extreme = -1, value
            elif high - low >= prominence:
                movement, extreme = (1 if value >= (high + low) / 2 else -1), value
        elif movement == 1:
            if value >= extreme:
                extreme = value
            elif extreme - value >= prominence:
                turns.append("hump")
                movement, extreme = -1, value
        elif value <= extreme:
            extreme = value
        elif value - extreme >= prominence:
            turns.append("dip")
            movement, extreme = 1, value
    return "swing" if len(turns) > 1 else turns[0] if turns else direction


def trend_rows(daily: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Summarize observed daily values without losing units, identity or coverage.

    Thirds refer to observed numeric days, not equal calendar intervals. The
    descriptive direction threshold is 2% of mean absolute magnitude; it is not a
    clinical significance test. A slope uses actual elapsed local calendar days.
    Missing numeric days are counted, never filled with zero. Offset or unknown
    cross-unit conversions abstain rather than fabricate a common numeric basis.
    """
    grouped: dict[tuple[str, ...], list[Mapping[str, object]]] = {}
    for row in daily:
        series_id = str(row.get("series") or "")
        key = (("series", series_id) if series_id else
               ("identity", *(str(row.get(k) or "") for k in ("indicator", "system", "code"))))
        grouped.setdefault(key, []).append(row)

    out: list[dict[str, object]] = []
    for source in grouped.values():
        ordered = sorted(source, key=lambda row: str(row.get("period") or row.get("date") or ""))
        latest = ordered[-1]
        periods = [str(row.get("period") or row.get("date") or "") for row in ordered]
        numeric: list[tuple[Mapping[str, object], float]] = []
        for row in ordered:
            raw = row.get("avg", row.get("value"))
            if raw is None or isinstance(raw, bool):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(value):
                numeric.append((row, value))
        totals = [len(source)]
        for row in source:
            try:
                totals.append(int(row.get("total") or 0))
            except (TypeError, ValueError, OverflowError):
                continue
        reported = any(row.get("provenance") == "reported" for row in source)
        result: dict[str, object] = {
            **{k: str(latest.get(k) or "") for k in ("indicator", "series", "system", "code")},
            "n": len(numeric), "source_days": len(source), "total_days": max(totals),
            "input_truncated": max(totals) > len(source),
            "missing_numeric_days": len(source) - len(numeric),
            "unverified": any(bool(row.get("unverified")) for row in source),
            "day_known": all(bool(row.get("day_known", True)) for row in source),
            "provenance": "reported" if reported else str(latest.get("provenance") or "computed"),
            "first_date": periods[0], "last_date": periods[-1],
            "unit": "", "direction": "unsupported", "shape": "unavailable", "reason": "",
            **dict.fromkeys(("first_third_mean", "last_third_mean", "delta", "delta_pct",
                             "slope_per_day", "min", "max")),
        }
        if latest.get("kind"):
            result["kind"] = latest["kind"]
        out.append(result)
        if reported or not numeric:
            result["reason"] = "reported_entries" if reported else "no_numeric_values"
            continue
        try:
            days = [date.fromisoformat(period) for period in periods]
        except ValueError:
            result["reason"] = "invalid_dates"
            continue
        if len(set(days)) != len(days):
            result["reason"] = "invalid_dates"
            continue
        first_unit = str(numeric[0][0].get("unit") or "")
        target = units.normalize_unit(first_unit) or first_unit
        result["unit"] = target
        values: list[float] = []
        for row, value in numeric:
            raw_unit = str(row.get("unit") or "")
            incoming = units.normalize_unit(raw_unit) or raw_unit
            system = str(row.get("system") or "").casefold()
            code = str(row.get("code") or "") if system in {"loinc", "http://loinc.org", "https://loinc.org"} else ""
            factor = units.conversion_factor(incoming, target, loinc_code=code)
            if factor is None or not math.isfinite(value * factor):
                result.update(direction="incomparable", reason="incompatible_units")
                break
            values.append(value * factor)
        if result["direction"] == "incomparable":
            continue
        if len(values) < 3:
            result.update(direction="insufficient", reason="insufficient_points")
            continue
        n = len(values)
        cut = n // 3
        first_third, last_third = statistics.mean(values[:cut]), statistics.mean(values[-cut:])
        delta = last_third - first_third
        magnitude = statistics.mean(abs(value) for value in values)
        relative = delta / magnitude * 100 if magnitude else None
        direction = ("flat" if relative is None or _below(abs(relative), 2.0)
                     else "up" if delta > 0 else "down")
        origin = days[0]
        xs = [(date.fromisoformat(str(row.get("period") or row.get("date"))) - origin).days for row, _ in numeric]
        x_mean, mean = statistics.mean(xs), statistics.mean(values)
        denominator = sum((x - x_mean) ** 2 for x in xs)
        slope = sum((x - x_mean) * (value - mean) for x, value in zip(xs, values, strict=True)) / denominator
        result.update(
            # Significant figures, not decimal places: a hormone series in the 1e-4 range
            # rounds to zero at three decimals while `direction` still reads "up", so the row
            # would report a movement it cannot express.
            first_third_mean=_significant(first_third), last_third_mean=_significant(last_third),
            delta=_significant(delta), delta_pct=round(relative, 2) if relative is not None else None,
            slope_per_day=_significant(slope, 6), direction=direction,
            shape=_trend_shape(values, magnitude, direction),
            min=_significant(min(values)), max=_significant(max(values)),
        )
    return out


# --- the tool: schema and dispatch -------------------------------------------------------

#: One value per bucket of this size; day and coarser read the elected day.
BUCKETS = ("minute", "hour", "day", "week", "month")
VIEWS = ("raw", *BUCKETS, "stats", "latest", "trend")
#: Raw rows per indicator, newest first. A budget, not a question: the model
#: that could raise it used to, and got a longer table instead of an answer.
#: Narrowing the window or asking for `stats` is what a cut row count means.
ROW_CAP = 50
#: Points per indicator in a minute…month view: the newest are kept, oldest
#: first. Uncapped, MiniCPM5-2B asked for `view="day"` with no dates and got
#: the whole record back, 13,930 and 33,657 characters (2026-10-06); the second
#: was evicted to a file it paged until the context overflowed. 92 is the
#: longest three calendar months, so the three-month daily chart that
#: evaluation asked for is never cut, and a year is.
BUCKET_CAP = 92

TOOL_NAME = "query_health_indicators"

#: The one schema both the chat tool and the MCP tool publish. Flat, five
#: parameters, every one applicable to every call. `view` is one enum rather
#: than `resolution` × `aggregate`: two of those eighteen cells were refused,
#: `limit` applied to one of them, and the prompt spent two paragraphs teaching
#: which was which.
TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 20,
            "description": "Free-text terms to match against the person's catalogue, any language (e.g. [\"blood pressure\", \"血压\"], or a symptom: [\"headache\"] also finds an entry written 头疼). Use when you do not know the exact indicator names. Omit both keywords and indicators to get the catalogue.",
        },
        "indicators": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 50,
            "description": "Exact indicator names copied from a previous result. Preferred once known; several in one call.",
        },
        "start": {
            "type": "string",
            "pattern": r"^\d{4}-\d{2}-\d{2}$",
            "description": "First local date, inclusive (YYYY-MM-DD, the person's time zone). Omit both dates for the whole record.",
        },
        "end": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$", "description": "Last local date, inclusive."},
        "view": {
            "type": "string",
            "enum": list(VIEWS),
            "default": "raw",
            "description": "raw: the readings, newest first, each with the file it came from. minute/hour/day/week/month: one point per period, for a chart. stats: count/min/max/avg/sum/first/last/change over the window, for totals and a baseline. trend: numeric direction and shape, observed-day third means, delta, delta_pct and elapsed-day slope; at most 400 daily inputs, with partial coverage explicit. Unsupported or incomparable data keeps a reason; use day/stats for reported entries. The 2% direction threshold is descriptive, not clinical. latest: the most recent value per indicator.",
        },
    },
}

#: ``view`` → ``HealthQuery`` method.
DISPATCH: dict[str, str] = {
    "raw": "readings",
    **dict.fromkeys(BUCKETS, "buckets"),
    "stats": "stats",
    "latest": "latest",
    # A trend is a computation over the daily series, not another aggregation: first/last are
    # single readings and a noisy day either end flips the sign (sleep 6.18 -> 7.2 while the
    # third-to-third means fall 7.66 -> 6.96). `first_third`/`last_third`/`slope` are what a
    # shape question can be answered from.
    "trend": "trend",
}


@dataclass(frozen=True)
class QueryRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_request`."""

    selection: Selection = field(default_factory=Selection)
    start: str = ""
    end: str = ""
    view: str = "raw"

    @property
    def method(self) -> str:
        if self.selection.kind == "catalog":
            return "catalog"
        return DISPATCH[self.view]

    @property
    def view_unapplied(self) -> bool:
        """A view was named with nothing selected: the catalogue answers, and
        the caller should say the view waits for the names it lists."""
        return self.selection.kind == "catalog" and self.view != "raw"


@dataclass(frozen=True)
class Rejection:
    """Why a request is refused: the offending parameter and the reason."""

    parameter: str
    reason: str


_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def reject_unknown(args: Mapping[str, object], schema: Mapping[str, object]) -> list[Rejection]:
    """A parameter the schema does not have. Shared by every tool that
    publishes a declared schema, so an MCP client's typo is an answer rather
    than a silently dropped argument."""
    props = schema["properties"]  # type: ignore[index]
    return [Rejection(p, "unknown parameter") for p in sorted(args) if p not in props]  # type: ignore[operator]


def reject_view(args: Mapping[str, object], views: Sequence[str]) -> list[Rejection]:
    """A ``view`` that is not one of ``views``; no view is the default."""
    view = args.get("view")
    if view in (None, "") or view in views:
        return []
    return [Rejection("view", f"must be one of {', '.join(views)}")]


def reject_dates(args: Mapping[str, object]) -> list[Rejection]:
    """``start``/``end`` that are not a calendar day written ``YYYY-MM-DD``.
    The pattern alone let ``2025-06-31`` through: the readings tool then read
    it as no bound at all (an end of *now*) and the medications tool raised."""
    out = []
    for p in ("start", "end"):
        v = args.get(p)
        if v in (None, ""):
            continue
        if not _DATE.match(str(v)):
            out.append(Rejection(p, "must be YYYY-MM-DD"))
            continue
        try:
            date.fromisoformat(str(v))
        except ValueError:
            out.append(Rejection(p, "not a calendar date"))
    return out


def validate_request(args: Mapping[str, object]) -> tuple[Rejection, ...]:
    """Everything wrong with the raw arguments, in a stable order. Empty
    means :func:`parse_request` will succeed. Checks the enums, the selection
    rule and the dates.

    A view with no selection is not refused: it is a catalogue call
    (:attr:`QueryRequest.view_unapplied`). Refused, MiniCPM5-2B opened with
    `view="latest"` and no names 3 times and MiniCPM5-1B 7 times, then
    repeated it until the harness refused the repeat (`repeated_call`) 6
    times (2026-10-06), each attempt a model turn on an 8k-token prompt. The
    catalogue is the list the next call copies its names from."""
    out: list[Rejection] = reject_unknown(args, TOOL_SCHEMA)
    out.extend(reject_view(args, VIEWS))
    selectors = [p for p in ("keywords", "indicators") if args.get(p) not in (None, "", [], ())]
    if len(selectors) > 1:
        out.append(Rejection("keywords+indicators", "give keywords or indicators — not both"))
    out.extend(reject_dates(args))
    return tuple(out)


def parse_request(args: Mapping[str, object]) -> QueryRequest:
    """Raw arguments → a :class:`QueryRequest`; raises ``ValueError`` with the
    rejections when :func:`validate_request` finds any."""
    problems = validate_request(args)
    if problems:
        raise ValueError("; ".join(f"{r.parameter}: {r.reason}" for r in problems))
    sel = Selection(
        keywords=normalize_list_arg(args.get("keywords")),
        indicators=normalize_list_arg(args.get("indicators")),
    )
    return QueryRequest(
        selection=sel,
        start=str(args.get("start") or ""),
        end=str(args.get("end") or ""),
        view=str(args.get("view") or "raw"),
    )


# --- ports -----------------------------------------------------------------------------

Rows = Sequence[Mapping[str, object]]


class HealthQuery(Protocol):
    """The read authority for readings. Each method is one cell of the
    dispatch table; every leaf takes the same ``(subject_id, sel, window)``
    head. The implementation owns the SQL, the time-zone lookup and any
    read-time refresh. Day-grained values come from the elected daily
    authority: the same numbers a dashboard shows, by construction; raw
    rows are newest first; buckets are the newest ``limit`` per series,
    oldest first; both carry ``total`` per series so a cut can say so;
    ``latest`` is the most recent value *inside the window*."""

    def tz(self, subject_id: str) -> str: ...
    def on_read(self, subject_id: str) -> None: ...
    def catalog(self, subject_id: str, window: Window | None) -> Rows: ...
    def readings(self, subject_id: str, sel: Selection, window: Window, *, limit: int) -> Rows: ...
    def buckets(self, subject_id: str, sel: Selection, window: Window, *, resolution: str, limit: int) -> Rows: ...
    def stats(self, subject_id: str, sel: Selection, window: Window) -> Rows: ...
    def latest(self, subject_id: str, sel: Selection, window: Window) -> Rows: ...


__all__ = [
    "BUCKETS",
    "BUCKET_CAP",
    "DISPATCH",
    "HealthQuery",
    "QueryRequest",
    "ROW_CAP",
    "Rejection",
    "Rows",
    "SEMANTICS_DATE_PADDED",
    "SEMANTICS_TZ_EXACT",
    "SYNONYMS",
    "Selection",
    "TOOL_NAME",
    "TOOL_SCHEMA",
    "VIEWS",
    "Window",
    "compact",
    "normalize_list_arg",
    "parse_bound",
    "parse_request",
    "rank_catalog",
    "reject_dates",
    "reject_unknown",
    "resolve_window",
    "trend_rows",
    "validate_request",
]

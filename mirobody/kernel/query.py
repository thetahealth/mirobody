"""One read authority, one tool for readings.

Every surface that shows a person their own readings — the chat agent's tool,
an MCP client, a dashboard, a daily summary — reads through
:class:`HealthQuery`. The tool the model sees, ``query_health_indicators``, has
one JSON schema (:data:`TOOL_SCHEMA`) shared by the chat and MCP surfaces and
a dispatch table from ``(resolution, aggregate)`` to the one ``HealthQuery``
method that answers it. Medications are a different data class with a
different grammar and their own tool: :mod:`mirobody.kernel.meds`.

Eight parameters, each one a decision the model has to make on every call, and
each one earning its place: what to read (``keywords`` or ``indicators``),
when (``start``/``end``), at what grain (``resolution``), reduced how
(``aggregate``), how many raw rows (``limit``), and about whom (``member``).
Everything a model could also get another way is not a parameter: a
baseline is ``aggregate=stats`` (``first``/``first_date``) rather than an
``order`` switch, a panel is its member names, one device's curve is a row
filter the answer already carries.

The pure parts live here: window resolution with an explicit time zone and an
explicit *now*; the selection rule (keywords or indicator names — at most
one); catalogue ranking (a zero-API lexical recall with a small zh↔en synonym
seed); the token-cheap table renderer; the request validator. The IO — SQL,
authorization, the read-time refresh — is the consumer's ``HealthQuery``
implementation.
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from importlib import resources
from typing import Protocol

from .. import lexical
from .series import zone

# --- windows -------------------------------------------------------------------------

SEMANTICS_TZ_EXACT = "tz_exact"
SEMANTICS_DATE_PADDED = "date_padded_naive"  # a store whose time column has no zone: bounds padded a day each way


@dataclass(frozen=True)
class Window:
    """An absolute window. ``start``/``end`` are the local dates the caller
    asked for (inclusive), ``start_ms``/``end_ms`` the instants they resolve
    to in ``tz`` (end exclusive). ``note`` says when the window was clamped."""

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
    to "which instant does this date mean" — a database's implicit cast
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
    A missing end is ``now``; a missing start is ``hours`` before the end; an
    inverted pair is repaired; a window longer than ``max_hours`` is clamped
    to its most recent part and says so in ``note``. ``now`` is a parameter
    so the same call gives the same answer in a test and at 23:59."""
    if hours < 1:
        raise ValueError("hours must be >= 1")
    z = zone(tz)
    now_local = now.astimezone(z)
    end_dt = parse_bound(end, tz, end_of_day=True) or now_local
    start_dt = parse_bound(start, tz) or (end_dt - timedelta(hours=hours))
    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=hours)
    note = ""
    if max_hours is not None and (end_dt - start_dt) > timedelta(hours=max_hours):
        start_dt = end_dt - timedelta(hours=max_hours)
        note = f"window clamped to {max_hours}h"
    return Window(int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000), tz, start, end, note)


# --- selection ------------------------------------------------------------------------


@dataclass(frozen=True)
class Selection:
    """What to read: free-text ``keywords`` or exact ``indicators`` — at most
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
    ``["Heart Rate"]`` and matches nothing — a silent empty result.

    **A comma splits a bare string, never an element of a list.** Clinical
    names contain commas — ``1,25-Dihydroxyvitamin D`` is one — and splitting
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
        text = resources.files("mirobody").joinpath("res", "recall_synonyms.tsv").read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
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
    base = {t.lower() for t in _CAMEL.split(text or "") if t}
    return base | {t.lower() for t in lexical.word_tokens(text or "")}


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
    returned; an empty result means "use a richer recall"."""
    q = (query or "").strip()
    if not q or not catalog:
        return ()
    syn = SYNONYMS if synonyms is None else synonyms
    qn = lexical.normalize(q)
    qtok = {t.lower() for t in lexical.word_tokens(q)} | _expand(q, syn)
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
            hits = sum(1 for t in qtok if len(t) >= 2 and t in nl)
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
    no rows read as — ``""`` by default; a caller that puts the table in a
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
    out.extend("|".join(rendered[c][i] for c in varying) for i in range(len(rows)))
    return "\n".join(out)


# --- the tool: schema and dispatch -------------------------------------------------------

RESOLUTIONS = ("raw", "minute", "hour", "day", "week", "month")
AGGREGATES = ("none", "stats", "latest")
MAX_LIMIT = 500
DEFAULT_LIMIT = 50

TOOL_NAME = "query_health_indicators"

#: The one schema both the chat tool and the MCP tool publish. Flat, eight
#: parameters, every one applicable to every call — no mode switch, so no
#: parameter that is silently ignored or refused depending on another.
TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 20,
            "description": "Free-text terms to match against the person's catalogue, any language (e.g. [\"blood pressure\", \"血压\"]). Use when you do not know the exact indicator names. Omit both keywords and indicators to get the catalogue.",
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
        "resolution": {
            "type": "string",
            "enum": list(RESOLUTIONS),
            "default": "raw",
            "description": "raw readings, or one value per minute/hour/day/week/month. day and coarser return the elected daily value.",
        },
        "aggregate": {
            "type": "string",
            "enum": list(AGGREGATES),
            "default": "none",
            "description": "none: rows; stats: count/min/max/avg/first/last/change over the window (use for 'how did it change' and for a baseline); latest: the most recent value per indicator.",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_LIMIT,
            "default": DEFAULT_LIMIT,
            "description": "Raw rows per indicator, newest first. Narrow the window or aggregate instead of raising it.",
        },
        "member": {
            "type": "string",
            "description": "Read another person's data you are authorised to see (a care-circle member id). Omit for the caller.",
        },
    },
}

#: ``(resolution, aggregate)`` → ``HealthQuery`` method. A missing cell is a
#: refused combination, never a guess.
DISPATCH: dict[tuple[str, str], str] = {
    ("raw", "none"): "readings",
    ("raw", "stats"): "stats",
    ("raw", "latest"): "latest",
    **{(r, "none"): "buckets" for r in ("minute", "hour", "day", "week", "month")},
    **{(r, "stats"): "stats" for r in ("minute", "hour", "day", "week", "month")},
    **{(r, "latest"): "latest" for r in ("day", "week", "month")},
}

#: What ``stats``/``latest`` are computed over, per resolution.
BASIS: dict[str, str] = {
    "raw": "readings",
    "minute": "buckets",
    "hour": "buckets",
    "day": "daily",
    "week": "daily",
    "month": "daily",
}


@dataclass(frozen=True)
class QueryRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_request`."""

    selection: Selection = field(default_factory=Selection)
    start: str = ""
    end: str = ""
    resolution: str = "raw"
    aggregate: str = "none"
    limit: int = DEFAULT_LIMIT
    member: str = ""

    @property
    def method(self) -> str:
        if self.selection.kind == "catalog":
            return "catalog"
        return DISPATCH[(self.resolution, self.aggregate)]

    @property
    def basis(self) -> str:
        return BASIS[self.resolution]


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


def reject_dates(args: Mapping[str, object]) -> list[Rejection]:
    out = []
    for p in ("start", "end"):
        v = args.get(p)
        if v not in (None, "") and not _DATE.match(str(v)):
            out.append(Rejection(p, "must be YYYY-MM-DD"))
    return out


def validate_request(args: Mapping[str, object]) -> tuple[Rejection, ...]:
    """Everything wrong with the raw arguments, in a stable order. Empty
    means :func:`parse_request` will succeed. Checks the enums, the selection
    rule, the dispatch table and the ranges."""
    out: list[Rejection] = reject_unknown(args, TOOL_SCHEMA)
    for p, allowed in (("resolution", RESOLUTIONS), ("aggregate", AGGREGATES)):
        v = args.get(p)
        if v not in (None, "") and v not in allowed:
            out.append(Rejection(p, f"must be one of {', '.join(allowed)}"))
    selectors = [p for p in ("keywords", "indicators") if args.get(p) not in (None, "", [], ())]
    if len(selectors) > 1:
        out.append(Rejection("keywords+indicators", "give keywords or indicators — not both"))
    res, agg = str(args.get("resolution") or "raw"), str(args.get("aggregate") or "none")
    if res in RESOLUTIONS and agg in AGGREGATES and (res, agg) not in DISPATCH:
        out.append(Rejection("aggregate", f"{agg} is not defined for resolution={res}"))
    if not selectors and agg != "none":
        out.append(Rejection("aggregate", "the catalogue cannot be aggregated; pick indicators first"))
    if not selectors and res != "raw":
        out.append(Rejection("resolution", "the catalogue has no resolution; pick indicators first"))
    if (res, agg) != ("raw", "none") and args.get("limit") not in (None, ""):
        out.append(Rejection("limit", "only applies to raw rows without aggregation"))
    lim = args.get("limit")
    if lim not in (None, "") and (not isinstance(lim, int) or not 1 <= lim <= MAX_LIMIT):
        out.append(Rejection("limit", f"must be an integer between 1 and {MAX_LIMIT}"))
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
        resolution=str(args.get("resolution") or "raw"),
        aggregate=str(args.get("aggregate") or "none"),
        limit=int(args.get("limit") or DEFAULT_LIMIT),
        member=str(args.get("member") or ""),
    )


# --- ports -----------------------------------------------------------------------------

Rows = Sequence[Mapping[str, object]]


class Denied(PermissionError):
    """The caller may not read this subject's data."""


class SubjectResolver(Protocol):
    """Who the data is about. ``member`` empty means the caller themself;
    otherwise the resolver checks the caller's authorisation and returns the
    subject id, or raises :class:`Denied`."""

    def resolve(self, caller_id: str, member: str) -> str: ...


class HealthQuery(Protocol):
    """The read authority for readings. Each method is one cell of the
    dispatch table; every leaf takes the same ``(subject_id, sel, window)``
    head. The implementation owns the SQL, the time-zone lookup and any
    read-time refresh. Day-grained values come from the elected daily
    authority — the same numbers a dashboard shows, by construction; raw
    rows are newest first; ``latest`` is the most recent value *inside the
    window*."""

    def tz(self, subject_id: str) -> str: ...
    def on_read(self, subject_id: str) -> None: ...
    def catalog(self, subject_id: str, window: Window | None) -> Rows: ...
    def readings(self, subject_id: str, sel: Selection, window: Window, *, limit: int) -> Rows: ...
    def buckets(self, subject_id: str, sel: Selection, window: Window, *, resolution: str) -> Rows: ...
    def stats(self, subject_id: str, sel: Selection, window: Window, *, basis: str) -> Rows: ...
    def latest(self, subject_id: str, sel: Selection, window: Window, *, basis: str) -> Rows: ...


__all__ = [
    "AGGREGATES",
    "BASIS",
    "DEFAULT_LIMIT",
    "DISPATCH",
    "Denied",
    "HealthQuery",
    "MAX_LIMIT",
    "QueryRequest",
    "RESOLUTIONS",
    "Rejection",
    "Rows",
    "SEMANTICS_DATE_PADDED",
    "SEMANTICS_TZ_EXACT",
    "SYNONYMS",
    "Selection",
    "SubjectResolver",
    "TOOL_NAME",
    "TOOL_SCHEMA",
    "Window",
    "compact",
    "normalize_list_arg",
    "parse_bound",
    "parse_request",
    "rank_catalog",
    "reject_dates",
    "reject_unknown",
    "resolve_window",
    "validate_request",
]

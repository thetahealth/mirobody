"""`query_journal`: what a person has reported about themselves.

The fourth data class beside readings, medications and genetics, and its own
tool for the same reason they are: its grammar shares nothing with theirs. A
journal entry is a symptom felt or a diagnosis given, in the person's own
words, with the ICPC-3 code the vocabulary placed it on, or none. It has no
value, no unit and no aggregate, so it cannot be a mode of the readings tool,
and `collect.query` now keeps these rows out of that tool entirely.

Written through `/api/v1/journal` (`server/routers/journal_router.py`), which
owns the write side and states what it will not do: it does not read a
sentence, and it takes no meal or dose. This tool is read-only, like every
other tool here.

Three facts the answer says out loud, because a reader not told them draws the
opposite conclusion:

* **Absent is not negative.** The log holds what the person chose to write
  down. A complaint missing from it was not recorded here, which says nothing
  about whether they had it.
* **The words are the record.** The standard name is a classification of
  them. An entry the vocabulary could not place is just as much the person's
  report as a coded one, and it is returned with the reason.
* **Reported, not diagnosed.** A `condition` entry is what the person says
  they were diagnosed with. It is not a clinical record.

It never raises, for the same reason as its siblings: the `eval` REPL can call
it directly (PTC), and a PTC call has nothing above it to contain a fault.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from mirobody import translate
from mirobody.collect import observations
from mirobody.kernel import query, tools
from ._authz import refused, subject_for
from ._base import RecordTool
from ._render import envelope_meta, render_compact

logger = logging.getLogger(__name__)

TOOL_NAME = "query_journal"

KIND_SYMPTOM = "symptom"
KIND_CONDITION = "condition"
KINDS: tuple[str, ...] = (KIND_SYMPTOM, KIND_CONDITION)

#: The window when neither date is given. "Recently" in a question about how
#: someone has been feeling is weeks, not days.
DEFAULT_DAYS = 90
#: The longest window one call may ask for; the REST surface's own ceiling.
MAX_DAYS = 400
MAX_LIMIT = 500
DEFAULT_LIMIT = 200
MAX_KEYWORDS = 20

#: Columns the answer renders, in order. `render_compact` drops a column no
#: row fills, so a log with no notes and every entry coded renders without
#: `note` and `reason`.
COLUMNS: tuple[str, ...] = ("date", "time", "kind", "text", "display", "code", "reason", "note")

_ABSENCE_NOTE = (
    "the journal holds what the person chose to write down: a complaint missing here was not "
    "recorded, which is not evidence they did not have it"
)
_WORDS_NOTE = (
    "text is the person's own words and is the record; display and code are ICPC-3's "
    "classification of them, empty where the vocabulary could not place the words"
)
_REPORTED_NOTE = "a condition entry is what the person reports being diagnosed with, not a clinical record"

TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "kind": {
            "type": "string",
            "enum": list(KINDS),
            "description": (
                "symptom: what the person felt (头痛, sore throat). condition: what they were diagnosed with "
                "(高血压, asthma). Omit for both."
            ),
        },
        "start": {"type": "string", "description": f"First day, YYYY-MM-DD, inclusive. Default: {DEFAULT_DAYS} days before end."},
        "end": {"type": "string", "description": "Last day, YYYY-MM-DD, inclusive. Default: today in the person's timezone."},
        "keywords": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": MAX_KEYWORDS,
            "description": (
                "Only entries about these, e.g. [\"headache\"]. Matches the person's words or the standard name, "
                "and also any entry the vocabulary codes the same way, so \"headache\" finds an entry written "
                "头疼. Omit to list everything in the window."
            ),
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_LIMIT,
            "default": DEFAULT_LIMIT,
            "description": "Entries returned, newest first.",
        },
        "member": {
            "type": "string",
            "description": "Read another person's journal you are authorised to see (a care-circle member id). Omit for the caller.",
        },
    },
}


@dataclass(frozen=True)
class JournalRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_query`."""

    kinds: tuple[str, ...] = KINDS
    start: str = ""
    end: str = ""
    keywords: tuple[str, ...] = ()
    limit: int = DEFAULT_LIMIT
    member: str = ""


def _day(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)) if value not in (None, "") else None
    except ValueError:
        return None


def validate_query(args: Mapping[str, Any]) -> tuple:
    """Everything wrong with the raw arguments (``query.Rejection`` rows);
    empty means :func:`parse_query` will succeed."""
    out = query.reject_unknown(args, TOOL_SCHEMA)
    kind = args.get("kind")
    if kind not in (None, "") and kind not in KINDS:
        out.append(query.Rejection("kind", f"must be one of: {', '.join(KINDS)}"))
    for name in ("start", "end"):
        if args.get(name) not in (None, "") and _day(args.get(name)) is None:
            out.append(query.Rejection(name, "must be YYYY-MM-DD"))
    start, end = _day(args.get("start")), _day(args.get("end"))
    if start and end and abs((end - start).days) > MAX_DAYS:
        out.append(query.Rejection("start", f"the window is longer than {MAX_DAYS} days"))
    if len(query.normalize_list_arg(args.get("keywords"))) > MAX_KEYWORDS:
        out.append(query.Rejection("keywords", f"at most {MAX_KEYWORDS} per call"))
    lim = args.get("limit")
    if lim not in (None, "") and (not isinstance(lim, int) or not 1 <= lim <= MAX_LIMIT):
        out.append(query.Rejection("limit", f"must be an integer between 1 and {MAX_LIMIT}"))
    return tuple(out)


def parse_query(args: Mapping[str, Any]) -> JournalRequest:
    """Raw arguments → a :class:`JournalRequest`; ``ValueError`` when
    :func:`validate_query` finds anything."""
    problems = validate_query(args)
    if problems:
        raise ValueError("; ".join(f"{r.parameter}: {r.reason}" for r in problems))
    kind = args.get("kind")
    return JournalRequest(
        kinds=(kind,) if kind in KINDS else KINDS,
        start=str(args.get("start") or ""),
        end=str(args.get("end") or ""),
        keywords=tuple(k.strip() for k in query.normalize_list_arg(args.get("keywords")) if k and k.strip()),
        limit=int(args.get("limit") or DEFAULT_LIMIT),
        member=str(args.get("member") or ""),
    )


def window_for(request: JournalRequest, today: date) -> tuple[date, date]:
    """The caller's dates as local days, inclusive, with the defaults filled."""
    end = _day(request.end) or today
    start = _day(request.start) or (end - timedelta(days=DEFAULT_DAYS - 1))
    return (start, end) if start <= end else (end, start)


def codes_for(keywords: Sequence[str], kinds: Sequence[str]) -> list[str]:
    """The ICPC-3 codes the keywords name, on the axes asked for.

    This is what makes "headache" find an entry written 头疼: both land on
    NS01. A keyword the vocabulary does not place contributes no code and is
    still matched as text.
    """
    out: list[str] = []
    for kw in keywords:
        for kind in kinds:
            coding = translate.resolve_symptom(kw) if kind == KIND_SYMPTOM else translate.resolve_condition(kw)
            if coding.coded and coding.code and coding.code not in out:
                out.append(coding.code)
    return out


def like_patterns(keywords: Sequence[str]) -> list[str]:
    """`ILIKE` patterns that match each keyword as a literal substring.

    `%` and `_` are escaped: a keyword is the model's text, and an unescaped
    `%` would match every entry in the window.
    """
    def esc(k: str) -> str:
        return k.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return [f"%{esc(k)}%" for k in keywords]


class JournalService(RecordTool):
    """The tool body. `__tools__` is the whole published surface; `envelope`
    is API for the chat adapter, not a tool."""

    __tools__ = (TOOL_NAME,)
    TOOL_NAME = TOOL_NAME
    input_schema = TOOL_SCHEMA

    def __init__(self, execute: Any = None, *, tz: Any = None, now: Any = None) -> None:
        # Injected so a test can answer without a database; the shared pool
        # and the subject's own zone otherwise.
        self._execute = execute
        self._tz = tz
        self._now = now

    async def query_journal(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """
        Read what this person has reported about themselves: symptoms they
        felt and diagnoses they were given, in their own words, with the
        ICPC-3 classification of each.

        USE IT for how someone has been feeling or what they have: "我最近哪里
        不舒服", "how often do I get headaches", "when did the cough start",
        "what conditions have I logged". Pair it with
        `query_health_indicators` when the question joins a complaint to a
        reading ("was my blood pressure up on the days I had headaches").

        DO NOT use it for readings (`query_health_indicators`), medications
        (`query_medications`), genotypes (`query_genetic_data`), or for what a
        symptom MEANS (that is knowledge, not this person's data). Read-only:
        it cannot add or remove an entry.

        The parameters are documented in the schema (`input_schema` IS
        `TOOL_SCHEMA`, published verbatim).

        Returns:
            A compact table (date, time, kind, text, display, code, and the
            reason an entry is uncoded) plus a `meta` block with the window.
            Absence means "not recorded here", never "did not have it".

        Notes for LLMs:
            - Quote the person's words (`text`). The standard name is a
              classification; do not replace what they said with it.
            - An entry with no code is still a report. Do not drop it or call
              it invalid.
            - A condition entry is self-reported. Do not treat it as a
              confirmed diagnosis.
        """
        envelope = await self.envelope(user_info, **args)
        return {"result": render_compact(envelope, self.columns(args)), **envelope_meta(envelope)}

    def columns(self, args: Mapping[str, Any]) -> tuple[str, ...]:
        """Which columns one answer renders. Read by the chat adapter too
        (`tool_loader`), so both surfaces render the same table; not a tool
        (`__tools__`). Fixed here: a journal entry has one shape."""
        return COLUMNS

    # --- the run ------------------------------------------------------------

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = validate_query(args)
        if problems:
            return refused(problems)
        request = parse_query(args)
        subject_id = await subject_for(caller_id, request.member)
        tz = await self._zone_of(subject_id)
        today = self._clock().astimezone(translate.zone_for(tz)).date()
        window = window_for(request, today)
        rows = await self._entries(subject_id, request, window)
        # One row over the limit is how "cut" is KNOWN rather than guessed.
        kept, truncated = rows[: request.limit], len(rows) > request.limit
        return _envelope_for(request, window, tz, kept, truncated=truncated)

    async def _entries(self, subject_id: str, request: JournalRequest, window: tuple[date, date]) -> list[dict]:
        params: dict[str, Any] = {
            "uid": str(subject_id),
            "kinds": list(request.kinds),
            "from_date": window[0],
            "to_date": window[1],
            "limit": request.limit + 1,
        }
        match = ""
        if request.keywords:
            # Text in either name, or the same code. Both bound, never
            # interpolated: the keywords are the model's text.
            params["patterns"] = like_patterns(request.keywords)
            codes = codes_for(request.keywords, request.kinds)
            # The code arm only when there is a code: an empty list bound to
            # ANY() leaves the driver no element type to infer.
            by_code = " OR o.code = ANY(:codes)" if codes else ""
            if codes:
                params["codes"] = codes
            match = f" AND (o.name_text ILIKE ANY(:patterns) OR o.display ILIKE ANY(:patterns){by_code})"
        rows = await self._read(
            # Local wall time the same way `collect.query` renders a reading:
            # each row carries its own zone, and `UTC+08:00` is not an IANA name.
            "SELECT o.id, o.kind, to_char(o.local_date, 'YYYY-MM-DD') AS date,"
            " to_char(CASE WHEN o.tz ~ '^UTC[+-]'"
            " THEN (o.observed_start AT TIME ZONE 'UTC') + (substr(o.tz, 4))::interval"
            " ELSE o.observed_start AT TIME ZONE o.tz END, 'HH24:MI') AS time,"
            " o.name_text AS text, o.display, o.code, o.outcome, o.reason,"
            " decrypt_content(o.note_text) AS note"
            " FROM v_observation o"
            " WHERE o.user_id = :uid AND o.kind = ANY(:kinds)"
            " AND o.local_date BETWEEN :from_date AND :to_date"
            f"{match}"
            " ORDER BY o.observed_start DESC, o.id DESC"
            " LIMIT :limit",
            params,
        )
        return [_row(r) for r in rows]

    async def _read(self, sql: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        if self._execute is None:
            from mirobody.utils import execute_query

            self._execute = execute_query
        return list(await self._execute(sql, dict(params)) or [])

    async def _zone_of(self, subject_id: str) -> str:
        if self._tz is not None:
            zone = self._tz(subject_id)
            return (await zone if hasattr(zone, "__await__") else zone) or "UTC"
        return await observations.user_tz(subject_id) or "UTC"


# --- pure --------------------------------------------------------------------


def _row(record: Mapping[str, Any]) -> dict[str, Any]:
    """One database row → one rendered row. `reason` is kept only for an
    uncoded entry: on a coded one it is empty, and a filled cell there would
    read as a caveat about a code the vocabulary was sure of."""
    coded = record.get("outcome") == "coded"
    return {
        "id": record.get("id"),
        "date": str(record.get("date") or ""),
        "time": str(record.get("time") or ""),
        "kind": str(record.get("kind") or ""),
        "text": str(record.get("text") or ""),
        "display": str(record.get("display") or "") if coded else "",
        "code": str(record.get("code") or "") if coded else "",
        "reason": "" if coded else str(record.get("reason") or ""),
        "note": str(record.get("note") or ""),
    }


def _envelope_for(
    request: JournalRequest,
    window: tuple[date, date],
    tz: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    truncated: bool = False,
) -> tools.Envelope:
    notes = [_ABSENCE_NOTE, _WORDS_NOTE]
    if KIND_CONDITION in request.kinds:
        notes.append(_REPORTED_NOTE)
    if not rows:
        what = "matching these keywords " if request.keywords else ""
        notes.append(
            f"nothing {what}in this window; widen it (start/end) before concluding there is nothing on file"
        )
    if truncated:
        notes.append(f"cut at limit={request.limit}; narrow the window or name keywords rather than raising it")
    return tools.Envelope(
        tools.STATUS_PARTIAL if truncated else tools.STATUS_OK,
        data=[{k: v for k, v in r.items() if k != "id"} for r in rows],
        meta=tools.Meta(
            window=(window[0].isoformat(), window[1].isoformat()),
            tz=tz,
            row_count=len(rows),
            truncated=truncated,
        ),
        provenance={str(r.get("id") or i): "reported" for i, r in enumerate(rows)},
        assumptions=tuple(notes),
        next_steps=(tools.NEXT_NARROW_WINDOW,) if truncated else (),
    )


#: This module's tool surface: nothing at module level. The schema, the
#: validator and the parser are the tool's CONTRACT, imported by name: a
#: module-level function without this list would be published as a tool.
__tools__: tuple[str, ...] = ()

__all__ = [
    "COLUMNS",
    "DEFAULT_DAYS",
    "DEFAULT_LIMIT",
    "JournalRequest",
    "JournalService",
    "KINDS",
    "MAX_DAYS",
    "MAX_LIMIT",
    "TOOL_NAME",
    "TOOL_SCHEMA",
    "codes_for",
    "like_patterns",
    "parse_query",
    "validate_query",
    "window_for",
]

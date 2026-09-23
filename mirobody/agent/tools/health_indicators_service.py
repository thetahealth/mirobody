"""`query_health_indicators`: the one tool for a person's readings.

The model sees ONE readings tool, and it has one JSON schema
(`query.TOOL_SCHEMA`) whether it arrives over MCP or in a chat turn. It
replaced a search tool paired with a read tool: the pair forced every client
to spend a model call discovering names before it could ask for numbers, and
it let the two halves disagree about what a window meant. Medications are a
different data class with a different grammar and their own tool
(`medications_service.py`); genetics likewise.

What the person reported (a symptom felt, a diagnosis given) is read here
too. It is the same table, the same series and the same window, coded on
ICPC-3 where a reading is coded on LOINC, so a second tool would only repeat
this one and make "was my blood pressure up on the days I had headaches" two
calls. Such rows carry `provenance="reported"` and render as their own table.

Three things make the flat schema safe:

* **eight parameters, all applicable to every call**, no mode switch, so a
  wrong combination is a refused `(resolution, aggregate)` cell, never a
  parameter that is silently ignored (`query.validate_request`);
* **a dispatch table**: `(resolution, aggregate)` maps to exactly one
  `HealthQuery` method (`query.DISPATCH`);
* **an envelope**: the model reads rendered text, but everything a *program*
  needs (did it work, is a retry pointless, how much was cut, where each number
  came from) travels beside it in a `tools.Envelope`. Governance reads the
  envelope, never the text, because a harness may truncate or evict text.

## Two renderings, one query

`render_compact` is what the model gets: pipe-delimited tables with constant
columns hoisted into one `(constants: unit=mmol/L)` line, because a third-party
MCP client pays per token for the unit repeated on 200 rows. `render_rest` is
what a browser gets: arrays of objects it can sort and paginate. Both are
generic over an envelope, so the medications and genetics tools render through
them too: each says which columns its rows have (`columns`), or lets the
readings shapes be derived.

## It never raises

The `eval` REPL can call this tool directly (PTC), and a PTC call bypasses the
tool middleware entirely: there is nothing above it to contain a fault. So
every path returns an envelope, including the ones that failed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mirobody.kernel import query, tools
from ._authz import refused, subject_for
from ._base import RecordTool
from ._render import awaited, envelope_meta, render_compact

#: The span a window covers when only ONE end is named. Three months: two lab
#: cycles and a season of wearable data.
DEFAULT_HOURS = 24 * 90
#: The longest window one call may cover. A wider request is clamped to its
#: most recent part and says so, rather than being refused or silently served.
MAX_HOURS = 24 * 366 * 5


#: Said on every readings answer, because a model that does not hear it draws
#: the opposite conclusion: an empty result is "not on file", never "not true".
_ABSENCE_NOTE = "no data for an indicator means it was never recorded, not that the condition is absent"

#: Said when the answer holds what the person reported.
_WORDS_NOTE = (
    "in the reported table, name is the person's own words and is the record; system/code are ICPC-3's "
    "classification of them, empty where the vocabulary could not place the words"
)
_SELF_DIAGNOSED_NOTE = "a condition entry is what the person reports being diagnosed with, not a clinical record"


class HealthIndicatorsService(RecordTool):
    """The tool body.

    `__tools__` is the whole published surface. `envelope` is public because
    the REST route and the chat adapter both need the envelope rather than a
    rendering, and `load_tools_from_class` would otherwise publish it as a
    second, undocumented MCP tool, which is exactly what it did until this
    list existed.
    """

    #: The ONE method that is a tool. See `mcp/tool.py::_declared_tool_names`.
    __tools__ = (query.TOOL_NAME,)
    TOOL_NAME = query.TOOL_NAME

    #: Published verbatim as the MCP `inputSchema` and as the chat tool's
    #: `args_schema`, so the two surfaces cannot drift. See `mcp/tool.py`.
    input_schema = query.TOOL_SCHEMA

    def __init__(self, health_query: Any = None, *, now: Any = None, catalog_cap: int | None = None) -> None:
        self._health_query = health_query
        self._now = now  # injected in tests; production reads the clock
        # A browser's catalogue is a table it scrolls, not a model's context
        # window. `None` leaves the implementation's model-facing default.
        self._catalog_cap = catalog_cap

    def _query(self) -> Any:
        if self._health_query is None:
            from mirobody.collect import PostgresHealthQuery
            self._health_query = PostgresHealthQuery()
        return self._health_query

    async def query_health_indicators(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """
        Read this person's health record over time: readings (labs, vitals,
        wearable metrics: anything with a value and a time) and what they
        reported (symptoms they felt, diagnoses they were given, in their own
        words, coded on ICPC-3).

        USE IT when the question is about their own data: "how has my LDL
        moved", "average resting heart rate this month", "how often do I get
        headaches", "was my blood pressure up on the days I had headaches".
        With no `keywords`/`indicators` it returns the CATALOGUE of what this
        person actually has, reported entries first, which is the right first
        call when you do not know the names. Ask for the shape you need:
        `aggregate="stats"` for change, a baseline or how often,
        `resolution="day"` for a trend line, `aggregate="latest"` for "what is
        it now", never raw rows you would reduce yourself.

        DO NOT use it for medications (`query_medications`), for general
        medical knowledge or reference ranges, or for a person outside the
        caller's care circle. Do not call it twice with the same arguments:
        the second call returns the same rows and costs another round trip.

        The parameters are documented in the schema, not here: `input_schema`
        IS `query.TOOL_SCHEMA`, published verbatim, and an `Args:` section in
        this docstring would silently overwrite the contract's own
        descriptions with a second copy that drifts.

        Returns:
            A compact table plus a `meta` block saying which window was read,
            in which time zone, at what resolution, how many rows came back and
            whether they were cut. `truncated` means narrow the window or
            aggregate, never raise `limit` and call again. No dates means the
            whole record. Row cap: 500 raw rows per indicator, 200 catalogue
            names.

        Notes for LLMs:
            - No data for an indicator means it was never recorded. It does NOT
              mean the person does not have the condition. Say so rather than
              concluding they are healthy.
            - In the reported table, quote the person's words (`name`). The
              ICPC-3 name classifies them; an entry without a code is still
              their report. A condition there is self-reported, not confirmed.
        """
        envelope = await self.envelope(user_info, **args)
        return {"result": render_compact(envelope), **envelope_meta(envelope)}


    # --- the run ------------------------------------------------------------

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = query.validate_request(args)
        if problems:
            return refused(problems)
        request = query.parse_request(args)
        hq = self._query()
        subject_id = await subject_for(caller_id, request.member)
        tz = await awaited(hq.tz(subject_id)) or "UTC"
        await awaited(hq.on_read(subject_id))

        # No dates named means the whole record, not "the last ninety days":
        # "how has my LDL moved" is a question about a life, and a default
        # window would answer it about a season while REPORTING the season as
        # if the rows had come from it.
        window = query.resolve_window(
            tz, start=request.start, end=request.end, hours=DEFAULT_HOURS, now=self._clock(), max_hours=MAX_HOURS
        )
        method = request.method
        rows = await self._dispatch(hq, method, subject_id, request, window)

        # Nothing matched a selection the caller DID give: answer with what the
        # person actually has rather than with an empty table. The alternative
        # costs a model call to learn the names, every time.
        fell_back = False
        if not rows and method != "catalog":
            rows = await self._dispatch(hq, "catalog", subject_id, request, window)
            method, fell_back = "catalog", True

        return _envelope_for(method, request, window, rows, fell_back=fell_back)

    async def _dispatch(
        self, hq: Any, method: str, subject_id: str, request: query.QueryRequest, window: query.Window
    ) -> Sequence[Mapping[str, Any]]:
        """`query.DISPATCH` already chose the method; this only calls it.

        The catalogue takes the window as `None` when the caller named no
        dates, because "everything I have" is a different question from
        "everything I have in the last 90 days" and the default window would
        quietly turn the first into the second.
        """
        sel = request.selection
        dated = bool(request.start or request.end)
        if method == "catalog":
            cap = {"cap": self._catalog_cap} if self._catalog_cap else {}
            return await awaited(hq.catalog(subject_id, window if dated else None, **cap))
        if method == "readings":
            return await awaited(hq.readings(subject_id, sel, window, limit=request.limit))
        if method == "buckets":
            return await awaited(hq.buckets(subject_id, sel, window, resolution=request.resolution))
        if method == "stats":
            return await awaited(hq.stats(subject_id, sel, window, basis=request.basis))
        if method == "latest":
            return await awaited(hq.latest(subject_id, sel, window, basis=request.basis))
        raise ValueError(f"no leaf for method {method!r}")


# --- envelope and renderings (pure) -----------------------------------------


def _envelope_for(
    method: str,
    request: query.QueryRequest,
    window: query.Window,
    rows: Sequence[Mapping[str, Any]],
    *,
    fell_back: bool = False,
) -> tools.Envelope:
    rows = list(rows)
    dated = bool(request.start or request.end)
    total = max((int(r.get("total") or 0) for r in rows), default=0)
    truncated = bool(total and total > len(rows)) or _per_indicator_truncated(rows)
    semantics = _semantics(rows)
    meta = tools.Meta(
        window=(window.start, window.end) if dated else ("", ""),
        tz=window.tz,
        window_semantics=semantics,
        resolution=request.resolution,
        aggregate=request.aggregate,
        aggregate_basis=request.basis if method in ("stats", "latest") else "",
        row_count=len(rows),
        truncated=truncated,
        catalog_total=total if method == "catalog" else 0,
    )
    assumptions: list[str] = []
    if dated and window.note:
        assumptions.append(window.note)
    if fell_back:
        assumptions.append("no indicator matched those terms; this is what this person has on file")
    if semantics == query.SEMANTICS_DATE_PADDED:
        assumptions.append("some rows predate the stored local day; their window is padded a day each way")
    assumptions.append(_ABSENCE_NOTE)
    reported = [r for r in rows if r.get("provenance") == tools.PROVENANCE_REPORTED]
    if reported:
        assumptions.append(_WORDS_NOTE)
    if any(r.get("kind") == "condition" for r in reported):
        assumptions.append(_SELF_DIAGNOSED_NOTE)

    # What to do next, and never something the next call would refuse: a
    # catalogue cannot be aggregated, so "aggregate" is not advice there.
    next_steps: list[str] = []
    if method == "catalog":
        next_steps.append(tools.NEXT_USE_INDICATORS)
        if truncated:
            next_steps.append(tools.NEXT_NARROW_WINDOW)
    elif not rows:
        next_steps.append(tools.NEXT_PICK_FROM_CATALOG)
    elif truncated:
        next_steps.extend((tools.NEXT_NARROW_WINDOW, tools.NEXT_AGGREGATE))

    status = tools.STATUS_PARTIAL if truncated else tools.STATUS_OK
    return tools.Envelope(
        status,
        data=rows,
        meta=meta,
        provenance={str(r.get("indicator") or ""): str(r.get("provenance") or "measured") for r in rows},
        assumptions=tuple(assumptions),
        next_steps=tuple(dict.fromkeys(next_steps)),
    )


def _per_indicator_truncated(rows: Sequence[Mapping[str, Any]]) -> bool:
    seen: dict[str, int] = {}
    totals: dict[str, int] = {}
    for r in rows:
        name = str(r.get("indicator") or "")
        seen[name] = seen.get(name, 0) + 1
        totals[name] = max(totals.get(name, 0), int(r.get("total") or 0))
    return any(totals[n] > seen[n] for n in seen if totals.get(n))


def _semantics(rows: Sequence[Mapping[str, Any]]) -> str:
    """`tz_exact` unless some row had no stored local day.

    Every leaf carries `day_known` for exactly this. Reported rather than
    assumed, because it is the difference between "on the 3rd" and "around the
    3rd": a row that predates `a4_series_data_day_authority.sql` is found by
    padding its naive timestamp a day each way, and a reader that is not told
    so will quote it as an exact date.
    """
    return (
        query.SEMANTICS_TZ_EXACT
        if all(r.get("day_known", True) for r in rows)
        else query.SEMANTICS_DATE_PADDED
    )


#: This module's tool surface: nothing. `envelope` is API for the REST route
#: and the chat adapter, not a tool a model may run.
__tools__: tuple[str, ...] = ()

__all__ = ["DEFAULT_HOURS", "MAX_HOURS", "HealthIndicatorsService"]

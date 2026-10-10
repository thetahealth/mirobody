"""`query_health_indicators`: the one tool for a person's readings.

The model sees ONE readings tool, and it has one JSON schema
(`query.TOOL_SCHEMA`) whether it arrives over MCP or in a chat turn. It
replaced a search tool paired with a read tool: the pair forced every client
to spend a model call discovering names before it could ask for numbers, and
it let the two halves disagree about what a window meant. Medications are a
different data class with a different grammar and their own tool
(`medications_service.py`); genetics likewise.

What the person reported (a symptom felt, a diagnosis given, a note on their
day) is read here too. It is the same table, the same series and the same
window, coded on ICPC-3 where a reading is coded on LOINC (a note is never
coded), so a second tool would only repeat
this one and make "was my blood pressure up on the days I had headaches" two
calls. Such rows carry `provenance="reported"` and render as their own table.

Three things make the flat schema safe:

* **five parameters, all applicable to every call**, no mode switch and no
  combination to get wrong: `view` is one enum (`query.validate_request`);
* **a dispatch table**: `view` maps to exactly one `HealthQuery` method
  (`query.DISPATCH`);
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
from datetime import date, datetime, timedelta
from typing import Any

from mirobody.kernel import citations, query, series, tools
from ._authz import refused
from ._base import RecordTool
from ._refs import record_eval_rids
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

#: Said on a day, week or month view: each of those reads one value per day
#: (`collect/query.py::_DAY_AUTHORITY_CTE`), while `stats` counts every
#: reading of a day nothing elected. Unsaid, the two disagree with no
#: visible reason: a month of home blood pressures averaged 106.0 in
#: `view="month"` against 111.0 over all its readings (2026-10-06).
_DAY_VALUE_NOTE = (
    "each day counts once here, as its elected value or else its last reading; "
    "view=stats counts every reading of such a day"
)

#: Repeated on every answer with citable rows, as the reference search repeats
#: its own citation rule: a small model drops standing instructions under load.
_RID_NOTE = "cite a row's rid, as (r3), next to any number you quote from it"


def _attach_rids(scope: str, method: str, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Give every citable row its `rid` (`kernel.citations`), and take the
    machinery back out of the model's view: `src_ids` is the registry's
    input, never a column — a model handed a hundred ids cites none of them.

    A raw reading mints on its `row_id`; an aggregate mints on the sorted ids
    it counted, so the same statistic shown twice keeps one rid. The registry
    scope is the record being read, which is the conversation from the
    model's side. A row with nothing to cite to keeps no rid rather than
    minting a dangling one (a catalogue row: it stands for a series, not for
    readings).
    """
    if not rows or method == "catalog":
        return list(rows)
    table = citations.table_for(scope)
    out: list[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        # Ids are text in the registry whatever the store returns (int PKs,
        # UUID objects), so a verifier compares like with like.
        src = tuple(map(str, row.pop("src_ids", ()) or ()))
        if src:
            key: Any = ("agg", tuple(sorted(src)))
        else:
            row_id = row.get("row_id")
            if row_id in (None, ""):
                out.append(row)
                continue
            key = ("row", str(row_id))
            src = (str(row_id),)
        row["rid"] = table.rid_for(key, src)
        out.append(row)
    # An `eval` in flight brackets itself with a sink (`tools._refs`) and the
    # middleware appends these as the computed answer's citation refs; a
    # direct call has no sink and this is a no-op.
    record_eval_rids(row["rid"] for row in out if row.get("rid"))
    return out


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

    def __init__(self, health_query: Any = None, *, now: Any = None, catalog_cap: int | None = None,
                 row_cap: int = query.ROW_CAP, bucket_cap: int = query.BUCKET_CAP,
                 outside_note: bool = True) -> None:
        self._health_query = health_query
        self._now = now  # injected in tests; production reads the clock
        # A browser's catalogue and reading list are tables it scrolls, not a
        # model's context window. `None` leaves the model-facing catalogue cap.
        self._catalog_cap = catalog_cap
        self._row_cap = row_cap
        self._bucket_cap = bucket_cap
        # The note costs a whole-record catalogue on every dated call, and is
        # written for a model; a caller that renders no notes turns it off.
        self._outside_note = outside_note

    def _query(self) -> Any:
        if self._health_query is None:
            from mirobody.collect import PostgresHealthQuery
            self._health_query = PostgresHealthQuery()
        return self._health_query

    async def query_health_indicators(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """
        Read this person's health record over time: readings (labs, vitals,
        wearable metrics: anything with a value and a time) and what they
        reported (symptoms they felt, diagnoses they were given, coded on
        ICPC-3; notes on their day, such as a meal, never coded; all in their
        own words).

        USE IT when the question is about their own data: "how has my LDL
        moved", "average resting heart rate this month", "how often do I get
        headaches", "was my blood pressure up on the days I had headaches".
        With no `keywords`/`indicators` it returns the CATALOGUE of what this
        person actually has, reported entries first, which is the right first
        call when you do not know the names. Ask for the shape you need:
        `view="stats"` for change, a baseline or how often, `view="day"` for a
        trend line, `view="latest"` for "what is it now", never raw rows you
        would reduce yourself.

        DO NOT use it for medications (`query_medications`) or for general
        medical knowledge or reference ranges. Do not call it twice with the same arguments:
        the second call returns the same rows and costs another round trip.

        The parameters are documented in the schema, not here: `input_schema`
        IS `query.TOOL_SCHEMA`, published verbatim, and an `Args:` section in
        this docstring would silently overwrite the contract's own
        descriptions with a second copy that drifts.

        Returns:
            A compact table plus a `meta` block saying which window was read,
            in which time zone, in which view, how many rows came back and
            whether they were cut. `truncated` means narrow the window or ask
            for `view="stats"` (or a coarser view). No dates means the whole
            record. Caps: 50 raw rows and 92 minute…month points per
            indicator (the newest), 200 catalogue names.

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
        subject_id = caller_id
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

        rows = _attach_rids(subject_id, method, rows)

        outside = ""
        if self._outside_note and method != "catalog" and (request.start or request.end):
            outside = _outside_note(await awaited(hq.catalog(subject_id, None)), rows, window)
        return _envelope_for(method, request, window, rows, fell_back=fell_back, bucket_cap=self._bucket_cap,
                             row_cap=self._row_cap, outside=outside)

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
            return await awaited(hq.readings(subject_id, sel, window, limit=self._row_cap))
        if method == "buckets":
            return await awaited(hq.buckets(subject_id, sel, window, resolution=request.view, limit=self._bucket_cap))
        if method == "stats":
            return await awaited(hq.stats(subject_id, sel, window))
        if method == "latest":
            return await awaited(hq.latest(subject_id, sel, window))
        raise ValueError(f"no leaf for method {method!r}")


# --- envelope and renderings (pure) -----------------------------------------


def _envelope_for(
    method: str,
    request: query.QueryRequest,
    window: query.Window,
    rows: Sequence[Mapping[str, Any]],
    *,
    fell_back: bool = False,
    bucket_cap: int = query.BUCKET_CAP,
    row_cap: int = query.ROW_CAP,
    outside: str = "",
) -> tools.Envelope:
    rows = list(rows)
    dated = bool(request.start or request.end)
    total = max((int(r.get("total") or 0) for r in rows), default=0)
    # A catalogue row's `total` is the catalogue's size, not its series' row
    # count: read per indicator, every complete catalogue of two was "cut".
    truncated = bool(total and total > len(rows)) or (method != "catalog" and bool(_cut_indicators(rows)))
    semantics = _semantics(rows)
    cut = ""
    if truncated and method == "buckets":
        cut = _cut_note(request.view, rows, bucket_cap)
    elif truncated and method == "readings":
        cut = _raw_cut_note(rows, row_cap)
    meta = tools.Meta(
        window=(window.start, window.end) if dated else ("", ""),
        tz=window.tz,
        window_semantics=semantics,
        view="" if method == "catalog" else request.view,
        row_count=len(rows),
        truncated=truncated,
        cut=cut,
        catalog_total=total if method == "catalog" else 0,
    )
    assumptions: list[str] = []
    if dated and window.note:
        assumptions.append(window.note)
    if outside:
        assumptions.append(outside)
    if fell_back:
        assumptions.append("no indicator matched those terms; this is what this person has on file")
    if method == "catalog" and request.view_unapplied:
        assumptions.append(
            f"view={request.view} was not applied: with no keywords or indicators the answer is this catalogue; "
            f"call again with indicators copied from it and view={request.view}"
        )
    if method == "buckets" and request.view in ("day", "week", "month"):
        assumptions.append(_DAY_VALUE_NOTE)
    if semantics == query.SEMANTICS_DATE_PADDED:
        assumptions.append("some rows predate the stored local day; their window is padded a day each way")
    assumptions.append(_ABSENCE_NOTE)
    if any(r.get("rid") for r in rows):
        assumptions.append(_RID_NOTE)
    reported = [r for r in rows if r.get("provenance") == tools.PROVENANCE_REPORTED]
    if reported:
        assumptions.append(_WORDS_NOTE)
    if any(r.get("kind") == "condition" for r in reported):
        assumptions.append(_SELF_DIAGNOSED_NOTE)

    # What to do next, and never something the next call would refuse or
    # answer the same way: stats with no names is this catalogue again, so
    # "ask for stats" is not advice there.
    next_steps: list[str] = []
    if method == "catalog":
        next_steps.append(tools.NEXT_USE_INDICATORS)
        if truncated:
            next_steps.append(tools.NEXT_NARROW_WINDOW)
    elif not rows:
        next_steps.append(tools.NEXT_PICK_FROM_CATALOG)
    elif truncated:
        next_steps.extend((tools.NEXT_NARROW_WINDOW, tools.NEXT_VIEW_STATS))

    status = tools.STATUS_PARTIAL if truncated else tools.STATUS_OK
    return tools.Envelope(
        status,
        data=rows,
        meta=meta,
        provenance={str(r.get("indicator") or ""): str(r.get("provenance") or "measured") for r in rows},
        assumptions=tuple(assumptions),
        next_steps=tuple(dict.fromkeys(next_steps)),
    )


#: Said in every cut notice: what a model concluded without it. MiniCPM5-2B,
#: handed the latest 92 days of a March-to-August window with the cut said
#: last, among the notes after the table, answered that March and April had
#: no data (benchmarks/local_models small-v2, 2026-10-07).
_NOT_MISSING = "exists and is not shown here; it is not missing"


def _cut_note(view: str, rows: Sequence[Mapping[str, Any]], cap: int) -> str:
    """What a cut bucket answer shows, in plain words, and the calls that show
    the rest: a coarser view, or the window that ends where this one starts.
    The span is stated because the window line still reads "all recorded
    data": MiniCPM5-2B, handed a year of days for "the past three months",
    answered with October to December, months the record does not have
    (2026-09-30, docs/local-models-roadmap.md)."""
    cut = _cut_indicators(rows)
    periods = sorted(str(r.get("period") or "") for r in rows if r.get("period") and str(r.get("indicator") or "") in cut)
    at = query.BUCKETS.index(view) if view in query.BUCKETS else -1
    coarser = query.BUCKETS[at + 1] if 0 <= at < len(query.BUCKETS) - 1 else ""
    ways = [f"view={coarser}"] if coarser else []
    if periods and (before := _day_before(periods[0])):
        ways.append(f"end={before}")
    span = f", {periods[0]} to {periods[-1]}" if periods else ""
    rest = f" For it, call again with {' or with '.join(ways)}." if ways else ""
    return f"Only part of the data is shown: the latest {cap} {view} points per indicator{span}. Earlier data {_NOT_MISSING}.{rest}"


def _raw_cut_note(rows: Sequence[Mapping[str, Any]], cap: int) -> str:
    """The same for raw rows, cut at `cap` per indicator, newest first."""
    cut = _cut_indicators(rows)
    times = sorted(str(r.get("time") or "") for r in rows if r.get("time") and str(r.get("indicator") or "") in cut)
    ways = ["view=day", "view=stats"]
    if times and (before := _day_before(times[0])):
        ways.append(f"end={before}")
    span = f", {times[0][:16]} to {times[-1][:16]}" if times else ""
    return (f"Only part of the data is shown: the latest {cap} readings per indicator{span}. "
            f"Older data {_NOT_MISSING}. For it, call again with {' or with '.join(ways)}.")


def _outside_note(catalog: Sequence[Mapping[str, Any]], rows: Sequence[Mapping[str, Any]],
                  window: query.Window) -> str:
    """One note naming each answered series that has readings before or after
    a window the caller named, from the whole-record catalogue; "" for none.

    Unsaid, a window reads as the whole story: asked how cholesterol moved, a
    small model read a window holding 2 of 3 readings and answered from those
    (newcomer review M5, 2026-10-07). With one end named the other is the
    default span, so the bounds compared are the ones the rows were read
    from, not the dates the caller wrote."""
    answered = {str(r.get("series") or "") for r in rows} - {""}
    zone = series.zone(window.tz)
    first_day = datetime.fromtimestamp(window.start_ms / 1000, zone).date().isoformat()
    last_day = datetime.fromtimestamp((window.end_ms - 1) / 1000, zone).date().isoformat()
    outside = []
    for entry in catalog:
        if str(entry.get("series") or "") not in answered:
            continue
        first, last = str(entry.get("first_date") or ""), str(entry.get("last_date") or "")
        spans = []
        if first and first < first_day:
            spans.append(f"from {first}")
        if last and last > last_day:
            spans.append(f"until {last}")
        if spans:
            outside.append(f"{entry.get('indicator')} ({', '.join(spans)})")
    if not outside:
        return ""
    return (f"this window ({first_day}..{last_day}) leaves out readings of " + "; ".join(outside)
            + "; call again with a wider start or end to include them")


def _day_before(stamp: str) -> str:
    """The day before the local date that starts `stamp`, or "" for none."""
    try:
        return (date.fromisoformat(stamp[:10]) - timedelta(days=1)).isoformat()
    except ValueError:
        return ""


def _cut_indicators(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    """The indicators whose rows stop short of their `total`."""
    seen: dict[str, int] = {}
    totals: dict[str, int] = {}
    for r in rows:
        name = str(r.get("indicator") or "")
        seen[name] = seen.get(name, 0) + 1
        totals[name] = max(totals.get(name, 0), int(r.get("total") or 0))
    return {n for n in seen if totals.get(n) and totals[n] > seen[n]}


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

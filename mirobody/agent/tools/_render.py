"""How a tool's envelope reaches its two readers.

`render_compact` is what the model gets: pipe-delimited tables with constant
columns hoisted into one `(constants: unit=mmol/L)` line, because a third-party
MCP client pays per token for the unit repeated on 200 rows. `render_rest` is
what a browser gets: arrays of objects it can sort and paginate.

Both are generic over an envelope, so readings, medications and genetics all
render through them; each tool says which columns its rows have (`columns`), or
lets the readings shapes be derived. A readings result renders what the person
reported as a second table (`_readings_table`). Underscore-prefixed so the tool loader
never publishes anything in here.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import Any

from mirobody.kernel import query, tools

#: The character budget for one rendered result. Past this the tool truncates
#: and tells the model how to ask again: an answer that blows the context
#: window is not an answer.
MAX_RENDER_CHARS = 40_000

#: Columns each method renders, in order. Anything not listed never reaches the
#: model: `row_id` is for the web client's edit button, `file_key` is the handle
#: it opens the document with, `total` and `day_known` are bookkeeping, and
#: `provenance` rides in the envelope.

#: `file` is the document's NAME. Handed `file_key` instead, the model cited
#: "web_uploads/17eaf4f6-….pdf" as the source of a value.

#: `system`/`code` ride on every method that prints a VALUE: withheld, the model
#: filled them in from memory and gave 1558-6, the [Mass/volume] glucose code,
#: for a value it had just printed as 5.4 mmol/L. Both, never one: a
#: device-namespace row carries the indicator's own name in `code`.
_COLUMNS: dict[str, tuple[str, ...]] = {
    "catalog": ("indicator", "system", "code", "count", "first_date", "last_date", "reason"),
    "readings": ("indicator", "name", "time", "value", "unit", "system", "code", "file"),
    "buckets": ("indicator", "period", "avg", "min", "max", "n", "unit", "system", "code"),
    "stats": ("indicator", "count", "min", "max", "avg", "first", "first_date", "last", "last_date",
              "change", "unit", "mixed_units", "system", "code"),
    "latest": ("indicator", "name", "date", "time", "value", "unit", "system", "code"),
}

#: The same, for what the person REPORTED: `name` is their words and is the
#: record, `system`/`code` classify it (ICPC-3), `reason` says why an entry is
#: uncoded. No value and no unit, because a symptom has neither. `count` in
#: stats is entries over raw rows and days over a daily basis.
_REPORTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "catalog": ("indicator", "name", "kind", "system", "code", "count", "first_date", "last_date", "reason"),
    "readings": ("indicator", "name", "kind", "time", "system", "code", "reason", "note"),
    "buckets": ("indicator", "period", "n", "system", "code"),
    "stats": ("indicator", "count", "first_date", "last_date", "system", "code"),
    "latest": ("indicator", "name", "kind", "date", "time", "system", "code", "reason", "note"),
}

_REPORTED_HEADING = "reported by the person (name = their words; system/code classify them):"


async def awaited(value: Any) -> Any:
    """A port is declared with plain `def` so an in-memory implementation is
    possible; the reference one is async. Accept either rather than forcing a
    choice on every consumer."""
    return await value if inspect.isawaitable(value) else value


def _method_of(first: Mapping[str, Any]) -> str:
    """Which leaf produced a readings result. Derived from the row shape rather
    than passed down, so a renderer can never disagree with its data."""
    if "period" in first:
        return "buckets"
    if "avg" in first and "count" in first:
        return "stats"
    if "first_date" in first:
        return "catalog"
    if "time" in first and "total" in first:
        return "readings"
    if "value" in first:
        return "latest"
    return ""


def _readings_table(rows: Sequence[Mapping[str, Any]]) -> str:
    """A readings result as one table, and what the person reported as a
    second one under its own heading: one table would put a blank value
    beside every symptom and let a self-reported diagnosis read as a result."""
    measured = [r for r in rows if r.get("provenance") != tools.PROVENANCE_REPORTED]
    reported = [r for r in rows if r.get("provenance") == tools.PROVENANCE_REPORTED]
    parts: list[str] = []
    for group, columns, heading in ((measured, _COLUMNS, ""), (reported, _REPORTED_COLUMNS, _REPORTED_HEADING)):
        if not group:
            continue
        method = _method_of(group[0])
        table = query.compact(group, columns[method] if method else tuple(group[0].keys()))
        parts.append(f"{heading}\n{table}" if heading else table)
    return "\n\n".join(parts)


def render_compact(envelope: tools.Envelope, columns: Sequence[str] | None = None) -> str:
    """What the model reads: the table, then the methodology. Never prose about
    findings: the model narrates, the tool reports. `columns` is for a tool
    whose rows are not readings; a readings result derives its own."""
    if envelope.status == tools.STATUS_ERROR:
        return _render_error(envelope)
    rows = list(envelope.data or [])
    if columns:
        table = query.compact(rows, tuple(columns)) if rows else ""
    else:
        table = _readings_table(rows)
    body = table or "(no rows)"
    if len(body) > MAX_RENDER_CHARS:
        body = body[:MAX_RENDER_CHARS] + f"\n… cut at {MAX_RENDER_CHARS} characters"
    lines = [body, "", _meta_line(envelope.meta)]
    if envelope.assumptions:
        lines.append("notes: " + "; ".join(envelope.assumptions))
    if envelope.next_steps:
        lines.append("next: " + ", ".join(envelope.next_steps))
    return "\n".join(lines)


def render_rest(envelope: tools.Envelope) -> dict[str, Any]:
    """What a browser reads: rows as objects, with the same meta block. The
    web client sorts and paginates these; it must never be handed a pipe
    table to parse back into the dicts it came from."""
    meta = envelope.meta
    return {
        "rows": [dict(r) for r in (envelope.data or [])],
        "count": meta.row_count,
        "total": meta.catalog_total or meta.row_count,
        "truncated": meta.truncated,
        "window": {"start": meta.window[0], "end": meta.window[1], "tz": meta.tz, "semantics": meta.window_semantics},
        "resolution": meta.resolution,
        "aggregate": meta.aggregate,
        "status": envelope.status,
        **({"error_kind": envelope.error_kind} if envelope.error_kind else {}),
    }


def _render_error(envelope: tools.Envelope) -> str:
    reason = "; ".join(envelope.assumptions) or "this lookup could not complete"
    hint = (
        "Fix the arguments and try once more."
        if envelope.error_class == tools.ERROR_RECOVERABLE
        else "Do not repeat this call. Continue with what you have, or tell the person it is unavailable."
    )
    return f"error ({envelope.error_kind}): {reason}. {hint}"


def _meta_line(meta: tools.Meta) -> str:
    # A tool whose data has no time axis (genetics) reports no zone, and gets
    # no window line: "window=all recorded data, tz=, dates=tz_exact" on a
    # genotype answer is three tokens of noise and one false claim: a
    # genotype is not dated at all, exactly or otherwise.
    bits: list[str] = []
    if meta.tz or any(meta.window):
        span = f"{meta.window[0]}..{meta.window[1]}" if any(meta.window) else "all recorded data"
        bits += [f"window={span}", f"tz={meta.tz}", f"dates={meta.window_semantics}"]
    if meta.resolution:
        bits.append(f"resolution={meta.resolution}")
    if meta.aggregate and meta.aggregate != "none":
        bits.append(f"aggregate={meta.aggregate}/{meta.aggregate_basis}")
    bits.append(f"rows={meta.row_count}")
    if meta.catalog_total:
        bits.append(f"of {meta.catalog_total}")
    if meta.truncated:
        bits.append("truncated")
    return "(" + ", ".join(bits) + ")"


def envelope_meta(envelope: tools.Envelope) -> dict[str, Any]:
    """The machine-readable half a PTC caller sees next to the rendering: it
    has no middleware above it to read the artifact."""
    return {
        "status": envelope.status,
        "row_count": envelope.meta.row_count,
        "truncated": envelope.meta.truncated,
        **({"error_kind": envelope.error_kind} if envelope.error_kind else {}),
    }

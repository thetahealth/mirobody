"""`query_genetic_data` — the one tool for a person's genotype calls.

Genetics is a third data class, next to readings
(`health_indicators_service.py`) and medications (`medications_service.py`),
and it gets its own tool for the same reason they do: its grammar shares
nothing with theirs. A genotype has no window, no resolution and no
aggregate — a call is "what did this person's array call at these rsIDs",
plus optionally the neighbours of each hit. Five parameters, every one
applicable to every call.

The tool shell is the same three steps as its siblings — authorize, run,
render — and the same envelope: the model reads a rendered table, and
everything a *program* needs (did it work, is a retry pointless, how much was
cut) travels beside it in a `tools.Envelope`.

Two facts about the data that the tool has to say out loud on every answer,
because a reader that is not told them draws the opposite conclusion:

* **Absent is not negative.** This reads THEIR uploaded genotype file, not a
  reference database. A consumer array types a small fraction of the genome,
  so an rsID missing from the result was not typed.
* **Near is not linked.** `include_nearby` returns variants near by POSITION.
  Proximity is not linkage disequilibrium and says nothing about the queried
  variant's trait.

It never raises, for the same reason as the readings tool: the `eval` REPL can
call it directly (PTC), and a PTC call has nothing above it to contain a fault.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ...kernel import query, tools
from ...kernel.ops import is_driver_exception
from ._authz import caller_of, denied, refused, subject_for
from .health_indicators_service import envelope_meta, render_compact

logger = logging.getLogger(__name__)

TOOL_NAME = "query_genetic_data"

#: rsIDs one call may name. The list becomes an `IN` clause of bound
#: parameters, and a model that wants a whole panel should ask twice.
MAX_RSIDS = 50
#: Variants one answer may carry, and the default. Direct hits only — the
#: neighbours of each hit are capped separately.
MAX_LIMIT = 500
DEFAULT_LIMIT = 100
#: Neighbours returned per hit. A megabase window on a dense array holds
#: hundreds of typed variants; twenty is a neighbourhood, not a dump.
MAX_NEARBY_PER_HIT = 20
#: Half-window for `include_nearby`, in base pairs.
DEFAULT_NEARBY_RANGE = 1_000_000

#: Columns the answer renders, in order. `distance` and `near` are empty on a
#: direct hit, and `render_compact` drops a column no row fills — so an exact
#: lookup renders four columns, not six.
COLUMNS: tuple[str, ...] = ("rsid", "chromosome", "position", "genotype", "distance", "near")

#: Said on every genetics answer. See the module docstring: a reader who is
#: not told these two things concludes the opposite of what the data supports.
_ABSENCE_NOTE = (
    "a consumer array types a fraction of the genome: an rsID missing here was not typed, "
    "which is not evidence about the allele"
)
_UNPHASED_NOTE = "genotypes are unphased: \"AG\" does not say which parent contributed which allele"
_PROXIMITY_NOTE = "nearby variants are near by POSITION only; proximity is not linkage — do not tie them to the queried variant's trait"

TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["rsids"],
    "properties": {
        "rsids": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": MAX_RSIDS,
            "description": (
                "dbSNP identifiers to look up, e.g. [\"rs4988235\", \"rs1801133\"]. Required: this reads the "
                "person's own genotype file, which has no catalogue to browse."
            ),
        },
        "include_nearby": {
            "type": "boolean",
            "default": True,
            "description": (
                "Also return typed variants within nearby_range of each hit (at most "
                f"{MAX_NEARBY_PER_HIT} per hit). False for exact lookups only."
            ),
        },
        "nearby_range": {
            "type": "integer",
            "minimum": 1,
            "default": DEFAULT_NEARBY_RANGE,
            "description": "Half-window for include_nearby, in base pairs (default 1,000,000).",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_LIMIT,
            "default": DEFAULT_LIMIT,
            "description": "Direct hits returned, ordered by chromosome and position. Name fewer rsIDs instead of raising it.",
        },
        "member": {
            "type": "string",
            "description": "Read another person's genotype you are authorised to see (a care-circle member id). Omit for the caller.",
        },
    },
}


@dataclass(frozen=True)
class GeneticRequest:
    """The tool's arguments after normalisation. Built by :func:`parse_query`."""

    rsids: tuple[str, ...] = ()
    include_nearby: bool = True
    nearby_range: int = DEFAULT_NEARBY_RANGE
    limit: int = DEFAULT_LIMIT
    member: str = ""


def validate_query(args: Mapping[str, Any]) -> tuple:
    """Everything wrong with the raw arguments (``query.Rejection`` rows);
    empty means :func:`parse_query` will succeed."""
    out = query.reject_unknown(args, TOOL_SCHEMA)
    rsids = query.normalize_list_arg(args.get("rsids"))
    if not rsids:
        out.append(query.Rejection("rsids", "name at least one rsID; this tool has no catalogue to browse"))
    elif len(rsids) > MAX_RSIDS:
        out.append(query.Rejection("rsids", f"at most {MAX_RSIDS} rsIDs per call"))
    lim = args.get("limit")
    if lim not in (None, "") and (not isinstance(lim, int) or not 1 <= lim <= MAX_LIMIT):
        out.append(query.Rejection("limit", f"must be an integer between 1 and {MAX_LIMIT}"))
    rng = args.get("nearby_range")
    if rng not in (None, "") and (not isinstance(rng, int) or rng < 1):
        out.append(query.Rejection("nearby_range", "must be a positive number of base pairs"))
    near = args.get("include_nearby")
    if near not in (None, "") and not isinstance(near, bool):
        out.append(query.Rejection("include_nearby", "must be true or false"))
    return tuple(out)


def parse_query(args: Mapping[str, Any]) -> GeneticRequest:
    """Raw arguments → a :class:`GeneticRequest`; ``ValueError`` when
    :func:`validate_query` finds anything."""
    problems = validate_query(args)
    if problems:
        raise ValueError("; ".join(f"{r.parameter}: {r.reason}" for r in problems))
    near = args.get("include_nearby")
    return GeneticRequest(
        # A bare string is split on commas and a JSON-stringified list is
        # parsed, the same normalisation the readings tool's name lists get:
        # an untreated `"rs1,rs2"` matches no rsID and answers "not typed".
        rsids=query.normalize_list_arg(args.get("rsids")),
        include_nearby=near if isinstance(near, bool) else True,
        nearby_range=int(args.get("nearby_range") or DEFAULT_NEARBY_RANGE),
        limit=int(args.get("limit") or DEFAULT_LIMIT),
        member=str(args.get("member") or ""),
    )


class GeneticService:
    """The tool body. `__tools__` is the whole published surface; `envelope`
    is API for the chat adapter, not a tool."""

    __tools__ = (TOOL_NAME,)
    input_schema = TOOL_SCHEMA

    def __init__(self, execute: Any = None) -> None:
        # Injected so a test can answer without a database; the shared pool
        # otherwise. There is no `HealthQuery`-style port here because there is
        # one query shape and one table.
        self._execute = execute

    async def query_genetic_data(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """
        Read this person's genotype calls at named variants (rsIDs), from the
        raw genotype file they uploaded.

        USE IT when the question names variants or asks what this person
        carries at one — "what is my rs4988235", "am I a C677T carrier". With
        include_nearby it also returns the typed variants around each hit.

        DO NOT use it for readings (`query_health_indicators`), for
        medications (`query_medications`), for what a variant MEANS (that is
        knowledge, not this person's data), or for a person outside the
        caller's care circle. It has no catalogue: name the rsIDs.

        The parameters are documented in the schema (`input_schema` IS
        `TOOL_SCHEMA`, published verbatim).

        Returns:
            A compact table — rsid, chromosome, position, genotype, and for a
            neighbour its distance and which query it is near — plus a `meta`
            block. Absence means "not typed", never "does not carry it".

        Notes for LLMs:
            - Report genotypes; do not interpret risk. A genotype call is not a
              diagnosis; direct clinical questions to a genetic counsellor.
            - Genotypes are unphased: "AG" does not say which parent
              contributed which allele.
            - Nearby variants are near by POSITION. Proximity is not linkage;
              never present one as related to the queried variant's trait.
        """
        envelope = await self.envelope(user_info, **args)
        return {"result": render_compact(envelope, self.columns(args)), **envelope_meta(envelope)}

    def columns(self, args: Mapping[str, Any]) -> tuple[str, ...]:
        """Which columns one answer renders. Read by the chat adapter too
        (`tool_loader`), so both surfaces render the same table; not a tool
        (`__tools__`). Fixed here — a genotype row has one shape."""
        return COLUMNS

    async def envelope(self, user_info: Mapping[str, Any], **args: Any) -> tools.Envelope:
        caller_id = caller_of(user_info)
        if not caller_id:
            return denied("authorization required")
        try:
            return await self._run(caller_id, args)
        except query.Denied:
            return denied("you may not read this person's data")
        except Exception as e:
            # Never hand the raw exception to the model: driver messages quote
            # the SQL with its bound parameters, and a model echoes what it is
            # given. The type goes to the log, the class to the envelope.
            tool_name = TOOL_NAME  # a local the PHI log lint can see is a name, not a value
            logger.error("[%s] error_type=%s", tool_name, type(e).__name__, exc_info=not is_driver_exception(e))
            return tools.fault_envelope(e)

    # --- the run ------------------------------------------------------------

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = validate_query(args)
        if problems:
            return refused(problems)
        request = parse_query(args)
        subject_id = await subject_for(caller_id, request.member)
        fetched = await self._variants(subject_id, request)
        # One row over the limit is how "cut" is KNOWN rather than guessed:
        # `len(rows) == limit` is the shape of both a full answer and a cut one,
        # and reporting `partial` on the first is as wrong as missing the second.
        hits, truncated = fetched[: request.limit], len(fetched) > request.limit
        nearby = await self._neighbours(subject_id, request, hits) if request.include_nearby and hits else []
        return _envelope_for(request, hits, nearby, truncated=truncated)

    async def _variants(self, subject_id: str, request: GeneticRequest) -> list[dict[str, Any]]:
        binds, params = _in_clause("rsid", request.rsids)
        rows = await self._read(
            "SELECT rsid, chromosome, position, genotype"
            " FROM th_series_data_genetic"
            " WHERE user_id = :user_id AND is_deleted = false"
            f" AND rsid IN ({binds})"
            " ORDER BY chromosome, position"
            " LIMIT :limit",
            {**params, "user_id": subject_id, "limit": request.limit + 1},
        )
        return [_row(r) for r in rows]

    async def _neighbours(
        self, subject_id: str, request: GeneticRequest, hits: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """The typed variants around each hit, nearest first, labelled with the
        query they belong to. Every rsID the caller named is excluded, so a hit
        never comes back a second time as its own neighbour."""
        binds, excluded = _in_clause("excl", request.rsids)
        out: list[dict[str, Any]] = []
        for hit in hits:
            rows = await self._read(
                "SELECT rsid, chromosome, position, genotype"
                " FROM th_series_data_genetic"
                " WHERE user_id = :user_id AND is_deleted = false"
                " AND chromosome = :chromosome"
                " AND position BETWEEN :min_pos AND :max_pos"
                f" AND rsid NOT IN ({binds})"
                " ORDER BY ABS(position - :target_pos)"
                " LIMIT :nearby_limit",
                {
                    **excluded,
                    "user_id": subject_id,
                    "chromosome": hit["chromosome"],
                    "min_pos": hit["position"] - request.nearby_range,
                    "max_pos": hit["position"] + request.nearby_range,
                    "target_pos": hit["position"],
                    "nearby_limit": MAX_NEARBY_PER_HIT,
                },
            )
            out.extend(
                {**_row(r), "distance": abs(int(r["position"]) - hit["position"]), "near": hit["rsid"]} for r in rows
            )
        return out

    async def _read(self, sql: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        if self._execute is None:
            from ...utils import execute_query

            self._execute = execute_query
        return list(await self._execute(sql, dict(params)) or [])


# --- pure --------------------------------------------------------------------


def _in_clause(prefix: str, values: Sequence[str]) -> tuple[str, dict[str, Any]]:
    """An `IN` list as named binds.

    rsIDs are BOUND, never interpolated. They come out of a user-uploaded
    genotype file that is split on whitespace with no format validation
    (`pulse/file_parser/services/genetic_processor.py`), so a single quote in
    an uploaded file breaks out of an interpolated literal — a stored SQL
    injection on the read path, which is what this was.
    """
    params = {f"{prefix}_{i}": v for i, v in enumerate(values)}
    return ", ".join(f":{k}" for k in params), params


def _row(record: Mapping[str, Any]) -> dict[str, Any]:
    """One database row → one rendered row. All four columns are `NOT NULL`,
    and none of them is a `Decimal` or a timestamp, so nothing here needs the
    JSON coercion the readings path does."""
    return {k: record[k] for k in ("rsid", "chromosome", "position", "genotype")}


def _envelope_for(
    request: GeneticRequest,
    hits: Sequence[Mapping[str, Any]],
    nearby: Sequence[Mapping[str, Any]],
    *,
    truncated: bool = False,
) -> tools.Envelope:
    """One table for both halves: a neighbour is a row with `distance` and
    `near` filled in. Two lists would make the model join them itself, and the
    renderer would have to be told which is which."""
    rows = [*hits, *nearby]
    untyped = tuple(r for r in request.rsids if r not in {str(h["rsid"]) for h in hits})
    notes = [_ABSENCE_NOTE, _UNPHASED_NOTE]
    if nearby:
        notes.append(_PROXIMITY_NOTE)
    if untyped:
        notes.append("not typed in this person's file: " + ", ".join(untyped))
    if not hits:
        notes.append("nothing typed for any of these rsIDs; the person may have uploaded no genotype file at all")
    if truncated:
        notes.append(f"cut at limit={request.limit}; name fewer rsIDs rather than raising it")
    return tools.Envelope(
        tools.STATUS_PARTIAL if truncated else tools.STATUS_OK,
        data=rows,
        meta=tools.Meta(row_count=len(rows), truncated=truncated),
        provenance={str(r["rsid"]): "measured" for r in rows},
        assumptions=tuple(notes),
    )


#: This module's tool surface: nothing at module level. The schema, the
#: validator and the parser are the tool's CONTRACT, imported by name — a
#: module-level function without this list would be published as a tool.
__tools__: tuple[str, ...] = ()

__all__ = [
    "COLUMNS",
    "DEFAULT_LIMIT",
    "DEFAULT_NEARBY_RANGE",
    "GeneticRequest",
    "GeneticService",
    "MAX_LIMIT",
    "MAX_NEARBY_PER_HIT",
    "MAX_RSIDS",
    "TOOL_NAME",
    "TOOL_SCHEMA",
    "parse_query",
    "validate_query",
]

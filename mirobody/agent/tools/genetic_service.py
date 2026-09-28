"""Bounded, provenance-bearing reads of a person's active genotype set."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from mirobody.kernel import query, tools

from ._authz import refused
from ._base import RecordTool
from ._render import envelope_meta, render_compact

logger = logging.getLogger(__name__)

TOOL_NAME = "query_genetic_data"
MAX_RSIDS = 50
#: Direct rows per answer, fixed rather than a parameter. A gene selects at most
#: seven typed sites (SLCO1B1, in the shipped site index), so only a region can
#: reach it, and the answer to a region that does is a narrower region.
ROW_CAP = 100
NO_CALL = "--"

COLUMNS = (
    "rsid", "gene", "chromosome", "position", "query_build", "raw_position", "pos37", "pos38", "genotype",
    "gt", "call_status", "zygosity", "strand_check",
)
PROFILE_COLUMNS = ("vendor", "build", "rows", "called", "sex_inferred", "normalizer_version", "site_table_version")

TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "rsids": {
            "type": "array", "items": {"type": "string"}, "maxItems": MAX_RSIDS,
            "description": "Look up these dbSNP identifiers in the person's active upload.",
        },
        "gene": {"type": "string", "description": "HGNC gene symbol, for example CYP2C19."},
        "chromosome": {"type": "string", "description": "Chromosome for a bounded region query (1–22, X, Y, PAR, MT)."},
        "start": {"type": "integer", "minimum": 1, "description": "Inclusive start coordinate in the chosen build."},
        "end": {"type": "integer", "minimum": 1, "description": "Inclusive end coordinate in the chosen build."},
        "build": {"type": "string", "enum": ["GRCh37", "GRCh38", "raw"],
                  "description": "Required with a region; raw uses unverified upload positions."},
    },
}


@dataclass(frozen=True)
class GeneticRequest:
    rsids: tuple[str, ...] = ()
    gene: str = ""
    chromosome: str = ""
    start: int = 0
    end: int = 0
    build: str = ""


def validate_query(args: Mapping[str, Any]) -> tuple[query.Rejection, ...]:
    out = query.reject_unknown(args, TOOL_SCHEMA)
    rsids = query.normalize_list_arg(args.get("rsids"))
    if len(rsids) > MAX_RSIDS:
        out.append(query.Rejection("rsids", f"at most {MAX_RSIDS} rsIDs per call"))
    if any(not rsid.startswith("rs") or not rsid[2:].isdigit() for rsid in rsids):
        out.append(query.Rejection("rsids", "each identifier must be an rsID"))
    gene = str(args.get("gene") or "").strip()
    chrom = str(args.get("chromosome") or "").strip().upper()
    region = _is_region(rsids, gene, chrom, args)
    if sum((bool(rsids), bool(gene), region)) > 1:
        out.append(query.Rejection("selector", "choose rsids, gene, or region in one call"))
    if gene and (len(gene) > 32 or not gene.replace("-", "").isalnum()):
        out.append(query.Rejection("gene", "use an HGNC gene symbol"))
    if region:
        if chrom not in {*(str(i) for i in range(1, 23)), "X", "Y", "PAR", "MT"}:
            out.append(query.Rejection("chromosome", "use 1–22, X, Y, PAR, or MT"))
        start, end = args.get("start"), args.get("end")
        if (not isinstance(start, int) or isinstance(start, bool) or not isinstance(end, int)
                or isinstance(end, bool) or start < 1 or end < start):
            out.append(query.Rejection("start/end", "give a positive, increasing interval"))
        if args.get("build") not in ("GRCh37", "GRCh38", "raw"):
            out.append(query.Rejection("build", "choose GRCh37, GRCh38, or raw"))
        elif chrom == "PAR" and args.get("build") != "raw":
            out.append(query.Rejection("build", "PAR uses only raw upload coordinates"))
    return tuple(out)


def _is_region(rsids: Sequence[str], gene: str, chrom: str, args: Mapping[str, Any]) -> bool:
    """A region is asked for by naming a chromosome. With rsIDs or a gene
    already given, start/end/build are placeholders: some models fill every
    schema field (`chromosome: ""`, `start: 1`, `end: 1`, `build: "GRCh38"`
    beside `gene`), and treating those as a second selector refused every such
    call and had the model retry the same shape, 4-6 refusals a turn (measured
    2026-09-28 on gpt). Coordinates with no other selector are a region whose
    chromosome is missing, which the validator names."""
    if chrom:
        return True
    return not (rsids or gene) and bool(args.get("start") or args.get("end"))


def parse_query(args: Mapping[str, Any]) -> GeneticRequest:
    problems = validate_query(args)
    if problems:
        raise ValueError("; ".join(f"{p.parameter}: {p.reason}" for p in problems))
    rsids = query.normalize_list_arg(args.get("rsids"))
    gene = str(args.get("gene") or "").strip().upper()
    chrom = str(args.get("chromosome") or "").strip().upper()
    region = _is_region(rsids, gene, chrom, args)
    return GeneticRequest(
        rsids=rsids,
        gene=gene,
        chromosome=chrom if region else "",
        start=int(args.get("start") or 0) if region else 0,
        end=int(args.get("end") or 0) if region else 0,
        # Only a region reads positions in a chosen build; otherwise a
        # placeholder build must not pick the coordinate column.
        build=str(args.get("build") or "") if region else "",
    )


class GeneticService(RecordTool):
    """Only an active upload can be read; all returned rows have a fixed cap."""

    __tools__ = (TOOL_NAME,)
    TOOL_NAME = TOOL_NAME
    input_schema = TOOL_SCHEMA

    def __init__(self, execute: Any = None) -> None:
        self._execute = execute

    async def query_genetic_data(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """Read an active genotype upload by rsID, HGNC gene, or genomic region.

        With no selector, return only a summary. A missing locus, no-call,
        not-applicable or unresolved call does not establish a normal genotype.
        Raw consumer-array calls are not a diagnosis. Region queries require
        a named genome build or explicit raw upload coordinates. State the
        upload's source and build in answers.
        """
        envelope = await self.envelope(user_info, **args)
        return {"result": render_compact(envelope, self.columns(args)), **envelope_meta(envelope)}

    def columns(self, args: Mapping[str, Any]) -> tuple[str, ...]:
        if not any(args.get(k) for k in ("rsids", "gene", "chromosome", "start", "end", "build")):
            return PROFILE_COLUMNS
        return COLUMNS

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = validate_query(args)
        if problems:
            return refused(problems)
        request = parse_query(args)
        subject_id = caller_id
        sets = await self._read(
            "SELECT id, vendor, format_id, build_declared, build_detected, n_rows, n_called, "
            "sex_inferred, normalizer_version, site_table_version "
            "FROM th_genotype_set WHERE user_id = :user_id AND status = 'active' LIMIT 1",
            {"user_id": subject_id},
        )
        if not sets:
            return tools.Envelope(tools.STATUS_OK, data=[], meta=tools.Meta(row_count=0),
                                  assumptions=("no active genotype upload for this person",))
        genotype_set = sets[0]
        source = _source_note(genotype_set)
        if not (request.rsids or request.gene or request.chromosome):
            row = {
                "vendor": genotype_set.get("vendor") or genotype_set["format_id"],
                "build": genotype_set["build_detected"], "rows": genotype_set["n_rows"],
                "called": genotype_set["n_called"], "sex_inferred": genotype_set["sex_inferred"],
                "normalizer_version": genotype_set["normalizer_version"],
                "site_table_version": genotype_set["site_table_version"],
            }
            return tools.Envelope(tools.STATUS_OK, data=[row], meta=tools.Meta(row_count=1),
                                  assumptions=(source, "a consumer array is incomplete; absent and no-call sites are not normal calls"))
        fetched = await self._variants(int(genotype_set["id"]), request, str(genotype_set["build_detected"]))
        hits, truncated = fetched[:ROW_CAP], len(fetched) > ROW_CAP
        return _envelope_for(
            request, hits, source=source, truncated=truncated,
            is_vcf="vcf" in str(genotype_set["format_id"]),
        )

    async def _variants(self, set_id: int, request: GeneticRequest, detected_build: str) -> list[dict[str, Any]]:
        position = "pos38" if request.build == "GRCh38" else "pos37" if request.build == "GRCh37" else "position_raw"
        query_build = request.build or detected_build
        sql = (
            f"SELECT set_id, rsid, gene, chrom AS chromosome, {position} AS position, "
            "position_raw AS raw_position, pos37, pos38, :query_build AS query_build, "
            "ref, alt, genotype_raw AS genotype, gt, call_status, zygosity, strand_check "
            "FROM th_genotype WHERE set_id = :set_id"
        )
        params: dict[str, Any] = {"set_id": set_id, "limit": ROW_CAP + 1,
                                  "query_build": query_build}
        if request.rsids:
            binds, rsid_params = _in_clause("rsid", request.rsids)
            sql += f" AND rsid IN ({binds})"
            params.update(rsid_params)
        elif request.gene:
            sql += " AND gene = :gene"
            params["gene"] = request.gene
        else:
            sql += f" AND chrom = :chromosome AND {position} BETWEEN :start AND :end"
            params.update(chromosome=request.chromosome, start=request.start, end=request.end)
        sql += f" ORDER BY chrom, {position}, rsid LIMIT :limit"
        return [dict(row) for row in await self._read(sql, params)]

    async def _read(self, sql: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        if self._execute is None:
            from mirobody.utils import execute_query

            self._execute = execute_query
        return list(await self._execute(sql, dict(params)) or [])


def _source_note(genotype_set: Mapping[str, Any]) -> str:
    return (
        f"source: genotype set {genotype_set['id']}, vendor={genotype_set.get('vendor') or genotype_set['format_id']}, "
        f"declared build={genotype_set.get('build_declared') or 'unknown'}, "
        f"detected build={genotype_set['build_detected']}; "
        f"normalizer={genotype_set['normalizer_version']}, sites={genotype_set['site_table_version']}"
    )


def _in_clause(prefix: str, values: Sequence[str]) -> tuple[str, dict[str, Any]]:
    params = {f"{prefix}_{i}": value for i, value in enumerate(values)}
    return ", ".join(f":{key}" for key in params), params


def _envelope_for(request: GeneticRequest, hits: Sequence[Mapping[str, Any]], *,
                  source: str, truncated: bool, is_vcf: bool = False) -> tools.Envelope:
    rows = list(hits)
    notes = [source, (
        "VCF phasing is preserved from the upload but not independently validated; calls do not establish a diagnosis"
        if is_vcf else "consumer-array calls are unphased and do not establish a diagnosis"
    )]
    if request.build == "raw":
        notes.append("raw positions are exactly as uploaded; the reference assembly was not verified for these rows")
    missing = tuple(rsid for rsid in request.rsids if rsid not in {row["rsid"] for row in hits})
    if missing:
        notes.append("was not typed in this upload: " + ", ".join(missing))
    for status in ("no_call", "not_applicable", "unresolved"):
        affected = [str(row["rsid"]) for row in hits if row["call_status"] == status]
        if affected:
            label = status.replace("_", " ")
            notes.append(f"{label} (not a normal call): " + ", ".join(affected))
    if truncated:
        notes.append(f"cut at {ROW_CAP} rows; narrow the region")
    return tools.Envelope(
        tools.STATUS_PARTIAL if truncated else tools.STATUS_OK,
        data=rows,
        meta=tools.Meta(row_count=len(rows), truncated=truncated),
        provenance={str(row["rsid"]): "measured" for row in rows},
        assumptions=tuple(notes),
    )


__tools__: tuple[str, ...] = ()

__all__ = [
    "COLUMNS", "GeneticRequest", "GeneticService",
    "MAX_RSIDS", "NO_CALL", "ROW_CAP", "TOOL_NAME", "TOOL_SCHEMA",
    "parse_query", "validate_query",
]

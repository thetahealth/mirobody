"""Map free-text clinical terms to LOINC / RxNorm / SNOMED CT codes.

Uses Gemini embeddings + pgvector cosine similarity against the
fhir_indicators table to find the best matching standard codes.
"""

from __future__ import annotations

import json
import logging
from argparse import Namespace
from dataclasses import asdict, dataclass

from mirobody.utils import execute_query

from mirobody.utils.embedding import text_embedding

log = logging.getLogger(__name__)

VALID_SYSTEMS = {"LOINC", "SNOMED_CT", "RXNORM"}


@dataclass
class MappingResult:
    system: str      # e.g. "LOINC", "SNOMED_CT", "RXNORM"
    code: str        # e.g. "2345-7", "73211009"
    display: str     # human-readable description
    score: float     # cosine similarity (0-1, higher = better)


async def map_term(
    term: str,
    *,
    systems: list[str] | None = None,
    top_k: int = 5,
    threshold: float = 0.0,
) -> list[MappingResult]:
    """Map a free-text term to standard medical codes.

    Args:
        term: Clinical term to map (e.g. "blood glucose", "metformin").
        systems: Filter to specific code systems. None = all systems.
                 Valid values: "LOINC", "SNOMED_CT", "RXNORM".
        top_k: Maximum number of results to return.
        threshold: Minimum similarity score (0-1) to include in results.

    Returns:
        List of MappingResult sorted by descending similarity score.
    """
    if not term or not term.strip():
        return []

    # Validate systems filter
    if systems:
        systems = [s.upper() for s in systems]
        invalid = set(systems) - VALID_SYSTEMS
        if invalid:
            raise ValueError(f"Invalid systems: {invalid}. Valid: {VALID_SYSTEMS}")

    # Embed the input term
    embeddings = await text_embedding([term.strip()])
    vector_str = "[" + ",".join(map(str, embeddings[0])) + "]"

    # Build query
    system_clause = ""
    params: dict = {"query_vector": vector_str, "top_k": top_k}
    if systems:
        system_clause = "AND fi.indicator_standard = ANY(:systems)"
        params["systems"] = systems

    sql = f"""
    SELECT
        fi.indicator_standard AS system,
        fi.code,
        fi.llm_description AS display,
        1 - (fi.embedding_gemini <=> CAST(:query_vector AS vector)) AS score
    FROM fhir_indicators fi
    WHERE fi.embedding_gemini IS NOT NULL
      AND fi.code IS NOT NULL
      {system_clause}
    ORDER BY score DESC
    LIMIT :top_k
    """

    rows = await execute_query(sql, params) or []

    results = [
        MappingResult(
            system=row["system"] or "",
            code=row["code"] or "",
            display=row["display"] or "",
            score=round(float(row["score"]), 4),
        )
        for row in rows
        if float(row["score"]) >= threshold
    ]
    return results


# ─── CLI ─────────────────────────────────────────────────────────────

async def cmd_mapping(args: Namespace) -> None:
    """Subcommand: mapping -- map a term to standard medical codes."""
    results = await map_term(
        term=args.term,
        systems=args.systems,
        top_k=args.top_k,
        threshold=args.threshold,
    )
    print(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2))

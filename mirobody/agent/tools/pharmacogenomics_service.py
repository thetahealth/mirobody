"""Conservative CPIC drug-gene lookup against a person's active genotype set.

CPIC level A/B says a guideline exists. It does not mean this upload establishes
a diplotype or a prescribing recommendation; without validated phasing and
complete allele definitions the result is explicitly undetermined.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mirobody.kernel import query, tools
from mirobody.translate.pgx import load_cpic

from ._authz import refused, subject_for
from ._base import RecordTool
from ._render import envelope_meta, render_compact

TOOL_NAME = "query_pharmacogenomics"
MAX_ITEMS = 10
MAX_PAIRS = 30
COLUMNS = ("drug", "gene", "cpic_level", "status", "called_sites", "required_sites",
           "missing_sites", "no_call_sites", "guideline_url", "knowledge_version")
TOOL_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "drugs": {"type": "array", "items": {"type": "string"}, "maxItems": MAX_ITEMS,
                  "description": "Exact CPIC generic drug names; omit with genes to compare current medication plans."},
        "genes": {"type": "array", "items": {"type": "string"}, "maxItems": MAX_ITEMS,
                  "description": "HGNC symbols such as CYP2C19; omit with drugs for current medication plans."},
        "member": {"type": "string", "description": "Authorised care-circle member id; omit for caller."},
    },
}


class PharmacogenomicsService(RecordTool):
    """Answer drug-gene relevance and array coverage without guessing phenotype."""

    __tools__ = (TOOL_NAME,)
    TOOL_NAME = TOOL_NAME
    input_schema = TOOL_SCHEMA

    def __init__(self, execute: Any = None, medication_service: Any = None) -> None:
        self._execute = execute
        self._medication_service = medication_service

    async def query_pharmacogenomics(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """Check CPIC drug-gene links and typed defining sites in the active upload.

        No array-only phenotype or treatment advice is returned. A CPIC A/B
        level is about evidence for a gene-drug guideline, not this person's
        genotype. Without drugs or genes, compare recorded active medication
        plans. Missing/no-call sites never mean a normal diplotype.
        """
        envelope = await self.envelope(user_info, **args)
        return {"result": render_compact(envelope, COLUMNS), **envelope_meta(envelope)}

    def columns(self, _args: Mapping[str, Any]) -> tuple[str, ...]:
        return COLUMNS

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = query.reject_unknown(args, TOOL_SCHEMA)
        drugs = query.normalize_list_arg(args.get("drugs"))
        genes = query.normalize_list_arg(args.get("genes"))
        if len(drugs) > MAX_ITEMS or len(genes) > MAX_ITEMS:
            problems.append(query.Rejection("drugs/genes", f"at most {MAX_ITEMS} items each"))
        if any(len(value) > 100 or not value.strip() for value in (*drugs, *genes)):
            problems.append(query.Rejection("drugs/genes", "use nonempty names up to 100 characters"))
        if problems:
            return refused(tuple(problems))

        member = str(args.get("member") or "")
        subject = await subject_for(caller_id, member)
        sets = await self._read(
            "SELECT id, format_id, vendor, build_declared, build_detected, site_table_version "
            "FROM th_genotype_set WHERE user_id = :user_id AND status = 'active' LIMIT 1",
            {"user_id": subject},
        )
        if not sets:
            return tools.Envelope(tools.STATUS_OK, data=[], meta=tools.Meta(row_count=0),
                                  assumptions=("no active genotype upload",))
        active = sets[0]
        notes = [
            (f"source: genotype set {active['id']}, vendor={active.get('vendor') or active['format_id']}, "
             f"declared build={active.get('build_declared') or 'unknown'}, "
             f"detected build={active['build_detected']}, sites={active['site_table_version']}"),
            "CPIC A/B marks a gene-drug guideline, not a patient-specific phenotype or prescribing advice",
            "array calls alone cannot establish phase, copy number or exclusion of untyped alleles",
        ]
        if not drugs and not genes:
            medication_service = self._medication_service
            if medication_service is None:
                from .medications_service import MedicationsService

                medication_service = MedicationsService()
            plan = await medication_service.envelope({"user_id": caller_id}, view="plan", member=member)
            if plan.status == tools.STATUS_ERROR:
                return plan
            drugs = tuple(str(row["medication"]) for row in plan.data if row.get("status") == "active")[:MAX_ITEMS]
            notes.append("current medications means active plans, not doses confirmed taken")
        if not drugs and not genes:
            notes.append("no active medication plan found; specify a CPIC drug or gene")
            return tools.Envelope(tools.STATUS_OK, data=[], meta=tools.Meta(row_count=0),
                                  assumptions=tuple(notes))

        knowledge = load_cpic()
        pairs = knowledge.drug_pairs(drugs=drugs, genes=genes)
        if len(pairs) > MAX_PAIRS:
            notes.append(f"cut at {MAX_PAIRS} gene-drug pairs; narrow the query")
        rows = []
        coverages = {}
        for pair in pairs[:MAX_PAIRS]:
            if pair.gene not in coverages:
                required = knowledge.array_coverage(pair.gene, {}).missing_sites
                calls: dict[str, str] = {}
                if required:
                    binds = {f"site_{i}": site for i, site in enumerate(required)}
                    selected = await self._read(
                        "SELECT rsid, call_status FROM th_genotype WHERE set_id = :set_id "
                        "AND rsid IN (" + ", ".join(f":{key}" for key in binds) + ")",
                        {"set_id": active["id"], **binds},
                    )
                    calls = {str(row["rsid"]): str(row["call_status"]) for row in selected}
                coverages[pair.gene] = knowledge.array_coverage(pair.gene, calls)
            coverage = coverages[pair.gene]
            rows.append({
                "drug": pair.drug, "gene": pair.gene, "cpic_level": pair.cpic_level,
                "status": coverage.status, "called_sites": coverage.called_sites,
                "required_sites": coverage.required_sites,
                "missing_sites": len(coverage.missing_sites),
                "no_call_sites": len(coverage.no_call_sites),
                "guideline_url": pair.guideline_url, "knowledge_version": knowledge.version,
            })
        if not rows:
            notes.append("no exact CPIC A/B gene-drug pair matched; this does not prove no interaction")
        return tools.Envelope(
            tools.STATUS_PARTIAL if len(pairs) > MAX_PAIRS else tools.STATUS_OK,
            data=rows, meta=tools.Meta(row_count=len(rows), truncated=len(pairs) > MAX_PAIRS),
            assumptions=tuple(notes),
        )

    async def _read(self, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if self._execute is None:
            from mirobody.utils import execute_query

            self._execute = execute_query
        return [dict(row) for row in await self._execute(sql, dict(params)) or []]

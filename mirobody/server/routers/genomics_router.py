"""Metadata for the active genotype upload on the Data page."""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from mirobody.agent.tools.genetic_service import GeneticService
from mirobody.server.auth import subject_for, verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/genomics", tags=["genomics"])
VCF_CHROMS = tuple(str(i) for i in range(1, 23)) + ("X", "Y", "MT")
FHIR_VARIANT_PROFILE = "http://hl7.org/fhir/uv/genomics-reporting/StructureDefinition/variant"
_FHIR_VALUES = {"called_present": ("LA9633-4", "Present"),
                "called_absent": ("LA9634-2", "Absent"),
                "no_call": ("LA18198-4", "No call")}
_FHIR_BUILDS = {"GRCh37": "LA14029-5", "GRCh38": "LA26806-2"}
_FHIR_ZYGOSITY = {"heterozygous": ("LA6706-1", "Heterozygous"),
                   "homozygous": ("LA6705-3", "Homozygous"),
                   "hemizygous": ("LA6707-9", "Hemizygous")}


def _vcf_meta(value: object) -> str:
    """Keep uploaded provenance from creating another VCF header line."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(value or "unknown"))[:80]


def _loinc(code: str) -> dict[str, Any]:
    return {"coding": [{"system": "http://loinc.org", "code": code}]}


def _fhir_component(code: str, value_key: str, value: Any) -> dict[str, Any]:
    return {"code": _loinc(code), value_key: value}


def _fhir_variant(row: Mapping[str, Any], owner: str, build: str) -> dict[str, Any] | None:
    """Represent only mapped calls; incomplete coordinates must stay visible as omissions."""
    pos = row.get("pos38" if build == "GRCh38" else "pos37")
    status = row.get("call_status")
    gt = row.get("gt")
    if (not pos or not row.get("ref") or not row.get("alt")
            or row.get("chromosome") not in VCF_CHROMS
            or status not in ("called", "no_call")):
        return None
    if status == "called":
        alleles = re.split(r"[/|]", str(gt or ""))
        if not alleles or any(not allele.isdigit() for allele in alleles):
            return None
        value = _FHIR_VALUES["called_present" if any(int(allele) > 0 for allele in alleles)
                             else "called_absent"]
    else:
        value = _FHIR_VALUES["no_call"]
    components = [
        _fhir_component("81252-9", "valueCodeableConcept", {"text": row["rsid"]}),
        _fhir_component("62374-4", "valueCodeableConcept", {
            "coding": [{"system": "http://loinc.org", "code": _FHIR_BUILDS[build], "display": build}]
        }),
        _fhir_component("48000-4", "valueCodeableConcept", {"text": str(row["chromosome"])}),
        _fhir_component("81254-5", "valueRange", {"low": {"value": int(pos)}}),
        _fhir_component("69547-8", "valueString", str(row["ref"])),
        _fhir_component("69551-0", "valueString", str(row["alt"])),
    ]
    if row.get("gene"):
        components.append(_fhir_component("48018-6", "valueCodeableConcept", {"text": row["gene"]}))
    if value[1] == "Present" and row.get("zygosity") in _FHIR_ZYGOSITY:
        code, display = _FHIR_ZYGOSITY[row["zygosity"]]
        components.append(_fhir_component("53034-5", "valueCodeableConcept", {
            "coding": [{"system": "http://loinc.org", "code": code, "display": display}]
        }))
    observation = {
        "resourceType": "Observation",
        "id": f"g{row['set_id']}-{row['rsid']}",
        "meta": {"profile": [FHIR_VARIANT_PROFILE]},
        "status": "final",
        "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                   "code": "laboratory"}]}],
        "code": _loinc("69548-6"),
        "subject": {"reference": f"Patient/{owner}"},
        "valueCodeableConcept": {"coding": [{"system": "http://loinc.org",
                                              "code": value[0], "display": value[1]}]},
        "component": components,
    }
    if status == "called":
        observation["note"] = [{"text": f"VCF GT {gt}; raw call {row['genotype']}"}]
    return observation


@router.get("/export.fhir.json")
async def export_fhir_variants(
    rsids: list[str] = Query(..., description="One to fifty dbSNP identifiers"),
    build: str = Query("GRCh38", pattern="^GRCh(37|38)$"),
    target_user_id: str | None = Query(None),
    user_id: str = Depends(verify_token),
):
    """Export a bounded FHIR Variant bundle inside the normal API envelope."""
    if not 1 <= len(rsids) <= 50 or any(not re.fullmatch(r"rs[0-9]+", rsid) for rsid in rsids):
        return ErrorResponse(code=400, msg="Give one to fifty dbSNP rsIDs.")
    owner = await subject_for(user_id, target_user_id)
    if owner is None:
        return ErrorResponse(code=403, msg="Not permitted to read this member's genetic data.")
    result = await GeneticService().envelope({"user_id": owner}, rsids=rsids, limit=50)
    if result.status == "error":
        return ErrorResponse(code=500, msg="Genetic export is temporarily unavailable.")
    observations = [_fhir_variant(row, owner, build) for row in result.data]
    exported = [value for value in observations if value is not None]
    return StandardResponse(data={
        "bundle": {"resourceType": "Bundle", "type": "collection",
                   "entry": [{"resource": value} for value in exported]},
        "requested": len(rsids), "exported": len(exported),
        "missing_rsids": [rsid for rsid in rsids if rsid not in {row["rsid"] for row in result.data}],
        "omitted_rsids": [row["rsid"] for row, value in zip(result.data, observations, strict=True) if value is None],
    })


@router.get("/active-set")
async def active_set(
    target_user_id: str | None = Query(None, description="Authorised care-circle member; omit for caller"),
    user_id: str = Depends(verify_token),
):
    """Expose counts and provenance; no genotype rows enter the browser page."""
    owner = await subject_for(user_id, target_user_id)
    if owner is None:
        return ErrorResponse(code=403, msg="Not permitted to read this member's genetic data.")

    try:
        rows = await execute_query(
            """SELECT id, status, format_id, vendor, build_declared, build_detected,
                      n_rows, n_called, sex_inferred, activated_at
                 FROM th_genotype_set
                WHERE user_id = :user_id AND status = 'active'
                LIMIT 1""",
            {"user_id": owner},
            log_sql=False,
        )
        if not rows:
            return StandardResponse(data={"active_set": None})
        row = dict(rows[0])
        counts = await execute_query(
            """SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE callable) AS decidable
                 FROM th_pgx_result WHERE set_id = :set_id""",
            {"set_id": row["id"]},
            log_sql=False,
        )
        row["pgx_decidable_genes"] = int(counts[0]["decidable"]) if counts[0]["total"] else None
        row["activated_at"] = row["activated_at"].isoformat() if row["activated_at"] else None
        return StandardResponse(data={"active_set": row})
    except Exception as exc:
        from mirobody.kernel.ops import is_driver_exception

        logger.error("active genotype summary failed: error_type=%s", type(exc).__name__,
                     exc_info=not is_driver_exception(exc))
        return ErrorResponse(code=500, msg="Genetic summary is temporarily unavailable.")


@router.get("/export.vcf")
async def export_vcf(
    build: str = Query(..., pattern="^GRCh(37|38)$"),
    target_user_id: str | None = Query(None),
    user_id: str = Depends(verify_token),
):
    """Stream mapped, defensible calls from the active set in VCF 4.2 form."""
    owner = await subject_for(user_id, target_user_id)
    if owner is None:
        return ErrorResponse(code=403, msg="Not permitted to read this member's genetic data.")
    try:
        rows = await execute_query(
            """SELECT id, format_id, vendor, normalizer_version, site_table_version,
                      build_declared, build_detected
                 FROM th_genotype_set WHERE user_id=:user_id AND status='active' LIMIT 1""",
            {"user_id": owner}, log_sql=False,
        )
        if not rows:
            return ErrorResponse(code=404, msg="No active genotype upload.")
        active = rows[0]
        set_id = int(active["id"])
        position = "pos38" if build == "GRCh38" else "pos37"
        skipped = await execute_query(
            f"""SELECT COUNT(*) AS n FROM th_genotype WHERE set_id=:set_id AND
                ({position} IS NULL OR ref IS NULL OR alt IS NULL OR
                 chrom NOT IN ({', '.join(repr(chrom) for chrom in VCF_CHROMS)}) OR
                 call_status NOT IN ('called','no_call') OR
                 (call_status='called' AND gt IS NULL))""",
            {"set_id": set_id}, log_sql=False,
        )
        skipped_count = int(skipped[0]["n"])
    except Exception as exc:
        from mirobody.kernel.ops import is_driver_exception

        logger.error("genotype VCF export failed: error_type=%s", type(exc).__name__,
                     exc_info=not is_driver_exception(exc))
        return ErrorResponse(code=500, msg="Genetic export is temporarily unavailable.")

    async def stream():
        yield "##fileformat=VCFv4.2\n"
        yield f"##reference={build}\n"
        yield f"##mirobody_genotype_set={set_id}\n"
        for field in ("format_id", "vendor", "normalizer_version", "site_table_version",
                      "build_declared", "build_detected"):
            yield f"##mirobody_{field}={_vcf_meta(active[field])}\n"
        yield f"##mirobody_unresolved_or_unmapped_rows={skipped_count}\n"
        yield "##FORMAT=<ID=GT,Number=1,Type=String,Description=Genotype>\n"
        for chrom in VCF_CHROMS:
            yield f"##contig=<ID=chr{chrom}>\n"
        yield "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE\n"
        # VCF tools expect natural contig order. Walking each indexed contig
        # also keeps memory bounded without sorting the whole genotype set.
        for chrom in VCF_CHROMS:
            last_pos, last_rsid = 0, ""
            while True:
                batch = await execute_query(
                    f"""SELECT {position} AS position, rsid, ref, alt, gt, call_status
                        FROM th_genotype WHERE set_id=:set_id AND chrom=:chrom
                          AND {position} IS NOT NULL AND ref IS NOT NULL AND alt IS NOT NULL
                          AND call_status IN ('called','no_call')
                          AND (call_status='no_call' OR gt IS NOT NULL)
                          AND ({position}, rsid) > (:last_pos, :last_rsid)
                        ORDER BY {position}, rsid LIMIT 10000""",
                    {"set_id": set_id, "chrom": chrom, "last_pos": last_pos,
                     "last_rsid": last_rsid}, log_sql=False,
                )
                if not batch:
                    break
                for row in batch:
                    gt = row["gt"] if row["call_status"] == "called" else "./."
                    yield f"chr{chrom}\t{row['position']}\t{row['rsid']}\t{row['ref']}\t{row['alt']}\t.\tPASS\t.\tGT\t{gt}\n"
                last = batch[-1]
                last_pos, last_rsid = last["position"], last["rsid"]

    return StreamingResponse(
        stream(), media_type="text/vcf",
        headers={"Content-Disposition": f"attachment; filename=genotypes-{build}.vcf"},
    )

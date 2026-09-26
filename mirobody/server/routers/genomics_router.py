"""Metadata for the active genotype upload on the Data page."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse
from mirobody.user.care_circle import CareCircleDenied, resolve_subject
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/genomics", tags=["genomics"])


async def _owner(caller: str, target_user_id: str | None) -> str | None:
    if not target_user_id or target_user_id == caller:
        return caller
    try:
        await resolve_subject(caller, target_user_id)
    except CareCircleDenied:
        return None
    return target_user_id


@router.get("/active-set")
async def active_set(
    target_user_id: str | None = Query(None, description="Authorised care-circle member; omit for caller"),
    user_id: str = Depends(verify_token),
):
    """Expose counts and provenance; no genotype rows enter the browser page."""
    owner = await _owner(user_id, target_user_id)
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
    owner = await _owner(user_id, target_user_id)
    if owner is None:
        return ErrorResponse(code=403, msg="Not permitted to read this member's genetic data.")
    try:
        rows = await execute_query(
            "SELECT id FROM th_genotype_set WHERE user_id=:user_id AND status='active' LIMIT 1",
            {"user_id": owner}, log_sql=False,
        )
        if not rows:
            return ErrorResponse(code=404, msg="No active genotype upload.")
        set_id = int(rows[0]["id"])
        position = "pos38" if build == "GRCh38" else "pos37"
        skipped = await execute_query(
            f"""SELECT COUNT(*) AS n FROM th_genotype WHERE set_id=:set_id AND
                ({position} IS NULL OR ref IS NULL OR alt IS NULL OR chrom='PAR' OR
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
        yield f"##mirobody_unresolved_or_unmapped_rows={skipped_count}\n"
        yield "##FORMAT=<ID=GT,Number=1,Type=String,Description=Genotype>\n"
        for chrom in sorted([*(str(i) for i in range(1, 23)), "X", "Y", "MT"]):
            yield f"##contig=<ID=chr{chrom}>\n"
        yield "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE\n"
        last_chrom, last_pos, last_rsid = "", 0, ""
        while True:
            batch = await execute_query(
                f"""SELECT chrom, {position} AS position, rsid, ref, alt, gt, call_status
                    FROM th_genotype WHERE set_id=:set_id AND {position} IS NOT NULL
                      AND ref IS NOT NULL AND alt IS NOT NULL AND chrom <> 'PAR'
                      AND call_status IN ('called','no_call')
                      AND (call_status='no_call' OR gt IS NOT NULL)
                      AND (chrom, {position}, rsid) > (:last_chrom, :last_pos, :last_rsid)
                    ORDER BY chrom, {position}, rsid LIMIT 10000""",
                {"set_id": set_id, "last_chrom": last_chrom,
                 "last_pos": last_pos, "last_rsid": last_rsid},
                log_sql=False,
            )
            if not batch:
                break
            for row in batch:
                gt = row["gt"] if row["call_status"] == "called" else "./."
                yield f"chr{row['chrom']}\t{row['position']}\t{row['rsid']}\t{row['ref']}\t{row['alt']}\t.\tPASS\t.\tGT\t{gt}\n"
            last = batch[-1]
            last_chrom, last_pos, last_rsid = last["chrom"], last["position"], last["rsid"]

    return StreamingResponse(
        stream(), media_type="text/vcf",
        headers={"Content-Disposition": f"attachment; filename=genotypes-{build}.vcf"},
    )

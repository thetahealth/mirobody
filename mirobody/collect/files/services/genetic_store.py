"""Atomic visibility for a person's genotype uploads.

Rows may be written in batches, but only an active set is queryable. The
activation transaction checks the row count and supersedes the previous set
before publishing the new one; a failed batch leaves the old set active.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.db import execute_query, transaction

logger = logging.getLogger(__name__)

_INSERT = """
    INSERT INTO th_genotype
        (set_id, rsid, rsid_raw, chrom, position_raw, pos37, pos38,
         genotype_raw, ref, alt, gene, gt, call_status, zygosity, strand_check)
    VALUES
        (:set_id, :rsid, :rsid_raw, :chrom, :position_raw, :pos37, :pos38,
         :genotype_raw, :ref, :alt, :gene, :gt, :call_status, :zygosity, :strand_check)
"""


class GenotypeStore:
    """The write side of one genotype set, backed by the shared DB connection."""

    async def create_set(
        self, user_id: str, *, file_key: str | None, format_id: str,
        vendor: str = "", build_declared: str = "",
        normalizer_version: str = "pending", site_table_version: str = "pending",
    ) -> int:
        rows = await execute_query(
            """INSERT INTO th_genotype_set
                   (user_id, file_key, format_id, vendor, build_declared, build_detected,
                    normalizer_version, site_table_version)
               VALUES (:user_id, :file_key, :format_id, :vendor, :build_declared, 'unknown',
                       :normalizer_version, :site_table_version)
               RETURNING id""",
            {
                "user_id": user_id,
                "file_key": file_key,
                "format_id": format_id,
                "vendor": vendor or None,
                "build_declared": build_declared or None,
                "normalizer_version": normalizer_version,
                "site_table_version": site_table_version,
            },
        )
        return int(rows[0]["id"])

    async def write_batch(self, set_id: int, records: Sequence[Mapping[str, Any]]) -> None:
        if not records:
            return
        params = [
            {
                "set_id": set_id,
                "rsid": r["rsid"],
                "rsid_raw": r.get("rsid_raw", r["rsid"]),
                "chrom": r["chromosome"],
                "position_raw": r["position"],
                "pos37": r.get("pos37"),
                "pos38": r.get("pos38"),
                "genotype_raw": r.get("genotype_raw", r["genotype"]),
                "call_status": r.get("call_status", "no_call" if r["genotype"] == "--" else "unresolved"),
                "ref": r.get("ref"),
                "alt": r.get("alt"),
                "gt": r.get("gt"),
                "gene": r.get("gene"),
                "zygosity": r.get("zygosity"),
                "strand_check": r.get("strand_check"),
            }
            for r in records
        ]
        await execute_query(_INSERT, params)

    async def activate_set(
        self, set_id: int, user_id: str, *, n_rows: int, n_called: int,
        build_detected: str = "unknown", sex_inferred: str = "unknown",
    ) -> None:
        if n_rows <= 0 or not 0 <= n_called <= n_rows:
            raise ValueError("an active genotype set needs valid nonzero counts")

        async with transaction() as tx:
            # Serialize two uploads for the same person before the partial
            # unique index is checked; completion order decides the active set.
            await tx.execute("SELECT pg_advisory_xact_lock(hashtextextended(:user_id, 0))", {"user_id": user_id})
            actual = await tx.execute("SELECT COUNT(*) AS n FROM th_genotype WHERE set_id = :id", {"id": set_id})
            if int(actual[0]["n"]) != n_rows:
                raise ValueError("genotype row count differs from parsed row count")
            if sex_inferred == "female":
                await tx.execute(
                    """UPDATE th_genotype SET call_status = 'not_applicable'
                       WHERE set_id = :id AND chrom = 'Y' AND call_status = 'no_call'""",
                    {"id": set_id},
                )
            elif sex_inferred == "male":
                await tx.execute(
                    """UPDATE th_genotype SET gt = split_part(gt, '/', 1), zygosity = 'hemizygous'
                       WHERE set_id = :id AND chrom IN ('X', 'Y') AND call_status = 'called'
                         AND gt ~ '^[0-9]+/[0-9]+$'
                         AND split_part(gt, '/', 1) = split_part(gt, '/', 2)""",
                    {"id": set_id},
                )
            await tx.execute(
                """UPDATE th_genotype_set SET status = 'superseded', superseded_at = now()
                   WHERE user_id = :user_id AND status = 'active'""",
                {"user_id": user_id},
            )
            updated = await tx.execute(
                """UPDATE th_genotype_set
                   SET status = 'active', n_rows = :n_rows, n_called = :n_called,
                       build_detected = :build_detected, sex_inferred = :sex_inferred,
                       activated_at = now()
                   WHERE id = :id AND user_id = :user_id AND status = 'loading'""",
                {"id": set_id, "user_id": user_id, "n_rows": n_rows, "n_called": n_called,
                 "build_detected": build_detected, "sex_inferred": sex_inferred},
            )
            if updated["record_count"] != 1:
                raise ValueError("genotype set is no longer loading")

    async def fail_set(self, set_id: int) -> None:
        await execute_query(
            "UPDATE th_genotype_set SET status = 'failed' WHERE id = :id AND status = 'loading'",
            {"id": set_id},
        )


async def delete_genetic_data_by_source(user_id: str, source_table: str, source_table_id: str) -> bool:
    """Erase a deleted file's genotype sets and any unmigrated legacy rows."""
    try:
        async with transaction() as tx:
            await tx.execute(
                "DELETE FROM th_genotype_set WHERE user_id = :user_id AND file_key = :file_key",
                {"user_id": user_id, "file_key": source_table_id},
            )
            await tx.execute(
                """DELETE FROM th_series_data_genetic
                   WHERE user_id = :user_id AND source_table = :source_table
                     AND source_table_id = :source_table_id""",
                {"user_id": user_id, "source_table": source_table, "source_table_id": source_table_id},
            )
        logger.info("genotype file erased", extra={"user_id": user_id})
        return True
    except Exception as e:
        logger.error("genotype file erase failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
        return False

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
from mirobody.translate.genotype import PAR_RANGES
from mirobody.utils.db import execute_query, transaction

logger = logging.getLogger(__name__)

_BATCH_COLUMNS = (
    "rsid", "rsid_raw", "chrom", "position_raw", "pos37", "pos38",
    "genotype_raw", "ref", "alt", "gene", "gt", "call_status", "zygosity", "strand_check",
)
_INTEGER_COLUMNS = {"position_raw", "pos37", "pos38"}
_INSERT = f"""
    INSERT INTO th_genotype (set_id, {', '.join(_BATCH_COLUMNS)})
    SELECT :set_id, {', '.join(_BATCH_COLUMNS)}
    FROM unnest({', '.join(f'CAST(:{column} AS {"integer" if column in _INTEGER_COLUMNS else "text"}[])' for column in _BATCH_COLUMNS)})
         AS batch({', '.join(_BATCH_COLUMNS)})
"""

_POS37 = "COALESCE(pos37, CASE WHEN :build_detected = 'GRCh37' THEN position_raw END)"
_POS38 = "COALESCE(pos38, CASE WHEN :build_detected = 'GRCh38' THEN position_raw END)"
_PAR_CLAUSES = []
for _chrom in ("X", "Y"):
    for _build, _position in (("GRCh37", _POS37), ("GRCh38", _POS38)):
        for _start, _end in PAR_RANGES[_build][_chrom]:
            _PAR_CLAUSES.append(f"(chrom = '{_chrom}' AND {_position} BETWEEN {_start} AND {_end})")
_NON_PAR = "NOT COALESCE((" + " OR ".join(_PAR_CLAUSES) + "), FALSE)"
_MULTI = "replace(gt, '|', '/') ~ '^[0-9]+(/[0-9]+)+$'"
_FIRST = "split_part(replace(gt, '|', '/'), '/', 1)"
_CONFLICT = ("EXISTS (SELECT 1 FROM unnest(regexp_split_to_array(gt, '[/|]')) AS allele(value) "
             f"WHERE allele.value <> {_FIRST})")


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
        columns = {column: [] for column in _BATCH_COLUMNS}
        for record in records:
            values = {
                "rsid": record["rsid"],
                "rsid_raw": record.get("rsid_raw", record["rsid"]),
                "chrom": record["chromosome"],
                "position_raw": record["position"],
                "pos37": record.get("pos37"),
                "pos38": record.get("pos38"),
                "genotype_raw": record.get("genotype_raw", record["genotype"]),
                "call_status": record.get("call_status", "no_call" if record["genotype"] == "--" else "unresolved"),
                "ref": record.get("ref"), "alt": record.get("alt"),
                "gt": record.get("gt"), "gene": record.get("gene"),
                "zygosity": record.get("zygosity"), "strand_check": record.get("strand_check"),
            }
            for column in _BATCH_COLUMNS:
                columns[column].append(values[column])
        await execute_query(_INSERT, {"set_id": set_id, **columns})

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
                    f"""UPDATE th_genotype
                        SET gt = NULL, zygosity = NULL, call_status = 'unresolved',
                            strand_check = CASE WHEN {_POS37} IS NULL AND {_POS38} IS NULL
                                THEN 'par_unknown' ELSE 'haploid_conflict' END
                        WHERE set_id = :id AND chrom IN ('X', 'Y') AND call_status = 'called'
                          AND {_MULTI} AND {_NON_PAR}
                          AND ({_CONFLICT} OR {_POS37} IS NULL AND {_POS38} IS NULL)""",
                    {"id": set_id, "build_detected": build_detected},
                )
                await tx.execute(
                    f"""UPDATE th_genotype SET gt = {_FIRST}, zygosity = 'hemizygous'
                        WHERE set_id = :id AND chrom IN ('X', 'Y') AND call_status = 'called'
                          AND {_MULTI} AND {_NON_PAR} AND NOT {_CONFLICT}""",
                    {"id": set_id, "build_detected": build_detected},
                )
            counted = await tx.execute(
                "SELECT COUNT(*) AS n FROM th_genotype WHERE set_id = :id AND call_status = 'called'",
                {"id": set_id},
            )
            n_called = int(counted[0]["n"])
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

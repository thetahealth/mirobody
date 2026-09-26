"""Move a 1.5.1 genotype upload into the active-set model.

Legacy rows have neither a verified reference build nor strand. The migration
preserves their raw calls and marks them unresolved; a fresh upload is needed
before a reference-relative GT can be asserted. It never replaces a newer
active set and publishes all of one person's rows in one transaction.
"""

from __future__ import annotations

import logging

from mirobody.utils.db import execute_query, transaction

logger = logging.getLogger(__name__)

_USERS = """
SELECT DISTINCT old.user_id
  FROM th_series_data_genetic old
 WHERE old.is_deleted = false
   AND NOT EXISTS (
       SELECT 1 FROM th_genotype_set active
        WHERE active.user_id = old.user_id AND active.status = 'active'
   )
   {user_filter}
 ORDER BY old.user_id LIMIT :max_users
"""

_INSERT = """
INSERT INTO th_genotype
    (set_id, rsid, rsid_raw, chrom, position_raw, genotype_raw,
     call_status, strand_check)
SELECT :set_id, rsid, rsid, chrom, "position", genotype,
       CASE WHEN genotype IN ('--', '00') THEN 'no_call' ELSE 'unresolved' END,
       'legacy_unverified'
  FROM (
    SELECT DISTINCT ON (rsid)
           rsid,
           CASE upper(replace(chromosome, 'CHR', ''))
             WHEN '23' THEN 'X' WHEN '24' THEN 'Y'
             WHEN '25' THEN 'PAR' WHEN '26' THEN 'MT'
             WHEN '0' THEN 'PAR' WHEN 'XY' THEN 'PAR'
             WHEN 'M' THEN 'MT'
             ELSE upper(replace(chromosome, 'CHR', ''))
           END AS chrom,
           "position", genotype
      FROM th_series_data_genetic
     WHERE user_id = :user_id AND is_deleted = false
       AND source_table IS NOT DISTINCT FROM :source_table
       AND source_table_id IS NOT DISTINCT FROM :source_table_id
     ORDER BY rsid, id DESC
  ) latest
 WHERE chrom IN ('X', 'Y', 'MT', 'PAR',
                  '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11',
                  '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22')
   AND "position" > 0
   AND rsid <> ''
"""


async def migrate(*, user_id: str | None = None, max_users: int = 1000) -> dict[str, int]:
    """Migrate each user's latest legacy source once; return aggregate counts."""
    if max_users < 1:
        raise ValueError("max_users must be positive")
    params: dict[str, object] = {"max_users": max_users}
    if user_id:
        params["user_id"] = user_id
    users = await execute_query(
        _USERS.format(user_filter="AND old.user_id = :user_id" if user_id else ""),
        params, log_sql=False,
    )
    counts = {"users": 0, "rows": 0, "skipped_active": 0, "skipped_invalid": 0}
    for candidate in users:
        subject = str(candidate["user_id"])
        async with transaction() as tx:
            await tx.execute("SELECT pg_advisory_xact_lock(hashtextextended(:user_id, 0))", {"user_id": subject})
            active = await tx.execute(
                "SELECT id FROM th_genotype_set WHERE user_id = :user_id AND status = 'active' LIMIT 1",
                {"user_id": subject},
            )
            if active:
                counts["skipped_active"] += 1
                continue
            latest = await tx.execute(
                """SELECT source_table, source_table_id FROM th_series_data_genetic
                    WHERE user_id = :user_id AND is_deleted = false ORDER BY id DESC LIMIT 1""",
                {"user_id": subject},
            )
            if not latest:
                continue
            source = latest[0]
            binds = {"user_id": subject, "source_table": source["source_table"],
                     "source_table_id": source["source_table_id"]}
            total = await tx.execute(
                """SELECT COUNT(DISTINCT rsid) AS n FROM th_series_data_genetic
                    WHERE user_id = :user_id AND is_deleted = false
                      AND source_table IS NOT DISTINCT FROM :source_table
                      AND source_table_id IS NOT DISTINCT FROM :source_table_id""",
                binds,
            )
            created = await tx.execute(
                """INSERT INTO th_genotype_set
                    (user_id, file_key, format_id, vendor, build_detected,
                     normalizer_version, site_table_version)
                    VALUES (:user_id, :source_table_id, 'legacy_1.5.1',
                            'legacy', 'unknown', 'legacy_unverified', 'none')
                    RETURNING id""",
                binds,
            )
            set_id = int(created[0]["id"])
            inserted = await tx.execute(_INSERT, {**binds, "set_id": set_id})
            row_count = int(inserted["record_count"])
            if not row_count:
                raise ValueError("latest legacy genotype source has no valid rows")
            await tx.execute(
                """UPDATE th_genotype_set SET status = 'active', n_rows = :n_rows,
                    n_called = 0, activated_at = now() WHERE id = :set_id""",
                {"set_id": set_id, "n_rows": row_count},
            )
            counts["users"] += 1
            counts["rows"] += row_count
            counts["skipped_invalid"] += int(total[0]["n"]) - row_count
        logger.info("migrate-genotypes batch complete", extra={
            "user_count": counts["users"], "row_count": counts["rows"],
            "skipped_invalid_count": counts["skipped_invalid"],
        })
    return counts


__all__ = ["migrate"]

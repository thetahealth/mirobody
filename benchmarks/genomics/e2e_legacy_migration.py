"""Check one-way 1.5.1 migration using pinned public 1000G genotype truth.

Requires an isolated PostgreSQL schema already provisioned with the package
schema and MIROBODY_GENOMICS_TEST_DSN (psycopg URL). Never run on a live DB.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from mirobody.collect.migrate_genotypes import migrate
from mirobody.utils.db import execute_query, use_engines


async def run(manifest_path: Path) -> None:
    dsn = os.environ["MIROBODY_GENOMICS_TEST_DSN"]
    engine = create_async_engine(dsn, connect_args={"options": "-c search_path=theta_ai,public"})
    use_engines(lambda _name: engine)
    subject = "public-migration-" + uuid.uuid4().hex
    truth = json.loads(manifest_path.read_text())["calls"]
    try:
        await execute_query(
            """INSERT INTO th_series_data_genetic
                (user_id, rsid, chromosome, "position", genotype, source_table, source_table_id)
               VALUES (:user_id, :rsid, '10', :position, :genotype, 'th_files', 'public-1000g')""",
            [{"user_id": subject, "rsid": row["rsid"], "position": row["pos37"],
              "genotype": row["genotype"]} for row in truth], log_sql=False,
        )
        result = await migrate(user_id=subject)
        assert result["users"] == 1 and result["rows"] == len(truth), result
        rows = await execute_query(
            """SELECT g.rsid, g.position_raw, g.genotype_raw, g.gt, g.call_status,
                      s.build_detected, s.n_called
                 FROM th_genotype g JOIN th_genotype_set s ON s.id = g.set_id
                WHERE s.user_id = :user_id AND s.status = 'active' ORDER BY g.rsid""",
            {"user_id": subject}, log_sql=False,
        )
        assert {row["rsid"]: (row["position_raw"], row["genotype_raw"]) for row in rows} == {
            row["rsid"]: (row["pos37"], row["genotype"]) for row in truth
        }, rows
        assert all(row["gt"] is None and row["call_status"] == "unresolved"
                   and row["build_detected"] == "unknown" and row["n_called"] == 0 for row in rows)
        assert (await migrate(user_id=subject))["users"] == 0
        print(f"legacy migration passed: {len(rows)} public rows, conservative status, repeat safe")
    finally:
        await execute_query("DELETE FROM th_genotype_set WHERE user_id = :user_id",
                            {"user_id": subject}, log_sql=False)
        await execute_query("DELETE FROM th_series_data_genetic WHERE user_id = :user_id",
                            {"user_id": subject}, log_sql=False)
        await engine.dispose()
        use_engines(None)


if __name__ == "__main__":
    asyncio.run(run(Path("internal/genomics/corpus/generated_public/MANIFEST.json")))

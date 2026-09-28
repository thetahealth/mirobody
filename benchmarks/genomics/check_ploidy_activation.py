"""Exercise the activation transaction with pinned public 1000G X calls.

MIROBODY_GENOMICS_TEST_DSN must point at a disposable database with the
genotype tables. The published female non-PAR call is intentionally evaluated
as a sex-inferred male conflict; no allele or genotype is invented.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import re
import uuid
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from mirobody.collect.files.services.genetic_store import GenotypeStore
from mirobody.utils.db import execute_query, use_engines


def _public_rows(*, mapped: bool) -> list[dict]:
    path = Path(__file__).parent / "fixtures/public-1000g-x.tsv"
    with path.open() as source:
        truth = list(csv.DictReader((line for line in source if not line.startswith("#")), delimiter="\t"))
    rows = []
    for item in truth:
        pos = int(item["pos37"])
        gt = item["gt"]
        alleles = gt.replace("|", "/").split("/")
        rows.append({
            "rsid": f"X:{pos}:{item['ref']}:{item['alt']}",
            "chromosome": "X", "position": pos, "pos37": pos if mapped else None,
            "pos38": None, "ref": item["ref"], "alt": item["alt"],
            "genotype": gt, "genotype_raw": gt, "gt": gt, "call_status": "called",
            "zygosity": ("hemizygous" if len(alleles) == 1 else
                          "homozygous" if len(set(alleles)) == 1 else "heterozygous"),
            "strand_check": "vcf_plus",
        })
    return rows


async def _case(sex: str, build: str, *, mapped: bool, expected_called: int) -> None:
    store = GenotypeStore()
    user = f"public-ploidy-{uuid.uuid4()}"
    set_id = await store.create_set(user, file_key=None, format_id="public-1000g-x-vcf")
    try:
        await store.write_batch(set_id, _public_rows(mapped=mapped))
        await store.activate_set(set_id, user, n_rows=4, n_called=4,
                                 build_detected=build, sex_inferred=sex)
        rows = await execute_query(
            "SELECT position_raw, gt, call_status, zygosity, strand_check "
            "FROM th_genotype WHERE set_id = :id ORDER BY position_raw",
            {"id": set_id}, log_sql=False,
        )
        by_pos = {row["position_raw"]: row for row in rows}
        summary = await execute_query("SELECT n_called FROM th_genotype_set WHERE id = :id",
                                      {"id": set_id}, log_sql=False)
        assert summary[0]["n_called"] == expected_called, (sex, build, summary)
        if sex == "male" and mapped:
            assert by_pos[60052]["gt"] == "0|1" and by_pos[60052]["call_status"] == "called"
            assert by_pos[60026]["gt"] == "0|0" and by_pos[60026]["zygosity"] == "homozygous"
            assert by_pos[3000679]["gt"] is None and by_pos[3000679]["call_status"] == "unresolved"
            assert by_pos[3000679]["strand_check"] == "haploid_conflict"
            assert by_pos[3000166]["gt"] == "0" and by_pos[3000166]["call_status"] == "called"
        elif sex == "male":
            assert all(by_pos[pos]["strand_check"] == "par_unknown" for pos in (60052, 60026, 3000679))
            assert by_pos[3000166]["call_status"] == "called"
        else:
            assert all(row["call_status"] == "called" for row in rows)
    finally:
        await execute_query("DELETE FROM th_genotype_set WHERE id = :id", {"id": set_id}, log_sql=False)


async def run(schema: str) -> None:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema):
        raise ValueError("schema must be a simple PostgreSQL identifier")
    engine = create_async_engine(
        os.environ["MIROBODY_GENOMICS_TEST_DSN"],
        connect_args={"options": f"-c search_path={schema},public -c app.encryption_key=public-genomics-fixture-key"},
    )
    use_engines(lambda _name: engine)
    try:
        await _case("male", "GRCh37", mapped=True, expected_called=3)
        await _case("unknown", "GRCh37", mapped=True, expected_called=4)
        await _case("male", "unknown", mapped=False, expected_called=1)
        print("public X activation passed: PAR, non-PAR conflict, unknown sex/build, n_called")
    finally:
        await engine.dispose()
        use_engines(None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default=os.environ.get("PG_SCHEMA", "theta_ai"))
    asyncio.run(run(parser.parse_args().schema))

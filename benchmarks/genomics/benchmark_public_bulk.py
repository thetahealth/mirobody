"""Measure a 1.3M-row genotype import using only pinned public GIAB calls.

This benchmark runs against an isolated disposable database. It keeps the
source and generated VCF outside the source tree, never in a wheel or
committed fixture. Selecting SNVs with dbSNP identifiers preserves the GIAB
sample's actual genotype; no genotype value is fabricated.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import re
import time
from pathlib import Path

from sqlalchemy.ext.asyncio import create_async_engine

from mirobody.collect.files.services.genetic_processor import GeneticDataLoader
from mirobody.utils.db import execute_query, use_engines

SOURCE_SHA256 = "15063360be7bba3a6e0e1d555342ba1aaacc285c11dd835e901e66c81818f554"
SOURCE_URL = (
    "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/"
    "ChineseTrio/HG005_NA24631_son/NISTv4.2.1/GRCh37/"
    "HG005_GRCh37_1_22_v4.2.1_benchmark.vcf.gz"
)
RSID = re.compile(r"rs[1-9][0-9]*\Z")
GT = re.compile(r"[01](?:[/|][01])?\Z")
CHROMS = {str(i) for i in range(1, 23)}


def generate(source: Path, target: Path, *, limit: int) -> dict:
    if hashlib.sha256(source.read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("GIAB source checksum mismatch")
    target.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    count = 0
    sample = ""
    with gzip.open(source, "rt") as rows, target.open("w") as output:
        output.write("##fileformat=VCFv4.2\n##reference=GRCh37\n")
        for line in rows:
            if line.startswith("#CHROM"):
                sample = line.rstrip("\n").split("\t")[9]
                output.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + sample + "\n")
                continue
            if line.startswith("#"):
                continue
            chrom, position, rsid, ref, alt, _qual, _filter, _info, _format, call, *_ = line.rstrip("\n").split("\t")
            gt = call.split(":", 1)[0]
            if (chrom not in CHROMS or not RSID.fullmatch(rsid) or rsid in seen
                    or len(ref) != 1 or len(alt) != 1 or ref not in "ACGT"
                    or alt not in "ACGT" or not GT.fullmatch(gt)):
                continue
            output.write(f"{chrom}\t{position}\t{rsid}\t{ref}\t{alt}\t.\tPASS\t.\tGT\t{gt}\n")
            seen.add(rsid)
            count += 1
            if count == limit:
                break
    if count != limit or not sample:
        target.unlink(missing_ok=True)
        raise ValueError(f"GIAB source supplied {count} eligible rows; expected {limit}")
    return {
        "source_url": SOURCE_URL,
        "source_sha256": SOURCE_SHA256,
        "sample": sample,
        "selected_rows": count,
        "selection": "first unique dbSNP biallelic SNVs with called GT, chromosomes 1-22",
        "vcf_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "vcf_bytes": target.stat().st_size,
    }


async def measure(target: Path, *, batch_size: int, user_id: str, schema: str = "public") -> dict:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("schema must be a PostgreSQL identifier")
    engine = create_async_engine(
        os.environ["MIROBODY_GENOMICS_TEST_DSN"],
        connect_args={"options": f"-c search_path={schema},public -c app.encryption_key=public-genomics-fixture-key"},
    )
    use_engines(lambda _name: engine)
    try:
        size_sql = "SELECT pg_total_relation_size('th_genotype') AS bytes"
        before = int((await execute_query(size_sql, log_sql=False))[0]["bytes"])
        started = time.perf_counter()
        count = await GeneticDataLoader().load_user_genetic_data(
            user_id, str(target), batch_size=batch_size, is_up_progress=False,
        )
        seconds = time.perf_counter() - started
        after = int((await execute_query(size_sql, log_sql=False))[0]["bytes"])
        active = await execute_query(
            """SELECT id, n_rows, n_called, status FROM th_genotype_set
                WHERE user_id=:user_id AND status='active'""",
            {"user_id": user_id}, log_sql=False,
        )
        assert len(active) == 1 and active[0]["n_rows"] == count
        return {
            "rows": count, "seconds": round(seconds, 3),
            "table_and_indexes_bytes_delta": after - before,
            "active_set_id": active[0]["id"], "called": active[0]["n_called"],
            "batch_size": batch_size,
        }
    finally:
        await engine.dispose()
        use_engines(None)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=1_300_000)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--user", default="public-giab-bulk-benchmark")
    parser.add_argument("--schema", default="public", help="Disposable test schema containing 32_genomics.sql")
    parser.add_argument("--generate-only", action="store_true")
    options = parser.parse_args()
    manifest = generate(options.source, options.out, limit=options.limit)
    print(json.dumps(manifest, sort_keys=True), flush=True)
    if not options.generate_only:
        result = asyncio.run(measure(options.out, batch_size=options.batch_size,
                                     user_id=options.user, schema=options.schema))
        print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()

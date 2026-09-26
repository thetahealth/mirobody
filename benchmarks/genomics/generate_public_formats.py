"""Render one public 1000 Genomes individual's CPIC sites as vendor test files.

Inputs are a pinned public 1000G CYP2C19 region VCF (GRCh37) and PharmCAT
3.4.0 positions VCF (GRCh38). No private export or invented genotype is used.
The small SQLite file is a test catalog; it is not the released G0 asset.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

TRUTH_SHA256 = "c63f2e17f9fa7ed06d75c0c03824233eced60910d2a2e5a04cb7b51cab0921ca"
POSITIONS_SHA256 = "63f49b9004b6a5ca921deebe8ec59f8e45f6990543e07b7890c0d6201f5e69fc"
RSIDS = {"rs4244285": 96541616, "rs4986893": 96540410}
SOURCE = {
    "truth": "https://www.internationalgenome.org/data/",
    "positions": "https://github.com/PharmGKB/PharmCAT/releases/tag/v3.4.0",
}


def _verify(path: Path, expected: str) -> None:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != expected:
        raise ValueError(f"public source digest mismatch: {path.name}")


def _pharmcat(path: Path) -> dict[str, tuple[int, str, str]]:
    found = {}
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            continue
        chrom, pos, rsid, ref, alt, *_ = line.split("\t")
        if rsid in RSIDS:
            if chrom.removeprefix("chr") != "10":
                raise ValueError("unexpected PharmCAT chromosome")
            found[rsid] = (int(pos), ref, alt)
    if set(found) != set(RSIDS):
        raise ValueError("PharmCAT positions are incomplete")
    return found


def _truth(path: Path, sample: str) -> dict[str, tuple[int, str, str, str]]:
    index = None
    found = {}
    with gzip.open(path, "rt") as source:
        for line in source:
            if line.startswith("#CHROM"):
                samples = line.rstrip("\n").split("\t")[9:]
                index = samples.index(sample) + 9
            elif not line.startswith("#"):
                cells = line.rstrip("\n").split("\t")
                position = int(cells[1])
                if position in RSIDS.values():
                    gt = cells[index].split(":", 1)[0]
                    if any(part not in {"0", "1"} for part in gt.replace("|", "/").split("/")):
                        raise ValueError("selected public truth genotype is not biallelic")
                    rsid = next(key for key, value in RSIDS.items() if value == position)
                    found[rsid] = (position, cells[3], cells[4], gt)
    if set(found) != set(RSIDS):
        raise ValueError("1000G public truth sites are incomplete")
    return found


def build(truth_path: Path, positions_path: Path, out: Path, sample: str) -> dict:
    _verify(truth_path, TRUTH_SHA256)
    _verify(positions_path, POSITIONS_SHA256)
    truth, positions = _truth(truth_path, sample), _pharmcat(positions_path)
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for rsid in sorted(RSIDS, key=lambda key: RSIDS[key]):
        pos37, ref, alt, gt = truth[rsid]
        pos38, pharm_ref, pharm_alt = positions[rsid]
        if (ref, alt) != (pharm_ref, pharm_alt):
            raise ValueError("public sources disagree on REF/ALT")
        alleles = [ref, alt]
        bases = [alleles[int(part)] for part in gt.replace("|", "/").split("/")]
        rows.append((rsid, pos37, pos38, ref, alt, gt, "".join(sorted(bases))))

    array_header = "# 1000 Genomes phase 3 public sample; GRCh37\n# rsid\tchromosome\tposition\tgenotype\n"
    (out / "public_23andme.txt").write_text(array_header + "".join(
        f"{rsid}\t10\t{pos37}\t{genotype}\n" for rsid, pos37, _, _, _, _, genotype in rows
    ))
    (out / "public_ancestry.txt").write_text("# AncestryDNA public-truth rendering; build 37\nrsid\tchromosome\tposition\tallele1\tallele2\n" + "".join(
        f"{rsid}\t10\t{pos37}\t{genotype[0]}\t{genotype[1]}\n" for rsid, pos37, _, _, _, _, genotype in rows
    ))
    with (out / "public_myheritage.csv").open("w", newline="") as output:
        output.write("# Public 1000 Genomes truth rendered as MyHeritage CSV; GRCh37\n")
        writer = csv.writer(output)
        writer.writerow(("RSID", "CHROMOSOME", "POSITION", "RESULT"))
        writer.writerows((rsid, "10", pos37, genotype) for rsid, pos37, _, _, _, _, genotype in rows)
    (out / "public.vcf").write_text(
        "##fileformat=VCFv4.2\n##reference=GRCh37\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t" + sample + "\n" +
        "".join(f"10\t{pos37}\t{rsid}\t{ref}\t{alt}\t.\tPASS\t.\tGT\t{gt}\n"
                for rsid, pos37, _, ref, alt, gt, _ in rows)
    )
    db = out / "sites.sqlite3"
    db.unlink(missing_ok=True)
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE metadata(key text PRIMARY KEY, value text);
            CREATE TABLE sites(rsid text PRIMARY KEY, chrom text, pos37 integer, pos38 integer,
                               ref text, alt text, gene text);
            CREATE INDEX sites_pos37 ON sites(chrom, pos37);
            CREATE INDEX sites_pos38 ON sites(chrom, pos38);
            CREATE TABLE merged(old_rsid text PRIMARY KEY, rsid text);
        """)
        conn.execute("INSERT INTO metadata VALUES ('version', 'public-1000g-pharmcat-e2e')")
        conn.executemany("INSERT INTO sites VALUES (?, '10', ?, ?, ?, ?, 'CYP2C19')",
                         [(rsid, pos37, pos38, ref, alt) for rsid, pos37, pos38, ref, alt, _, _ in rows])
    manifest = {
        "sample": sample, "source_urls": SOURCE,
        "truth_sha256": TRUTH_SHA256, "positions_sha256": POSITIONS_SHA256,
        "calls": [{"rsid": rsid, "pos37": pos37, "pos38": pos38, "gt": gt, "genotype": genotype}
                  for rsid, pos37, pos38, _, _, gt, genotype in rows],
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--positions", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--sample", default="HG00096")
    args = parser.parse_args()
    print(json.dumps(build(args.truth, args.positions, args.out, args.sample)["calls"], indent=2))

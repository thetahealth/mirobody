"""Prepare rsID-only inputs from two openly shared PGP exports.

Raw participant files stay outside the distribution. The generated lists do
not contain calls, positions or participant labels; the final site catalog
contains only public dbSNP records selected by those identifiers.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
from pathlib import Path

PGP_SOURCES = {
    "pgp_23andme_v5": (
        "23andme_v5_2023-10_male.txt",
        "https://my.pgp-hms.org/user_file/download/4220",
        "515559e5019e1557a3ea18b0a19ac88ea394f6504ef918dada44e58bb168d506",
    ),
    "pgp_ancestry_v2": (
        "ancestrydna_v2_2025-11_male.txt",
        "https://my.pgp-hms.org/user_file/download/4200",
        "fdfed037ca0e250ccea89a52cf4a3c0786a414a4626a842f80c5467d0c7dff7b",
    ),
}
UCSC_SOURCES = {
    37: ("dbSnp155Common-pgx-hg19.bed", "https://hgdownload.soe.ucsc.edu/gbdb/hg19/snp/dbSnp155Common.bb"),
    38: ("dbSnp155Common-pgx-hg38.bed", "https://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155Common.bb"),
}
WINDOWS = Path(__file__).with_name("public-pgx-windows.json")
CPIC_EXTRACT = Path(__file__).resolve().parents[2] / "mirobody/res/genomics/cpic-v1.60.0.json.gz"
CPIC_SHA256 = "47af59c170dc2e498a81237392f3ead4291856aa1f3805b60e57b7b3a4affd7f"
CPIC_RELEASE_URL = "https://github.com/cpicpgx/cpic-data/releases/tag/v1.60.0"
RSID = re.compile(r"rs[1-9][0-9]*\Z")


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def prepare(pgp_dir: Path, ucsc_dir: Path, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    markers = []
    counts = {}
    for name, (filename, url, expected) in PGP_SOURCES.items():
        path = pgp_dir / filename
        if digest(path) != expected:
            raise ValueError(f"public PGP input hash mismatch: {filename}")
        ids = set()
        with path.open(encoding="utf-8-sig") as stream:
            for line in stream:
                if not line.strip() or line.startswith("#"):
                    continue
                value = line.split(None, 1)[0]
                if RSID.fullmatch(value):
                    ids.add(value)
        if len(ids) < 500_000:
            raise ValueError(f"public PGP marker extraction is unexpectedly small: {name}")
        marker_file = output_dir / f"{name}.tsv"
        marker_file.write_text("rsid\n" + "\n".join(sorted(ids, key=lambda rsid: int(rsid[2:]))) + "\n",
                               encoding="utf-8")
        counts[name] = len(ids)
        markers.append({
            "name": name, "path": marker_file.name, "url": url,
            "sha256": digest(marker_file), "license": "PGP-Open-Consent",
            "derived_from": f"public PGP export sha256:{expected}; rsID column only",
        })
    extract_sha = digest(CPIC_EXTRACT)
    if extract_sha != CPIC_SHA256:
        raise ValueError("bundled CPIC extract hash mismatch")
    cpic = json.loads(gzip.decompress(CPIC_EXTRACT.read_bytes()))
    if cpic.get("version") != "v1.60.0" or cpic.get("format") != "mirobody-cpic-extract-1":
        raise ValueError("unexpected bundled CPIC extract")
    cpic_ids = {
        value for row in cpic["tables"]["sequence_location"]
        if (value := row[4]) and RSID.fullmatch(value)
    }
    if not cpic_ids:
        raise ValueError("bundled CPIC extract has no rsIDs")
    cpic_file = output_dir / "cpic_definition.tsv"
    cpic_file.write_text("rsid\n" + "\n".join(sorted(cpic_ids, key=lambda rsid: int(rsid[2:]))) + "\n",
                         encoding="utf-8")
    counts["cpic_definition"] = len(cpic_ids)
    markers.append({
        "name": "cpic_definition", "path": cpic_file.name, "url": CPIC_RELEASE_URL,
        "sha256": digest(cpic_file), "license": "CC0-1.0",
        "derived_from": f"bundled CPIC extract sha256:{extract_sha}; SQL source sha256:{cpic['source_sha256']}",
    })
    gene_links: dict[str, set[str]] = {}
    for row in cpic["tables"]["sequence_location"]:
        if row[4] in cpic_ids and row[3]:
            gene_links.setdefault(row[4], set()).add(row[3])
    gene_file = output_dir / "cpic_genes.tsv"
    gene_file.write_text(
        "rsid\tgene\n" + "\n".join(
            f"{rsid}\t{','.join(sorted(genes))}"
            for rsid, genes in sorted(gene_links.items(), key=lambda item: int(item[0][2:]))
        ) + "\n", encoding="utf-8",
    )
    manifest = {
        "release": "dbsnp-b155-common-pgx", "scope": "candidate", "markers": markers,
        "gene_annotations": {
            "path": gene_file.name, "url": CPIC_RELEASE_URL,
            "sha256": digest(gene_file), "license": "CC0-1.0",
            "derived_from": f"bundled CPIC extract sha256:{extract_sha}; rsID-to-gene links only",
        },
    }
    window_sha = digest(WINDOWS)
    expected_extracts = json.loads(WINDOWS.read_text(encoding="utf-8"))["extracts_sha256"]
    for build, (filename, url) in UCSC_SOURCES.items():
        path = ucsc_dir / filename
        source_sha = digest(path)
        if source_sha != expected_extracts[str(build)]:
            raise ValueError(f"public UCSC GRCh{build} window extract hash mismatch: {filename}")
        manifest[f"ucsc{build}"] = {
            "path": str(path.resolve()), "url": url, "sha256": source_sha,
            "license": "UCSC-Public-Data",
            "derived_from": f"UCSC dbSNP 155 Common, bounded PharmCAT/1000G regions sha256:{window_sha}",
        }
    manifest_path = output_dir / "public-candidate.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"manifest": str(manifest_path), "marker_counts": counts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pgp-dir", type=Path, required=True)
    parser.add_argument("--ucsc-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.pgp_dir, args.ucsc_dir, args.output_dir), sort_keys=True))

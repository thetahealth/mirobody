"""Fetch bounded, public dbSNP 155 Common regions for the preview catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

WINDOWS = Path(__file__).with_name("public-pgx-windows.json")
URLS = {
    "37": "https://hgdownload.soe.ucsc.edu/gbdb/hg19/snp/dbSnp155Common.bb",
    "38": "https://hgdownload.soe.ucsc.edu/gbdb/hg38/snp/dbSnp155Common.bb",
}
FILENAMES = {"37": "dbSnp155Common-pgx-hg19.bed", "38": "dbSnp155Common-pgx-hg38.bed"}


def fetch(output_dir: Path, converter: str) -> dict:
    plan = json.loads(WINDOWS.read_text(encoding="utf-8"))
    windows = plan["windows"]
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {}
    for build in ("37", "38"):
        rows = set()
        for gene, versions in sorted(windows.items()):
            window = versions[build]
            command = [converter, f"-chrom={window['chrom']}",
                       f"-start={window['start']}", f"-end={window['end']}",
                       URLS[build], "stdout"]
            completed = subprocess.run(command, capture_output=True, text=True,
                                       timeout=120, check=False)
            if completed.returncode:
                raise ValueError(f"public {gene} GRCh{build} window fetch failed")
            for line in completed.stdout.splitlines():
                fields = line.split("\t")
                if len(fields) < 14 or fields[0] != window["chrom"]:
                    raise ValueError(f"unexpected public {gene} GRCh{build} row")
                rows.add(line)
        if not any("\trs4244285\t" in row for row in rows):
            raise ValueError(f"public GRCh{build} track lacks rs4244285")
        path = output_dir / FILENAMES[build]
        ordered = sorted(rows, key=lambda line: (line.split("\t")[0], int(line.split("\t")[1]), line))
        payload = ("\n".join(ordered) + "\n").encode("utf-8")
        sha256 = hashlib.sha256(payload).hexdigest()
        if sha256 != plan["extracts_sha256"][build]:
            raise ValueError(f"public GRCh{build} window extract changed")
        path.write_bytes(payload)
        result[build] = {"path": str(path), "rows": len(rows), "bytes": len(payload),
                         "sha256": sha256}
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--big-bed-to-bed", default="bigBedToBed")
    args = parser.parse_args()
    print(json.dumps(fetch(args.output_dir, args.big_bed_to_bed), sort_keys=True))

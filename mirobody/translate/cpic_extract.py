"""Extract CPIC COPY data into a deterministic, versioned runtime asset.

The downloaded SQL is data to parse, never a program to execute.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen


FORMAT = "mirobody-cpic-extract-1"
SOURCE_URL = "https://files.cpicpgx.org/data/database/cpic_db_dump-{version}.sql.gz"
TABLE_COLUMNS = {
    "drug": (
        "drugid", "name", "clinpgxid", "rxnormid", "drugbankid",
        "atcid", "umlscui", "flowchart", "version", "guidelineid",
    ),
    "guideline": (
        "id", "version", "name", "url", "genes", "notesonusage", "clinpgxid",
    ),
    "pair": (
        "pairid", "genesymbol", "drugid", "guidelineid", "usedforrecommendation",
        "version", "cpiclevel", "clinpgxlevel", "pgxtesting", "citations",
        "removed", "removeddate", "removedreason",
    ),
    "allele_definition": (
        "id", "version", "genesymbol", "name", "pharmvarid",
        "matchesreferencesequence", "structuralvariation",
    ),
    "allele_location_value": (
        "alleledefinitionid", "locationid", "variantallele", "version",
    ),
    "sequence_location": (
        "id", "version", "name", "chromosomelocation", "genelocation",
        "proteinlocation", "genesymbol", "dbsnpid", '"position"',
    ),
    "gene_result": (
        "id", "genesymbol", "result", "activityscore", "ehrpriority",
        "consultationtext", "version", "frequency",
    ),
    "gene_result_lookup": (
        "id", "phenotypeid", "lookupkey", "function1", "function2",
        "activityvalue1", "activityvalue2", "totalactivityscore", "description",
    ),
    "gene_result_diplotype": (
        "id", "functionphenotypeid", "diplotype", "diplotypekey", "frequency",
    ),
}
# The runtime only needs identifiers, defining-site links and phenotype labels.
# In particular, consultation prose, population frequency and guideline text
# are excluded until a separately validated recommendation engine exists.
EXTRACT_COLUMNS = {
    "drug": ("drugid", "name"),
    "guideline": ("id", "name", "url"),
    "pair": ("genesymbol", "drugid", "guidelineid", "usedforrecommendation", "cpiclevel", "removed"),
    "allele_definition": (
        "id", "version", "genesymbol", "name", "structuralvariation",
    ),
    "allele_location_value": TABLE_COLUMNS["allele_location_value"],
    "sequence_location": (
        "id", "version", "chromosomelocation", "genesymbol", "dbsnpid",
    ),
    "gene_result": ("id", "genesymbol", "result", "version"),
    "gene_result_lookup": ("id", "phenotypeid"),
    "gene_result_diplotype": ("id", "functionphenotypeid", "diplotype"),
}
COPY_RE = re.compile(r"^COPY cpic\.(\w+) \(([^)]+)\) FROM stdin;\n$")
VERSION_RE = re.compile(r"v\d+\.\d+\.\d+\Z")
RELEASES_URL = "https://api.github.com/repos/cpicpgx/cpic-data/releases?per_page=30"
MAX_SOURCE_BYTES = 64 * 1024 * 1024


def _field(raw: str) -> str | None:
    if raw == r"\N":
        return None
    # PostgreSQL COPY escaping is deliberately narrow here; a new source
    # encoding must fail loudly instead of producing subtly wrong definitions.
    if "\\" in raw:
        raise ValueError("unsupported COPY escape in selected table")
    return raw


def extract(source: Path, version: str) -> dict:
    if not VERSION_RE.fullmatch(version):
        raise ValueError("CPIC version must be an exact vX.Y.Z release")
    if source.name != f"cpic_db_dump-{version}.sql.gz":
        raise ValueError("CPIC dump filename and version disagree")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    tables: dict[str, list[list[str | None]]] = {name: [] for name in TABLE_COLUMNS}
    seen: set[str] = set()
    current: str | None = None
    with gzip.open(source, "rt", encoding="utf-8", newline="") as stream:
        for line in stream:
            if current is None:
                if line.startswith("COPY cpic."):
                    match = COPY_RE.fullmatch(line)
                    if match is None:
                        raise ValueError("unrecognized CPIC COPY declaration")
                    name, columns = match.groups()
                    if name in TABLE_COLUMNS:
                        if name in seen:
                            raise ValueError(f"duplicate CPIC table: {name}")
                        if tuple(columns.split(", ")) != TABLE_COLUMNS[name]:
                            raise ValueError(f"unsupported CPIC schema: {name}")
                        seen.add(name)
                    current = name
                continue
            if line == "\\.\n":
                current = None
                continue
            if current in TABLE_COLUMNS:
                fields = [_field(raw) for raw in line.removesuffix("\n").split("\t")]
                if len(fields) != len(TABLE_COLUMNS[current]):
                    raise ValueError(f"malformed CPIC row: {current}")
                indexes = [TABLE_COLUMNS[current].index(column) for column in EXTRACT_COLUMNS[current]]
                tables[current].append([fields[index] for index in indexes])
    if current is not None or seen != TABLE_COLUMNS.keys():
        raise ValueError("incomplete CPIC COPY data")
    if any(not rows for rows in tables.values()):
        raise ValueError("empty CPIC table")

    result_ids = {row[0] for row in tables["gene_result"]}
    lookup_ids = {row[0] for row in tables["gene_result_lookup"]}
    definition_ids = {row[0] for row in tables["allele_definition"]}
    location_ids = {row[0] for row in tables["sequence_location"]}
    if any(row[1] not in result_ids for row in tables["gene_result_lookup"]):
        raise ValueError("broken phenotype foreign key")
    if any(row[1] not in lookup_ids for row in tables["gene_result_diplotype"]):
        raise ValueError("broken diplotype foreign key")
    if any(
        row[0] not in definition_ids or row[1] not in location_ids
        for row in tables["allele_location_value"]
    ):
        raise ValueError("broken allele location foreign key")
    return {
        "format": FORMAT,
        "version": version,
        "source_url": SOURCE_URL.format(version=version),
        "source_sha256": digest,
        "columns": {name: list(columns) for name, columns in EXTRACT_COLUMNS.items()},
        "counts": {name: len(rows) for name, rows in tables.items()},
        "tables": tables,
    }


def write_extract(payload: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    with output.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as compressed:
        compressed.write(data)


def latest_version() -> str:
    """Resolve the newest official release that actually published a DB dump."""
    request = Request(RELEASES_URL, headers={"Accept": "application/vnd.github+json",
                                            "User-Agent": "mirobody-cpic-fetch"})
    with urlopen(request, timeout=30) as response:
        raw = response.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("latest CPIC release metadata exceeds the size limit")
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("CPIC release metadata has an unsupported shape")
    versions = []
    for release in payload:
        if not isinstance(release, dict) or release.get("draft") or release.get("prerelease"):
            continue
        version = release.get("tag_name")
        body = release.get("body") or ""
        if (isinstance(version, str) and VERSION_RE.fullmatch(version)
                and isinstance(body, str) and SOURCE_URL.format(version=version) in body):
            versions.append(version)
    if not versions:
        raise ValueError("no CPIC release in the current page publishes a database dump")
    return max(versions, key=lambda item: tuple(int(part) for part in item[1:].split(".")))


def fetch_extract(version: str, directory: Path, source_url: str | None = None) -> tuple[str, str, Path]:
    """Download a pinned public dump, parse data blocks, then atomically install it.

    The SQL is never executed. A malformed or changed schema cannot replace a
    working extract. ``source_url`` is a base URL for an operator's mirror.
    """
    if version == "latest":
        version = latest_version()
    if not VERSION_RE.fullmatch(version):
        raise ValueError("CPIC version must be an exact vX.Y.Z release")
    filename = f"cpic_db_dump-{version}.sql.gz"
    base_url = source_url or os.environ.get("CPIC_SOURCE_URL") or "https://files.cpicpgx.org/data/database/"
    if not base_url.endswith("/"):
        base_url += "/"
    directory = Path(directory).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cpic-fetch-", dir=directory) as scratch_name:
        scratch = Path(scratch_name)
        source = scratch / filename
        request = Request(base_url + filename, headers={"User-Agent": "mirobody-cpic-fetch"})
        with urlopen(request, timeout=60) as response, source.open("wb") as output:
            total = 0
            while chunk := response.read(1024 * 1024):
                total += len(chunk)
                if total > MAX_SOURCE_BYTES:
                    raise ValueError("CPIC dump exceeds the download limit")
                output.write(chunk)
        payload = extract(source, version)
        candidate = scratch / f"cpic-{version}.json.gz"
        write_extract(payload, candidate)
        # Run the runtime's semantic validation before exposing this version.
        from mirobody.translate.pgx import CpicKnowledge

        CpicKnowledge(json.loads(gzip.decompress(candidate.read_bytes())))
        installed = directory / candidate.name
        if installed.exists():
            if installed.read_bytes() != candidate.read_bytes():
                raise ValueError("pinned CPIC version already exists with different content")
            return version, str(payload["source_sha256"]), installed
        os.replace(candidate, installed)
        return version, str(payload["source_sha256"]), installed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path("mirobody/res/genomics") / f"cpic-{args.version}.json.gz"
    payload = extract(args.source, args.version)
    write_extract(payload, output)
    print(json.dumps({"output": str(output), "counts": payload["counts"], "source_sha256": payload["source_sha256"]}))


if __name__ == "__main__":
    main()

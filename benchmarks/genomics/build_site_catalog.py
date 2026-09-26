"""Build a pinned dbSNP b157 site index from public, hashed source files.

The input manifest names local files, their original HTTPS URLs, SHA-256 hashes
and redistribution licences. No download, personal genotype file or inferred
reference allele is accepted by this builder.
"""

from __future__ import annotations

import argparse
import bz2
import gzip
import hashlib
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

RSID = re.compile(r"rs[1-9][0-9]*\Z")
ALLELE = re.compile(r"[ACGTN]+\Z")
LICENSES = {"NCBI-PD", "BSD-3-Clause", "CC0-1.0", "MPL-2.0"}
ASSEMBLIES = {"GRCh37.p13": 37, "GRCh38.p14": 38}
VCF_URLS = {
    37: "https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.25.gz",
    38: "https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/VCF/GCF_000001405.40.gz",
}
MERGED_URL = "https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-merged.json.bz2"
NCBI_MD5 = {
    VCF_URLS[37]: "35db22bcd166f904e4775dbbc29f5965",
    VCF_URLS[38]: "6a6f313e92a39c337571174dad12cfe1",
    MERGED_URL: "5d9c56c9b8c3412eb0195a63f12e1ebf",
}
MAX_MARKERS = 5_000_000


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class WantedFilter:
    """A fixed 16 MiB prefilter; SQLite remains the exact membership check."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.bits = bytearray(1 << 24)
        for (rsid,) in db.execute("SELECT rsid FROM wanted"):
            self.add(rsid)

    def _indexes(self, rsid: str):
        number = int(rsid[2:])
        mask = (1 << 27) - 1
        first = (number * 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
        second = ((number ^ (number >> 31)) * 0xC2B2AE3D27D4EB4F) | 1
        for offset in range(3):
            yield (first + offset * second) & mask

    def add(self, rsid: str) -> None:
        for index in self._indexes(rsid):
            self.bits[index >> 3] |= 1 << (index & 7)

    def maybe(self, rsid: str) -> bool:
        return all(self.bits[index >> 3] & (1 << (index & 7)) for index in self._indexes(rsid))


def _source(entry: dict, base: Path, *, expected_url: str | None = None) -> tuple[Path, dict]:
    url = entry["url"]
    if not isinstance(url, str) or not url.startswith("https://"):
        raise ValueError("every source needs a public HTTPS URL")
    if expected_url is not None and url != expected_url:
        raise ValueError(f"expected pinned dbSNP b157 URL: {expected_url}")
    if entry["license"] not in LICENSES:
        raise ValueError(f"unrecognised redistribution licence: {entry['license']}")
    expected = entry["sha256"].lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("every source needs a SHA-256 digest")
    path = (base / entry["path"]).resolve()
    digest = hashlib.sha256()
    upstream_digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
            upstream_digest.update(block)
    if digest.hexdigest() != expected:
        raise ValueError(f"source hash mismatch: {entry['path']}")
    if expected_url is not None and upstream_digest.hexdigest() != NCBI_MD5[expected_url]:
        raise ValueError(f"source differs from NCBI's b157 checksum: {entry['path']}")
    provenance = {"url": url, "sha256": expected, "license": entry["license"]}
    if expected_url is not None:
        provenance["ncbi_md5"] = NCBI_MD5[expected_url]
    if "derived_from" in entry:
        provenance["derived_from"] = entry["derived_from"]
    return path, provenance


def _lines(path: Path):
    suffix = path.suffix.lower()
    opener = bz2.open if suffix == ".bz2" else gzip.open if suffix == ".gz" else open
    return opener(path, "rt", encoding="utf-8")


def _rsid(value: str) -> str | None:
    value = value.strip()
    return value if RSID.fullmatch(value) else None


def _chrom(value: str) -> str | None:
    if value.startswith("chr"):
        value = value[3:]
    if value in {"X", "Y", "MT", "M"}:
        return "MT" if value == "M" else value
    if value.startswith("NC_"):
        accession = value.split(".", 1)[0]
        if accession == "NC_012920":
            return "MT"
        number = accession.removeprefix("NC_")
        if number.isdigit():
            value = str(int(number))
            if value == "23":
                return "X"
            if value == "24":
                return "Y"
    if value.isdecimal() and 1 <= int(value) <= 22:
        return str(int(value))
    return None


def _alleles(ref: str, alt: str) -> tuple[str, str] | None:
    ref = ref.upper()
    alts = sorted(set(alt.upper().split(",")))
    if not ALLELE.fullmatch(ref) or len(ref) > 1000:
        return None
    if any(not ALLELE.fullmatch(a) or len(a) > 1000 or a == ref for a in alts):
        return None
    return ref, ",".join(alts)


def _genes_from_info(info: str) -> str | None:
    for field in info.split(";"):
        if field.startswith("GENEINFO="):
            symbols = sorted({item.split(":", 1)[0] for item in field[9:].split("|")})
            return ",".join(symbols) or None
    return None


def _record_placement(
    db: sqlite3.Connection, rsid: str, build: int, chrom: str, pos: int,
    ref: str, alt: str, gene: str | None,
) -> None:
    previous = db.execute(
        "SELECT chrom,pos,ref,alt,gene,conflict FROM placements WHERE rsid=? AND build=?",
        (rsid, build),
    ).fetchone()
    if previous is None:
        db.execute(
            "INSERT INTO placements VALUES (?,?,?,?,?,?,?,0)",
            (rsid, build, chrom, pos, ref, alt, gene),
        )
    elif (previous[0], previous[1], previous[2]) != (chrom, pos, ref):
        db.execute(
            "UPDATE placements SET conflict=1 WHERE rsid=? AND build=?", (rsid, build),
        )
    elif not previous[5]:
        union = ",".join(sorted(set(previous[3].split(",")) | set(alt.split(","))))
        merged_gene = previous[4] if previous[4] == gene or gene is None else gene if previous[4] is None else None
        db.execute(
            "UPDATE placements SET alt=?,gene=? WHERE rsid=? AND build=?",
            (union, merged_gene, rsid, build),
        )


def _load_markers(db: sqlite3.Connection, sources: list[tuple[str, Path]]) -> dict[str, int]:
    counts = {}
    for name, path in sources:
        count = 0
        with _lines(path) as stream:
            if stream.readline().rstrip("\r\n") != "rsid":
                raise ValueError(f"marker list must have a single rsid header: {name}")
            for line in stream:
                rsid = _rsid(line)
                if not rsid:
                    raise ValueError(f"invalid marker rsID in {name}")
                db.execute("INSERT OR IGNORE INTO marker_ids VALUES (?,?)", (name, rsid))
                db.execute("INSERT OR IGNORE INTO wanted VALUES (?)", (rsid,))
                count += 1
        counts[name] = count
    if not counts or not any(counts.values()):
        raise ValueError("at least one public marker is required")
    total = db.execute("SELECT COUNT(*) FROM wanted").fetchone()[0]
    if total > MAX_MARKERS:
        raise ValueError(f"marker union exceeds {MAX_MARKERS:,} rsIDs")
    return counts


def _merge_targets(record: dict) -> tuple[str, list[str]] | None:
    old = _rsid("rs" + str(record.get("refsnp_id", "")))
    merged = record.get("merged_snapshot_data")
    if not old or not isinstance(merged, dict):
        return None
    targets = [_rsid("rs" + str(item)) for item in merged.get("merged_into", [])]
    return old, sorted({target for target in targets if target})


def _edge(db: sqlite3.Connection, old: str, target: str) -> None:
    if db.execute("SELECT 1 FROM ambiguous_merge WHERE old_rsid=?", (old,)).fetchone():
        return
    existing = db.execute("SELECT target FROM merge_edges WHERE old_rsid=?", (old,)).fetchone()
    if existing is not None and existing[0] != target:
        db.execute("DELETE FROM merge_edges WHERE old_rsid=?", (old,))
        db.execute("INSERT OR IGNORE INTO ambiguous_merge VALUES (?)", (old,))
    elif existing is None:
        db.execute("INSERT INTO merge_edges VALUES (?,?)", (old, target))


def _load_merges(db: sqlite3.Connection, path: Path, wanted: WantedFilter) -> int:
    # A selected marker can itself be a merged ID. Repeat the small lookup
    # frontier until later merge generations no longer add a target.
    passes = 0
    for _ in range(20):
        added = 0
        passes += 1
        with _lines(path) as stream:
            for line in stream:
                parsed = _merge_targets(json.loads(line))
                if parsed is None:
                    continue
                old, targets = parsed
                if not _wanted(db, old, wanted):
                    continue
                if len(targets) != 1:
                    db.execute("INSERT OR IGNORE INTO ambiguous_merge VALUES (?)", (old,))
                    continue
                target = targets[0]
                _edge(db, old, target)
                if db.execute("INSERT OR IGNORE INTO wanted VALUES (?)", (target,)).rowcount:
                    wanted.add(target)
                    added += 1
        if not added:
            break
        expanded = db.execute("SELECT COUNT(*) FROM wanted").fetchone()[0]
        if expanded > 2 * MAX_MARKERS:
            raise ValueError("merge expansion exceeded the ten-million-rsID build bound")
    else:
        raise ValueError("dbSNP merge chain exceeded 20 generations")
    # A final pass collects old IDs of selected current sites for lookup.
    with _lines(path) as stream:
        for line in stream:
            parsed = _merge_targets(json.loads(line))
            if parsed is None:
                continue
            old, targets = parsed
            if len(targets) == 1 and _wanted(db, targets[0], wanted):
                _edge(db, old, targets[0])
    return passes + 1


def _wanted(db: sqlite3.Connection, rsid: str, wanted: WantedFilter) -> bool:
    if not wanted.maybe(rsid):
        return False
    return db.execute("SELECT 1 FROM wanted WHERE rsid=?", (rsid,)).fetchone() is not None


def _load_vcf(db: sqlite3.Connection, path: Path, build: int, wanted: WantedFilter) -> dict[str, int]:
    counts = {"records": 0, "selected": 0, "invalid": 0}
    with _lines(path) as stream:
        for line in stream:
            if line.startswith("#"):
                continue
            counts["records"] += 1
            fields = line.rstrip("\r\n").split("\t", 8)
            if len(fields) < 8:
                counts["invalid"] += 1
                continue
            ids = [_rsid(item) for item in fields[2].split(";")]
            if not any(ids):
                for field in fields[7].split(";"):
                    if field.startswith("RS="):
                        ids = [_rsid("rs" + item.removeprefix("rs")) for item in field[3:].split(",")]
                        break
            ids = [item for item in ids if item and _wanted(db, item, wanted)]
            if not ids:
                continue
            chrom = _chrom(fields[0])
            alleles = _alleles(fields[3], fields[4])
            try:
                pos = int(fields[1])
            except ValueError:
                pos = 0
            if chrom is None or pos < 1 or alleles is None:
                counts["invalid"] += 1
                continue
            gene = _genes_from_info(fields[7])
            for rsid in ids:
                _record_placement(db, rsid, build, chrom, pos, *alleles, gene)
                counts["selected"] += 1
    return counts


def _json_placements(record: dict, build: int):
    data = record.get("primary_snapshot_data") or {}
    for item in data.get("placements_with_allele", []):
        traits = item.get("placement_annot", {}).get("seq_id_traits_by_assembly", [])
        if not any(
            ASSEMBLIES.get(t.get("assembly_name")) == build
            and t.get("is_chromosome") and t.get("is_top_level")
            and not t.get("is_alt") and not t.get("is_patch") for t in traits
        ):
            continue
        chrom = _chrom(item.get("seq_id", ""))
        alleles = [a.get("allele", {}).get("spdi", {}) for a in item.get("alleles", [])]
        identities = [a for a in alleles if a.get("deleted_sequence") == a.get("inserted_sequence")]
        if chrom is None or len(identities) != 1:
            continue
        identity = identities[0]
        pos = identity.get("position")
        ref = identity.get("deleted_sequence", "")
        if not isinstance(pos, int):
            continue
        alts = [a.get("inserted_sequence", "") for a in alleles if a != identity
                and a.get("position") == pos and a.get("deleted_sequence") == ref]
        checked = _alleles(ref, ",".join(alts)) if alts else None
        if checked:
            yield chrom, pos + 1, *checked


def _json_gene(record: dict) -> str | None:
    symbols = set()
    for allele in (record.get("primary_snapshot_data") or {}).get("allele_annotations", []):
        for annotation in allele.get("assembly_annotation", []):
            for gene in annotation.get("genes", []):
                if gene.get("locus"):
                    symbols.add(gene["locus"])
    return ",".join(sorted(symbols)) or None


def _load_json(db: sqlite3.Connection, paths: list[Path], wanted: WantedFilter) -> dict[str, int]:
    counts = {"records": 0, "selected": 0, "invalid": 0}
    for path in paths:
        with _lines(path) as stream:
            for line in stream:
                counts["records"] += 1
                record = json.loads(line)
                rsid = _rsid("rs" + str(record.get("refsnp_id", "")))
                if not rsid or not _wanted(db, rsid, wanted):
                    continue
                counts["selected"] += 1
                gene = _json_gene(record)
                builds = 0
                for build in (37, 38):
                    for chrom, pos, ref, alt in _json_placements(record, build):
                        _record_placement(db, rsid, build, chrom, pos, ref, alt, gene)
                        builds += 1
                if builds != 2:
                    counts["invalid"] += 1
                for merge in record.get("dbsnp1_merges", []):
                    old = _rsid("rs" + str(merge.get("merged_rsid", "")))
                    if old:
                        _edge(db, old, rsid)
    return counts


def _resolve_merges(db: sqlite3.Connection) -> int:
    count = 0
    for old, _ in db.execute("SELECT old_rsid,target FROM merge_edges ORDER BY old_rsid"):
        seen = {old}
        current = old
        for _ in range(20):
            row = db.execute("SELECT target FROM merge_edges WHERE old_rsid=?", (current,)).fetchone()
            if row is None:
                break
            current = row[0]
            if current in seen:
                current = ""
                break
            seen.add(current)
        if current and current != old:
            db.execute("INSERT OR REPLACE INTO merged VALUES (?,?)", (old, current))
            count += 1
    return count


def _finish(db: sqlite3.Connection) -> dict[str, int]:
    stats = {"missing_build": 0, "conflict": 0, "allele_mismatch": 0, "sites": 0}
    for (rsid,) in db.execute("SELECT rsid FROM wanted ORDER BY rsid"):
        if db.execute("SELECT 1 FROM merged WHERE old_rsid=?", (rsid,)).fetchone():
            continue
        a = db.execute("SELECT chrom,pos,ref,alt,gene,conflict FROM placements WHERE rsid=? AND build=37", (rsid,)).fetchone()
        b = db.execute("SELECT chrom,pos,ref,alt,gene,conflict FROM placements WHERE rsid=? AND build=38", (rsid,)).fetchone()
        if a is None or b is None:
            stats["missing_build"] += 1
        elif a[5] or b[5] or a[0] != b[0]:
            stats["conflict"] += 1
        elif a[2] != b[2] or a[3] != b[3]:
            # One REF/ALT pair cannot truthfully describe two different builds.
            stats["allele_mismatch"] += 1
        else:
            db.execute("INSERT INTO sites VALUES (?,?,?,?,?,?,?)", (rsid, b[0], a[1], b[1], b[2], b[3], b[4] or a[4]))
            stats["sites"] += 1
    db.execute("DELETE FROM merged WHERE rsid NOT IN (SELECT rsid FROM sites)")
    stats["merged"] = db.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
    stats["marker_union"] = db.execute("SELECT COUNT(DISTINCT rsid) FROM marker_ids").fetchone()[0]
    stats["marker_covered"] = db.execute(
        "SELECT COUNT(DISTINCT marker_ids.rsid) FROM marker_ids "
        "LEFT JOIN merged ON merged.old_rsid=marker_ids.rsid "
        "WHERE COALESCE(merged.rsid, marker_ids.rsid) IN (SELECT rsid FROM sites)"
    ).fetchone()[0]
    return stats


def build(manifest_path: Path, output: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("release") != "dbsnp-b157":
        raise ValueError("release must be dbsnp-b157")
    if manifest.get("scope") not in {"sample", "candidate"}:
        raise ValueError("scope must be sample or candidate")
    base = manifest_path.resolve().parent
    sources = {}
    marker_files = []
    for entry in manifest.get("markers", []):
        name = entry["name"]
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", name) or name in sources:
            raise ValueError("marker names must be distinct ASCII identifiers")
        path, provenance = _source(entry, base)
        sources[name] = provenance
        marker_files.append((name, path))
    json_mode = "refsnp_json" in manifest
    if json_mode == ("vcf37" in manifest or "vcf38" in manifest):
        raise ValueError("choose either RefSNP JSONL or both b157 VCF files")
    if json_mode:
        json_paths = []
        for entry in manifest["refsnp_json"]:
            path, provenance = _source(entry, base)
            if "/snp/archive/b157/JSON/" not in provenance["url"]:
                raise ValueError("RefSNP JSON must come from the b157 archive")
            json_paths.append(path)
            sources[f"refsnp_{len(json_paths)}"] = provenance
        if not json_paths:
            raise ValueError("at least one b157 RefSNP JSONL source is required")
    else:
        vcf_paths = {}
        for build_id in (37, 38):
            expected_url = VCF_URLS[build_id] if manifest["scope"] == "candidate" else None
            path, provenance = _source(manifest[f"vcf{build_id}"], base, expected_url=expected_url)
            if manifest["scope"] == "sample" and "/snp/archive/b157/" not in provenance["url"]:
                raise ValueError("sample VCF must derive from the b157 archive")
            vcf_paths[build_id] = path
            sources[f"vcf{build_id}"] = provenance
    merged_path = None
    if "merged" in manifest:
        expected_url = MERGED_URL if manifest["scope"] == "candidate" else None
        merged_path, provenance = _source(manifest["merged"], base, expected_url=expected_url)
        if manifest["scope"] == "sample" and not (
            "/snp/archive/b157/JSON/" in provenance["url"] or
            provenance["url"].startswith("https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/")
        ):
            raise ValueError("sample merge source must be NCBI dbSNP")
        sources["merged"] = provenance
    if manifest["scope"] == "candidate" and (
        json_mode or merged_path is None or
        {name for name, _ in marker_files} != {"wegene", "23andme_v5", "ancestry_v2", "gsa"}
    ):
        raise ValueError("candidate requires four marker lists, both VCFs and merged history")
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(output.name + f".tmp.{os.getpid()}")
    temp.unlink(missing_ok=True)
    try:
        db = sqlite3.connect(temp)
        try:
            db.executescript("""
                PRAGMA page_size=4096;
                CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE marker_ids (source TEXT NOT NULL, rsid TEXT NOT NULL, PRIMARY KEY(source,rsid));
                CREATE TABLE wanted (rsid TEXT PRIMARY KEY);
                CREATE TABLE merge_edges (old_rsid TEXT PRIMARY KEY, target TEXT NOT NULL);
                CREATE TABLE ambiguous_merge (old_rsid TEXT PRIMARY KEY);
                CREATE TABLE placements (rsid TEXT NOT NULL, build INTEGER NOT NULL, chrom TEXT NOT NULL,
                    pos INTEGER NOT NULL, ref TEXT NOT NULL, alt TEXT NOT NULL, gene TEXT,
                    conflict INTEGER NOT NULL, PRIMARY KEY(rsid,build));
                CREATE TABLE sites (rsid TEXT PRIMARY KEY, chrom TEXT NOT NULL, pos37 INTEGER NOT NULL,
                    pos38 INTEGER NOT NULL, ref TEXT NOT NULL, alt TEXT NOT NULL, gene TEXT);
                CREATE TABLE merged (old_rsid TEXT PRIMARY KEY, rsid TEXT NOT NULL);
                CREATE INDEX sites_pos37 ON sites(chrom,pos37);
                CREATE INDEX sites_pos38 ON sites(chrom,pos38);
            """)
            marker_counts = _load_markers(db, marker_files)
            wanted = WantedFilter(db)
            merge_passes = _load_merges(db, merged_path, wanted) if merged_path else 0
            record_counts = _load_json(db, json_paths, wanted) if json_mode else {
                str(k): _load_vcf(db, path, k, wanted) for k, path in vcf_paths.items()
            }
            _resolve_merges(db)
            stats = _finish(db)
            if not stats["sites"]:
                raise ValueError("no dual-build dbSNP sites matched the public marker lists")
            stats.update({"marker_rows": marker_counts, "records": record_counts,
                          "merge_passes": merge_passes,
                          "ambiguous_merges": db.execute("SELECT COUNT(*) FROM ambiguous_merge").fetchone()[0]})
            version = f"dbsnp-b157-{manifest['scope']}"
            metadata = {
                "version": version, "scope": manifest["scope"], "sources_json": json.dumps(sources, sort_keys=True),
                "stats_json": json.dumps(stats, sort_keys=True),
            }
            db.executemany("INSERT INTO metadata VALUES (?,?)", sorted(metadata.items()))
            for table in ("marker_ids", "wanted", "merge_edges", "ambiguous_merge", "placements"):
                db.execute(f"DROP TABLE {table}")
            db.commit()
            db.execute("VACUUM")
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            db.close()
        os.replace(temp, output)
    finally:
        temp.unlink(missing_ok=True)
    return {"path": str(output), "bytes": output.stat().st_size, "sha256": _hash_file(output),
            "version": version, **stats}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(build(args.manifest, args.output), sort_keys=True))
    except (OSError, ValueError, KeyError, sqlite3.Error, json.JSONDecodeError) as exc:
        print(f"catalog build failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

"""Health-domain graph builder: bridge + sibling CSVs → concept graph binary.

Reads the CSV files produced by bridge.py and siblings.py, resolves
code → fhir_id, and returns bridges/siblings for ConceptGraphBuilder.
"""

from __future__ import annotations

import contextlib
import csv
import logging
import os
import tempfile
from collections.abc import Iterator

from ..concept_graph import ConceptGraphBuilder

log = logging.getLogger(__name__)

# Bridge files: (filename, columns, max_codes)
_BRIDGE_FILES = [
    ("_bridges_icd.csv", ["snomed_codes", "loinc_codes"], 0),
    ("_bridges_mrrel.csv", ["snomed_codes", "loinc_codes"], 0),
    ("_bridges_jaccard.csv", ["snomed_codes", "loinc_codes"], 150),
    ("_bridges_rxnorm.csv", ["snomed_codes", "rxnorm_codes"], 0),
    ("_bridges_loinc_rxnorm.csv", ["loinc_codes", "rxnorm_codes"], 0),
]

# Sibling files: (filename, system)
_SIBLING_FILES = [
    ("_siblings_snomed.csv", "snomed"),
    ("_siblings_loinc.csv", "loinc"),
    ("_siblings_rxnorm.csv", "rxnorm"),
]


@contextlib.contextmanager
def _csv_field_size_limit(limit: int = 1 << 20) -> Iterator[None]:
    """Temporarily raise csv.field_size_limit, restoring the original on exit."""
    old = csv.field_size_limit(limit)
    try:
        yield
    finally:
        csv.field_size_limit(old)


class HealthGraphBuilder(ConceptGraphBuilder):
    """Build concept graph from FHIR bridge + sibling CSVs."""

    def __init__(self) -> None:
        super().__init__()
        self._code_map: dict[str, list[int]] | None = None
        self._code_map_dir: str = ""

    def _load_code_map(self, src_dir: str) -> dict[str, list[int]]:
        """Load code → fhir_id mapping from cache CSV (cached on instance)."""
        if self._code_map is not None and self._code_map_dir == src_dir:
            return self._code_map
        code_to_fhir: dict[str, list[int]] = {}
        cache_path = os.path.join(src_dir, "_fhir_id_codes.csv")
        if not os.path.isfile(cache_path):
            raise FileNotFoundError(
                f"{cache_path} not found. Run sync_fhir_ids first to generate it.")
        with open(cache_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code = row.get("code", "")
                ids = [int(x) for x in row.get("fhir_ids", "").split("|") if x]
                if code and ids:
                    code_to_fhir[code] = ids
        log.info("  Code→FHIR (cached): %s codes ← %s", f"{len(code_to_fhir):,}", cache_path)
        self._code_map = code_to_fhir
        self._code_map_dir = src_dir
        return code_to_fhir

    def load_bridges(self, src_dir: str) -> dict[int, set[int]]:
        code_to_fhir = self._load_code_map(src_dir)
        bridges: dict[int, set[int]] = {}

        n_loaded = n_skipped = 0
        for filename, columns, max_codes in _BRIDGE_FILES:
            path = os.path.join(src_dir, filename)
            if not os.path.exists(path):
                continue
            with _csv_field_size_limit(), open(path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    groups: list[list[str]] = []
                    for col in columns:
                        codes = [c for c in row.get(col, "").split("|") if c]
                        groups.append(codes)
                    if max_codes and sum(len(g) for g in groups) > max_codes:
                        n_skipped += 1
                        continue
                    fid_groups: list[set[int]] = []
                    for code_list in groups:
                        fids: set[int] = set()
                        for c in code_list:
                            fids.update(code_to_fhir.get(c, ()))
                        fid_groups.append(fids)
                    for i in range(len(fid_groups)):
                        for j in range(i + 1, len(fid_groups)):
                            if not fid_groups[i] or not fid_groups[j]:
                                continue
                            for sf in fid_groups[i]:
                                bridges.setdefault(sf, set()).update(fid_groups[j])
                            for sf in fid_groups[j]:
                                bridges.setdefault(sf, set()).update(fid_groups[i])
                    n_loaded += 1
        for nid, neighbors in bridges.items():
            neighbors.discard(nid)
        log.info("  Bridges: %d loaded, %d skipped, %s nodes",
                 n_loaded, n_skipped, f"{len(bridges):,}")
        return bridges

    def load_siblings(self, src_dir: str) -> list[list[int]]:
        code_to_fhir = self._load_code_map(src_dir)
        siblings: list[list[int]] = []

        n_sib_code_groups = 0
        for filename, system in _SIBLING_FILES:
            path = os.path.join(src_dir, filename)
            if not os.path.exists(path):
                continue
            n_groups = 0
            with _csv_field_size_limit(), open(path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    codes = [c for c in row.get("codes", "").split("|") if c]
                    if len(codes) < 2:
                        continue
                    group: list[int] = []
                    for c in codes:
                        group.extend(code_to_fhir.get(c, ()))
                    if len(group) >= 2:
                        siblings.append(group)
                    n_groups += 1
            n_sib_code_groups += n_groups
            log.info("  Siblings %s: %d groups", system, n_groups)

        log.info("  Result: %s sibling groups (from %s code groups)",
                 f"{len(siblings):,}", f"{n_sib_code_groups:,}")
        return siblings


# ── Sync fhir_id cache from DB ──────────────────────────────────────


async def sync_fhir_ids(out_dir: str) -> None:
    """Query DB for code→fhir_id mapping and write _fhir_id_codes.csv."""
    cache_path = os.path.join(out_dir, "_fhir_id_codes.csv")
    if os.path.isfile(cache_path):
        log.info("  FHIR ID cache exists, skipping DB query: %s", cache_path)
        return

    from mirobody.utils import execute_query
    result = await execute_query(
        "SELECT id, code, indicator_standard FROM fhir_indicators WHERE code IS NOT NULL")
    if not result:
        log.warning("No fhir_indicators found in DB")
        return

    code_map: dict[str, dict] = {}
    for row in result:
        code = row["code"]
        if code not in code_map:
            code_map[code] = {"system": row.get("indicator_standard", ""), "ids": []}
        code_map[code]["ids"].append(row["id"])

    tmp_fd, tmp_path = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["code", "system", "fhir_ids"])
            for code in sorted(code_map):
                info = code_map[code]
                writer.writerow([code, info["system"], "|".join(map(str, info["ids"]))])
        os.replace(tmp_path, cache_path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    log.info("  FHIR ID cache written: %s codes → %s", f"{len(code_map):,}", cache_path)

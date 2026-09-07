"""FHIR-vocabulary graph builder: bridge + sibling CSVs → concept graph binary.

Reads the CSV files produced by bridge.py and siblings.py and emits a
canonical-keyed concept graph. Node keys are :func:`code_to_fhir_id`
packed bigints, derived purely from ``(system, code)`` — no DB hop.

This is deliberate: siblings exist for query-expansion recall, so they
must retain retired/inactive codes that never made it into the live
``fhir_indicators`` corpus. The previous DB-keyed build dropped any
sibling member missing from the DB, which silently collapsed groups
where only one member survived corpus filtering.
"""

from __future__ import annotations

import contextlib
import csv
import logging
import os
from collections.abc import Iterator

from ..concept_graph import ConceptGraphBuilder
from .common import FHIR_GRAPH_BIN, code_to_fhir_id

log = logging.getLogger(__name__)


# Bridge files: (filename, [(column, system), ...], max_codes)
_BRIDGE_FILES = [
    ("_bridges_icd.csv",          [("snomed_codes", "SNOMED_CT"), ("loinc_codes", "LOINC")],   0),
    ("_bridges_mrrel.csv",        [("snomed_codes", "SNOMED_CT"), ("loinc_codes", "LOINC")],   0),
    ("_bridges_jaccard.csv",      [("snomed_codes", "SNOMED_CT"), ("loinc_codes", "LOINC")], 150),
    ("_bridges_rxnorm.csv",       [("snomed_codes", "SNOMED_CT"), ("rxnorm_codes", "RXNORM")], 0),
    ("_bridges_loinc_rxnorm.csv", [("loinc_codes",  "LOINC"),     ("rxnorm_codes", "RXNORM")], 0),
]

# Sibling files: (filename, system)
_SIBLING_FILES = [
    ("_siblings_snomed.csv", "SNOMED_CT"),
    ("_siblings_loinc.csv",  "LOINC"),
    ("_siblings_rxnorm.csv", "RXNORM"),
]


@contextlib.contextmanager
def _csv_field_size_limit(limit: int = 1 << 20) -> Iterator[None]:
    """Temporarily raise csv.field_size_limit, restoring the original on exit."""
    old = csv.field_size_limit(limit)
    try:
        yield
    finally:
        csv.field_size_limit(old)


def _encode_codes(codes: list[str], system: str) -> list[int]:
    """Map vocab code strings to canonical fhir_id ints, skipping malformed."""
    out: list[int] = []
    for c in codes:
        try:
            out.append(code_to_fhir_id(system, c))
        except (ValueError, KeyError):
            # Codes that don't fit code_to_int's contract (e.g. non-numeric
            # SNOMED garbage, oversized values). Rare; dropping is the right
            # call since they have no canonical id.
            continue
    return out


class FhirGraphBuilder(ConceptGraphBuilder):
    DEFAULT_BIN_NAME = FHIR_GRAPH_BIN

    """Build concept graph from FHIR bridge + sibling CSVs."""

    def load_bridges(self, src_dir: str) -> dict[int, set[int]]:
        bridges: dict[int, set[int]] = {}

        n_loaded = n_skipped = 0
        for filename, columns, max_codes in _BRIDGE_FILES:
            path = os.path.join(src_dir, filename)
            if not os.path.exists(path):
                continue
            with _csv_field_size_limit(), open(path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    groups: list[list[int]] = []
                    total = 0
                    for col, system in columns:
                        raw = [c for c in row.get(col, "").split("|") if c]
                        ids = _encode_codes(raw, system)
                        groups.append(ids)
                        total += len(raw)
                    if max_codes and total > max_codes:
                        n_skipped += 1
                        continue
                    for i in range(len(groups)):
                        for j in range(i + 1, len(groups)):
                            if not groups[i] or not groups[j]:
                                continue
                            for sf in groups[i]:
                                bridges.setdefault(sf, set()).update(groups[j])
                            for sf in groups[j]:
                                bridges.setdefault(sf, set()).update(groups[i])
                    n_loaded += 1
        for nid, neighbors in bridges.items():
            neighbors.discard(nid)
        log.info("  Bridges: %d loaded, %d skipped, %s nodes",
                 n_loaded, n_skipped, f"{len(bridges):,}")
        return bridges

    def load_siblings(self, src_dir: str) -> list[list[int]]:
        siblings: list[list[int]] = []

        n_sib_code_groups = 0
        for filename, system in _SIBLING_FILES:
            path = os.path.join(src_dir, filename)
            if not os.path.exists(path):
                continue
            n_groups = 0
            with _csv_field_size_limit(), open(path, encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    codes = [c for c in row.get("codes", "").split("|") if c]
                    if len(codes) < 2:
                        continue
                    group = _encode_codes(codes, system)
                    if len(group) >= 2:
                        siblings.append(group)
                    n_groups += 1
            n_sib_code_groups += n_groups
            log.info("  Siblings %s: %d groups", system, n_groups)

        log.info("  Result: %s sibling groups (from %s code groups)",
                 f"{len(siblings):,}", f"{n_sib_code_groups:,}")
        return siblings

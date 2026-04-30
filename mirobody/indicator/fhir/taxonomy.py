"""FHIR taxonomy: map fhir_indicators to user-facing categories (body systems,
demographics, vitals, lifestyle, etc.) for the FHIR API server's directory view.

Pipeline:
  1. Parse SNOMED RF2 Relationship snapshot once:
       - is-a edges (116680003)      → child → parents
       - finding / procedure sites   → concept → anatomical sites
  2. BFS descendants of each label's SNOMED root via is-a.
  3. Parse UMLS MRCONSO to bridge LOINC ↔ SNOMED via CUI.
  4. For each fhir_indicators row, assign one label:
       SNOMED_CT: direct ancestry lookup (+ finding-site fallback)
       LOINC:     via CUI bridge → SNOMED → ancestry / finding site
       other:     unassigned (skipped)
  5. Delegate serialisation to the base TaxonomyBuilder → fhir_taxonomy.bin
"""

from __future__ import annotations

import asyncio
import csv
import glob
import logging
import os
from argparse import Namespace
from collections import defaultdict

from ..taxonomy import Label, TaxonomyBuilder

log = logging.getLogger(__name__)

# Output filename for the FHIR-domain taxonomy binary. Lives under
# ``mirobody/res/`` at runtime; uses the ``fhir_`` content prefix
# (matches ``fhir_embeddings.npy`` / ``fhir_concept_graph.bin`` / etc.).
FHIR_TAXONOMY_BIN = "fhir_taxonomy.bin"

# SNOMED relationship type IDs
_IS_A = 116680003
_FINDING_SITE = 363698007
_PROCEDURE_SITE = 363704007
_DIRECT_SITE = 704327008  # Direct site (attribute)

# Taxonomy labels: (label_id, parent_id, snomed_code, name)
# snomed_code == 0 means a UI-only container (no direct SNOMED root);
# such labels receive fhir_ids only transitively via their children
# (queried with ``tax.indicators_of(label_id, recursive=True)``).
# Labels are English-only; UI localisation is the frontend's job.
LABELS: list[tuple[int, int, int, str]] = [
    # ── Root categories ───────────────────────────────────────────
    (1, 0, 0,         "Demographics"),
    (2, 0, 248326004, "Body measures"),
    (3, 0, 46680005,  "Vital signs"),
    (4, 0, 160476009, "Lifestyle & Social"),
    (5, 0, 0,         "Lab & Clinical"),
    # ── Demographics children ─────────────────────────────────────
    (12, 1, 184099003, "Date of birth"),
    (13, 1, 263495000, "Gender"),
    (14, 1, 103579009, "Race"),
    (16, 1, 365636006, "Blood type"),
    # ── Lifestyle children ────────────────────────────────────────
    (21, 4, 365980008, "Tobacco use"),
    (22, 4, 228273003, "Alcohol"),
    (23, 4, 68130003,  "Physical activity"),
    (24, 4, 258158006, "Sleep"),
    (25, 4, 364393001, "Diet & Nutrition"),
    # ── Lab & Clinical children (11 body systems) ─────────────────
    (31, 5, 113257007, "Cardiovascular"),
    (32, 5, 20139000,  "Respiratory"),
    (33, 5, 86762007,  "Digestive"),
    (34, 5, 25087005,  "Nervous"),
    (35, 5, 113331007, "Endocrine"),
    (36, 5, 21514008,  "Genitourinary"),
    (37, 5, 26107004,  "Musculoskeletal"),
    (38, 5, 48075008,  "Integumentary"),
    (39, 5, 414387006, "Hematological"),
    (40, 5, 116003000, "Immune/Lymphatic"),
    (41, 5, 57645008,  "Special sense organ"),
]


def _label_depths(labels: dict[int, Label]) -> dict[int, int]:
    """Depth of each label in the tree: 0 = root, 1 = child of root, ..."""
    depth: dict[int, int] = {}
    for lid in labels:
        d = 0
        cur = labels[lid].parent_id
        while cur != 0 and cur in labels:
            d += 1
            cur = labels[cur].parent_id
        depth[lid] = d
    return depth


# ── SNOMED RF2 parsing ──────────────────────────────────────────────


def _find_rf2(snomed_dir: str, basename_glob: str) -> str:
    matches = glob.glob(os.path.join(snomed_dir, "Snapshot", "Terminology", basename_glob))
    if not matches:
        matches = glob.glob(
            os.path.join(snomed_dir, "**", basename_glob), recursive=True)
    if not matches:
        raise FileNotFoundError(f"RF2 file not found: {basename_glob} in {snomed_dir}")
    return matches[0]


def _parse_relationships(
    snomed_dir: str,
) -> tuple[dict[int, list[int]], dict[int, list[int]]]:
    """Single pass over Relationship_Snapshot.

    Returns:
        is_a:        child_sctid → [parent_sctids]
        site_of:     concept_sctid → [anatomical site sctids]
                     (union of Finding/Procedure/Direct site attributes)
    """
    path = _find_rf2(snomed_dir, "sct2_Relationship_Snapshot*.txt")
    is_a: dict[int, list[int]] = defaultdict(list)
    site_of: dict[int, list[int]] = defaultdict(list)
    site_types = {_FINDING_SITE, _PROCEDURE_SITE, _DIRECT_SITE}
    n_isa = n_site = 0
    with open(path, "r", encoding="utf-8") as f:
        next(f)  # header
        for line in f:
            parts = line.split("\t")
            if len(parts) < 8 or parts[2] != "1":  # inactive
                continue
            try:
                type_id = int(parts[7])
            except ValueError:
                continue
            if type_id == _IS_A:
                is_a[int(parts[4])].append(int(parts[5]))
                n_isa += 1
            elif type_id in site_types:
                site_of[int(parts[4])].append(int(parts[5]))
                n_site += 1
    log.info("SNOMED relationships: %s is-a edges, %s site-of edges",
             f"{n_isa:,}", f"{n_site:,}")
    return dict(is_a), dict(site_of)


def _descendant_map(
    is_a: dict[int, list[int]],
    root_to_label: dict[int, int],
) -> dict[int, int]:
    """BFS each root down the is-a tree; return sctid → label_id.

    When a concept is reachable from multiple roots, the first-seen wins
    (roots processed in ``root_to_label`` insertion order).
    """
    parent_to_children: dict[int, list[int]] = defaultdict(list)
    for child, parents in is_a.items():
        for p in parents:
            parent_to_children[p].append(child)

    sctid_to_label: dict[int, int] = {}
    for root, label_id in root_to_label.items():
        if root in sctid_to_label:
            continue
        sctid_to_label[root] = label_id
        stack = [root]
        while stack:
            cur = stack.pop()
            for child in parent_to_children.get(cur, ()):
                if child in sctid_to_label:
                    continue
                sctid_to_label[child] = label_id
                stack.append(child)
    log.info("SNOMED body-system coverage: %s concepts", f"{len(sctid_to_label):,}")
    return sctid_to_label


# ── UMLS MRCONSO bridge ─────────────────────────────────────────────


def _parse_loinc_snomed_bridge(umls_dir: str) -> dict[int, list[int]]:
    """Parse MRCONSO.RRF → LOINC code int → [SNOMED SCTIDs sharing CUI]."""
    path = os.path.join(umls_dir, "META", "MRCONSO.RRF")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"MRCONSO.RRF not found: {path}")

    cui_to_loinc: dict[str, set[int]] = defaultdict(set)
    cui_to_snomed: dict[str, set[int]] = defaultdict(set)

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.split("|")
            if len(parts) < 15:
                continue
            cui, sab, code = parts[0], parts[11], parts[13]
            if sab == "LNC":
                try:
                    cui_to_loinc[cui].add(int(code.replace("-", "")))
                except ValueError:
                    continue
            elif sab == "SNOMEDCT_US":
                try:
                    cui_to_snomed[cui].add(int(code))
                except ValueError:
                    continue

    loinc_to_snomed: dict[int, list[int]] = defaultdict(list)
    for cui, loincs in cui_to_loinc.items():
        snomeds = cui_to_snomed.get(cui)
        if not snomeds:
            continue
        for l in loincs:
            loinc_to_snomed[l].extend(snomeds)
    log.info("LOINC↔SNOMED via CUI: %s LOINC codes bridged", f"{len(loinc_to_snomed):,}")
    return dict(loinc_to_snomed)


# ── fhir_indicators lookup ─────────────────────────────────────────


async def _fetch_fhir_indicators(cache_path: str) -> list[tuple[int, str, str]]:
    if os.path.isfile(cache_path):
        out: list[tuple[int, str, str]] = []
        with open(cache_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                out.append((int(row["id"]), row["indicator_standard"], row["code"]))
        log.info("fhir_indicators loaded from cache: %d rows ← %s", len(out), cache_path)
        return out

    from mirobody.utils import execute_query
    rows = await execute_query(
        "SELECT id, indicator_standard, code FROM fhir_indicators WHERE code IS NOT NULL"
    )
    out = [(r["id"], r.get("indicator_standard") or "", r.get("code") or "") for r in rows]
    with open(cache_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "indicator_standard", "code"])
        w.writerows(out)
    log.info("fhir_indicators cached: %d rows → %s", len(out), cache_path)
    return out


def _code_to_int(code: str) -> int | None:
    try:
        return int(code.replace("-", ""))
    except ValueError:
        return None


def _resolve_snomed(
    sctid: int,
    sctid_to_label: dict[int, int],
    site_of: dict[int, list[int]],
) -> int | None:
    """SCTID → label_id. Try ancestry first, fall back to finding site."""
    lid = sctid_to_label.get(sctid)
    if lid is not None:
        return lid
    for site in site_of.get(sctid, ()):
        lid = sctid_to_label.get(site)
        if lid is not None:
            return lid
    return None


# ── Builder subclass ────────────────────────────────────────────────


class FhirTaxonomyBuilder(TaxonomyBuilder):
    def __init__(
        self,
        snomed_dir: str,
        umls_dir: str,
        indicators: list[tuple[int, str, str]],
    ) -> None:
        super().__init__()
        self._snomed_dir = snomed_dir
        self._umls_dir = umls_dir
        self._indicators = indicators

    def load_labels(self, src_dir: str) -> dict[int, Label]:
        labels = {
            lid: Label(lid, pid, sctid, name)
            for lid, pid, sctid, name in LABELS
        }
        path = os.path.join(src_dir, "_taxonomy_labels.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["label_id", "parent_id", "snomed_code", "name"])
            for lab in labels.values():
                w.writerow([lab.id, lab.parent_id, lab.external_code, lab.name])
        log.info("Taxonomy labels: %d → %s", len(labels), path)
        return labels

    def load_assignments(self, src_dir: str) -> dict[int, int]:
        cache = os.path.join(src_dir, "_taxonomy_assignments.csv")
        if os.path.isfile(cache):
            return _load_assignments_csv(cache)

        is_a, site_of = _parse_relationships(self._snomed_dir)

        # Process labels deepest-first so specific leaves (e.g. Tobacco)
        # claim their concepts before their parent (e.g. Lifestyle).
        # Labels without a SNOMED root (external_code == 0) are UI-only
        # containers and get fhir_ids transitively via children at query time.
        depths = _label_depths(self.labels)
        priority = sorted(
            (l for l in self.labels.values() if l.external_code),
            key=lambda l: (-depths[l.id], l.id),
        )
        root_to_label = {l.external_code: l.id for l in priority}
        sctid_to_label = _descendant_map(is_a, root_to_label)
        loinc_to_snomed = _parse_loinc_snomed_bridge(self._umls_dir)

        assignments: dict[int, int] = {}
        per_label: dict[int, int] = defaultdict(int)
        n_sn = n_ln = n_miss = 0
        for fhir_id, std, code in self._indicators:
            label_id: int | None = None
            if std == "SNOMED_CT":
                sctid = _code_to_int(code)
                if sctid is not None:
                    label_id = _resolve_snomed(sctid, sctid_to_label, site_of)
                    if label_id is not None:
                        n_sn += 1
            elif std == "LOINC":
                lcode = _code_to_int(code)
                if lcode is not None:
                    for sctid in loinc_to_snomed.get(lcode, ()):
                        label_id = _resolve_snomed(sctid, sctid_to_label, site_of)
                        if label_id is not None:
                            n_ln += 1
                            break

            if label_id is not None:
                assignments[fhir_id] = label_id
                per_label[label_id] += 1
            else:
                n_miss += 1

        log.info("Assignments: SNOMED=%d, LOINC=%d, unresolved=%d (of %d)",
                 n_sn, n_ln, n_miss, len(self._indicators))
        log.info("Per-label counts:")
        for lid in sorted(per_label):
            log.info("  %2d %-24s %d", lid, self.labels[lid].name, per_label[lid])

        with open(cache, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["fhir_id", "label_id"])
            for fhir_id, lid in sorted(assignments.items()):
                w.writerow([fhir_id, lid])
        log.info("Assignments written: %s", cache)
        return assignments


def _load_assignments_csv(path: str) -> dict[int, int]:
    out: dict[int, int] = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[int(row["fhir_id"])] = int(row["label_id"])
    log.info("Assignments loaded from cache: %d rows ← %s", len(out), path)
    return out


# ── CLI entry ───────────────────────────────────────────────────────


async def cmd_taxonomy(args: Namespace) -> None:
    os.makedirs(args.output, exist_ok=True)

    if not args.snomed_dir or not os.path.isdir(args.snomed_dir):
        raise SystemExit(f"--snomed-dir required (got: {args.snomed_dir!r})")
    if not args.umls_dir or not os.path.isdir(args.umls_dir):
        raise SystemExit(f"--umls-dir required (got: {args.umls_dir!r})")

    indicators = await _fetch_fhir_indicators(
        os.path.join(args.output, "_fhir_indicators.csv")
    )

    builder = FhirTaxonomyBuilder(
        snomed_dir=args.snomed_dir,
        umls_dir=args.umls_dir,
        indicators=indicators,
    )
    dest = args.bin_output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "res", FHIR_TAXONOMY_BIN,
    )
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    await asyncio.to_thread(builder.build, args.output, dest)

"""Build cross-vocabulary bridge files.

Bridge paths (from MRCONSO CUI sharing + MRREL relations):
  - ICD bridge:          SNOMED → ICD → LOINC     (_bridges_icd.csv)
  - MRREL direct:        SNOMED ↔ LOINC           (_bridges_mrrel.csv)
  - RxNorm bridge:       SNOMED ↔ RxNorm          (_bridges_rxnorm.csv)
  - LOINC-RxNorm bridge: LOINC ↔ RxNorm           (_bridges_loinc_rxnorm.csv)
  - Jaccard similarity:  SNOMED ↔ LOINC           (_bridges_jaccard.csv)
"""

import csv
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict

import polars as pl

from .common import ICD_BRIDGE_SABS, TARGET_SYSTEMS, csv_field_size_limit, load_loinc_skip_codes, read_rrf

log = logging.getLogger(__name__)

# Broad concept name patterns to exclude from bridges.
# These are high-level SNOMED concepts (dosage forms, measurement units, etc.)
# that produce enormous bridge rows linking thousands of unrelated codes.
_BROAD_DOSAGE_FORM = [
    "dosage form", "dose form", "drug preparation", "drug solution",
    "drug suspension", "liquid dose", "oral tablet", "oral capsule",
    "extended release oral", "chewable tablet",
    "solution for infusion", "solution for injection",
    "conventional release solution", "conventional release chewable",
    "parenteral dosage", "parenteral form",
    "solid dose form", "semi-solid dose form", "gaseous dose form",
    "drops dose form", "rectal dosage", "nasal dosage", "ocular dosage",
    "oromucosal", "gingival dosage",
]
_BROAD_DOSAGE_FORM_STARTS = [
    "pharmaceutical", "medicinal product", "capsule", "cream",
    "ointment", "suppository", "lozenge", "spray",
]
_BROAD_MEASUREMENT = [
    "unit of ", "si unit", "si-derived unit",
    "concentration (property", "quantity concentration",
    "measurement scale", "additional values",
    "arabic numeral", "general adjectival modifier",
    "industrial site",
]


def _is_broad_concept(name: str) -> bool:
    """Check if a SNOMED concept name matches known broad patterns."""
    n = name.lower()
    if any(kw in n for kw in _BROAD_DOSAGE_FORM):
        return True
    if any(n.startswith(s) for s in _BROAD_DOSAGE_FORM_STARTS):
        return True
    if any(kw in n for kw in _BROAD_MEASUREMENT):
        return True
    return False


# ─── MRCONSO + MRREL bridge ─────────────────────────────────────────

def build_bridge(
    mrconso_path: str,
    mrrel_path: str,
    out_dir: str,
    loinc_dir: str = "",
) -> dict[str, str]:
    """Build SNOMED ↔ LOINC bridges via two paths:
    1. ICD bridge: SNOMED → ICD → LOINC  (outputs _bridges_icd.csv)
    2. MRREL direct: SNOMED ↔ LOINC      (outputs _bridges_mrrel.csv)

    Single MRCONSO + MRREL scan for both.
    CUIs with fanout above the given percentile are pruned as too broad.
    """
    skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]
    loinc_sab = TARGET_SYSTEMS["LOINC"]["sab"]  # "LNC"

    # Load LOINC skip codes (surveys, docs, admin — same as siblings)
    loinc_skip: set[str] = set()
    loinc_core = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv") if loinc_dir else ""
    if loinc_core and os.path.isfile(loinc_core):
        loinc_skip = load_loinc_skip_codes(loinc_core)
        log.info(f"Bridge: excluding {len(loinc_skip):,} non-lab LOINC codes")

    # Step 1: Scan MRCONSO via polars — collect CUI → codes per vocabulary
    log.info("Bridge: scanning MRCONSO")
    relevant_sabs = {"SNOMEDCT_US", loinc_sab, "RXNORM"} | set(ICD_BRIDGE_SABS)
    conso_df = read_rrf(mrconso_path, ["CUI", "SAB", "TTY", "CODE", "STR"])
    conso_df = conso_df.filter(pl.col("SAB").is_in(list(relevant_sabs)))

    snomed_by_cui: dict[str, set[str]] = defaultdict(set)
    loinc_by_cui: dict[str, set[str]] = defaultdict(set)
    icd_by_cui: dict[str, set[str]] = defaultdict(set)
    rxnorm_by_cui: dict[str, set[str]] = defaultdict(set)
    snomed_code_name: dict[str, str] = {}

    for cui, sab, tty, code, name in conso_df.iter_rows():
        if sab == "SNOMEDCT_US":
            snomed_by_cui[cui].add(code)
            if tty == "PT" or (code not in snomed_code_name and tty in ("FN", "SY")):
                snomed_code_name[code] = name
        elif sab == loinc_sab and not code.startswith(skip_prefixes) and code not in loinc_skip:
            loinc_by_cui[cui].add(code)
        elif sab in ICD_BRIDGE_SABS:
            icd_by_cui[cui].add(code)
        elif sab == "RXNORM":
            rxnorm_by_cui[cui].add(code)

    snomed_cuis = set(snomed_by_cui)
    loinc_cuis = set(loinc_by_cui)
    icd_cuis = set(icd_by_cui)
    rxnorm_cuis = set(rxnorm_by_cui)

    log.info(f"  SNOMED CUIs: {len(snomed_cuis):,}, LOINC CUIs: {len(loinc_cuis):,}, "
             f"ICD CUIs: {len(icd_cuis):,}, RxNorm CUIs: {len(rxnorm_cuis):,}")

    # CUI sharing
    snomed_to_icd: dict[str, set[str]] = defaultdict(set)
    icd_to_loinc: dict[str, set[str]] = defaultdict(set)
    snomed_to_loinc: dict[str, set[str]] = defaultdict(set)
    snomed_to_rxnorm: dict[str, set[str]] = defaultdict(set)
    loinc_to_rxnorm: dict[str, set[str]] = defaultdict(set)

    for cui in snomed_cuis & icd_cuis:
        snomed_to_icd[cui].add(cui)
    for cui in icd_cuis & loinc_cuis:
        icd_to_loinc[cui].add(cui)
    for cui in snomed_cuis & loinc_cuis:
        snomed_to_loinc[cui].add(cui)
    for cui in snomed_cuis & rxnorm_cuis:
        snomed_to_rxnorm[cui].add(cui)
    for cui in loinc_cuis & rxnorm_cuis:
        loinc_to_rxnorm[cui].add(cui)

    # Step 2: Scan MRREL — collect all cross-vocabulary relations in one pass
    _BRIDGE_SABS = {"SNOMEDCT_US", "LNC", "ICD10CM", "ICD10", "MTH", "MEDCIN", "RXNORM"}
    log.info(f"Bridge: scanning MRREL ({len(snomed_cuis) + len(loinc_cuis) + len(icd_cuis) + len(rxnorm_cuis):,} relevant CUIs)")

    relevant_cuis = snomed_cuis | icd_cuis | loinc_cuis | rxnorm_cuis
    with open(mrrel_path, "r", encoding="utf-8") as f:
        for line in f:
            # Extract CUI1 without splitting — ~80% of lines are skipped here,
            # so avoiding the full split() on irrelevant lines is a major speedup.
            cui1 = line[:line.index("|")]
            if cui1 not in relevant_cuis:
                continue
            fields = line.split("|")
            cui2 = fields[4]
            if cui2 not in relevant_cuis:
                continue
            sab = fields[10]
            if sab not in _BRIDGE_SABS:
                continue
            # SNOMED ↔ ICD
            if cui1 in snomed_cuis and cui2 in icd_cuis:
                snomed_to_icd[cui1].add(cui2)
            elif cui1 in icd_cuis and cui2 in snomed_cuis:
                snomed_to_icd[cui2].add(cui1)
            # ICD ↔ LOINC
            if cui1 in icd_cuis and cui2 in loinc_cuis:
                icd_to_loinc[cui1].add(cui2)
            elif cui1 in loinc_cuis and cui2 in icd_cuis:
                icd_to_loinc[cui2].add(cui1)
            # SNOMED ↔ LOINC (direct)
            if cui1 in snomed_cuis and cui2 in loinc_cuis:
                snomed_to_loinc[cui1].add(cui2)
            elif cui1 in loinc_cuis and cui2 in snomed_cuis:
                snomed_to_loinc[cui2].add(cui1)
            # SNOMED ↔ RxNorm
            if cui1 in snomed_cuis and cui2 in rxnorm_cuis:
                snomed_to_rxnorm[cui1].add(cui2)
            elif cui1 in rxnorm_cuis and cui2 in snomed_cuis:
                snomed_to_rxnorm[cui2].add(cui1)
            # LOINC ↔ RxNorm
            if cui1 in loinc_cuis and cui2 in rxnorm_cuis:
                loinc_to_rxnorm[cui1].add(cui2)
            elif cui1 in rxnorm_cuis and cui2 in loinc_cuis:
                loinc_to_rxnorm[cui2].add(cui1)

    log.info(f"  SNOMED→ICD: {len(snomed_to_icd):,}, ICD→LOINC: {len(icd_to_loinc):,}, "
             f"SNOMED→LOINC direct: {len(snomed_to_loinc):,}, "
             f"SNOMED→RxNorm: {len(snomed_to_rxnorm):,}, "
             f"LOINC→RxNorm: {len(loinc_to_rxnorm):,}")

    # Step 2b: Remove CUIs whose SNOMED codes are all broad concepts
    # (dosage forms, measurement units, etc.)
    def _remove_broad(mapping: dict[str, set[str]], label: str) -> None:
        broad_cuis = set()
        for cui in mapping:
            codes = snomed_by_cui.get(cui, set())
            if codes and all(_is_broad_concept(snomed_code_name.get(c, "")) for c in codes):
                broad_cuis.add(cui)
        for cui in broad_cuis:
            del mapping[cui]
        if broad_cuis:
            log.info(f"  Removed {len(broad_cuis)} broad-concept CUIs from {label}")

    _remove_broad(snomed_to_icd, "SNOMED→ICD")
    _remove_broad(snomed_to_loinc, "SNOMED→LOINC")
    _remove_broad(snomed_to_rxnorm, "SNOMED→RxNorm")

    # Step 3a: ICD bridge — SNOMED → ICD → LOINC
    icd_merged: dict[tuple[str, str], set[str]] = {}
    for s_cui, icd_cui_set in snomed_to_icd.items():
        s_codes = snomed_by_cui.get(s_cui, set())
        if not s_codes:
            continue
        for icd_cui in icd_cui_set:
            icd_codes = icd_by_cui.get(icd_cui, set())
            l_cuis = icd_to_loinc.get(icd_cui, set())
            if not l_cuis:
                continue
            l_codes: set[str] = set()
            for l_cui in l_cuis:
                l_codes.update(loinc_by_cui.get(l_cui, set()))
            if l_codes:
                sk = "|".join(sorted(s_codes))
                lk = "|".join(sorted(l_codes))
                icd_merged.setdefault((sk, lk), set()).update(icd_codes)

    out_icd = os.path.join(out_dir, "_bridges_icd.csv")
    with open(out_icd, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["snomed_codes", "loinc_codes", "icd_codes"])
        for (sk, lk), icd_codes in sorted(icd_merged.items()):
            writer.writerow([sk, lk, "|".join(sorted(icd_codes))])
    log.info(f"  ICD bridge: {len(icd_merged):,} rows → {out_icd}")

    # Step 3b: MRREL bridge — SNOMED ↔ LOINC direct
    mrrel_merged: dict[str, set[str]] = {}
    for s_cui, l_cui_set in snomed_to_loinc.items():
        s_codes = snomed_by_cui.get(s_cui, set())
        if not s_codes:
            continue
        l_codes: set[str] = set()
        for l_cui in l_cui_set:
            l_codes.update(loinc_by_cui.get(l_cui, set()))
        if l_codes:
            sk = "|".join(sorted(s_codes))
            mrrel_merged.setdefault(sk, set()).update(l_codes)

    out_mrrel = os.path.join(out_dir, "_bridges_mrrel.csv")
    with open(out_mrrel, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["snomed_codes", "loinc_codes"])
        for sk, l_codes in sorted(mrrel_merged.items()):
            writer.writerow([sk, "|".join(sorted(l_codes))])
    log.info(f"  MRREL bridge: {len(mrrel_merged):,} rows → {out_mrrel}")

    # Step 3c: RxNorm bridge — SNOMED ↔ RxNorm (CUI sharing + MRREL)
    rxnorm_merged: dict[str, set[str]] = {}
    for s_cui, r_cui_set in snomed_to_rxnorm.items():
        s_codes = snomed_by_cui.get(s_cui, set())
        if not s_codes:
            continue
        r_codes: set[str] = set()
        for r_cui in r_cui_set:
            r_codes.update(rxnorm_by_cui.get(r_cui, set()))
        if r_codes:
            sk = "|".join(sorted(s_codes))
            rxnorm_merged.setdefault(sk, set()).update(r_codes)

    out_rxnorm = os.path.join(out_dir, "_bridges_rxnorm.csv")
    with open(out_rxnorm, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["snomed_codes", "rxnorm_codes"])
        for sk, r_codes in sorted(rxnorm_merged.items()):
            writer.writerow([sk, "|".join(sorted(r_codes))])
    log.info(f"  RxNorm bridge: {len(rxnorm_merged):,} rows → {out_rxnorm}")

    # Step 3d: LOINC ↔ RxNorm bridge (e.g. drug tests ↔ drug ingredients)
    # Keep per-row: loinc_codes, rxnorm_codes, loinc_cui, rxnorm_cuis
    loinc_rxnorm_rows: list[tuple[str, str, str, str]] = []
    for l_cui, r_cui_set in sorted(loinc_to_rxnorm.items()):
        l_codes = loinc_by_cui.get(l_cui, set())
        if not l_codes:
            continue
        r_codes: set[str] = set()
        for r_cui in r_cui_set:
            r_codes.update(rxnorm_by_cui.get(r_cui, set()))
        if r_codes:
            loinc_rxnorm_rows.append((
                "|".join(sorted(l_codes)),
                "|".join(sorted(r_codes)),
                l_cui,
                "|".join(sorted(r_cui_set)),
            ))

    out_loinc_rxnorm = os.path.join(out_dir, "_bridges_loinc_rxnorm.csv")
    with open(out_loinc_rxnorm, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["loinc_codes", "rxnorm_codes", "loinc_cui", "rxnorm_cuis"])
        for row in loinc_rxnorm_rows:
            writer.writerow(row)
    log.info(f"  LOINC→RxNorm bridge: {len(loinc_rxnorm_rows):,} rows → {out_loinc_rxnorm}")

    return snomed_code_name


# ─── Jaccard similarity bridge ──────────────────────────────────────

def build_jaccard_bridge(
    out_dir: str,
    loinc_dir: str = "",
    min_jaccard: float = 0.5,
) -> None:
    """Build SNOMED ↔ LOINC bridge via Jaccard similarity on tokenized names.

    Uses _siblings_snomed.csv names and LoincTableCore.csv LONG_COMMON_NAME
    (one entry per LOINC code, original abbreviations preserved).
    Outputs _bridges_jaccard.csv with columns: snomed_codes, loinc_codes, intersection.
    """
    _stop_words = {
        # Standard English stop words
        "a", "an", "and", "at", "by", "for", "from", "in", "is", "it",
        "of", "on", "or", "the", "to", "with", "not", "no", "as", "be",
        # SNOMED structural terms (high-frequency, no clinical specificity)
        "structure", "product", "containing", "only", "form", "dose",
        "finding", "disorder", "procedure", "measurement", "other",
        "entire", "nos", "due", "o", "e", "x",
        # Drug formulation / route terms (cause cross-domain false matches)
        "oral", "injection", "solution", "mg", "ml", "tablet", "capsule",
        "topical", "rectal", "ophthalmic", "nasal", "intravenous",
        # LOINC axis terms (specimen, property — not clinically specific)
        "serum", "plasma", "urine", "volume", "mass", "presence",
        "units", "specimen", "method", "fluid", "moles", "titer",
        "detection", "probe", "deprecated",
        # Generic qualifiers
        "left", "right", "upper", "lower", "primary", "congenital",
        "open", "malignant", "family",
    }

    def _tokenize(name: str) -> set[str]:
        tokens = set(re.findall(r'[a-z0-9]+', name.lower()))
        return tokens - _stop_words

    # Load SNOMED siblings: name → (tokens, codes)
    log.info("Jaccard bridge: loading names")
    snomed_entries: list[tuple[str, set[str], set[str]]] = []
    snomed_path = os.path.join(out_dir, "_siblings_snomed.csv")
    if os.path.isfile(snomed_path):
        with open(snomed_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = row.get("name", "").strip()
                if not name:
                    continue
                tokens = _tokenize(name)
                if len(tokens) >= 2:
                    codes = set(row["codes"].split("|"))
                    snomed_entries.append((name, tokens, codes))

    # Load LOINC codes directly from LoincTableCore.csv — one entry per code
    skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]
    loinc_entries: list[tuple[str, set[str], str]] = []  # (name, tokens, code)
    loinc_core = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv") if loinc_dir else ""
    if loinc_core and os.path.isfile(loinc_core):
        loinc_skip = load_loinc_skip_codes(loinc_core)
        with open(loinc_core, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code = row.get("LOINC_NUM", "")
                if code.startswith(skip_prefixes) or code in loinc_skip:
                    continue
                lcn = row.get("LONG_COMMON_NAME", "").strip()
                if not lcn:
                    continue
                tokens = _tokenize(lcn)
                if len(tokens) >= 2:
                    loinc_entries.append((lcn, tokens, code))

    log.info(f"  SNOMED entries: {len(snomed_entries):,}, LOINC entries: {len(loinc_entries):,}")

    # Build inverted index on LOINC tokens for fast lookup
    token_to_loinc_idx: dict[str, list[int]] = defaultdict(list)
    for i, (_, tokens, _) in enumerate(loinc_entries):
        for t in tokens:
            token_to_loinc_idx[t].append(i)

    # For each SNOMED entry, find LOINC candidates via shared tokens, compute Jaccard
    merged: dict[tuple[str, str], set[str]] = {}
    for _, s_tokens, s_codes in snomed_entries:
        candidate_counts: dict[int, int] = defaultdict(int)
        for t in s_tokens:
            for idx in token_to_loinc_idx.get(t, []):
                candidate_counts[idx] += 1

        matched_loinc: set[str] = set()
        best_intersection: set[str] = set()
        for l_idx in candidate_counts:
            l_name, l_tokens, l_code = loinc_entries[l_idx]
            intersection = s_tokens & l_tokens
            if not intersection:
                continue
            union = s_tokens | l_tokens
            jaccard = len(intersection) / len(union) if union else 0
            cover_s = len(intersection) / len(s_tokens)
            cover_l = len(intersection) / len(l_tokens)
            if jaccard >= min_jaccard or (cover_s >= 0.5 and cover_l >= 0.5):
                matched_loinc.add(l_code)
                if len(intersection) > len(best_intersection):
                    best_intersection = intersection

        if matched_loinc:
            lk = "|".join(sorted(matched_loinc))
            ik = " ".join(sorted(best_intersection))
            merged.setdefault((lk, ik), set()).update(s_codes)

    out_path = os.path.join(out_dir, "_bridges_jaccard.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["snomed_codes", "loinc_codes", "intersection"])
        for (lk, ik), s_codes in sorted(merged.items()):
            writer.writerow(["|".join(sorted(s_codes)), lk, ik])

    log.info(f"  Jaccard bridge (>={min_jaccard}): {len(merged):,} matches → {out_path}")


# ─── CLI subcommand ──────────────────────────────────────────────────

def cmd_bridge(args: Namespace) -> None:
    """Subcommand: bridge — build bridge files + merged siblings.csv."""
    out_dir = args.output
    os.makedirs(out_dir, exist_ok=True)

    umls_meta = os.path.join(args.umls_dir, "META")
    mrconso_path = os.path.join(umls_meta, "MRCONSO.RRF")
    mrrel_path = os.path.join(umls_meta, "MRREL.RRF")
    if not os.path.isfile(mrconso_path) or not os.path.isfile(mrrel_path):
        log.error(f"MRCONSO.RRF or MRREL.RRF not found in {umls_meta}")
        return
    loinc_dir = getattr(args, "loinc_dir", "")
    snomed_code_name = build_bridge(mrconso_path, mrrel_path, out_dir, loinc_dir=loinc_dir)

    # Jaccard similarity bridge
    if loinc_dir:
        build_jaccard_bridge(out_dir, loinc_dir=loinc_dir)

    # Build bridged SNOMED set from all bridge files
    with csv_field_size_limit():
        bridged_snomed: set[str] = set()
        for bridge_file in ("_bridges_icd.csv", "_bridges_mrrel.csv", "_bridges_jaccard.csv", "_bridges_rxnorm.csv"):
            bridge_path = os.path.join(out_dir, bridge_file)
            if os.path.isfile(bridge_path):
                with open(bridge_path, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        bridged_snomed.update(c for c in row["snomed_codes"].split("|") if c)
        bridge_set_path = os.path.join(out_dir, "_bridged_snomed.csv")
        with open(bridge_set_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["snomed_code", "name"])
            for code in sorted(bridged_snomed):
                writer.writerow([code, snomed_code_name.get(code, "")])
        log.info(f"  Bridged SNOMED codes: {len(bridged_snomed):,} → {bridge_set_path}")

        # Collect all LOINC codes that appear in any bridge file
        bridged_loinc: set[str] = set()
        for bridge_file, code_col in [
            ("_bridges_icd.csv", "loinc_codes"),
            ("_bridges_mrrel.csv", "loinc_codes"),
            ("_bridges_jaccard.csv", "loinc_codes"),
            ("_bridges_loinc_rxnorm.csv", "loinc_codes"),
        ]:
            bridge_path = os.path.join(out_dir, bridge_file)
            if os.path.isfile(bridge_path):
                with open(bridge_path, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        bridged_loinc.update(c for c in row.get(code_col, "").split("|") if c)

    # Find LOINC codes from siblings that have no bridge
    loinc_sib_path = os.path.join(out_dir, "_siblings_loinc.csv")
    unbridged_loinc: dict[str, tuple[str, str]] = {}
    if os.path.isfile(loinc_sib_path):
        with open(loinc_sib_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                codes = set(c for c in row.get("codes", "").split("|") if c)
                name = row.get("name", "")
                if codes and not (codes & bridged_loinc) and name not in unbridged_loinc:
                    unbridged_loinc[name] = (row.get("codes", ""), row.get("note", ""))

    unbridged_loinc_path = os.path.join(out_dir, "_unbridged_loinc.csv")
    with open(unbridged_loinc_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["name", "codes", "note"])
        for name in sorted(unbridged_loinc):
            codes, note = unbridged_loinc[name]
            writer.writerow([name, codes, note])
    log.info(f"  Unbridged LOINC groups: {len(unbridged_loinc):,} → {unbridged_loinc_path}")

    # Find RxNorm codes from siblings that have no bridge
    bridged_rxnorm: set[str] = set()
    for bridge_file in [
        "_bridges_rxnorm.csv",
        "_bridges_loinc_rxnorm.csv",
    ]:
        bridge_path = os.path.join(out_dir, bridge_file)
        if os.path.isfile(bridge_path):
            with open(bridge_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    bridged_rxnorm.update(c for c in row.get("rxnorm_codes", "").split("|") if c)

    rxnorm_sib_path = os.path.join(out_dir, "_siblings_rxnorm.csv")
    unbridged_rxnorm: dict[str, tuple[str, str]] = {}
    if os.path.isfile(rxnorm_sib_path):
        with open(rxnorm_sib_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                codes = set(c for c in row.get("codes", "").split("|") if c)
                name = row.get("name", "")
                if codes and not (codes & bridged_rxnorm) and name not in unbridged_rxnorm:
                    unbridged_rxnorm[name] = (row.get("codes", ""), row.get("note", ""))

    unbridged_rxnorm_path = os.path.join(out_dir, "_unbridged_rxnorm.csv")
    with open(unbridged_rxnorm_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["name", "codes", "note"])
        for name in sorted(unbridged_rxnorm):
            codes, note = unbridged_rxnorm[name]
            writer.writerow([name, codes, note])
    log.info(f"  Unbridged RxNorm groups: {len(unbridged_rxnorm):,} → {unbridged_rxnorm_path}")

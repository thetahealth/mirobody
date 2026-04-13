"""Merge siblings + bridge files into concepts.csv."""

import csv
import logging
import os
import time
from argparse import Namespace
from collections import defaultdict
from typing import NamedTuple

from .common import TARGET_SYSTEMS, csv_field_size_limit
from .siblings import _load_loinc_skip_codes, expand_abbrevs

log = logging.getLogger(__name__)


class ConceptRow(NamedTuple):
    """A single concept row with sibling (native) and bridged code sets."""
    name: str
    snomed_codes: set[str]
    loinc_codes: set[str]
    rxnorm_codes: set[str]
    loinc_bridged: set[str]
    rxnorm_bridged: set[str]



def _load_siblings(path: str) -> dict[str, set[str]]:
    """Load a siblings CSV (name, codes) into {name: {codes}} dict."""
    result: dict[str, set[str]] = defaultdict(set)
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                name = row.get("name", "").strip()
                if name:
                    result[name].update(c for c in row.get("codes", "").split("|") if c)
    return result


def _load_bridge(path: str, src_col: str, tgt_col: str) -> dict[str, set[str]]:
    """Load a bridge CSV into {src_code: {tgt_codes}} dict."""
    result: dict[str, set[str]] = defaultdict(set)
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                s_codes = [c for c in row.get(src_col, "").split("|") if c]
                t_codes = [c for c in row.get(tgt_col, "").split("|") if c]
                for sc in s_codes:
                    result[sc].update(t_codes)
    return result


def _inject_bridge_codes(
    out_dir: str,
    name_snomed: dict[str, set[str]],
    name_loinc: dict[str, set[str]],
    name_rxnorm: dict[str, set[str]],
) -> tuple[int, int]:
    """Inject bridged LOINC/RxNorm codes into name rows via SNOMED and LOINC bridges.

    Returns (n_loinc_injected, n_rxnorm_injected).
    """
    snomed_to_loinc: dict[str, set[str]] = defaultdict(set)
    snomed_to_rxnorm: dict[str, set[str]] = defaultdict(set)

    for bridge_file, target_map, col_name in [
        ("_bridges_icd.csv", snomed_to_loinc, "loinc_codes"),
        ("_bridges_mrrel.csv", snomed_to_loinc, "loinc_codes"),
        # ("_bridges_jaccard.csv", snomed_to_loinc, "loinc_codes"),
        ("_bridges_rxnorm.csv", snomed_to_rxnorm, "rxnorm_codes"),
    ]:
        loaded = _load_bridge(os.path.join(out_dir, bridge_file), "snomed_codes", col_name)
        for k, v in loaded.items():
            target_map[k].update(v)

    # LOINC → RxNorm bridge (drug tests ↔ drug ingredients)
    loinc_to_rxnorm = _load_bridge(
        os.path.join(out_dir, "_bridges_loinc_rxnorm.csv"), "loinc_codes", "rxnorm_codes",
    )

    n_loinc_injected = 0
    n_rxnorm_injected = 0

    # SNOMED → LOINC/RxNorm
    for name, s_codes in name_snomed.items():
        new_l: set[str] = set()
        new_r: set[str] = set()
        for sc in s_codes:
            new_l.update(snomed_to_loinc.get(sc, ()))
            new_r.update(snomed_to_rxnorm.get(sc, ()))
        if new_l:
            new_l -= name_loinc[name]
            name_loinc[name].update(new_l)
            n_loinc_injected += len(new_l)
        if new_r:
            new_r -= name_rxnorm[name]
            name_rxnorm[name].update(new_r)
            n_rxnorm_injected += len(new_r)

    # LOINC → RxNorm
    for name, l_codes in name_loinc.items():
        new_r = set()
        for lc in l_codes:
            new_r.update(loinc_to_rxnorm.get(lc, ()))
        if new_r:
            new_r -= name_rxnorm[name]
            name_rxnorm[name].update(new_r)
            n_rxnorm_injected += len(new_r)

    return n_loinc_injected, n_rxnorm_injected


def _prefix_subset_dedup(
    candidates: list[ConceptRow],
) -> tuple[list[ConceptRow], int]:
    """Remove rows whose name is a prefix of another row with superset codes.

    Uses a trie so prefix lookup is O(len(name)), not O(bucket_size).
    Returns (kept_rows, n_deduped).
    """
    candidates.sort(key=lambda r: len(r.name), reverse=True)  # long names first

    # Exact match dict: segment → list of code tuples (fast O(1) lookup)
    exact: dict[str, list[tuple[set[str], set[str], set[str]]]] = defaultdict(list)
    kept: list[ConceptRow] = []
    n_deduped = 0

    for row in candidates:
        all_sc = row.snomed_codes
        all_lc = row.loinc_codes | row.loinc_bridged if row.loinc_bridged else row.loinc_codes
        all_rc = row.rxnorm_codes | row.rxnorm_bridged if row.rxnorm_bridged else row.rxnorm_codes
        len_sc, len_lc, len_rc = len(all_sc), len(all_lc), len(all_rc)

        deduped = False
        for seg in row.name.split(";"):
            seg = seg.strip()
            if not seg:
                continue
            for k_sc, k_lc, k_rc in exact.get(seg, ()):
                if len_sc <= len(k_sc) and len_lc <= len(k_lc) and len_rc <= len(k_rc) \
                        and all_sc <= k_sc and all_lc <= k_lc and all_rc <= k_rc:
                    deduped = True
                    break
            if deduped:
                break

        if deduped:
            n_deduped += 1
        else:
            kept.append(row)
            codes = (all_sc, all_lc, all_rc)
            for seg in row.name.split(";"):
                seg = seg.strip()
                if seg:
                    exact[seg].append(codes)

    return kept, n_deduped


def merge_siblings(out_dir: str, loinc_dir: str = "") -> None:
    """Merge _siblings_{snomed,loinc,rxnorm}.csv into concepts.csv.

    Groups by name, collecting SNOMED, LOINC and RxNorm codes separately.
    Sibling-native codes and bridge-injected codes are kept in separate columns
    (e.g. loinc_codes vs loinc_bridged) to preserve provenance.
    Filters out SNOMED-only rows whose codes have no LOINC/RxNorm bridge.
    Injects orphan LOINC codes not covered by any sibling group.
    Injects bridge-linked LOINC/RxNorm codes into SNOMED concept rows.
    Deduplicates rows where one name is a prefix of another with subset codes.
    """
    t0 = time.monotonic()
    with csv_field_size_limit():
        # Load bridged SNOMED codes
        bridged_snomed: set[str] = set()
        bridge_set_path = os.path.join(out_dir, "_bridged_snomed.csv")
        if os.path.isfile(bridge_set_path):
            with open(bridge_set_path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    code = row.get("snomed_code", "").strip()
                    if code:
                        bridged_snomed.add(code)
        log.info(f"  [{time.monotonic()-t0:.2f}s] Bridged SNOMED codes: {len(bridged_snomed):,}")

        # Load siblings
        name_snomed = _load_siblings(os.path.join(out_dir, "_siblings_snomed.csv"))
        name_loinc = _load_siblings(os.path.join(out_dir, "_siblings_loinc.csv"))
        name_rxnorm = _load_siblings(os.path.join(out_dir, "_siblings_rxnorm.csv"))
        log.info(f"  [{time.monotonic()-t0:.2f}s] Loaded siblings: "
                 f"{len(name_snomed):,} SNOMED, {len(name_loinc):,} LOINC, {len(name_rxnorm):,} RxNorm")

        covered_loinc: set[str] = set()
        for codes in name_loinc.values():
            covered_loinc.update(codes)

        # Inject orphan LOINC codes from LoincTableCore.csv
        skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]
        n_orphan = 0
        loinc_core = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv") if loinc_dir else ""
        if loinc_core and os.path.isfile(loinc_core):
            loinc_skip = _load_loinc_skip_codes(loinc_core)
            with open(loinc_core, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    code = row.get("LOINC_NUM", "")
                    if code.startswith(skip_prefixes) or code in covered_loinc or code in loinc_skip:
                        continue
                    lcn = row.get("LONG_COMMON_NAME", "").strip()
                    if lcn:
                        name_loinc[expand_abbrevs(lcn)].add(code)
                        n_orphan += 1
            log.info(f"  [{time.monotonic()-t0:.2f}s] Orphan LOINC codes injected: {n_orphan:,}")

        # Snapshot sibling-native codes before bridge injection
        native_loinc = {name: set(codes) for name, codes in name_loinc.items()}
        native_rxnorm = {name: set(codes) for name, codes in name_rxnorm.items()}
        log.info(f"  [{time.monotonic()-t0:.2f}s] Native snapshot: "
                 f"{len(native_loinc):,} LOINC, {len(native_rxnorm):,} RxNorm name groups")

        n_loinc_injected, n_rxnorm_injected = _inject_bridge_codes(
            out_dir, name_snomed, name_loinc, name_rxnorm,
        )
        log.info(f"  [{time.monotonic()-t0:.2f}s] Bridge injection: {n_loinc_injected:,} LOINC codes, "
                 f"{n_rxnorm_injected:,} RxNorm codes into concept rows")

        # Build candidate rows, filtering SNOMED-only rows without bridge
        all_names = sorted(name_snomed.keys() | name_loinc.keys() | name_rxnorm.keys())
        candidates: list[ConceptRow] = []
        n_bridge_filtered = 0
        for name in all_names:
            s_codes = name_snomed.get(name, set())
            l_all = name_loinc.get(name, set())
            r_all = name_rxnorm.get(name, set())
            l_native = native_loinc.get(name, set())
            r_native = native_rxnorm.get(name, set())
            if l_all or r_all or (s_codes & bridged_snomed):
                candidates.append(ConceptRow(
                    name, s_codes,
                    l_native, r_native,
                    l_all - l_native, r_all - r_native,
                ))
            else:
                n_bridge_filtered += 1
        log.info(f"  [{time.monotonic()-t0:.2f}s] Candidates: {len(candidates):,} rows, "
                 f"{n_bridge_filtered:,} bridge-filtered")

        kept, n_deduped = _prefix_subset_dedup(candidates)
        log.info(f"  [{time.monotonic()-t0:.2f}s] Prefix dedup: {n_deduped:,} removed, {len(kept):,} kept")

        # Write
        out_path = os.path.join(out_dir, "concepts.csv")
        kept.sort(key=lambda r: r.name)
        _join = "|".join
        rows_out = [
            [row.name,
             _join(sorted(row.snomed_codes)),
             _join(sorted(row.loinc_codes)),
             _join(sorted(row.loinc_bridged)),
             _join(sorted(row.rxnorm_codes)),
             _join(sorted(row.rxnorm_bridged))]
            for row in kept
        ]
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow([
                "name", "snomed_codes",
                "loinc_codes", "loinc_bridged",
                "rxnorm_codes", "rxnorm_bridged",
            ])
            writer.writerows(rows_out)

        log.info(f"  [{time.monotonic()-t0:.2f}s] Merged concepts: {len(kept):,} rows written → {out_path}")


# ─── CLI subcommand ──────────────────────────────────────────────────

async def cmd_merge(args: Namespace) -> None:
    """Subcommand: merge — merge siblings + bridge into concepts.csv + concept_graph.bin."""
    out_dir = args.output
    os.makedirs(out_dir, exist_ok=True)

    loinc_dir = getattr(args, "loinc_dir", "")
    log.info(f"merge: out_dir={out_dir}, loinc_dir={loinc_dir}")
    merge_siblings(out_dir, loinc_dir=loinc_dir)

    # Sync fhir_id cache from DB (if not already cached)
    from .graph_builder import HealthGraphBuilder, sync_fhir_ids

    await sync_fhir_ids(out_dir)

    # Build graph and save binary
    HealthGraphBuilder().build(out_dir)

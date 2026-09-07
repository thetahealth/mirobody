"""Inspect a FHIR concept-graph node — bridges and siblings.

Debug helper for ``fhir_concept_graph.bin``. Resolves ``(SYSTEM, CODE)``
to its graph_int via :func:`code_to_int`, queries the graph for bridge
and sibling neighbors, and resolves each neighbor back to a display
name via the row-aligned meta sidecar in the local bundle.

Neighbors absent from the local corpus (e.g. LOINC codes filtered as
PHENX/SURVEY at corpus build time, leaving orphan bridge edges from
the cross-vocab side) are reported as ``<not in corpus>``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from argparse import Namespace
from collections import defaultdict
from functools import lru_cache

from ..concept_graph import ConceptGraph
from .common import (
    FHIR_GRAPH_BIN,
    SYSTEMS, _CODE_BITS, _CODE_MASK, code_to_fhir_id, int_to_code,
)
from .index import RES_DIR as _RES_DIR, load as _load_local_fhir_cache

log = logging.getLogger(__name__)

# 6 LOINC axes + 2 corpus-filter-relevant fields. Order matches the standard
# LN format (COMPONENT:PROPERTY:TIME:SYSTEM:SCALE:METHOD); CLASS/STATUS appended
# because they decide whether a code makes it into corpus + skip mask.
_LOINC_AXIS_FIELDS: tuple[tuple[str, str], ...] = (
    ("COMPONENT", "COMPONENT"),
    ("PROPERTY",  "PROPERTY"),
    ("TIME",      "TIME_ASPCT"),
    ("SYSTEM",    "SYSTEM"),
    ("SCALE",     "SCALE_TYP"),
    ("METHOD",    "METHOD_TYP"),
    ("CLASS",     "CLASS"),
    ("STATUS",    "STATUS"),
)


@lru_cache(maxsize=4)
def _load_loinc_axes(loinc_core_csv: str) -> dict[str, dict[str, str]]:
    """LOINC_NUM → {axis_label: value} for all rows in LoincTableCore.csv.

    Cached by path so repeated inspect calls in the same process pay
    the ~1s parse only once. Returns an empty dict on read error.
    """
    out: dict[str, dict[str, str]] = {}
    try:
        with open(loinc_core_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                num = row.get("LOINC_NUM", "")
                if not num:
                    continue
                out[num] = {
                    label: (row.get(col, "") or "").strip()
                    for label, col in _LOINC_AXIS_FIELDS
                }
    except OSError:
        log.exception("failed to read %s", loinc_core_csv)
    return out


def _resolve_loinc_core_csv(args: Namespace) -> str | None:
    """Return path to LoincTableCore.csv from --loinc-dir, or None."""
    loinc_dir = getattr(args, "loinc_dir", None)
    if not loinc_dir:
        return None
    path = os.path.join(loinc_dir, "LoincTableCore", "LoincTableCore.csv")
    return path if os.path.isfile(path) else None


def _build_reverse_index(cache: dict) -> dict[int, list[tuple[str, str, str]]]:
    """Canonical packed fhir_id → list of ``(system, code_str, name)``.

    Keys match what the graph stores (``code_to_fhir_id`` packed form),
    so a lookup with the same canonical resolves the display tuple. The
    list is per-key because DCM/THETA codes share a hash space and could
    in principle alias, but for vocabs with deterministic encoding each
    canonical maps to exactly one ``(system, code_str)``.
    """
    canonical = cache["canonical"]
    names = cache["names"] or [""] * len(canonical)
    code_strs = cache["code_strs"] or {}
    rev: dict[int, list[tuple[str, str, str]]] = defaultdict(list)
    for r in range(len(canonical)):
        fid = int(canonical[r])
        sys_idx = fid >> _CODE_BITS
        code_int = fid & _CODE_MASK
        sys_name = SYSTEMS[sys_idx] if sys_idx < len(SYSTEMS) else "?"
        if sys_name in ("DCM", "THETA"):
            code_str = code_strs.get(r, str(code_int))
        else:
            code_str = int_to_code(code_int, sys_name)
        rev[fid].append((sys_name, code_str, names[r]))
    return rev


def _format_neighbors(
    ids: list[int], rev: dict[int, list[tuple[str, str, str]]]
) -> list[dict]:
    out: list[dict] = []
    for nid in ids:
        matches = rev.get(nid, [])
        if matches:
            for sys, code, name in matches:
                out.append({
                    "system": sys, "code": code, "name": name, "graph_int": nid,
                })
        else:
            out.append({
                "system": None, "code": None, "name": None, "graph_int": nid,
            })
    return out


def cmd_inspect(args: Namespace) -> None:
    """Subcommand: inspect — show concept-graph bridges/siblings for a code."""
    cache = _load_local_fhir_cache(
        load_meta=True, bundle_dir=getattr(args, "bundle_dir", None),
    )
    if cache is None:
        log.error("local FHIR bundle not found; cannot resolve neighbor names")
        return

    bundle_dir = cache["_bundle_dir"]
    graph_path = os.path.join(bundle_dir, FHIR_GRAPH_BIN)
    if not os.path.isfile(graph_path):
        graph_path = os.path.join(_RES_DIR, FHIR_GRAPH_BIN)
    graph = ConceptGraph.get(graph_path)
    rev = _build_reverse_index(cache)

    system = args.system.upper()
    if system not in SYSTEMS:
        raise ValueError(f"unknown system {system!r}; valid: {list(SYSTEMS)}")
    graph_int = code_to_fhir_id(system, args.code)

    self_match = rev.get(graph_int, [])
    bridges = sorted(graph.bridge_neighbors(graph_int))
    siblings = sorted(graph.sibling_neighbors(graph_int))

    axes: dict[str, str] | None = None
    if system == "LOINC":
        loinc_core = _resolve_loinc_core_csv(args)
        if loinc_core:
            axes = _load_loinc_axes(loinc_core).get(args.code)
        elif getattr(args, "loinc_dir", None):
            log.warning("LoincTableCore.csv not found under --loinc-dir %s", args.loinc_dir)

    payload = {
        "input": {"system": system, "code": args.code, "graph_int": graph_int},
        "self_in_corpus": [
            {"system": s, "code": c, "name": n} for s, c, n in self_match
        ],
        "axes": axes,
        "bridges": _format_neighbors(bridges, rev),
        "siblings": _format_neighbors(siblings, rev),
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(f"{system} {args.code}  graph_int={graph_int}")
    if self_match:
        for s, c, n in self_match:
            print(f"  in corpus: {s:10s} {c:20s} {n}")
    else:
        print("  in corpus: <no — node was filtered out at corpus build>")

    if axes:
        print("  axes:")
        for label, _ in _LOINC_AXIS_FIELDS:
            v = axes.get(label, "")
            print(f"    {label:10s} {v}" if v else f"    {label:10s} —")
    elif system == "LOINC" and not getattr(args, "loinc_dir", None):
        print("  axes: <pass --loinc-dir to show LOINC axes>")

    for label, items in (("bridges", payload["bridges"]),
                         ("siblings", payload["siblings"])):
        print(f"  {label} ({len(items)}):")
        for it in items:
            if it["system"] is None:
                print(f"    ?          (graph_int={it['graph_int']})  <not in corpus>")
            else:
                print(f"    {it['system']:10s} {it['code']:20s} {it['name']}")

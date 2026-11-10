"""Derive resolver aliases from LOINC's name history.

A code's name changes: a term is sharpened ("Troponin I.cardiac" gains its
specimen), a display name is replaced, a consumer name is introduced. Reports
in the wild still print the old wording, so the old wording has to answer. The
release carries every name a code has ever had in
`AccessoryFiles/LoincNamesEffectiveDateHistory`, and the shipped bundle indexes
only the names current in `Loinc.csv` — which is why 434 names of the 287 codes
this catalogue carries resolve to nothing today.

Two rules keep the rows safe, the same ones `loinc_aliases.py` uses:

  fill a gap, never redirect a hit
      a name that already answers a different code is dropped. 4 of the 438
      candidates would have redirected a deliberate decision.

  the target must be reachable
      the right-hand side is a name the index already answers for that code
      (long common name, then component, then short name), so a row can never
      point at a dead key.

Usage:
    python -m standardize_build.loinc_name_history \
        --loinc /path/to/Loinc_2.83 \
        --catalog mirobody/res/catalog/metrics.tsv \
        --out /tmp/name_history_rows.tsv \
        --manifest standardize_build/name_history_manifest.tsv
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

#: The name types a report can print. `FullySpecifiedName` is left out: it is
#: the ontology's own rendering, never a label on a result.
NAME_TYPES = ("LONG_COMMON_NAME", "SHORTNAME", "CONSUMER_NAME", "DisplayName")

#: Words that describe how a value is reported rather than what was measured.
GENERIC = frozenset({
    "chemistry", "level", "point in time", "random", "quantitative", "qnt",
    "pl", "plsm", "plasma", "ser", "serum", "bld", "blood", "ur", "urine",
    "-", "1", "2",
})

MIN_LEN = 3
TARGET_FIELDS = ("LONG_COMMON_NAME", "COMPONENT", "SHORTNAME")


def catalogue_codes(path: Path) -> set[str]:
    with path.open(encoding="utf-8") as fh:
        return {(row.get("loinc") or "").strip()
                for row in csv.DictReader(fh, delimiter="\t") if (row.get("loinc") or "").strip()}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--loinc", required=True, type=Path)
    ap.add_argument("--catalog", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--manifest", type=Path)
    args = ap.parse_args(argv)

    codes = catalogue_codes(args.catalog)
    from mirobody.engine import get_resolver
    resolver = get_resolver()

    # One reachable target per code. "Reachable" means the index answers the
    # target ITSELF: probing with resolver.resolve() is circular, because it
    # consults this same override table, and a target that only resolves
    # through an override leaves the new row dead (411 rows were written that
    # way before this check existed).
    from mirobody.lexical import index_fold

    def index_code(name: str) -> str | None:
        posting = resolver._posting(index_fold(name))
        if posting is None or not len(posting):
            return None
        return resolver._axis_row(int(posting[0]))[0]

    target_of: dict[str, str] = {}
    with (args.loinc / "LoincTable" / "Loinc.csv").open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            code = row["LOINC_NUM"]
            if code not in codes or row.get("STATUS") != "ACTIVE":
                continue
            for field in TARGET_FIELDS:
                name = (row.get(field) or "").strip()
                if name and index_code(name) == code:
                    target_of[code] = name
                    break

    history = args.loinc / "AccessoryFiles" / "LoincNamesEffectiveDateHistory" / \
        "LoincNamesEffectiveDateHistory.csv"
    seen: set[str] = set()
    rows: list[tuple[str, str, str]] = []
    reasons: Counter[str] = Counter()

    with history.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            code = row["LoincNumber"]
            if code not in codes or row["NameType"] not in NAME_TYPES:
                continue
            name = (row["NameValue"] or "").strip()
            if len(name) < MIN_LEN or name.lower() in GENERIC or name in seen:
                continue
            seen.add(name)
            target = target_of.get(code)
            if not target:
                reasons["码没有可答出的名字"] += 1
                continue
            hit = resolver.resolve(name)
            if hit.resolved and hit.loinc == code:
                reasons["已经答对，无需新增"] += 1
                continue
            if hit.resolved:
                # a deliberate decision already answers this name
                reasons["会改写已有命中"] += 1
                continue
            rows.append((name, target, code))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fh:
        fh.write("# Derived by standardize_build.loinc_name_history from LOINC's\n")
        fh.write("# LoincNamesEffectiveDateHistory: retired and alternative names of the\n")
        fh.write("# codes res/catalog/metrics.tsv carries.\n")
        for name, target, _code in rows:
            fh.write(f"{name}\t{target}\n")

    if args.manifest:
        with args.manifest.open("w", encoding="utf-8") as fh:
            fh.write("# <printed name>\t<the LOINC code it must resolve to>\n")
            fh.write("# Generated by standardize_build/loinc_name_history.py; checked in\n")
            fh.write("# mirobody/tests/test_name_history_aliases.py.\n")
            for name, _target, code in rows:
                fh.write(f"{name}\t{code}\n")

    print(f"catalogue codes:  {len(codes)}")
    print(f"rows written:     {len(rows)}")
    for reason, count in reasons.most_common():
        print(f"   skipped {count:4d}  {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

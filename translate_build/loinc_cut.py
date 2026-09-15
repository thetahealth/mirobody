"""Cut the Tier-2 axis table out of a LOINC release.

Build-time only: reads the licensed ``Loinc.csv`` from an unpacked LOINC
release directory and writes the rows the resolver is allowed to answer with.
Rows are deleted, never edited (LOINC licence 5.8 section 3); every column that
leaves here is a LOINC column with its content unchanged.

The rule is the one measured on LOINC 2.83 (63,542 of 99,737 ACTIVE codes):

    STATUS == ACTIVE
    and CLASSTYPE in {1 laboratory, 2 clinical}
    and CLASS family not in DROP_FAMILIES
    and SCALE_TYP not in {Doc, Nar, -, Set, Multi}
    and (CLASS in H&P classes implies SCALE_TYP in {Qn, Ord})
    and (CLASS is PANEL.* implies subclass in PANEL_KEEP)
    or the code is on the explicit whitelist (device base table + metrics)
    minus rows carrying EXTERNAL_COPYRIGHT_NOTICE (owner's call, 2026-09-14)

Usage:
    python -m translate_build.loinc_cut --loinc /path/to/Loinc_2.83 --out mirobody/translate/data/loinc
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

# CLASS families that never hold a reading. "PATIENT SAFETY" and "NR STATS" are
# single families with a space in them, so this is a literal tuple, not split().
DROP_FAMILIES = frozenset((
    "SURVEY", "DOC", "RAD", "ATTACH", "ADMIN", "PHENX", "NIH", "DENTAL", "CELLMARK", "DOCUMENT", "NEMSIS",
    "CLINTRIAL", "PATIENT SAFETY", "TIMP", "ART", "MEPS", "APTA", "UPDRS", "HL7", "LABORDERS", "NR STATS",
    "OB", "EYE", "TUMRRGT", "SURG", "ED", "TRAUMA", "IO_OUT", "IO_IN", "IO", "IO_IN_SALTS+CALS", "VACCIN", "MEDS",
    "PUBLICHEALTH", "NEONAT", "TRNSPLNT", "ONCOLOGY", "AUDIO", "ICU", "GEN", "GI", "FUNCTION", "VOLUME", "MISC",
    "DRUGDOSE", "SPEC",
))

DROP_SCALES = frozenset({"Doc", "Nar", "-", "Set", "Multi"})

HP_CLASSES = frozenset({"H&P.HX", "H&P.PX", "H&P.HX.LAB", "H&P.SURG PROC"})
HP_KEEP_SCALES = frozenset({"Qn", "Ord"})

PANEL_KEEP = frozenset(
    "CHEM HEM/BC UA COAG SERO MICRO ALLERGY DRUG/TOX MOLPATH BLDBK CLIN H&P NUTRITION&DIET ABXBACT CHAL HLA".split()
)

AXIS_COLUMNS = (
    "LOINC_NUM COMPONENT PROPERTY TIME_ASPCT SYSTEM SCALE_TYP METHOD_TYP CLASS CLASSTYPE STATUS "
    "LONG_COMMON_NAME SHORTNAME DisplayName CONSUMER_NAME EXAMPLE_UCUM_UNITS COMMON_TEST_RANK "
    "PanelType VersionFirstReleased VersionLastChanged"
).split()

GATED_COLUMNS = "LOINC_NUM CLASS CLASSTYPE SCALE_TYP LONG_COMMON_NAME gate".split()


def family(loinc_class: str) -> str:
    return loinc_class.split(".", 1)[0]


def panel_subclass(loinc_class: str) -> str:
    parts = loinc_class.split(".")
    return parts[1] if len(parts) > 1 else ""


def gate(row: dict[str, str]) -> str:
    """Why a row is out, or "" when it stays. Checked in the order the rule lists."""
    if row["STATUS"] != "ACTIVE":
        return "status"
    if row["CLASSTYPE"] not in ("1", "2"):
        return "classtype"
    cls = row["CLASS"]
    if family(cls) in DROP_FAMILIES:
        return "class"
    if family(cls) == "PANEL":
        # Panels are organizers, not readings: their SCALE is "-" by design
        # and the scale gate does not apply to them.
        return "" if panel_subclass(cls) in PANEL_KEEP else "panel"
    scale = row["SCALE_TYP"]
    if scale in DROP_SCALES:
        return "scale"
    if cls in HP_CLASSES and scale not in HP_KEEP_SCALES:
        return "hp-scale"
    return ""


def read_whitelist(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            code, _, reason = line.partition("\t")
            out[code] = reason
    return out


def cut(loinc_csv: Path, whitelist: dict[str, str]) -> tuple[list[dict[str, str]], list[dict[str, str]], Counter]:
    kept: list[dict[str, str]] = []
    gated: list[dict[str, str]] = []
    reasons: Counter = Counter()
    with loinc_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = [c for c in AXIS_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise SystemExit(f"{loinc_csv}: missing columns {missing}")
        for row in reader:
            why = gate(row)
            if why and row["LOINC_NUM"] in whitelist:
                why = ""
                reasons["whitelist"] += 1
            if not why and row["EXTERNAL_COPYRIGHT_NOTICE"].strip():
                why = "licence"
            if why:
                reasons[why] += 1
                if row["STATUS"] == "ACTIVE":
                    gated.append({**{c: row[c] for c in GATED_COLUMNS if c != "gate"}, "gate": why})
                continue
            kept.append({c: row[c] for c in AXIS_COLUMNS})
    return kept, gated, reasons


def write_tsv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--loinc", required=True, type=Path, help="unpacked LOINC release directory")
    ap.add_argument("--out", required=True, type=Path, help="output directory (axis.tsv, gated.tsv)")
    ap.add_argument("--whitelist", type=Path, default=Path(__file__).with_name("whitelist.tsv"))
    args = ap.parse_args(argv)

    loinc_csv = args.loinc / "LoincTable" / "Loinc.csv"
    if not loinc_csv.exists():
        raise SystemExit(f"missing release file: {loinc_csv}")
    whitelist = read_whitelist(args.whitelist)
    kept, gated, reasons = cut(loinc_csv, whitelist)

    kept.sort(key=lambda r: r["LOINC_NUM"])
    gated.sort(key=lambda r: r["LOINC_NUM"])
    write_tsv(args.out / "axis.tsv", kept, AXIS_COLUMNS)
    write_tsv(args.out / "gated.tsv", gated, GATED_COLUMNS)

    print(f"kept {len(kept)}  gated(active) {len(gated)}")
    for why, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"  {why:10} {n}")
    fam = Counter(family(r["CLASS"]) for r in kept)
    print("  families:", ", ".join(f"{k} {v}" for k, v in fam.most_common(12)))
    return 0


if __name__ == "__main__":
    sys.exit(main())

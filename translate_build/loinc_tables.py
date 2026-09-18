"""Derive the accessory tables for the Tier-2 codes from a LOINC release.

Runs after ``loinc_cut`` and reads its ``axis.tsv`` to know which codes are in
scope. Every output row is a LOINC row filtered to those codes; field contents
are copied unchanged (LOINC licence 5.8 sections 2 and 3). Outputs, all TSV:

    names_en.tsv   code, source, text      designations: LONG_COMMON_NAME, SHORTNAME,
                                           DisplayName, CONSUMER_NAME, RELATEDNAMES2 (split on ;)
    names_zh.tsv   code, source, text      the zh-CN linguistic variant: COMPONENT and
                                           RELATEDNAMES2 (split on ;), verbatim
    parts.tsv      code, part_type, part_number, part_name     supplementary Part links
    groups.tsv     code, group_id, group, parent_group_id, parent_group, archetype, molar_mass
    panels.tsv     parent_loinc, sequence, loinc, loinc_name, required
    map_to.tsv     loinc, map_to, comment  (whole file; deprecations point out of Tier-2 too)

Usage:
    python -m translate_build.loinc_tables --loinc /path/to/Loinc_2.83 --out mirobody/translate/data/loinc
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

from translate_build.loinc_cut import write_tsv

EN_SOURCES = ("LONG_COMMON_NAME", "SHORTNAME", "DisplayName")
ZH_VARIANT = Path("AccessoryFiles/LinguisticVariants/zhCN5LinguisticVariant.csv")
MOLAR_MASS = re.compile(r"\|([0-9.]+) g/mole$")


def tier2_codes(axis_tsv: Path) -> set[str]:
    with axis_tsv.open(newline="", encoding="utf-8") as f:
        return {r["LOINC_NUM"] for r in csv.DictReader(f, delimiter="\t")}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"missing release file: {path}")
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def split_names(field: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for part in field.split(";"):
        text = part.strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def names_en(release: Path, codes: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with (release / "LoincTable" / "Loinc.csv").open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            code = r["LOINC_NUM"]
            if code not in codes:
                continue
            for source in EN_SOURCES:
                if r[source].strip():
                    rows.append({"code": code, "source": source, "text": r[source].strip()})
            for text in split_names(r["RELATEDNAMES2"]):
                rows.append({"code": code, "source": "RELATEDNAMES2", "text": text})
    for r in read_csv(release / "AccessoryFiles" / "ConsumerName" / "ConsumerName.csv"):
        if r["LoincNumber"] in codes and r["ConsumerName"].strip():
            rows.append({"code": r["LoincNumber"], "source": "CONSUMER_NAME", "text": r["ConsumerName"].strip()})
    return rows


def names_zh(release: Path, codes: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for r in read_csv(release / ZH_VARIANT):
        code = r["LOINC_NUM"]
        if code not in codes:
            continue
        if r["COMPONENT"].strip():
            rows.append({"code": code, "source": "COMPONENT", "text": r["COMPONENT"].strip()})
        for text in split_names(r["RELATEDNAMES2"]):
            rows.append({"code": code, "source": "RELATEDNAMES2", "text": text})
    return rows


# The supplementary link file repeats the six axes and CLASS for every code;
# those already sit in axis.tsv. What it adds is the decomposition below.
PART_TYPES = frozenset({
    "DIVISOR", "SUFFIX", "CHALLENGE", "ADJUSTMENT", "SUPER SYSTEM", "GENE", "NUMERATOR", "TIME MODIFIER", "COUNT",
})


def parts(release: Path, codes: set[str]) -> list[dict[str, str]]:
    path = release / "AccessoryFiles" / "PartFile" / "LoincPartLink_Supplementary.csv"
    return [
        {"code": r["LoincNumber"], "part_type": r["PartTypeName"], "part_number": r["PartNumber"], "part_name": r["PartName"]}
        for r in read_csv(path)
        if r["LoincNumber"] in codes and r["PartTypeName"] in PART_TYPES
    ]


def groups(release: Path, codes: set[str]) -> list[dict[str, str]]:
    gdir = release / "AccessoryFiles" / "GroupFile"
    parent = {r["ParentGroupId"]: r["ParentGroup"] for r in read_csv(gdir / "ParentGroup.csv")}
    group = {r["GroupId"]: r for r in read_csv(gdir / "Group.csv")}
    rows: list[dict[str, str]] = []
    for r in read_csv(gdir / "GroupLoincTerms.csv"):
        if r["LoincNumber"] not in codes:
            continue
        g = group.get(r["GroupId"])
        if g is None:
            continue
        m = MOLAR_MASS.search(g["Group"])
        rows.append({
            "code": r["LoincNumber"],
            "group_id": g["GroupId"],
            "group": g["Group"],
            "parent_group_id": g["ParentGroupId"],
            "parent_group": parent.get(g["ParentGroupId"], ""),
            "archetype": r["Archetype"],
            "molar_mass": m.group(1) if m else "",
        })
    return rows


def panels(release: Path, codes: set[str]) -> list[dict[str, str]]:
    path = release / "AccessoryFiles" / "PanelsAndForms" / "PanelsAndForms.csv"
    return [
        {
            "parent_loinc": r["ParentLoinc"],
            "sequence": r["SEQUENCE"],
            "loinc": r["Loinc"],
            "loinc_name": r["LoincName"],
            "required": r["ObservationRequiredInPanel"],
        }
        for r in read_csv(path)
        if r["ParentLoinc"] in codes or r["Loinc"] in codes
    ]


def map_to(release: Path) -> list[dict[str, str]]:
    return [
        {"loinc": r["LOINC"], "map_to": r["MAP_TO"], "comment": r["COMMENT"]}
        for r in read_csv(release / "LoincTable" / "MapTo.csv")
    ]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--loinc", required=True, type=Path, help="unpacked LOINC release directory")
    ap.add_argument("--out", required=True, type=Path, help="directory holding axis.tsv; outputs go beside it")
    args = ap.parse_args(argv)

    axis = args.out / "axis.tsv"
    if not axis.exists():
        raise SystemExit(f"run loinc_cut first: {axis} not found")
    codes = tier2_codes(axis)

    outputs = {
        "names_en.tsv": (names_en(args.loinc, codes), ["code", "source", "text"]),
        "names_zh.tsv": (names_zh(args.loinc, codes), ["code", "source", "text"]),
        "parts.tsv": (parts(args.loinc, codes), ["code", "part_type", "part_number", "part_name"]),
        "groups.tsv": (
            groups(args.loinc, codes),
            ["code", "group_id", "group", "parent_group_id", "parent_group", "archetype", "molar_mass"],
        ),
        "panels.tsv": (panels(args.loinc, codes), ["parent_loinc", "sequence", "loinc", "loinc_name", "required"]),
        "map_to.tsv": (map_to(args.loinc), ["loinc", "map_to", "comment"]),
    }
    for name, (rows, columns) in outputs.items():
        write_tsv(args.out / name, rows, columns)
        print(f"{name:14} {len(rows):>9} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Propose catalogue rows for the LOINC Universal Lab Orders a product lacks.

`res/catalog/metrics.tsv` lists the indicators a tool can query. The standard
carries a set of laboratory orders an implementation is expected to support
(`AccessoryFiles/LoincUniversalLabOrdersValueSet`); the overlap is the honest
measure of clinical-lab alignment, and this script produces the rows that close
it.

Facts come from LOINC, shape comes from the catalogue. A row is written only
when it can actually be queried:

  the code must already answer to a name
      otherwise the catalogue would carry an indicator no query can reach. On
      LOINC 2.83 with the bundle as shipped, 315 of the 370 uncovered CHEM
      orders qualify; the other 55 need the bundle rebuilt first.

  unit must match the code's PROPERTY
      mg/dL is a mass concentration, mmol/L a substance concentration, U/L a
      catalytic one. A row whose unit falls outside its code's PROPERTY would
      convert by the wrong chemistry.

  state and policy come from TIME_ASPCT
      Pt -> instant/last, 24H -> provider_daily/provider_value.

  member names follow the specimen
      Every generated name carries its specimen suffix. Existing serum names
      cannot hide urine or CSF siblings, and adding another specimen later
      cannot change an already generated name.

  qualitative orders are out of scope
      PROPERTY PrThr (presence/absence) cannot be a numeric series; modelling
      it needs a data_type the catalogue does not have. They are counted, not
      written.

Usage:
    python -m standardize_build.loinc_catalog_rows \
        --loinc /path/to/Loinc_2.83 \
        --catalog mirobody/res/catalog/metrics.tsv \
        --class CHEM --out /tmp/chem_rows.tsv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter
from pathlib import Path

from mirobody.lexical import index_fold

#: LOINC PROPERTY -> the unit dimensions that may carry it.
PROPERTY_UNITS = {
    "MCnc": {"mg/dL", "g/dL", "mg/L", "ug/mL", "ng/mL", "pg/mL", "g/L", "ug/dL", "ng/dL", "ug/L"},
    "SCnc": {"mmol/L", "umol/L", "nmol/L", "pmol/L", "mol/L"},
    "CCnc": {"U/L", "[IU]/L", "mU/L", "U/mL"},
    "ACnc": {"[IU]/L", "[IU]/mL", "U/L"},
    "NCnc": {"/uL", "10*3/uL", "10*6/uL", "/L"},
    "MFr": {"%"},
    "VFr": {"%", "mL/dL"},
    "Time": {"s", "min", "h"},
}

#: TIME_ASPCT -> (state_class, aggregation_policy)
TIMING = {
    "Pt": ("instant", "last"),
    "24H": ("provider_daily", "provider_value"),
}

MANIFEST_FIELDS = ("member", "loinc", "unit_ucum", "component", "property", "scale", "system", "method", "time")


def snake(text: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", (text or "").strip()).strip("_")
    return re.sub(r"_+", "_", text).upper()


def camel(text: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", text or "")
    if not words:
        return "indicator"
    head = words[0][0].lower() + words[0][1:]
    return head + "".join(w[:1].upper() + w[1:] for w in words[1:])


def assign_names(rows: list[dict]) -> None:
    """Stable identities independent of which siblings are already installed."""
    for row in rows:
        tag = snake(row["_system"])
        row["member"] = f"{row['_base']}_{tag}"
        row["name"] = camel(row["_component"]) + "".join(w.capitalize() for w in tag.split("_"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--loinc", required=True, type=Path)
    ap.add_argument("--catalog", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--manifest", type=Path, help="also write the source units and six LOINC axes")
    ap.add_argument("--class", dest="klass", action="append", default=[],
                    help="LOINC CLASS to include (repeatable); default CHEM")
    args = ap.parse_args(argv)

    classes = set(args.klass or ["CHEM"])

    with args.catalog.open(encoding="utf-8") as fh:
        existing = list(csv.DictReader(fh, delimiter="\t"))
    have_member = {r["member"] for r in existing}
    have_name = {r["name"] for r in existing}
    have_loinc = {(r.get("loinc") or "").strip() for r in existing} - {""}

    order_set = {
        (r.get("LOINC_NUM") or list(r.values())[0]).strip()
        for r in csv.DictReader(
            (args.loinc / "AccessoryFiles" / "LoincUniversalLabOrdersValueSet"
             / "LoincUniversalLabOrdersValueSet.csv").open(encoding="utf-8"))
    }

    from mirobody.engine import get_resolver
    from mirobody.units import normalize_unit

    resolver = get_resolver()
    rows: list[dict] = []
    reasons: Counter[str] = Counter()

    for rec in csv.DictReader((args.loinc / "LoincTable" / "Loinc.csv").open(encoding="utf-8")):
        code = rec["LOINC_NUM"]
        if code not in order_set or code in have_loinc:
            continue
        if rec.get("CLASS") not in classes or rec.get("STATUS") != "ACTIVE":
            continue
        prop = (rec.get("PROPERTY") or "").strip()
        timing = (rec.get("TIME_ASPCT") or "").strip()
        if prop == "PrThr":
            reasons["定性检验（PrThr），超出目录模型"] += 1
            continue
        if timing not in TIMING:
            reasons["unsupported time aspect"] += 1
            continue
        if prop not in PROPERTY_UNITS:
            reasons[f"PROPERTY {prop or '空白'} 无单位维度定义"] += 1
            continue
        unit = normalize_unit(rec.get("EXAMPLE_UCUM_UNITS") or "") or ""
        if not unit:
            reasons["missing or unrecognized example unit"] += 1
            continue
        if unit not in PROPERTY_UNITS[prop]:
            reasons[f"单位 {unit} 与 PROPERTY {prop} 不符"] += 1
            continue
        component = (rec.get("COMPONENT") or "").strip()
        system = (rec.get("SYSTEM") or "").strip()
        if system in {"", "XXX", "-"}:
            reasons["标本未指明（SYSTEM XXX）"] += 1
            continue
        base = snake(component)
        axes = (index_fold(component), prop, (rec.get("SCALE_TYP") or "").strip(), system,
                (rec.get("METHOD_TYP") or "").strip(), timing)
        bundled = resolver.axes_of(code)
        if bundled is None or bundled[:6] != axes:
            reasons["code axes differ from the shipped bundle"] += 1
            continue
        # Some name of the code must answer today, or the row is unreachable.
        # That name also becomes the display: a synthesised label ("Creatinine
        # (body_fld)") is not a key the index knows, so the resolver would fall
        # back to the serum code and the row would claim one concept while
        # answering another. 94 of the first 240 proposals failed exactly this
        # way.
        answered = None
        for name in (rec.get("LONG_COMMON_NAME"), component, rec.get("SHORTNAME")):
            if not name:
                continue
            hit = resolver.resolve(name)
            if hit.resolved and hit.loinc == code:
                answered = name
                break
        if answered is None:
            reasons["解析器认不出这个码（需重建 bundle）"] += 1
            continue
        state, policy = TIMING[timing]
        rows.append({
            "_component": component, "_system": system, "_base": base, "_axes": axes,
            "member": "", "name": "", "display": answered,
            "category": "Medical Metrics", "standard_unit": unit, "unit_ucum": unit,
            "data_type": "summary" if state == "provider_daily" else "series",
            "state_class": state, "aggregation_policy": policy,
            "loinc": code, "confidence": "confident", "window": "00:00", "panel": "",
            "aggregation_methods": "" if state == "provider_daily" else "last", "derived": "",
            "description": (rec.get("LONG_COMMON_NAME") or component)[:80],
            "since": "1.4.0",
        })

    assign_names(rows)
    # the suffix decides uniqueness, so the collision check belongs here
    seen_members: set[str] = set()
    seen_names: set[str] = set()
    unique_rows = []
    for row in rows:
        if (row["member"] in seen_members or row["member"] in have_member
                or row["name"] in seen_names or row["name"] in have_name):
            reasons["命名后仍与其他成员重名"] += 1
            continue
        seen_members.add(row["member"])
        seen_names.add(row["name"])
        unique_rows.append(row)
    rows = unique_rows

    header = list(existing[0].keys())
    with args.out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header, delimiter="\t", lineterminator="\n")
        for row in rows:
            writer.writerow({key: row[key] for key in header})

    if args.manifest:
        with args.manifest.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
            writer.writerow(MANIFEST_FIELDS)
            for row in rows:
                writer.writerow((row["member"], row["loinc"], row["unit_ucum"], *row["_axes"]))

    print(f"class {sorted(classes)}: {len(rows)} rows proposed")
    for reason, count in reasons.most_common():
        print(f"   skipped {count:4d}  {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

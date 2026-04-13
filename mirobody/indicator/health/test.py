"""Verify output files against known test cases."""

import csv
import logging
import os
from argparse import Namespace

from .common import csv_field_size_limit

log = logging.getLogger(__name__)

LOINC_CHECKS = {
    "24966-4": "dxa",
    "38267-1": "dxa",
    "85394-5": "dxa",
    "8867-4": "heart rate",
    "93832-4": "sleep",
    "4548-4": "hemoglobin a1c",
    "8480-6": "blood pressure",
    "11330-8": "alcohol",
    "29463-7": "weight",
    "2093-3": "cholesterol",
    "61008-9": "temperature",
    "8310-5": "temperature",
    "26499-4": "neutrophil",
    "770-8": "neutrophil",
    "4544-3": "hematocrit",
    "48703-3": "hematocrit",
    "786-4": "mchc",
}

SNOMED_CHECKS = {
    "391073007": "bone",
    "1167364002": "bone",
    "386725007": "temperature",
    "428377003": "lumbar spine",
}

CROSS_GROUP_CHECKS = [
    ("Hematocrit Auto + Est in same group", "loinc_codes", ["4544-3", "48703-3"]),
    ("DXA lumbar BMD + T-score + Z-score in same group", "loinc_codes", ["24966-4", "38267-1", "85394-5"]),
    ("Neutrophils Bld + NFr in same group", "loinc_codes", ["26499-4", "770-8"]),
]

# ─── Bridge checks ───────────────────────────────────────────────────

# _bridges_loinc_rxnorm.csv: LOINC drug tests ↔ RxNorm drug ingredients
LOINC_RXNORM_BRIDGE_CHECKS = [
    # (loinc_code, rxnorm_code, description)
    ("4003-0", "8896", "Pseudoephedrine urine test → pseudoephedrine"),
    ("427-5", "9384", "rifAMPin susceptibility MLC → rifampin"),
    ("428-3", "9384", "rifAMPin susceptibility MIC → rifampin"),
]

# _siblings_loinc.csv: Chinese name presence
LOINC_CHINESE_CHECKS = [
    # (search_term, expected_cn_substring)
    ("Hematocrit", "红细胞"),
    ("Glucose", "葡萄糖"),
    ("rifAMPin", "利福平"),
]

# _siblings_loinc.csv: survey/questionnaire exclusion
LOINC_EXCLUDED_CHECKS = [
    # These should NOT appear in siblings (CLASSTYPE 3/4, SURVEY, PHENX)
    "How often did you eat doughnuts",
    "Trouble concentrating on things",
    "PHQ-9",
]

# _siblings_rxnorm.csv: brand name + Chinese name presence
RXNORM_NAME_CHECKS = [
    # (atc_code, expected_substring)
    ("A02BC", "Prilosec"),      # PPI brand name
    ("A02BC", "奥美拉唑"),       # PPI Chinese name
    ("V03AC", "去铁胺"),         # Iron chelating agent Chinese name
    ("V03AC", "deferasirox"),   # Iron chelating agent generic name
]


def cmd_test(args: Namespace) -> None:
    """Subcommand: test -- verify concepts.csv against known test cases."""
    out_dir = args.output

    with csv_field_size_limit():
        n_pass = 0
        n_fail = 0

        # concepts.csv checks (optional — may not exist if merge step hasn't run)
        concepts_path = os.path.join(out_dir, "concepts.csv")
        if os.path.isfile(concepts_path):
            rows = list(csv.DictReader(open(concepts_path)))
            log.info(f"Loaded {len(rows):,} rows from {concepts_path}")

            # LOINC checks
            print("\n=== LOINC codes ===")
            for code, kw in LOINC_CHECKS.items():
                found = any(
                    code in (r.get("loinc_codes", "").split("|") + r.get("loinc_bridged", "").split("|"))
                    and kw in r["name"].lower()
                    for r in rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {code:10s} ({kw})")
                else:
                    n_fail += 1
                    any_row = next((r for r in rows if code in (
                        r.get("loinc_codes", "").split("|") + r.get("loinc_bridged", "").split("|")
                    )), None)
                    if any_row:
                        print(f"  FAIL  {code:10s} ({kw}) -- found in: {any_row['name'][:50]}")
                    else:
                        print(f"  FAIL  {code:10s} ({kw}) -- NOT in concepts.csv")

            # SNOMED checks
            print("\n=== SNOMED codes ===")
            for code, kw in SNOMED_CHECKS.items():
                found = any(
                    code in r.get("snomed_codes", "").split("|") and kw in r["name"].lower()
                    for r in rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {code:12s} ({kw})")
                else:
                    n_fail += 1
                    any_row = next((r for r in rows if code in r.get("snomed_codes", "").split("|")), None)
                    if any_row:
                        print(f"  FAIL  {code:12s} ({kw}) -- found in: {any_row['name'][:50]}")
                    else:
                        print(f"  FAIL  {code:12s} ({kw}) -- NOT in concepts.csv")

            # Cross-group checks
            print("\n=== Cross-group checks ===")
            for desc, field, codes in CROSS_GROUP_CHECKS:
                bridged_field = field.replace("_codes", "_bridged")
                found = any(
                    all(c in (r.get(field, "").split("|") + r.get(bridged_field, "").split("|"))
                        for c in codes)
                    for r in rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {desc}")
                else:
                    n_fail += 1
                    print(f"  FAIL  {desc}")
        else:
            log.warning(f"concepts.csv not found, skipping concept checks")

        # LOINC↔RxNorm bridge checks
        bridge_loinc_rxnorm_path = os.path.join(out_dir, "_bridges_loinc_rxnorm.csv")
        if os.path.isfile(bridge_loinc_rxnorm_path):
            print("\n=== LOINC↔RxNorm bridge ===")
            bridge_rows = list(csv.DictReader(open(bridge_loinc_rxnorm_path)))
            for loinc_code, rxnorm_code, desc in LOINC_RXNORM_BRIDGE_CHECKS:
                found = any(
                    loinc_code in r["loinc_codes"].split("|")
                    and rxnorm_code in r["rxnorm_codes"].split("|")
                    for r in bridge_rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {desc}")
                else:
                    n_fail += 1
                    print(f"  FAIL  {desc}")

        # LOINC siblings checks
        loinc_sib_path = os.path.join(out_dir, "_siblings_loinc.csv")
        if os.path.isfile(loinc_sib_path):
            sib_rows = list(csv.DictReader(open(loinc_sib_path)))

            # Chinese name presence
            print("\n=== LOINC Chinese names ===")
            for search_term, cn_sub in LOINC_CHINESE_CHECKS:
                found = any(
                    search_term in r["name"] and cn_sub in r["name"]
                    for r in sib_rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {search_term} contains '{cn_sub}'")
                else:
                    n_fail += 1
                    print(f"  FAIL  {search_term} missing '{cn_sub}'")

            # Survey/questionnaire exclusion
            print("\n=== LOINC excluded (surveys/questionnaires) ===")
            all_names = " ".join(r["name"] for r in sib_rows)
            for excluded_term in LOINC_EXCLUDED_CHECKS:
                found = excluded_term.lower() in all_names.lower()
                if not found:
                    n_pass += 1
                    print(f"  pass  '{excluded_term}' correctly excluded")
                else:
                    n_fail += 1
                    print(f"  FAIL  '{excluded_term}' should not appear in siblings")

        # RxNorm siblings checks
        rxnorm_sib_path = os.path.join(out_dir, "_siblings_rxnorm.csv")
        if os.path.isfile(rxnorm_sib_path):
            print("\n=== RxNorm names (brand + Chinese) ===")
            rxn_rows = list(csv.DictReader(open(rxnorm_sib_path)))
            for atc_code, expected_sub in RXNORM_NAME_CHECKS:
                found = any(
                    r.get("note") == atc_code and expected_sub in r["name"]
                    for r in rxn_rows
                )
                if found:
                    n_pass += 1
                    print(f"  pass  {atc_code} contains '{expected_sub}'")
                else:
                    n_fail += 1
                    print(f"  FAIL  {atc_code} missing '{expected_sub}'")

        # Summary
        total = n_pass + n_fail
        print(f"\n{'=' * 40}")
        print(f"  {n_pass}/{total} passed, {n_fail} failed")
        if n_fail == 0:
            print("  All tests passed!")

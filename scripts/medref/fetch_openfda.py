#!/usr/bin/env python3
"""Download openFDA drug labels for a curated chronic-disease generic list.

The full openFDA drug-label bulk dump is gigabytes; the reference tool only
needs the labels a primary-care conversation actually names. One label per
generic (the API's best-ranked result for that generic name) is enough text
for the retrieval index and keeps the built bundle small.

Source: https://api.fda.gov/drug/label.json — public domain (CC0 1.0
Universal, https://open.fda.gov/license/). Raw responses land in
``scripts/medref/raw/openfda/<GENERIC>.json`` next to the MedlinePlus dump
(gitignored; this script is the only network step — indexing itself is
offline).

Usage: python3 fetch_openfda.py [--out DIR] [--limit N] [--sleep SECONDS]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger("fetch_openfda")

# One generic per line would drift into prose; grouped by therapy area so a
# gap (a whole missing class) is visible at a glance. The builder indexes
# whatever it finds on disk, so trimming this list needs no code change.
GENERICS: tuple[str, ...] = (
    # lipids
    "ATORVASTATIN", "ROSUVASTATIN", "SIMVASTATIN", "PRAVASTATIN",
    "LOVASTATIN", "FLUVASTATIN", "PITAVASTATIN", "EZETIMIBE", "FENOFIBRATE",
    "GEMFIBROZIL", "NIACIN", "ICOSAPENT ETHYL", "BEMPEDOIC ACID",
    # diabetes
    "METFORMIN", "GLIPIZIDE", "GLIMEPIRIDE", "GLYBURIDE", "PIOGLITAZONE",
    "SITAGLIPTIN", "LINAGLIPTIN", "SAXAGLIPTIN", "EMPAGLIFLOZIN",
    "DAPAGLIFLOZIN", "CANAGLIFLOZIN", "LIRAGLUTIDE", "SEMAGLUTIDE",
    "DULAGLUTIDE", "EXENATIDE", "TIRZEPATIDE", "INSULIN GLARGINE",
    "INSULIN ASPART", "INSULIN LISPRO", "INSULIN DEGLUDEC", "ACARBOSE",
    "REPAGLINIDE",
    # hypertension / cardiovascular
    "LISINOPRIL", "ENALAPRIL", "RAMIPRIL", "BENAZEPRIL", "PERINDOPRIL",
    "LOSARTAN", "VALSARTAN", "OLMESARTAN", "IRBESARTAN", "TELMISARTAN",
    "CANDESARTAN", "AMLODIPINE", "NIFEDIPINE", "DILTIAZEM", "VERAPAMIL",
    "FELODIPINE", "METOPROLOL", "ATENOLOL", "PROPRANOLOL", "CARVEDILOL",
    "BISOPROLOL", "NEBIVOLOL", "LABETALOL", "HYDROCHLOROTHIAZIDE",
    "CHLORTHALIDONE", "INDAPAMIDE", "FUROSEMIDE", "SPIRONOLACTONE",
    "EPLERENONE", "CLONIDINE", "HYDRALAZINE", "DOXAZOSIN",
    "SACUBITRIL AND VALSARTAN", "DIGOXIN", "ISOSORBIDE MONONITRATE",
    "RANOLAZINE", "IVABRADINE",
    # anticoagulants / antiplatelets
    "WARFARIN", "APIXABAN", "RIVAROXABAN", "DABIGATRAN", "EDOXABAN",
    "CLOPIDOGREL", "ASPIRIN", "PRASUGREL", "TICAGRELOR", "DIPYRIDAMOLE",
    # thyroid
    "LEVOTHYROXINE", "LIOTHYRONINE", "METHIMAZOLE", "PROPYLTHIOURACIL",
    # respiratory / allergy
    "ALBUTEROL", "SALMETEROL", "FLUTICASONE", "BUDESONIDE", "FORMOTEROL",
    "TIOTROPIUM", "MONTELUKAST", "THEOPHYLLINE", "CETIRIZINE", "LORATADINE",
    "FEXOFENADINE", "DIPHENHYDRAMINE",
    # steroids
    "PREDNISONE", "PREDNISOLONE", "METHYLPREDNISOLONE", "DEXAMETHASONE",
    # gastrointestinal
    "OMEPRAZOLE", "ESOMEPRAZOLE", "LANSOPRAZOLE", "PANTOPRAZOLE",
    "FAMOTIDINE", "METOCLOPRAMIDE", "ONDANSETRON", "MESALAMINE",
    "LOPERAMIDE", "POLYETHYLENE GLYCOL 3350", "LACTULOSE",
    # psychiatric / neurology
    "SERTRALINE", "FLUOXETINE", "ESCITALOPRAM", "CITALOPRAM", "PAROXETINE",
    "VENLAFAXINE", "DULOXETINE", "BUPROPION", "MIRTAZAPINE", "TRAZODONE",
    "AMITRIPTYLINE", "QUETIAPINE", "ARIPIPRAZOLE", "OLANZAPINE",
    "RISPERIDONE", "LAMOTRIGINE", "LEVETIRACETAM", "DIVALPROEX",
    "CARBAMAZEPINE", "GABAPENTIN", "PREGABALIN", "TOPIRAMATE", "LITHIUM",
    "BUSPIRONE", "LORAZEPAM", "CLONAZEPAM", "ALPRAZOLAM", "ZOLPIDEM",
    "DONEPEZIL", "MEMANTINE", "SUMATRIPTAN",
    # pain / gout / bone
    "ACETAMINOPHEN", "IBUPROFEN", "NAPROXEN", "DICLOFENAC", "CELECOXIB",
    "MELOXICAM", "TRAMADOL", "CYCLOBENZAPRINE", "ALLOPURINOL", "FEBUXOSTAT",
    "COLCHICINE", "METHOTREXATE", "HYDROXYCHLOROQUINE", "ALENDRONATE",
    "RISEDRONATE", "DENOSUMAB",
    # urology / men's health
    "TAMSULOSIN", "FINASTERIDE", "OXYBUTYNIN", "SILDENAFIL", "TADALAFIL",
    # common anti-infectives
    "AMOXICILLIN", "AZITHROMYCIN", "DOXYCYCLINE", "CEPHALEXIN",
    "CIPROFLOXACIN", "LEVOFLOXACIN", "METRONIDAZOLE", "FLUCONAZOLE",
    "VALACYCLOVIR", "OSELTAMIVIR", "NITROFURANTOIN",
    "SULFAMETHOXAZOLE AND TRIMETHOPRIM",
)

API = "https://api.fda.gov/drug/label.json"
DEFAULT_OUT = Path(__file__).resolve().parent / "raw" / "openfda"


def fetch_one(generic: str, limit: int) -> dict:
    """One label for one generic name (salt forms match: a token search, not
    .exact, so ATORVASTATIN finds ATORVASTATIN CALCIUM).

    The API's first hit is often a COMBINATION product ("sitagliptin and
    metformin") for single-ingredient queries; among the top candidates we
    keep the label with the fewest generic names, preferring one whose name
    list starts with the queried generic."""
    quoted = f'openfda.generic_name:"{generic}"'
    plain = f"openfda.generic_name:{generic}"
    payload: dict | None = None
    for search in (quoted, plain):
        url = API + "?" + urllib.parse.urlencode({"search": search, "limit": limit})
        req = urllib.request.Request(url, headers={"User-Agent": "mirobody-medref-builder/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
    if not payload or not payload.get("results"):
        raise LookupError(f"no label found for {generic}")

    def rank(label: dict) -> tuple[int, int, int]:
        names = ((label.get("openfda") or {}).get("generic_name")) or ["?"]
        # A combination label ("X AND Y") for a single-ingredient query ranks
        # last: it answered a different product the first time this ran.
        combo = 1 if " AND " in names[0].upper() and " AND " not in generic.upper() else 0
        starts = 0 if names[0].upper().startswith(generic.split()[0]) else 1
        return combo, len(names), starts

    best = min((payload["results"]), key=rank)
    return {"results": [best], "candidates": len(payload["results"])}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--limit", type=int, default=5, help="labels per generic (search result count)")
    ap.add_argument("--sleep", type=float, default=0.4, help="seconds between requests (no API key)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    ok, failed, skipped = 0, [], []
    for generic in GENERICS:
        path = out_dir / f"{urllib.parse.quote(generic, safe='')}.json"
        if path.exists():
            skipped.append(generic)
            continue
        try:
            payload = fetch_one(generic, args.limit)
        except Exception as e:
            # No values in the log line on purpose: the generic name is the
            # fetch key, the type names the failure; counts land in the summary.
            logger.warning("fetch failed: error_type=%s", type(e).__name__)
            failed.append(generic)
        else:
            payload["_mirobody_query"] = {"generic": generic, "fetched": time.strftime("%Y-%m-%d")}
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            ok += 1
            logger.info("fetched %d results (%d done)", len(payload.get("results", [])), ok)
        time.sleep(args.sleep)

    manifest = {
        "source": API,
        "license": "CC0 1.0 Universal (https://open.fda.gov/license/)",
        "fetched": time.strftime("%Y-%m-%d"),
        "generics_requested": len(GENERICS),
        "generics_fetched": ok + len(skipped),
        "generics_failed": failed,
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("done: %d fetched, %d reused, %d failed", ok, len(skipped), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

"""Fetch the ICPC-3 tabular list, and build the table the package ships.

Build-time only, like the rest of this directory: nothing in `mirobody/`
imports it. The output is `mirobody/res/icpc3.tsv`.

The source is one request. WONCA's ICPC-3 Coding Tool downloads the whole
classification as JSON and caches it in the browser, and the same endpoint
answers a script:

    GET https://ct.icpc-3.info/download.php?language=english

10,886 rubrics over 1,623 classes, no key, no session. The browser API at
`browser.icpc-3.info/browse.php` answers the same content one class at a time
and took about 1,100 requests to walk; it is the wrong tool for a redistributor
and is not used here. `language` also accepts dutch, french and portuguese.
There is no Chinese: a Chinese ICPC-3 would have to be made under WONCA's
translation licence, not derived here.

WHAT IS SHIPPED, AND WHAT IS LEFT BEHIND

Shipped: the S (symptoms and complaints) and D (diagnoses and diseases)
components, with the classification's own editorial content for each code, its
preferred term, description, inclusions and exclusions.

Left behind, deliberately: every crosswalk rubric (`snomed-CT`, `icd10`,
`icd11`, `icpc-1`, `icpc-2`, `gbd`, `icf`, `sdg`, `uhc`) and the `indexwords`
the per-class browser API carries. Those are not ICPC-3's own words. Measured
on the 3,139 index words in the crawled capture, 1,273 of them (40.6%) are,
once the reference and its semantic tag are removed, character-identical to a
SNOMED CT description listed on the same code ("hyperpyrexia (finding)" ->
"hyperpyrexia"), and much of the remainder carries ICD phrasing. Shipping them
would put SNOMED CT and ICD derivatives back into `mirobody/res/`, which 1.5.0
emptied of exactly that, and would make `LICENSE-3RD-PARTY`'s "nothing in this
tree now requires a SNOMED CT Affiliate License" untrue.

ONE transformation, applied everywhere: `<Reference>X</Reference>` is unwrapped
to `X`. It is markup, not content, and left in it would put strings like
"fear of prostate cancer <Reference>GS94.00</Reference>" into the resolver's
index as a surface nobody can type. Nothing else is changed: no translation, no
re-wording, no re-coding, and no code dropped from a component that is present.
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import pathlib
import re
import sys
import urllib.parse
import urllib.request

DOWNLOAD = "https://ct.icpc-3.info/download.php"
UA = "mirobody/1.5.1 (+https://github.com/thetahealth/mirobody)"

COMPONENTS = "SD"
COLUMNS = ["component", "chapter", "code", "kind", "preferred",
           "description", "inclusions", "exclusions", "depth"]

#: The rubric kinds that are ICPC-3's own editorial content. Everything else
#: the payload carries is a pointer into another vocabulary; see the module
#: docstring for why none of it ships.
KEPT = {"preferred": "preferred", "description": "description",
        "inclusion": "inclusions", "exclusion": "exclusions"}

_REFERENCE = re.compile(r"<[Rr]eference[^>]*>(.*?)</[Rr]eference>", re.S)


def unwrap(label: str) -> str:
    """`fever <Reference>AS03</Reference>` -> `fever AS03`."""
    return _REFERENCE.sub(r"\1", label or "").strip()


def fetch(out: pathlib.Path, language: str = "english") -> int:
    """The whole classification, one request, saved verbatim."""
    url = f"{DOWNLOAD}?{urllib.parse.urlencode({'language': language})}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r:
        payload = json.loads(r.read().decode("utf-8"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    return len(payload)


def build(source: pathlib.Path, components: str = COMPONENTS) -> list[dict[str, str]]:
    """The named components as table rows, in code order."""
    payload = json.loads(source.read_text(encoding="utf-8"))
    grouped: dict[str, dict[str, list[str]]] = collections.defaultdict(lambda: collections.defaultdict(list))
    kinds: dict[str, str] = {}
    for r in payload:
        code = (r.get("code") or "").strip()
        field = KEPT.get(r.get("rkind") or "")
        if not code or field is None:
            continue
        kinds[code] = r.get("ckind") or ""
        grouped[code][field].append(unwrap(r.get("rubric") or ""))

    rows = []
    for code in sorted(grouped):
        if len(code) < 3 or not code[0].isalpha() or code[1] not in components:
            continue
        cell = grouped[code]
        rows.append({
            "component": code[1],
            "chapter": code[0],
            "code": code,
            "kind": kinds.get(code, ""),
            "preferred": "; ".join(cell["preferred"]).strip(),
            "description": " ".join(cell["description"]).strip(),
            "inclusions": ";".join(v for v in cell["inclusions"] if v),
            "exclusions": ";".join(v for v in cell["exclusions"] if v),
            "depth": str(code.count(".")),
        })
    return rows


def write(rows: list[dict[str, str]], out: pathlib.Path) -> None:
    with out.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=["fetch", "build"])
    ap.add_argument("--language", default="english")
    ap.add_argument("--components", default=COMPONENTS)
    ap.add_argument("--source", type=pathlib.Path,
                    default=root / "internal/indicators/icpc3/coding-tool-english.json")
    ap.add_argument("--out", type=pathlib.Path, default=root / "mirobody/res/icpc3.tsv")
    args = ap.parse_args(argv)

    if args.command == "fetch":
        n = fetch(args.source, args.language)
        print(f"{n} rubrics -> {args.source}")
        return 0

    rows = build(args.source, args.components)
    write(rows, args.out)
    counts = collections.Counter(r["component"] for r in rows)
    print(f"{len(rows)} codes -> {args.out} ({dict(sorted(counts.items()))})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

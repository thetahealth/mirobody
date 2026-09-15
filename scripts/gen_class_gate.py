"""Regenerate `mirobody/res/loinc_class_gated.tsv` from the shipped bundle.

    python -m scripts.gen_class_gate

Reads `loinc_axis.csv` out of the bundle — the full LOINC axis table including
the `CLASS` column, which the runtime index (`axis_fields.bin`, 9 fields) does
not carry — and writes the codes whose CLASS puts them in a discipline a
printed lab report cannot contain.

The gated classes are a deliberately SHORT list, and the list is short because
the obvious longer one is wrong. See the generated file's header for the two
measured defects this fixes and the five wearable codes a broader gate lost.
"""

from __future__ import annotations

import collections
import csv
import io
import pathlib
import re

from mirobody import bundle

#: CLASS prefixes that cannot appear as a row on a lab report.
#:
#: `RAD` is radiology, `OB.US` obstetric ultrasound, `DENTAL` dentistry,
#: `CELLMARK` flow-cytometry cell-surface markers, `NIH.*` the NIH Toolbox
#: cognitive/motor/sensory instruments.
#:
#: NOT here, and this is the load-bearing part: `H&P.HX` (sleep duration and
#: stages), `EKG.MEAS` (heart-rate variability), `CLIN` (step count, HRV SDNN)
#: and `BDYWGT.*` (body composition). Consumer and wearable metrics live in
#: classes that look "not lab", exactly as they live under the `^Patient`
#: SYSTEM — gating on the intuition loses them.
GATED = re.compile(r"^(RAD|OB\.US|DENTAL|CELLMARK|NIH\.)")

_OUT = pathlib.Path(__file__).resolve().parent.parent / "mirobody" / "res" / "loinc_class_gated.tsv"


def main() -> None:
    raw = bundle.read_member("loinc_axis.csv").decode("utf-8", "replace")
    rows = list(csv.DictReader(io.StringIO(raw)))
    gated = [(r["LOINC_NUM"], r["CLASS"]) for r in rows if GATED.match(r["CLASS"])]
    by_class = collections.Counter(c for _, c in gated)

    print(f"{len(gated)} of {len(rows)} codes gated ({100 * len(gated) / len(rows):.1f}%)")
    for cls, n in by_class.most_common():
        print(f"  {cls:<16} {n}")
    print(f"\nRewrite {_OUT} by hand-editing its header, then paste these rows.")


if __name__ == "__main__":
    main()

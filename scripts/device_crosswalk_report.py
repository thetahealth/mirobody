"""Render the device crosswalk (mirobody/res/crosswalks/) as Markdown tables.

    python scripts/device_crosswalk_report.py vendors    per-vendor counts and sources
    python scripts/device_crosswalk_report.py reasons    the unmappable metrics by reason
    python scripts/device_crosswalk_report.py base       the base table, one row per code
    python scripts/device_crosswalk_report.py all        the three in order

docs/device-crosswalk.md carries the output; rerun this after editing a table.
"""

from __future__ import annotations

import sys
from collections import Counter

from mirobody.translate import devices


def vendors() -> str:
    lines = ["| Vendor | Fields read | With a code | Confident | Source |", "| --- | ---: | ---: | ---: | --- |"]
    total = Counter()
    for vendor, counts in devices.summary().items():
        src = devices.SOURCES[vendor]
        links = " · ".join(f"[{i + 1}]({u})" for i, u in enumerate(src.urls))
        lines.append(
            f"| {src.name} (`{vendor}.tsv`) | {counts['fields']} | {counts['coded']} | {counts['confident']} | {links} |"
        )
        total.update(counts)
    lines.append(f"| **13 vendors** | **{total['fields']}** | **{total['coded']}** | **{total['confident']}** | |")
    return "\n".join(lines)


def reasons() -> str:
    counts = Counter(u.reason for u in devices.unmappable())
    lines = ["| Reason | Metrics | Meaning |", "| --- | ---: | --- |"]
    meaning = {
        "ALGO_MISMATCH": "LOINC has the concept but codes another algorithm",
        "VENDOR_SCORE": "a proprietary score; LOINC does not code these",
        "NO_CONCEPT": "the concept is absent from the axis table",
        "UNIT_INCOMPARABLE": "a near code exists, but the vendor's unit or weighting makes the number incomparable",
        "RELATIVE": "a deviation from a baseline, not an absolute quantity",
        "NOT_MEASUREMENT": "a goal, a recommendation or a plan",
        "QUALITY_META": "a data-quality or device-state signal",
        "NOT_PERSON": "an environmental quantity",
        "OUT_OF_SCOPE": "raw waveforms, location, nutrition",
    }
    for reason in devices.REASONS:
        lines.append(f"| `{reason}` | {counts.get(reason, 0)} | {meaning[reason]} |")
    lines.append(f"| **total** | **{sum(counts.values())}** | |")
    return "\n".join(lines)


def base() -> str:
    lines = ["| LOINC | Name | Catalogue metric | Confidence | Vendors |", "| --- | --- | --- | --- | --- |"]
    for row in devices.base_table():
        metric = f"`{row.metric}`" if row.metric else ""
        vendors_ = ", ".join(v for v in devices.VENDORS if v in row.fields)
        lines.append(f"| {row.loinc} | {row.name} | {metric} | {row.confidence} | {vendors_} |")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    which = argv[1] if len(argv) > 1 else "all"
    parts = {"vendors": vendors, "reasons": reasons, "base": base}
    if which == "all":
        print("\n\n".join(fn() for fn in parts.values()))
        return 0
    if which not in parts:
        print(__doc__)
        return 2
    print(parts[which]())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

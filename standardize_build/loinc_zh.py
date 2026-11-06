"""Build the Chinese LOINC names the runtime reads, and the file that goes back to LOINC.

Build-time only. Inputs:

- an unpacked LOINC release: ``Loinc.csv`` (the English Parts, ConsumerName) and the
  zh-CN linguistic variant ``AccessoryFiles/LinguisticVariants/zhCN5LinguisticVariant.csv``;
- ``standardize_build/zh_CN/parts.tsv``: every Part the commonly ordered codes use, the
  variant's rendering and the reviewed one (``axis, english, official, chinese, note``);
- ``standardize_build/zh_CN/consumer.tsv``: LOINC's English ConsumerName -> Chinese;
- ``mirobody/res/loinc/zh_CN_names.tsv``: display name and long name per code.

Outputs:

- ``mirobody/res/loinc/zh_CN_parts.tsv.gz``: the six Chinese Parts of every code in the
  shipped bundle that the variant covers. A Part with a reviewed rendering takes it;
  every other Part is the variant's text, unchanged. Sorted by code, because
  ``mirobody.standardize.names`` bisects it; gzip mtime 0, so a rebuild is
  byte-identical.
- with ``--submission PATH``: the translation in the LinguisticVariant CSV layout, for
  the codes in ``zh_CN_names.tsv``. LOINC License 5.8 section 12 says every translation
  is transmitted to Regenstrief; this is that file.

    python -m standardize_build.loinc_zh --loinc /path/to/Loinc_2.83 [--submission zhCN.csv]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import sys
from pathlib import Path

from mirobody._bundle import AXIS_CODE, RES_DIR, load_axis

AXES = ("COMPONENT", "PROPERTY", "TIME_ASPCT", "SYSTEM", "SCALE_TYP", "METHOD_TYP")
VARIANT = Path("AccessoryFiles/LinguisticVariants/zhCN5LinguisticVariant.csv")
HERE = Path(__file__).with_name("zh_CN")
PARTS_OUT = Path(RES_DIR) / "loinc" / "zh_CN_parts.tsv.gz"
NAMES = Path(RES_DIR) / "loinc" / "zh_CN_names.tsv"
SUBMISSION_COLUMNS = (
    "LOINC_NUM", *AXES, "CLASS", "SHORTNAME", "LONG_COMMON_NAME", "RELATEDNAMES2",
    "LinguisticVariantDisplayName", "ConsumerName",
)


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"missing release file: {path}")
    csv.field_size_limit(sys.maxsize)
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def bundle_codes() -> set[str]:
    table, order, _ = load_axis()
    return {table.field(i, AXIS_CODE) for i in range(len(order))}


def chinese_parts(main: dict[str, dict[str, str]], variant: dict[str, dict[str, str]],
                  reviewed: dict[tuple[str, str], str], code: str) -> list[str]:
    """The six Parts of `code`: the reviewed rendering of its English Part when there
    is one, else the variant's own text."""
    return [reviewed.get((axis, main[code][axis]), variant[code][axis]) if main[code][axis] else variant[code][axis]
            for axis in AXES]


def build_parts(main, variant, reviewed, codes) -> bytes:
    out = io.StringIO()
    w = csv.writer(out, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_NONE, escapechar="\\")
    w.writerow(("LOINC_NUM", *AXES))
    rows = 0
    for code in sorted(codes):  # str order is utf-8 byte order for these ASCII codes
        if code in variant and code in main:
            parts = chinese_parts(main, variant, reviewed, code)
            if any("\t" in p or "\n" in p for p in parts):
                raise SystemExit(f"{code}: a Part holds a tab or newline")
            w.writerow((code, *parts))
            rows += 1
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, compresslevel=9) as gz:
        gz.write(out.getvalue().encode("utf-8"))
    print(f"zh_CN_parts.tsv.gz: {rows} codes")
    return buf.getvalue()


def submission(main, variant, reviewed, consumer, names, path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUBMISSION_COLUMNS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for row in names:
            code = row["LOINC_NUM"]
            parts = chinese_parts(main, variant, reviewed, code)
            w.writerow({
                "LOINC_NUM": code, **dict(zip(AXES, parts, strict=True)),
                "CLASS": variant[code]["CLASS"], "SHORTNAME": "",
                "LONG_COMMON_NAME": row["LONG_COMMON_NAME"],
                "RELATEDNAMES2": variant[code]["RELATEDNAMES2"],
                "LinguisticVariantDisplayName": row["LinguisticVariantDisplayName"],
                "ConsumerName": consumer.get(main[code]["CONSUMER_NAME"], ""),
            })
    print(f"{path}: {len(names)} codes")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--loinc", required=True, type=Path, help="unpacked LOINC release directory")
    ap.add_argument("--submission", type=Path, help="also write the LinguisticVariant CSV for Regenstrief")
    args = ap.parse_args(argv)

    main_rows = {r["LOINC_NUM"]: r for r in read_csv(args.loinc / "LoincTable" / "Loinc.csv")}
    consumer_file = args.loinc / "AccessoryFiles" / "ConsumerName" / "ConsumerName.csv"
    for r in read_csv(consumer_file):
        if r["LoincNumber"] in main_rows:
            main_rows[r["LoincNumber"]]["CONSUMER_NAME"] = r["ConsumerName"]
    variant = {r["LOINC_NUM"]: r for r in read_csv(args.loinc / VARIANT)}
    reviewed = {(r["axis"], r["english"]): r["chinese"] for r in read_tsv(HERE / "parts.tsv")}

    PARTS_OUT.write_bytes(build_parts(main_rows, variant, reviewed, bundle_codes()))
    if args.submission:
        consumer = {r["english"]: r["chinese"] for r in read_tsv(HERE / "consumer.tsv")}
        submission(main_rows, variant, reviewed, consumer, read_tsv(NAMES), args.submission)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Build ``mirobody/res/fhir_loinc_bundle.tar.gz`` from a LOINC release.

Build-time only. Reads the licensed release directory and writes the members
``mirobody.engine`` reads at runtime, in the blob-plus-offsets shape that
the 1.4.x runtime index established (one utf-8 blob per table, an
int32 offset array, nothing allocated per entry):

    axis_fields.bin / axis_index.npz     one row per code, AXIS_FIELDS fields
    corpus_names.bin / corpus_names.npz  the LONG_COMMON_NAME of each row
    alias_keys.bin / alias_index.npz     folded designation -> rows (CSR)
    loinc_rank_bonus.npy                 COMMON_TEST_RANK tier per row
    loinc_skip.txt                       the ACTIVE codes the cut left out
    loinc_units.tsv                      EXAMPLE_UCUM_UNITS per code
    NOTICE / VERSION                     the LOINC notice; loinc-<release>+<date>-<digest>

The rows are the Tier-2 cut of ``translate_build.loinc_cut`` and nothing
else: LOINC content only, one corpus row per code, so a posting names a code
directly and the corpus name always has an axis row. Designations come from
``Loinc.csv`` (COMPONENT, SHORTNAME, LONG_COMMON_NAME, RELATEDNAMES2,
DisplayName, CONSUMER_NAME) and from every ``LinguisticVariants/*.csv`` the
release ships. Field contents are copied unchanged; rows are only ever
dropped (LOINC licence 5.8 section 3).

    python -m translate_build.build_bundle --loinc /path/to/Loinc_2.83
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import io
import re
import sys
import tarfile
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

from mirobody._bundle import AXIS_FIELDS, BUNDLE_PATH, RES_DIR
from mirobody.lexical import index_fold
from translate_build.loinc_cut import gate, read_whitelist

RELEASE_RE = re.compile(r"Loinc_(\d+\.\d+)")

#: Designation columns read from Loinc.csv and from each linguistic variant.
MAIN_DESIGNATIONS = ("COMPONENT", "SHORTNAME", "LONG_COMMON_NAME", "RELATEDNAMES2", "DisplayName", "CONSUMER_NAME")
VARIANT_DESIGNATIONS = (
    "COMPONENT", "SHORTNAME", "LONG_COMMON_NAME", "RELATEDNAMES2", "LinguisticVariantDisplayName", "ConsumerName",
)

#: Key filters, unchanged from the index this replaces: a key is at most 50
#: characters; a CJK key at least 2; a Latin key at least 3 with a letter.
#: A key naming more than 500 codes is a classifier word, not a name.
KEY_MAX_LEN = 50
CJK_MIN_LEN = 2
LATIN_MIN_LEN = 3
DF_CAP = 500
_CJK = re.compile(r"[぀-ヿ㐀-䶿一-鿿가-힯]")

#: COMMON_TEST_RANK tiers, unchanged: a soft tie-breaker among one alias's codes.
RANK_TIERS = ((100, 0.020), (1000, 0.015), (5000, 0.010), (20000, 0.005))

NOTICE_HEAD = """This bundle (fhir_loinc_bundle.tar.gz) contains data derived from LOINC(R)
(Logical Observation Identifiers Names and Codes), copyright (c) 1995
Regenstrief Institute, Inc. and the Logical Observation Identifiers
Names and Codes (LOINC) Committee.

This product includes all or a portion of the LOINC(R) table, LOINC panels
and forms file, LOINC document ontology file, and/or LOINC hierarchies file,
or is derived from one or more of the foregoing, subject to a license from
Regenstrief Institute, Inc. LOINC is a registered United States trademark
of Regenstrief Institute, Inc. Full license: https://loinc.org/license/
"""


def rank_bonus(rank: str) -> float:
    try:
        n = int(rank or 0)
    except ValueError:
        return 0.0
    if n <= 0:
        return 0.0
    for limit, bonus in RANK_TIERS:
        if n <= limit:
            return bonus
    return 0.0


def explode(raw: str) -> set[str]:
    """One designation field to its candidate surfaces: split on ``;``; a
    CJK chunk with spaces also yields its space-separated parts."""
    out: set[str] = set()
    for chunk in (raw or "").split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        out.add(chunk)
        if " " in chunk and _CJK.match(chunk):
            out.update(p for p in chunk.split() if p)
    return out


def key_ok(key: str) -> bool:
    if not key or len(key) > KEY_MAX_LEN:
        return False
    if _CJK.match(key):
        return len(key) >= CJK_MIN_LEN
    return len(key) >= LATIN_MIN_LEN and any(c.isalpha() for c in key)


def pack(strings) -> tuple[bytes, np.ndarray]:
    buf = bytearray()
    offs = np.zeros(len(strings) + 1, dtype=np.int32)
    for i, s in enumerate(strings):
        buf += s.encode("utf-8")
        offs[i + 1] = len(buf)
    return bytes(buf), offs


def npz(**arrays) -> bytes:
    out = io.BytesIO()
    np.savez_compressed(out, **arrays)
    return out.getvalue()


def content_digest(members: dict[str, bytes]) -> str:
    """The digest `scripts/stamp_bundle_version.py --check` recomputes."""
    h = hashlib.sha256()
    for name in sorted(n for n in members if n != "VERSION"):
        data = members[name]
        h.update(name.encode("utf-8"))
        h.update(b"\0")
        h.update(str(len(data)).encode("ascii"))
        h.update(b"\0")
        h.update(data)
    return h.hexdigest()[:12]


def read_release(loinc: Path, whitelist: dict[str, str]) -> tuple[list[dict[str, str]], list[str], Counter]:
    kept: list[dict[str, str]] = []
    skipped: list[str] = []
    reasons: Counter = Counter()
    with (loinc / "LoincTable" / "Loinc.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            why = gate(row)
            if why and row["LOINC_NUM"] in whitelist:
                why = ""
                reasons["whitelist"] += 1
            if not why and row["EXTERNAL_COPYRIGHT_NOTICE"].strip():
                why = "licence"
            if why:
                reasons[why] += 1
                if row["STATUS"] == "ACTIVE":
                    skipped.append(row["LOINC_NUM"])
                continue
            kept.append(row)
    kept.sort(key=lambda r: r["LOINC_NUM"])
    codes = [r["LOINC_NUM"] for r in kept]
    if len(set(codes)) != len(codes):
        raise SystemExit("Loinc.csv repeats a LOINC_NUM")
    return kept, sorted(skipped), reasons


def build_axis(kept: list[dict[str, str]]) -> tuple[bytes, bytes]:
    fields: list[str] = []
    codes: list[bytes] = []
    folded: list[bytes] = []
    for r in kept:
        component, lcn = r["COMPONENT"], r["LONG_COMMON_NAME"]
        head = component.split("^")[0].strip()
        fold_lcn = index_fold(lcn)
        fields += [
            r["LOINC_NUM"], index_fold(component), r["PROPERTY"], r["SCALE_TYP"], r["SYSTEM"], r["METHOD_TYP"],
            lcn, index_fold(head) if head else "", fold_lcn, r["TIME_ASPCT"], r["CLASS"],
        ]
        codes.append(r["LOINC_NUM"].encode("utf-8"))
        folded.append(fold_lcn.encode("utf-8"))
    if len(fields) != AXIS_FIELDS * len(kept):
        raise SystemExit(f"axis row width is {len(fields) // len(kept)}, mirobody._bundle.AXIS_FIELDS is {AXIS_FIELDS}")
    blob, off = pack(fields)
    order_code = np.array(sorted(range(len(codes)), key=lambda i: (codes[i], i)), dtype=np.int32)
    order_name = np.array(sorted(range(len(folded)), key=lambda i: (folded[i], i)), dtype=np.int32)
    return blob, npz(off=off, order_code=order_code, order_name=order_name)


def build_aliases(kept: list[dict[str, str]], loinc: Path) -> tuple[bytes, bytes, Counter]:
    row_of = {r["LOINC_NUM"]: i for i, r in enumerate(kept)}
    keys: dict[str, set[int]] = defaultdict(set)
    stats: Counter = Counter()

    def add(code: str, fields: dict[str, str], columns) -> None:
        row = row_of.get(code)
        if row is None:
            return
        for col in columns:
            for surface in explode(fields.get(col, "")):
                key = index_fold(surface)
                if key_ok(key):
                    keys[key].add(row)

    for r in kept:
        add(r["LOINC_NUM"], r, MAIN_DESIGNATIONS)
    stats["keys:en"] = len(keys)
    for path in sorted((loinc / "AccessoryFiles" / "LinguisticVariants").glob("*LinguisticVariant.csv")):
        before = len(keys)
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                add(row.get("LOINC_NUM", ""), row, VARIANT_DESIGNATIONS)
        stats[f"keys:{path.name.split('LinguisticVariant')[0]}"] = len(keys) - before

    kept_keys = sorted(k for k, rows in keys.items() if len(rows) <= DF_CAP)
    stats["keys:dropped-df"] = len(keys) - len(kept_keys)
    blob, koff = pack(kept_keys)
    for i in range(len(kept_keys) - 1):
        if kept_keys[i].encode("utf-8") >= kept_keys[i + 1].encode("utf-8"):
            raise SystemExit(f"alias keys not in utf-8 order at {i}")
    offsets = np.zeros(len(kept_keys) + 1, dtype=np.int32)
    rows: list[int] = []
    for i, k in enumerate(kept_keys):
        rows.extend(sorted(keys[k]))
        offsets[i + 1] = len(rows)
    stats["keys"] = len(kept_keys)
    stats["postings"] = len(rows)
    return blob, npz(keys_off=koff, offsets=offsets, rows=np.array(rows, dtype=np.int32)), stats


def notice(release: str, kept: int, skipped: int, reasons: Counter, alias_keys: int, langs: list[str]) -> str:
    lines = [NOTICE_HEAD, f"Source: LOINC release {release} (LoincTable/Loinc.csv, AccessoryFiles/LinguisticVariants/).", ""]
    lines += [
        "Bundle members and their LOINC sources:",
        "",
        f"  axis_fields.bin / axis_index.npz    {kept} codes: LOINC_NUM, COMPONENT, PROPERTY, SCALE_TYP,",
        "                                      SYSTEM, METHOD_TYP, LONG_COMMON_NAME, TIME_ASPCT, CLASS, each",
        "                                      copied unchanged, plus two folded copies for lookup.",
        "  corpus_names.bin / corpus_names.npz LONG_COMMON_NAME per code, same row order.",
        f"  alias_keys.bin / alias_index.npz    {alias_keys} folded designations -> codes, from COMPONENT,",
        "                                      SHORTNAME, LONG_COMMON_NAME, RELATEDNAMES2, DisplayName and",
        "                                      CONSUMER_NAME, and from these linguistic variants:",
        "                                      " + ", ".join(langs) + ".",
        "  loinc_rank_bonus.npy                a tie-breaker per row from COMMON_TEST_RANK.",
        f"  loinc_skip.txt                      {skipped} ACTIVE codes the cut leaves out (see below).",
        "  loinc_units.tsv                     EXAMPLE_UCUM_UNITS per code.",
        "",
        "The cut (translate_build/loinc_cut.py): STATUS ACTIVE; CLASSTYPE laboratory or",
        "clinical; CLASS families that never hold a reading dropped (surveys, documents,",
        "radiology, administrative, ...); SCALE_TYP Doc, Nar, -, Set and Multi dropped;",
        "H&P classes kept only at Qn or Ord; panels kept for the laboratory subclasses;",
        "the device base table and the catalogue whitelisted; rows carrying",
        "EXTERNAL_COPYRIGHT_NOTICE dropped rather than reproduced. Counts by reason:",
        "  " + ", ".join(f"{k} {v}" for k, v in sorted(reasons.items())),
        "",
        "Rows are dropped, never edited; every value here is a LOINC value. The input",
        "spellings under res/aliases_src/ and res/resolver_overrides.tsv are this",
        "project's own and are not LOINC names.",
        "",
        "Full LOINC license: https://loinc.org/license/",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--loinc", required=True, type=Path, help="unpacked LOINC release directory")
    ap.add_argument("--out", type=Path, default=Path(BUNDLE_PATH))
    ap.add_argument("--date", default=datetime.date.today().strftime("%Y.%m.%d"))
    ap.add_argument("--whitelist", type=Path, default=Path(__file__).with_name("whitelist.tsv"))
    args = ap.parse_args(argv)

    m = RELEASE_RE.search(str(args.loinc))
    if not m:
        return ap.error("--loinc must be a Loinc_<release> directory")
    release = m.group(1)

    kept, skipped, reasons = read_release(args.loinc, read_whitelist(args.whitelist))
    print(f"kept {len(kept)} codes; skipped ACTIVE {len(skipped)}; reasons {dict(sorted(reasons.items()))}")

    members: dict[str, bytes] = {}
    members["axis_fields.bin"], members["axis_index.npz"] = build_axis(kept)
    members["corpus_names.bin"], members["corpus_names.npz"] = (lambda p: (p[0], npz(off=p[1])))(
        pack([r["LONG_COMMON_NAME"] for r in kept])
    )
    members["alias_keys.bin"], members["alias_index.npz"], stats = build_aliases(kept, args.loinc)
    print("aliases:", dict(stats))
    rank = np.array([rank_bonus(r["COMMON_TEST_RANK"]) for r in kept], dtype=np.float32)
    buf = io.BytesIO()
    np.save(buf, rank)
    members["loinc_rank_bonus.npy"] = buf.getvalue()
    members["loinc_skip.txt"] = ("\n".join(skipped) + "\n").encode("utf-8")
    members["loinc_units.tsv"] = (
        "LOINC_NUM\tEXAMPLE_UCUM_UNITS\n"
        + "".join(f"{r['LOINC_NUM']}\t{r['EXAMPLE_UCUM_UNITS']}\n" for r in kept if r["EXAMPLE_UCUM_UNITS"].strip())
    ).encode("utf-8")
    langs = sorted(k.split(":", 1)[1] for k in stats if k.startswith("keys:") and k not in ("keys:en", "keys:dropped-df"))
    text = notice(release, len(kept), len(skipped), reasons, stats["keys"], langs)
    members["NOTICE"] = text.encode("utf-8")
    members["VERSION"] = f"loinc-{release}+{args.date}-{content_digest(members)}\n".encode("utf-8")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(args.out, "w:gz") as tf:
        for name in sorted(members):
            data = members[name]
            ti = tarfile.TarInfo(name=name)
            ti.size, ti.mtime, ti.mode = len(data), 0, 0o644
            tf.addfile(ti, io.BytesIO(data))
    Path(RES_DIR, "fhir_loinc_bundle.NOTICE").write_text(text, encoding="utf-8")
    for name in sorted(members):
        print(f"  {name:22s} {len(members[name]) / 1e6:7.2f} MB")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB): {members['VERSION'].decode().strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Re-cut the three tables the resolver reads, as byte blobs it can use as-is.

    python scripts/build_runtime_index.py            # rewrite the members
    python scripts/build_runtime_index.py --check    # verify, exit 1 on drift

**Why.** `OfflineResolver` held 921,172 alias keys, 677,643 corpus names and
97,314 axis rows as Python objects — about 1.6 million of them — because that
is what the artifacts hand you: `loinc_alias_index.npz` stored its keys as a
*pickled object array*, and `fhir_meta.csv.gz` and `loinc_axis.csv` are text
that has to be parsed. There is no way to read those formats without paying
for the objects, and the objects were 180 MB of the resolver's 339 MB.

The same content as a utf-8 blob plus an int32 offset array costs what the
text costs and nothing per entry. On disk it is a wash, because the tarball
compresses a blob about as well as it compressed the pickle:

    loinc_alias_index.npz  13.28 MB  ->  alias_keys.bin + alias_index.npz
    fhir_meta.csv.gz        6.90 MB  ->  corpus_names.bin + corpus_names.npz
    (loinc_axis.csv stays; axis_fields.bin + axis_index.npz are derived)

`fhir_meta.csv.gz` then leaves the wheel altogether — `engine.py` was its only
runtime reader; the passes that also read it are bundle-build tooling and are
pruned from the artifact already.

**The blobs are plain tar members, not arrays inside an .npz.** That is not
cosmetic: `np.load` on a compressed npz decompresses into an ndarray and
`.tobytes()` then copies it, so a 30 MB blob costs 75 MB of steady RSS once the
allocator keeps the freed arenas. `tarfile.extractfile().read()` produces the
`bytes` object directly, in one allocation. Measured, same machine: 75 MB vs
2 MB. Only the small offset/order arrays go in an .npz, where the copy is
irrelevant.

**This is not the memory-mapped variant**, which was considered and rejected:
mmap needs the data uncompressed ON DISK, taking the installed `res/` from
22 MB to ~120 MB to save the last few tens of MB of RAM. Reading a compressed
blob into one `bytes` gets nearly all of it back and costs nobody a download.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import sys

import numpy as np

from mirobody._bundle import BUNDLE_PATH, RES_DIR, read_member
from mirobody.lexical import index_fold

META_PATH = os.path.join(RES_DIR, "fhir_meta.csv.gz")

#: (blob member, index member) per table.
#: Records which inputs the blobs were derived from — see `input_digest`.
STAMP_MEMBER = "runtime_index.inputs"

MEMBERS = {
    "alias": ("alias_keys.bin", "alias_index.npz"),
    "names": ("corpus_names.bin", "corpus_names.npz"),
    "axis": ("axis_fields.bin", "axis_index.npz"),
}

#: Axis fields, row-major. 7 is the analyte HEAD — COMPONENT with any
#: `^challenge` suffix stripped, folded; 8 is the folded LONG_COMMON_NAME. Both
#: are stored rather than derived at runtime because the split and the fold
#: must happen in that order on the RAW value, and deriving one from the other
#: would differ wherever NFKC produces a caret that was not in the source.
AXIS_FIELDS = 9


def _pack(strings) -> tuple[bytes, np.ndarray]:
    buf = bytearray()
    offs = np.zeros(len(strings) + 1, dtype=np.int32)
    for i, s in enumerate(strings):
        buf += s.encode("utf-8")
        offs[i + 1] = len(buf)
    return bytes(buf), offs


#: The members these blobs are derived FROM. `--check` hashes them and compares
#: against what the last build recorded, because "someone re-ran `loinc-alias`
#: and did not re-cut the blob" is otherwise invisible: the runtime keeps
#: reading a stale index that is present, well-formed, and wrong.
INPUTS = ("loinc_alias_index.npz", "loinc_axis.csv")


def input_digest(bundle: str) -> str:
    """SHA-256 over the derived tables' inputs, in a fixed order."""
    import hashlib

    h = hashlib.sha256()
    for name in INPUTS:
        raw = read_member(name, bundle_path=bundle)
        h.update(name.encode())
        h.update(b"\0")
        h.update(hashlib.sha256(raw or b"").digest())
    with open(META_PATH, "rb") as f:
        h.update(hashlib.sha256(f.read()).digest())
    return h.hexdigest()[:16]


def _npz(**arrays) -> bytes:
    out = io.BytesIO()
    np.savez_compressed(out, **arrays)
    return out.getvalue()


def build_alias(bundle: str) -> tuple[bytes, bytes]:
    raw = read_member("loinc_alias_index.npz", bundle_path=bundle)
    if raw is None:
        sys.exit(f"loinc_alias_index.npz not in {bundle} — run `git lfs pull`")
    with np.load(io.BytesIO(raw), allow_pickle=True) as z:
        keys = [str(k) for k in z["aliases"]]
        offsets, rows = z["offsets"].astype(np.int32), z["rows"].astype(np.int32)

    blob, koff = _pack(keys)
    _assert_sorted(blob, koff, "alias keys")
    return blob, _npz(keys_off=koff, offsets=offsets, rows=rows)


def build_names() -> tuple[bytes, bytes]:
    """Column 0 of fhir_meta.csv.gz, parsed here instead of on every load.

    Parsed with `csv`, not sliced at the first comma: 33,662 of the 677,643
    names are quoted because they contain one (`Intraductal carcinoma, low
    grade`), and a byte slice would truncate every one of them.
    """
    with gzip.open(META_PATH, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)
        names = [row[0] for row in reader]
    blob, off = _pack(names)
    return blob, _npz(off=off)


def build_axis(bundle: str) -> tuple[bytes, bytes]:
    raw = read_member("loinc_axis.csv", bundle_path=bundle)
    if raw is None:
        sys.exit(f"loinc_axis.csv not in {bundle} — run `git lfs pull`")
    reader = csv.reader(io.StringIO(raw.decode("utf-8")))
    col = {name: i for i, name in enumerate(next(reader))}

    fields: list[str] = []
    codes: list[bytes] = []
    folded: list[bytes] = []
    for row in reader:
        component = row[col["COMPONENT"]]
        lcn = row[col["LONG_COMMON_NAME"]]
        # COMPONENT is the analyte axis, and the part before `^` is the analyte
        # itself with any challenge/timing modifier stripped: 1558-6 is
        # `Glucose^post CFst`, 2339-0 is `Glucose`. Same head = same substance
        # measured differently; different head = a different test.
        head = component.split("^")[0].strip()
        fold_lcn = index_fold(lcn)
        fields += [
            row[col["LOINC_NUM"]],
            index_fold(component),
            row[col["PROPERTY"]],
            row[col["SCALE_TYP"]],
            row[col["SYSTEM"]],
            row[col["METHOD_TYP"]],
            lcn,
            index_fold(head) if head else "",
            fold_lcn,
        ]
        codes.append(row[col["LOINC_NUM"]].encode("utf-8"))
        folded.append(fold_lcn.encode("utf-8"))

    blob, off = _pack(fields)
    # A duplicate key keeps the LAST row, which is what `dict[key] = i` did.
    order_code = np.array(sorted(range(len(codes)), key=lambda i: (codes[i], i)), dtype=np.int32)
    order_name = np.array(sorted(range(len(folded)), key=lambda i: (folded[i], i)), dtype=np.int32)
    return blob, _npz(off=off, order_code=order_code, order_name=order_name)


def _assert_sorted(blob: bytes, off: np.ndarray, what: str) -> None:
    bad = [
        i for i in range(len(off) - 2)
        if blob[off[i]:off[i + 1]] > blob[off[i + 1]:off[i + 2]]
    ]
    assert not bad, (
        f"{what} are not in utf-8 byte order at {bad[:3]}; the runtime bisect "
        "would return another entry's row instead of failing"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bundle", default=BUNDLE_PATH)
    ap.add_argument("--check", action="store_true", help="verify, do not write")
    args = ap.parse_args()

    if args.check:
        missing = [
            name
            for pair in MEMBERS.values()
            for name in pair
            if read_member(name, bundle_path=args.bundle) is None
        ]
        if missing:
            print(f"FAIL {args.bundle}: missing runtime members {missing}")
            return 1
        stamped = read_member(STAMP_MEMBER, bundle_path=args.bundle)
        want = input_digest(args.bundle)
        if stamped is None:
            print(f"FAIL {args.bundle}: no {STAMP_MEMBER}; re-run this script without --check")
            return 1
        if stamped.decode().strip() != want:
            print(
                f"FAIL {args.bundle}: the derived blobs are stale — their inputs "
                f"hash to {want}, the last build recorded "
                f"{stamped.decode().strip()}. Re-run this script, then "
                "scripts/stamp_bundle_version.py."
            )
            return 1
        print(f"OK   {args.bundle}: {len(MEMBERS) * 2} runtime members, derived from inputs {want}")
        return 0

    new: dict[str, bytes] = {}
    for table, builder in (
        ("alias", lambda: build_alias(args.bundle)),
        ("names", build_names),
        ("axis", lambda: build_axis(args.bundle)),
    ):
        blob_name, index_name = MEMBERS[table]
        blob, index = builder()
        new[blob_name] = blob
        new[index_name] = index
        print(f"{table:6s} {blob_name:20s} {len(blob)/1e6:6.1f} MB   "
              f"{index_name:20s} {len(index)/1e6:5.2f} MB")

    new[STAMP_MEMBER] = (input_digest(args.bundle) + "\n").encode()

    # One rewrite for all seven. `write_member` repacks the whole 23 MB gzip per
    # call, so six calls would gzip 138 MB to change 45 MB of members.
    _replace_members(args.bundle, new)
    print("now re-run scripts/stamp_bundle_version.py")
    return 0


def _replace_members(path: str, new: dict[str, bytes]) -> None:
    """Add or replace several members in one atomic repack."""
    import tarfile
    import tempfile

    parent = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".bundle-", suffix=".tar.gz", dir=parent)
    os.close(fd)
    try:
        keep: list[tuple[tarfile.TarInfo, bytes]] = []
        with tarfile.open(path, "r:gz") as inp:
            for m in inp.getmembers():
                if m.name in new or not m.isfile():
                    continue
                f = inp.extractfile(m)
                keep.append((m, f.read() if f is not None else b""))
        with tarfile.open(tmp, "w:gz") as out:
            for m, data in keep:
                ti = tarfile.TarInfo(name=m.name)
                ti.size, ti.mtime, ti.mode = len(data), m.mtime, m.mode
                out.addfile(ti, io.BytesIO(data))
            for name, data in new.items():
                ti = tarfile.TarInfo(name=name)
                ti.size = len(data)
                out.addfile(ti, io.BytesIO(data))
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Stamp a VERSION member into ``mirobody/res/fhir_loinc_bundle.tar.gz``.

The package version and the corpus version are different facts, and until now
only the first existed. A consumer that generates a seed from the bundle at
build time and pins the package at runtime had nothing to assert the two came
from the same release against — ``mirobody==1.2.2`` says nothing about which
LOINC cut produced the alias index inside it, and re-cutting the bundle without
bumping the package was possible and undetectable.

The stamp is ``loinc-<release>+<YYYY.MM.DD>-<12 hex>``:

* the LOINC release the bundle was cut from. This is the half a consumer
  actually wants to pin against — "which vocabulary am I resolving with" is a
  different question from "which package version", and until 1.3.0 neither the
  package nor the data answered it. The NOTICE has to carry it anyway (LOINC
  license §9: each copy must include "the appropriate version number");
* the date the bundle was cut (``--date``, default today), for humans;
* a SHA-256 over every other member's ``(name, bytes)`` in sorted order, so the
  stamp **cannot** disagree with the content. Restamping unchanged content on a
  different day changes only the date; changing any member changes the hex.

    python scripts/stamp_bundle_version.py            # stamp res/…tar.gz
    python scripts/stamp_bundle_version.py --check    # verify, exit 1 on drift

``--check`` is the CI gate: it recomputes the digest and fails when the stamp
no longer matches what the bundle holds.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import sys
import tarfile

from mirobody._bundle import BUNDLE_PATH, VERSION_MEMBER, bundle_version


def content_digest(path: str) -> str:
    """SHA-256 over every member except VERSION, in sorted name order."""
    h = hashlib.sha256()
    with tarfile.open(path, "r:gz") as tf:
        members = sorted(
            (m for m in tf.getmembers() if m.isfile() and m.name != VERSION_MEMBER),
            key=lambda m: m.name,
        )
        for m in members:
            f = tf.extractfile(m)
            data = f.read() if f is not None else b""
            h.update(m.name.encode("utf-8"))
            h.update(b"\0")
            h.update(str(len(data)).encode("ascii"))
            h.update(b"\0")
            h.update(data)
    return h.hexdigest()[:12]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", default=BUNDLE_PATH)
    ap.add_argument("--date", default=datetime.date.today().strftime("%Y.%m.%d"))
    ap.add_argument("--loinc", default="2.82", help="the LOINC release the bundle was cut from")
    ap.add_argument("--check", action="store_true", help="verify the stamp, do not write")
    args = ap.parse_args()

    digest = content_digest(args.bundle)
    stamped = bundle_version(bundle_path=args.bundle)

    if args.check:
        if not stamped:
            print(f"FAIL {args.bundle}: no {VERSION_MEMBER} member — run this script without --check")
            return 1
        if not stamped.startswith("loinc-"):
            print(f"FAIL {args.bundle}: stamp {stamped!r} does not name a LOINC release")
            return 1
        if not stamped.endswith(f"-{digest}"):
            print(f"FAIL {args.bundle}: stamp {stamped!r} does not match content digest {digest!r}")
            return 1
        print(f"OK   {args.bundle}: {stamped}")
        return 0

    # Imported here so `--check` stays on the read-only path.
    from mirobody.indicator.fhir.embeddings.bundle import write_member

    version = f"loinc-{args.loinc}+{args.date}-{digest}"
    write_member(VERSION_MEMBER, (version + "\n").encode("utf-8"), bundle_path=args.bundle)
    bundle_version.cache_clear()
    print(f"stamped {args.bundle}: {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

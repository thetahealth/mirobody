#!/usr/bin/env python3
"""Fail the build if a release artifact does not carry the engine's data bundles.

Two ways an artifact silently loses its data, both of which have already
happened:

1. ``actions/checkout`` without ``lfs: true`` — the Git LFS files are replaced
   by 133-byte pointer stubs, and the build packages the stubs.
2. ``[tool.setuptools.package-data]`` missing a glob — the file simply is not
   in the artifact at all.

Either way ``import mirobody`` still succeeds, so an import smoke test does not
catch it; only the first real ``resolve()`` call fails, in a user's terminal.
Release 1.0.62 shipped both defects at once. This script is the gate — and it
checks BOTH artifacts ``python -m build`` produces, because twine uploads both:
pip serves the sdist to any platform/version the wheel doesn't match, so a
broken sdist ships too.

    python scripts/check_wheel_data.py dist/*
"""

from __future__ import annotations

import sys
import tarfile
import zipfile

# path inside the artifact (wheel layout) -> minimum plausible size in bytes
REQUIRED = {
    "mirobody/res/fhir_loinc_bundle.tar.gz": 1_000_000,
    "mirobody/res/fhir_meta.csv.gz": 1_000_000,
    "mirobody/res/aliases_src/zh.tsv": 100_000,
    "mirobody/res/aliases_src/ja.tsv": 100_000,
    "mirobody/res/resolver_overrides.tsv": 1_000,
}

# The other direction, and it is worth a gate of its own: the package-data globs
# are deliberately broad (``**/*.bin``, ``**/*.npy``) so a new data file cannot
# be forgotten, which means anything dropped into ``res/`` ships by default.
# These four are read by NOTHING at runtime — the build tooling and the v2
# semantic pipeline are their only callers — and they were 28 MB of every
# wheel. One of them, the SNOMED bundle, also put an Affiliate-Licence
# obligation on every downstream recipient.
FORBIDDEN = (
    "mirobody/res/fhir_concept_graph.bin",
    "mirobody/res/fhir_id_map.npy",
    "mirobody/res/fhir_taxonomy.bin",
    "mirobody/res/fhir_snomed_ct_bundle.tar.gz",
)

# Git LFS pointer files start with this line and are a few hundred bytes.
LFS_MAGIC = b"version https://git-lfs.github.com/spec/"


def _wheel_entries(path: str):
    """Yield (member_path, size, head_bytes) for a wheel."""
    with zipfile.ZipFile(path) as z:
        for info in z.infolist():
            yield info.filename, info.file_size, z.read(info.filename)[: len(LFS_MAGIC)]


def _sdist_entries(path: str):
    """Yield (member_path, size, head_bytes) for an sdist.

    Members are prefixed ``<name>-<version>/``; strip it so the same REQUIRED
    keys apply to both artifact kinds.
    """
    with tarfile.open(path) as t:
        for m in t.getmembers():
            if not m.isfile():
                continue
            rel = m.name.split("/", 1)[1] if "/" in m.name else m.name
            f = t.extractfile(m)
            head = f.read(len(LFS_MAGIC)) if f else b""
            yield rel, m.size, head


def check(path: str) -> list[str]:
    entries = _wheel_entries(path) if path.endswith(".whl") else _sdist_entries(path)
    seen: dict[str, tuple[int, bytes]] = {}
    stowaways: list[tuple[str, int]] = []
    for name, size, head in entries:
        if name in REQUIRED:
            seen[name] = (size, head)
        elif name in FORBIDDEN:
            stowaways.append((name, size))

    problems: list[str] = []
    for name, size in stowaways:
        problems.append(
            f"UNWANTED  {name} — {size/1e6:.1f} MB that no runtime code path reads; "
            "add it to [tool.setuptools.exclude-package-data]"
        )
    for member, min_size in REQUIRED.items():
        if member not in seen:
            problems.append(f"MISSING   {member} — not in the artifact (check package-data globs)")
            continue
        size, head = seen[member]
        if head == LFS_MAGIC:
            problems.append(
                f"LFS STUB  {member} — {size} B pointer file; the checkout needs `lfs: true`"
            )
        elif size < min_size:
            problems.append(f"TOO SMALL {member} — {size} B, expected >= {min_size} B")
    return problems


def main() -> int:
    artifacts = [a for a in sys.argv[1:] if a.endswith((".whl", ".tar.gz"))]
    if not artifacts:
        print("usage: check_wheel_data.py dist/*  (wheels and sdists)", file=sys.stderr)
        return 2

    failed = False
    for artifact in artifacts:
        problems = check(artifact)
        if problems:
            failed = True
            print(f"\n{artifact}: data bundles are BROKEN")
            for p in problems:
                print(f"  {p}")
        else:
            print(f"{artifact}: all {len(REQUIRED)} engine data bundles present and real; none of the {len(FORBIDDEN)} build-time-only artifacts shipped")

    if failed:
        print(
            "\nAn artifact in this state installs fine and imports fine, but "
            "`mirobody resolve` raises for every user. Refusing to publish.",
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

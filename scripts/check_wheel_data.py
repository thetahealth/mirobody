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
    "mirobody/res/loinc/fhir_loinc_bundle.tar.gz": 1_000_000,
    "mirobody/res/loinc/aliases_src/zh.tsv": 100_000,
    "mirobody/res/loinc/resolver_overrides.tsv": 1_000,
    # The CLASS gate. Absent, `_skipped()` logs a warning and every radiology,
    # dental and cell-marker code becomes reachable again — `癌胚抗原` would go
    # back to answering the flow-cytometry marker. A silent recall regression
    # is exactly what this gate exists to catch.
    # 1.4.0: the indicator catalogue, its Chinese labels and the dose-form
    # table are read at import time by `mirobody.kernel.metrics` / `mirobody.kernel.meds`.
    "mirobody/res/catalog/metrics.tsv": 40_000,
    "mirobody/res/catalog/labels/zh.tsv": 10_000,
    "mirobody/res/dose_forms.tsv": 300,
    # UCUM, verbatim: the License requires the whole file, its notice and the
    # licence text to travel together (`res/ucum/ucum-essence.NOTICE`).
    "mirobody/res/ucum/ucum-essence.xml": 80_000,
    "mirobody/res/ucum/ucum-essence.NOTICE": 500,
    "mirobody/res/ucum/UCUM-LICENSE.md": 10_000,
    "mirobody/kernel/decoders/samples/garmin/dailies.json": 500,
}

# The other direction, and it is worth a gate of its own: the package-data globs
# are deliberately broad (``**/*.bin``, ``**/*.npy``) so a new data file cannot
# be forgotten, which means anything dropped into ``res/`` ships by default.
# These four are read by NOTHING at runtime — the build tooling and the v2
# semantic pipeline are their only callers — and they were 28 MB of every
# wheel. One of them, the SNOMED bundle, also put an Affiliate-Licence
# obligation on every downstream recipient.
FORBIDDEN = (
    # 1.5.0: a UMLS-derived Japanese alias file (MSHJPN / MDRJPN) that was
    # listed as a LOINC linguistic variant; LOINC has none for Japanese.
    "mirobody/res/loinc/aliases_src/ja.tsv",
    # 1.5.0: the five machine-derived language files. Their claimed upstream
    # was LOINC's LinguisticVariants, and a term-by-term measurement did not
    # confirm it (de 68.5%, es 86.4%, fr 76.1%, ko 80.6%, ru 75.2%). This
    # release resolves English first and Chinese beside it, so they are gone
    # rather than carried unaudited. What LOINC itself publishes for those
    # languages is still in the bundle's alias index, which is built from the
    # release: 7,612 of their 9,866 terms resolve without them.
    "mirobody/res/loinc/aliases_src/de.tsv",
    "mirobody/res/loinc/aliases_src/es.tsv",
    "mirobody/res/loinc/aliases_src/fr.tsv",
    "mirobody/res/loinc/aliases_src/ko.tsv",
    "mirobody/res/loinc/aliases_src/ru.tsv",
    "mirobody/res/loinc/fhir_concept_graph.bin",
    "mirobody/res/loinc/fhir_snomed_ct_bundle.tar.gz",
    # 1.3.0: superseded by `corpus_names.bin` inside the bundle. The resolver
    # used to parse this CSV on every load, which is where 677,643 of its
    # Python strings came from.
    "mirobody/res/loinc/fhir_meta.csv.gz",
    # 1.5.0: deleted, not merely unshipped. It was the manual overlay for
    # `mirobody indicator analyte-digit`, a build command that went with
    # `indicator/`; nothing in translate_build or the resolver reads digits.
    "mirobody/res/loinc/analyte_digit_src/analyte_digit_curated.tsv",
    # fhir_id_map.npy is not listed because it no longer exists: it mapped
    # canonical ids to `fhir_indicators.id`, one database's PRIMARY KEYS, and was
    # deleted rather than merely unshipped.
)

# Same standard, applied to CODE. These two subtrees are 19,000 lines nobody
# who installs the package can run — the bundle-build passes need raw
# LOINC/UMLS releases that are licensed per user, and the v2 semantic pipeline
# needs a multi-GB embedding matrix that ships on a volume. They are pruned by
# scripts/build_backend.py::_BUILD_ONLY_CODE (wheel) and MANIFEST.in (sdist).
#
# The gate matters because the prune is easy to defeat by accident: any runtime
# module that imports into these trees makes them load-bearing again, which is
# exactly the state `engine.py` was in until 1.3.0 (it read the shipped bundle
# through `embeddings.bundle` and folded keys with `embeddings.alias._normalize`).
FORBIDDEN_PREFIXES = (
    # 1.4.0: the demo data lives in the repo-root `demo/`, beside `frontend/`
    # and for the same reason. The application is a checkout, so a library
    # install paid for a demo it can never run. A prefix, not the file names:
    # 1.4.4 replaced a vendored fixture with generated files, and the gate
    # should not have to be edited every time one is added.
    "mirobody/demo/",
)

# The bundle members a `pip install` must have, and the ones it must not. The
# runtime reads the first list on every `resolve()`; the second is inputs to the
# build passes, repacked out of the shipped copy by
# scripts/build_backend.py::_BUNDLE_RUNTIME_MEMBERS. Shipping them was 17 MB of
# every artifact that no installed code path could open.
BUNDLE = "mirobody/res/loinc/fhir_loinc_bundle.tar.gz"
BUNDLE_REQUIRED = (
    "VERSION", "alias_keys.bin", "alias_index.npz",
    "corpus_names.bin", "corpus_names.npz",
    "axis_fields.bin", "axis_index.npz", "loinc_rank_bonus.npy", "loinc_skip.txt",
    # NOTICE is a licence obligation, not a convenience: LOINC 5.8 requires the
    # copyright notice to travel with the data. loinc_units.tsv is the
    # EXAMPLE_UCUM_UNITS column the unit gate reads.
    "NOTICE", "loinc_units.tsv",
)
BUNDLE_FORBIDDEN = (
    "loinc_alias_index.npz", "loinc_axis.csv", "loinc_demote.txt",
    "fhir_dose_index.npz", "analyte_digit.tsv", "analyte_digit_curated.tsv",
)

#: UCUM ships verbatim; a different digest is an edited copy, which its License
#: forbids distributing. Kept equal to `mirobody.units.essence.UCUM_ESSENCE_SHA256`
#: (the local suite compares them); read here without importing the package.
UCUM_ESSENCE = "mirobody/res/ucum/ucum-essence.xml"
UCUM_ESSENCE_SHA256 = "dfccea1b5dc284245ebae97edd1dc03c45864da4e87df55bc9851797b4fd0b61"

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


def _bundle_members(path: str) -> list[str] | None:
    """Member names inside the shipped bundle, or None if it is not readable."""
    import io

    try:
        if path.endswith(".whl"):
            with zipfile.ZipFile(path) as z:
                data = z.read(BUNDLE)
        else:
            with tarfile.open(path) as t:
                m = next(mm for mm in t.getmembers() if mm.name.endswith(BUNDLE))
                data = t.extractfile(m).read()
        with tarfile.open(fileobj=io.BytesIO(data)) as tf:
            return [mm.name for mm in tf.getmembers()]
    except Exception:
        return None


def _member_bytes(path: str, member: str) -> bytes | None:
    try:
        if path.endswith(".whl"):
            with zipfile.ZipFile(path) as z:
                return z.read(member)
        with tarfile.open(path) as t:
            m = next(mm for mm in t.getmembers() if mm.name.endswith(member))
            return t.extractfile(m).read()
    except Exception:
        return None


def check(path: str) -> list[str]:
    entries = _wheel_entries(path) if path.endswith(".whl") else _sdist_entries(path)
    seen: dict[str, tuple[int, bytes]] = {}
    stowaways: list[tuple[str, int]] = []
    code_stowaways: list[tuple[str, int]] = []
    for name, size, head in entries:
        if name in REQUIRED:
            seen[name] = (size, head)
        elif name in FORBIDDEN:
            stowaways.append((name, size))
        elif name.startswith(FORBIDDEN_PREFIXES):
            code_stowaways.append((name, size))

    problems: list[str] = []
    # Tests and their snapshots live in `tests/` outside the package. A
    # `test_*.py` or a `goldens/` inside the wheel means one was put back in the
    # package tree; the build no longer prunes them, so this is the gate.
    test_stowaways = [
        name for name, _, _ in entries
        if name.rsplit("/", 1)[-1].startswith("test_") or "/goldens/" in name
        or name.endswith("/conftest.py") or "/tests/" in name
    ]
    if test_stowaways:
        problems.append(
            f"UNWANTED  {len(test_stowaways)} test file(s) inside the wheel — tests belong in "
            f"tests/, outside the package (first: {test_stowaways[0]})"
        )
    for name, size in stowaways:
        problems.append(
            f"UNWANTED  {name} — {size/1e6:.1f} MB that no runtime code path reads; "
            "add it to scripts/build_backend.py::_BUILD_ONLY_DATA and MANIFEST.in"
        )
    if code_stowaways:
        total = sum(s for _, s in code_stowaways)
        problems.append(
            f"UNWANTED  {len(code_stowaways)} files ({total/1e6:.1f} MB) under "
            f"{'/, '.join(FORBIDDEN_PREFIXES)} — trees no install can run; "
            "check scripts/build_backend.py::_BUILD_ONLY_CODE "
            f"and MANIFEST.in (first: {code_stowaways[0][0]})"
        )
    members = _bundle_members(path)
    if members is None:
        problems.append(f"UNREADABLE {BUNDLE} — cannot list its members")
    else:
        for m in BUNDLE_REQUIRED:
            if m not in members:
                problems.append(
                    f"MISSING   {BUNDLE}:{m} — the resolver reads this on every "
                    "call; rebuild with python -m translate_build.build_bundle"
                )
        for m in BUNDLE_FORBIDDEN:
            if m in members:
                problems.append(
                    f"UNWANTED  {BUNDLE}:{m} — a build-time input no install can "
                    "use; check scripts/build_backend.py::_BUNDLE_RUNTIME_MEMBERS"
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

    import hashlib

    raw = _member_bytes(path, UCUM_ESSENCE)
    if raw is not None and hashlib.sha256(raw).hexdigest() != UCUM_ESSENCE_SHA256:
        problems.append(f"MODIFIED  {UCUM_ESSENCE} — not the published UCUM file (digest differs)")
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
            print(
                f"{artifact}: all {len(REQUIRED)} engine data bundles present and real; "
                f"none of the {len(FORBIDDEN)} build-time-only artifacts or "
                f"{len(FORBIDDEN_PREFIXES)} build-time-only code trees shipped"
            )

    if failed:
        print(
            "\nAn artifact in this state installs fine and imports fine, but "
            "`mirobody resolve` raises for every user. Refusing to publish.",
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

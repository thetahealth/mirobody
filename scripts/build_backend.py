"""Build backend: setuptools, minus the build-time-only data and code.

The local regression suite lives in a gitignored `tests/` at the repo root and
never reaches a build. What DOES reach one is `mirobody/tests/`, the gate suite
that ships in the repository because each module is EVIDENCE for a public claim
— the resolver score the README links, the README gates, the export tables.
Useful to anyone with a checkout, useless in someone's site-packages, so the
hook prunes the whole directory (:func:`_should_drop`), and
`scripts/check_wheel_data.py` fails the build if one comes back. Also dropped: the terminology artifacts nothing at
runtime reads (:data:`_BUILD_ONLY_DATA`) and the two bundle-build code trees
nobody who installs the package can run (:data:`_BUILD_ONLY_CODE`).

`[tool.setuptools.exclude-package-data]` cannot express the data half, because
the broad `**/*.bin` / `**/*.npy` globs in `package-data` win, and the exclusion
is not applied to files the include globs already matched. Rather than narrow
the include globs (which is how a newly added data file gets silently
forgotten), the exclusion happens here, where it is one list with the reason
next to each entry, and `scripts/check_wheel_data.py` fails the build if any
of it comes back.
"""

from __future__ import annotations

import os

from setuptools import build_meta as _orig

# Re-export the parts of the PEP 517 interface we do not change.
#
# The EDITABLE hooks matter as much as the build ones. A backend that defines
# `build_wheel` but not `build_editable` makes `pip install -e .` fail with
# `AttributeError: module 'build_backend' has no attribute 'build_editable'` —
# which is every contributor's first command, and exactly what this backend did
# until a first-time-contributor walkthrough tried it. An editable install
# points at the source tree, so there is nothing to prune: pass them straight
# through.
prepare_metadata_for_build_wheel = _orig.prepare_metadata_for_build_wheel
get_requires_for_build_wheel = _orig.get_requires_for_build_wheel
get_requires_for_build_sdist = _orig.get_requires_for_build_sdist

build_editable = _orig.build_editable
get_requires_for_build_editable = _orig.get_requires_for_build_editable
prepare_metadata_for_build_editable = _orig.prepare_metadata_for_build_editable

#: 28 MB of `mirobody/res/` that NO runtime code path reads — grep server/,
#: agent/, collect/, mcp/ and task/ for `concept_graph` or `taxonomy` and it comes
#: back empty. Their readers are `mirobody/indicator/`'s bundle-build tooling
#: (which runs from a git checkout) and the v2 semantic pipeline (which also
#: needs a ~200 MB embedding matrix that is not distributed). Shipping them made
#: `pip install mirobody` more than half data the installed code cannot use.
#:
#: The SNOMED bundle carries a second cost: its NOTICE requires every downstream
#: recipient to hold a SNOMED CT Affiliate Licence. Not shipping it keeps that
#: obligation off every pip user.
#:
#: scripts/check_wheel_data.py fails the build if any of them reappears.
_BUILD_ONLY_DATA = frozenset({
    "mirobody/res/loinc/fhir_concept_graph.bin",
    "mirobody/res/loinc/fhir_snomed_ct_bundle.tar.gz",
    # 1.3.0: the resolver reads `corpus_names.bin` out of the bundle instead of
    # parsing this on every load. `engine.py` was its only runtime reader; the
    # passes that still read it are bundle-build tooling, which does not ship.
    "mirobody/res/loinc/fhir_meta.csv.gz",
})

#: The bundle members a `pip install` can actually use. Everything else in
#: `fhir_loinc_bundle.tar.gz` is an INPUT to the build passes — the pickled
#: alias index that `translate_build/build_bundle.py` writes as `alias_keys.bin`, the
#: axis CSV it turns into `axis_fields.bin`, the dose/demote/analyte tables that
#: only the pruned `embeddings/` and `resolve/` trees read.
#:
#: They stay in the bundle in git, because that is the artifact a contributor
#: rebuilds from and splitting it in two would mean teaching six build modules
#: which half to look in. They are repacked out of the copy that ships, which
#: is the same call this file already makes about `res/` files — just one level
#: further in.
_BUNDLE_PATH = "mirobody/res/loinc/fhir_loinc_bundle.tar.gz"
_BUNDLE_RUNTIME_MEMBERS = frozenset({
    "VERSION",
    "alias_keys.bin",
    "alias_index.npz",
    "corpus_names.bin",
    "corpus_names.npz",
    "axis_fields.bin",
    "axis_index.npz",
    "loinc_rank_bonus.npy",
    "loinc_skip.txt",
    # NOTICE travels with the data because the LOINC licence says it must.
    "NOTICE",
    "loinc_units.tsv",
})


def _slim_bundle(data: bytes) -> bytes:
    """Repack the terminology bundle with only `_BUNDLE_RUNTIME_MEMBERS`."""
    import io
    import tarfile

    keep: list[tuple[tarfile.TarInfo, bytes]] = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as inp:
        for m in inp.getmembers():
            if not m.isfile() or m.name not in _BUNDLE_RUNTIME_MEMBERS:
                continue
            f = inp.extractfile(m)
            keep.append((m, f.read() if f is not None else b""))
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tf:
        for m, payload in keep:
            ti = tarfile.TarInfo(name=m.name)
            ti.size, ti.mtime, ti.mode = len(payload), m.mtime, m.mode
            tf.addfile(ti, io.BytesIO(payload))
    return out.getvalue()

#: Code that ships in git but has no business in anyone's site-packages.
#:
#: 1.5.0 emptied this. It held `indicator/`'s two build trees and the CLI that
#: drove them, 19,000 lines nobody who installed the package could run; the
#: whole package is deleted, so there is nothing left to prune. The tuple and
#: the gate in `scripts/check_wheel_data.py` stay, because the next tree that
#: ships-but-cannot-run should be caught rather than argued about:
#: `translate_build/` is the current one, and it is outside `mirobody/`.
_BUILD_ONLY_CODE: tuple[str, ...] = ()


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    name = _orig.build_wheel(wheel_directory, config_settings, metadata_directory)
    _rewrite_wheel(os.path.join(wheel_directory, name))
    return name


def build_sdist(sdist_directory, config_settings=None):
    """MANIFEST.in handles the file-level exclusions; the bundle needs more.

    `_BUNDLE_RUNTIME_MEMBERS` is a decision about members INSIDE a tarball, and
    no packaging config can express that, so the produced sdist is repacked
    with a slimmed bundle. pip serves the sdist to anything the wheel does not
    match, so an sdist carrying 17 MB the runtime cannot read is the same
    defect as a wheel carrying it.
    """
    name = _orig.build_sdist(sdist_directory, config_settings)
    _slim_sdist(os.path.join(sdist_directory, name))
    return name


def _slim_sdist(path: str) -> None:
    import io
    import tarfile

    members: list[tuple[tarfile.TarInfo, bytes | None]] = []
    with tarfile.open(path, "r:gz") as inp:
        for m in inp.getmembers():
            if not m.isfile():
                members.append((m, None))
                continue
            f = inp.extractfile(m)
            data = f.read() if f is not None else b""
            if m.name.endswith(_BUNDLE_PATH):
                data = _slim_bundle(data)
                m.size = len(data)
            members.append((m, data))
    tmp = path + ".tmp"
    with tarfile.open(tmp, "w:gz") as out:
        for m, data in members:
            out.addfile(m, io.BytesIO(data) if data is not None else None)
    os.replace(tmp, path)


def _rewrite_wheel(path: str) -> None:
    """Repack the wheel without test modules, fixing RECORD as we go."""
    import base64
    import csv
    import hashlib
    import shutil
    import tempfile
    import zipfile

    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(path) as z:
            keep = [n for n in z.namelist() if not _should_drop(n)]
            z.extractall(tmp, members=keep)

        bundle = os.path.join(tmp, _BUNDLE_PATH)
        if os.path.isfile(bundle):
            with open(bundle, "rb") as fh:
                slim = _slim_bundle(fh.read())
            with open(bundle, "wb") as fh:
                fh.write(slim)

        record = next(
            (os.path.join(r, f) for r, _, fs in os.walk(tmp) for f in fs if f == "RECORD"),
            None,
        )
        if record:
            rows = []
            for root, _, files in os.walk(tmp):
                for f in files:
                    full = os.path.join(root, f)
                    rel = os.path.relpath(full, tmp).replace(os.sep, "/")
                    if rel.endswith("RECORD"):
                        rows.append((rel, "", ""))
                        continue
                    data = open(full, "rb").read()
                    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
                    rows.append((rel, f"sha256={digest.decode()}", str(len(data))))
            with open(record, "w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerows(sorted(rows))

        tmp_whl = path + ".tmp"
        with zipfile.ZipFile(tmp_whl, "w", zipfile.ZIP_DEFLATED) as z:
            for root, _, files in os.walk(tmp):
                for f in files:
                    full = os.path.join(root, f)
                    z.write(full, os.path.relpath(full, tmp).replace(os.sep, "/"))
        shutil.move(tmp_whl, path)


#: `conftest.py` too: it is package *code* to setuptools, and a root conftest
#: in someone's site-packages changes how THEIR pytest collects.
_EXCLUDED_NAMES = ("conftest.py",)


def _is_test_artifact(path: str) -> bool:
    name = os.path.basename(path)
    return name.startswith("test_") and name.endswith(".py") or name in _EXCLUDED_NAMES


def _should_drop(member: str) -> bool:
    if member in _BUILD_ONLY_DATA:
        return True
    if member.startswith(_BUILD_ONLY_CODE):
        return True
    parts = member.split("/")
    if "goldens" in parts or "fixtures" in parts:
        return True
    # The gate suite by DIRECTORY, not by filename: `mirobody/tests/__init__.py`
    # is not a `test_*.py`, and a basename rule would have shipped it.
    if "tests" in parts:
        return True
    return _is_test_artifact(member)

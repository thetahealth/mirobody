"""Build backend: setuptools, minus the tests and the build-time-only data.

Tests live beside the code they cover and are collected by `pytest` straight
from the source tree. They must stay in git — `mirobody/test_engine_coverage.py`
is the 98/98 resolver score the README publishes, and CI runs all of them — but
they have no business in a consumer's `site-packages`: 33 modules plus 21
recorded JSON fixtures, ~206 KB, none of it usable by someone who merely
installed the library.

`[tool.setuptools.exclude-package-data]` cannot express this, because
`test_*.py` files are package *code* rather than data. Hooking the build is the
smallest thing that works, and it covers the wheel and the sdist alike.

The same hook drops the four terminology artifacts nothing at runtime reads
(:data:`_BUILD_ONLY_DATA`). They are DATA, so `exclude-package-data` ought to
have handled them — it does not, because the broad `**/*.bin` / `**/*.npy`
globs in `package-data` win, and the exclusion is not applied to files the
include globs already matched. Rather than narrow the include globs (which is
how a newly added data file gets silently forgotten), the exclusion happens
here, where it is one list with the reason next to it.
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

_EXCLUDED_NAMES = ("conftest.py",)

#: 28 MB of `mirobody/res/` that NO runtime code path reads — grep server/,
#: agent/, pulse/, mcp/ and task/ for `concept_graph` or `taxonomy` and it comes
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
    "mirobody/res/fhir_concept_graph.bin",
    "mirobody/res/fhir_id_map.npy",
    "mirobody/res/fhir_taxonomy.bin",
    "mirobody/res/fhir_snomed_ct_bundle.tar.gz",
})


def _is_test_artifact(path: str) -> bool:
    name = os.path.basename(path)
    return name.startswith("test_") and name.endswith(".py") or name in _EXCLUDED_NAMES


def _prune(directory: str) -> int:
    removed = 0
    for root, dirs, files in os.walk(directory):
        if os.path.basename(root) in ("fixtures", "gate_tests"):
            for f in files:
                os.remove(os.path.join(root, f))
                removed += 1
            continue
        for f in files:
            if _is_test_artifact(f):
                os.remove(os.path.join(root, f))
                removed += 1
    return removed


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    name = _orig.build_wheel(wheel_directory, config_settings, metadata_directory)
    _rewrite_wheel(os.path.join(wheel_directory, name))
    return name


def build_sdist(sdist_directory, config_settings=None):
    return _orig.build_sdist(sdist_directory, config_settings)


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


def _should_drop(member: str) -> bool:
    if member in _BUILD_ONLY_DATA:
        return True
    parts = member.split("/")
    if "gate_tests" in parts or "fixtures" in parts:
        return True
    return _is_test_artifact(member)

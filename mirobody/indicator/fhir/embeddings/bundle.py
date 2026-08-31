"""Bundle MUTATION — the build side of ``fhir_loinc_bundle.tar.gz``, plus the
SNOMED CT sibling (``fhir_snomed_ct_bundle.tar.gz``).

**The read side moved to** :mod:`mirobody._bundle`. Splitting them was the
point: ``engine.py`` used to reach through this module (and into
``alias._normalize``) for the data it serves on every ``resolve()`` call, so
the runtime imported the build tooling and the build tooling could never be
pruned from the wheel. Reads are runtime and live at the package root; writes
are build-time and live here, next to the passes that mint the members.

Member layout, and which pass writes each::

    fhir_loinc_bundle.tar.gz
    ├── VERSION                # scripts/stamp_bundle_version.py
    ├── loinc_axis.csv         # benchmarks/build_loinc_bundle.py   [input]
    ├── loinc_skip.txt         #   "
    ├── loinc_demote.txt       #   "
    ├── loinc_rank_bonus.npy   # `loinc-rank`
    ├── loinc_alias_index.npz  # `loinc-alias`                      [input]
    ├── fhir_dose_index.npz    # `dose-index`
    ├── alias_keys.bin         # scripts/build_runtime_index.py   [derived]
    ├── alias_index.npz        #   "
    ├── corpus_names.bin       #   "   (from res/fhir_meta.csv.gz)
    ├── corpus_names.npz       #   "
    ├── axis_fields.bin        #   "   (from loinc_axis.csv)
    ├── axis_index.npz         #   "
    └── runtime_index.inputs   #   "   digest of the [input] members

**The `[derived]` members are the ones the RUNTIME reads**; the `[input]` ones
are what the build passes write. So the chain has an order, and getting it
wrong is silent::

    loinc-alias / loinc-rank / a new fhir_meta.csv.gz
      -> scripts/build_runtime_index.py     re-cut the derived blobs
      -> scripts/stamp_bundle_version.py    re-stamp VERSION

Skip the middle step and the resolver keeps reading an index that is present,
well-formed and stale. ``build_runtime_index.py --check`` catches exactly that:
it hashes the inputs and compares against ``runtime_index.inputs``, which the
last build recorded.

All mutations are atomic via tempfile + ``os.replace``, so concurrent readers
always see a consistent state.

``aliases/{lang}.tsv`` used to be members here too, byte-identical to
``mirobody/res/aliases_src/{lang}.tsv``. They are gone: two copies of one table
had already drifted (see :mod:`mirobody._bundle`), and the loose files are the
ones the resolver reads and ``scripts/check_wheel_data.py`` gates.
:func:`.lexicon.load_all_aliases` reads them through
:func:`mirobody._bundle.load_alias_sources`, which also means a curated row
takes effect the moment it is written instead of at the next bundle re-cut.

.. note::
   ``benchmarks/`` is a maintainer-side working directory and has never been
   part of this repository — do not go looking for it here. Everything needed
   to CONSUME the bundle ships; the scripts that mint it from raw LOINC/UMLS
   releases do not, because those releases are licensed per user (see
   LICENSE-3RD-PARTY). The same caveat applies to every other
   ``benchmarks/...`` path named in this package.

The SNOMED CT bundle ships separately because its Affiliate License
obligations are scoped per artifact — see ``fhir_snomed_ct_bundle.NOTICE``:

::

    fhir_snomed_ct_bundle.tar.gz
    └── snomed_body_structure.txt  # concept IDs under 123037004
                                   #   |Body structure| (anatomy bias)

Built by ``benchmarks/build_snomed_bundle.py``.
"""

from __future__ import annotations

import io
import logging
import os
import tarfile
import tempfile

from mirobody._bundle import (
    BUNDLE_BASENAME,
    BUNDLE_PATH,
    RES_DIR,
    bundle_path,
    bundle_version,
    is_lfs_pointer,
    list_members,
    read_member,
    read_member_from,
)

log = logging.getLogger(__name__)

# Sibling bundle for SNOMED CT-derived runtime data (Body Structure subtree
# mask, etc.). Separate file because the SNOMED license terms differ from
# LOINC — shipping them apart keeps each NOTICE / Affiliate License obligation
# scoped to its own artifact. Only the build passes read it, so unlike the
# LOINC reader it stays here.
SNOMED_BUNDLE_BASENAME = "fhir_snomed_ct_bundle.tar.gz"
SNOMED_BUNDLE_PATH = os.path.join(RES_DIR, SNOMED_BUNDLE_BASENAME)

__all__ = [
    "BUNDLE_BASENAME",
    "BUNDLE_PATH",
    "RES_DIR",
    "SNOMED_BUNDLE_BASENAME",
    "SNOMED_BUNDLE_PATH",
    "bundle_path",
    "bundle_version",
    "is_lfs_pointer",
    "list_members",
    "read_member",
    "read_member_from",
    "read_snomed_member",
    "remove_member",
    "write_member",
]


def read_snomed_member(name: str, *, bundle_path: str | None = None) -> bytes | None:
    """Return the bytes of a member in the SNOMED bundle, or None if missing.

    Mirrors :func:`mirobody._bundle.read_member` for the sibling bundle.
    """
    return read_member_from(name, bundle_path or SNOMED_BUNDLE_PATH)


def write_member(
    name: str,
    data: bytes,
    *,
    bundle_path: str | None = None,
) -> None:
    """Add or replace a member in the bundle (atomic).

    Tar gzip files aren't seekable for in-place edits — the standard
    library can only append, and appending duplicates a member rather
    than replacing it. So we rewrite the whole archive: copy each
    existing member through, skipping any whose name collides with the
    incoming one, then add the new member. Atomic on Linux via
    :func:`os.replace`.
    """
    path = bundle_path or BUNDLE_PATH
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".bundle-", suffix=".tar.gz", dir=parent)
    os.close(fd)
    try:
        existing: list[tuple[tarfile.TarInfo, bytes]] = []
        if os.path.isfile(path):
            with tarfile.open(path, "r:gz") as inp:
                for m in inp.getmembers():
                    if m.name == name:
                        continue
                    if m.isfile():
                        f = inp.extractfile(m)
                        existing.append((m, f.read() if f is not None else b""))
                    else:
                        existing.append((m, b""))
        with tarfile.open(tmp_path, "w:gz") as out:
            for m, content in existing:
                ti = tarfile.TarInfo(name=m.name)
                ti.size = len(content)
                ti.mtime = m.mtime
                ti.mode = m.mode
                ti.type = m.type
                # Drop uid/gid for reproducibility — bundles are
                # vendor-shipped, identity meaningless.
                out.addfile(ti, io.BytesIO(content) if content else None)
            ti = tarfile.TarInfo(name=name)
            ti.size = len(data)
            out.addfile(ti, io.BytesIO(data))
        # tempfile.mkstemp defaults to 0o600; restore shipped-file mode
        # so the bundle stays world-readable for non-owner users.
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
        log.info("wrote %r (%d bytes) into %s", name, len(data), path)
    except Exception:
        # Best-effort cleanup of the temp tarball on failure.
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise


def remove_member(name: str, *, bundle_path: str | None = None) -> bool:
    """Drop *name* from the bundle if present; return True if removed.

    Same atomic-rewrite pattern as :func:`write_member` — tar gzip isn't
    seekable, so we copy through every other member into a tempfile and
    rename-replace. No-op (returns False) when the bundle doesn't exist
    or the named member isn't present.
    """
    path = bundle_path or BUNDLE_PATH
    if not os.path.isfile(path):
        return False
    parent = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".bundle-", suffix=".tar.gz", dir=parent)
    os.close(fd)
    removed = False
    try:
        existing: list[tuple[tarfile.TarInfo, bytes]] = []
        with tarfile.open(path, "r:gz") as inp:
            for m in inp.getmembers():
                if m.name == name:
                    removed = True
                    continue
                if m.isfile():
                    f = inp.extractfile(m)
                    existing.append((m, f.read() if f is not None else b""))
                else:
                    existing.append((m, b""))
        if not removed:
            os.remove(tmp_path)
            return False
        with tarfile.open(tmp_path, "w:gz") as out:
            for m, content in existing:
                ti = tarfile.TarInfo(name=m.name)
                ti.size = len(content)
                ti.mtime = m.mtime
                ti.mode = m.mode
                ti.type = m.type
                out.addfile(ti, io.BytesIO(content) if content else None)
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
        log.info("removed %r from %s", name, path)
        return True
    except Exception:
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise

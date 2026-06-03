"""Single-file LOINC-derived resolver bundle (``fhir_loinc_bundle.tar.gz``)
plus its SNOMED CT sibling (``fhir_snomed_ct_bundle.tar.gz``).

The unified LOINC bundle holds every static LOINC-derived lookup the
resolver needs:

::

    fhir_loinc_bundle.tar.gz
    ├── loinc_axis.csv               # axis values per LOINC code
    ├── loinc_skip.txt               # codes excluded from resolve
    ├── loinc_demote.txt             # codes soft-demoted in sort
    ├── loinc_rank_bonus.npy         # row-aligned float32 cosine bonus
    ├── loinc_alias_index.npz        # multilingual lexical alias index
    ├── fhir_dose_index.npz          # (value, UCUM unit) → corpus rows
    └── aliases/{lang}.tsv           # per-language src→canonical-EN

Built by ``benchmarks/build_loinc_bundle.py`` (axis + skip + demote)
followed by the ``loinc-rank``, ``loinc-alias``, ``dose-index``,
``loinc-lexicon`` CLI subcommands which add their respective members in
place. All mutations atomic via tempfile + ``os.replace`` so concurrent
readers always see a consistent state.

``aliases/*.tsv`` are loaded as a single merged dict by
:func:`.lexicon.load_all_aliases` and consumed by
:func:`.preprocess.augment_zh_aliases` (the function is multilingual
despite the legacy CN-only name in the docstring). Each ``{lang}.tsv``
is the deterministic union of LOINC LinguisticVariant-derived pairs and
the hand-edited ``mirobody/res/aliases_src/{lang}_curated.tsv`` source
file (curated entries win on key collisions); rebuilt in full by
``loinc-lexicon --lang X``.

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

from .local import RES_DIR

log = logging.getLogger(__name__)

BUNDLE_BASENAME = "fhir_loinc_bundle.tar.gz"
BUNDLE_PATH = os.path.join(RES_DIR, BUNDLE_BASENAME)

# Sibling bundle for SNOMED CT-derived runtime data (Body Structure
# subtree mask, etc.). Separate file because the SNOMED license terms
# differ from LOINC — shipping them apart keeps each NOTICE / Affiliate
# License obligation scoped to its own artifact.
SNOMED_BUNDLE_BASENAME = "fhir_snomed_ct_bundle.tar.gz"
SNOMED_BUNDLE_PATH = os.path.join(RES_DIR, SNOMED_BUNDLE_BASENAME)


def bundle_path() -> str:
    return BUNDLE_PATH


def snomed_bundle_path() -> str:
    return SNOMED_BUNDLE_PATH


def read_member(name: str, *, bundle_path: str | None = None) -> bytes | None:
    """Return the bytes of a member in the LOINC bundle, or None if missing.

    *bundle_path* overrides the default location — used when the
    runtime cache is loaded from a non-default ``res/`` directory.
    """
    return _read_member_from(name, bundle_path or BUNDLE_PATH)


def read_snomed_member(
    name: str, *, bundle_path: str | None = None,
) -> bytes | None:
    """Return the bytes of a member in the SNOMED bundle, or None if missing.

    Mirrors :func:`read_member` for the sibling SNOMED CT bundle.
    """
    return _read_member_from(name, bundle_path or SNOMED_BUNDLE_PATH)


def _read_member_from(name: str, path: str) -> bytes | None:
    if not os.path.isfile(path):
        return None
    try:
        with tarfile.open(path, "r:gz") as tf:
            try:
                f = tf.extractfile(name)
            except KeyError:
                return None
            if f is None:
                return None
            return f.read()
    except Exception:
        log.exception("failed to read %r from %s", name, path)
        return None


def list_members(*, bundle_path: str | None = None) -> list[str]:
    """Return all member names in the bundle (or [] if absent)."""
    path = bundle_path or BUNDLE_PATH
    if not os.path.isfile(path):
        return []
    try:
        with tarfile.open(path, "r:gz") as tf:
            return [m.name for m in tf.getmembers()]
    except Exception:
        log.exception("failed to list %s", path)
        return []


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

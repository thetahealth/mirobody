"""Read the shipped terminology bundle: ``mirobody/res/loinc/fhir_loinc_bundle.tar.gz``.

The bundle is a single tarball holding every static LOINC-derived lookup the
resolver needs::

    fhir_loinc_bundle.tar.gz
    ├── VERSION                      # the release this bundle was cut from
    ├── NOTICE                       # the LOINC copyright notice, per its licence
    ├── axis_fields.bin/.npz         # AXIS_FIELDS values per code, plus sort orders
    ├── corpus_names.bin/.npz        # LONG_COMMON_NAME per code, same row order
    ├── alias_keys.bin/.npz          # folded designation -> rows (CSR postings)
    ├── loinc_rank_bonus.npy         # row-aligned float32 commonness prior
    ├── loinc_skip.txt               # ACTIVE codes the cut leaves out
    └── loinc_units.tsv              # EXAMPLE_UCUM_UNITS per code

Every member is one blob plus an offset array, so a lookup slices bytes and
allocates nothing per entry. `translate_build/build_bundle.py` mints them all
from one LOINC release in one pass; before 1.5.0 they were repacked from a
second set of members (`loinc_axis.csv`, `loinc_alias_index.npz`) that the cut
no longer ships.

**Why this module is at the package root rather than inside
``indicator/fhir/embeddings/``, where it used to live.** ``engine/resolver.py`` (the
front door of ② Translate, and the one thing a `pip install mirobody`
actually runs) read its data through the bundle-BUILD package, and reached
into it for a private symbol (``alias._normalize``) besides. So the runtime
depended on the build tooling, which meant the build tooling could never be
pruned from the wheel and the layering was backwards on paper as well as in
the import graph. The read side is runtime; it lives here. The write side
(``write_member`` / ``remove_member``) and the SNOMED sibling stay in
``indicator/fhir/embeddings/bundle.py`` with the passes that mint them.

Everything needed to CONSUME the bundle ships. The scripts that mint it from
raw LOINC/UMLS releases do not, because those releases are licensed per user
(see LICENSE-3RD-PARTY).
"""

from __future__ import annotations

import logging
import os
import tarfile
from functools import lru_cache

log = logging.getLogger(__name__)

#: ``mirobody/res/``: one level up from this module.
RES_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "res"))

BUNDLE_BASENAME = "fhir_loinc_bundle.tar.gz"
#: `res/` groups by vocabulary since 1.5.1; see `res/README.md`.
BUNDLE_PATH = os.path.join(RES_DIR, "loinc", BUNDLE_BASENAME)

#: Member holding the corpus release string. Read via :func:`bundle_version`.
VERSION_MEMBER = "VERSION"


def bundle_path() -> str:
    return BUNDLE_PATH


def read_member(name: str, *, bundle_path: str | None = None) -> bytes | None:
    """Return the bytes of a member in the LOINC bundle, or None if missing.

    *bundle_path* overrides the default location: used when the runtime cache
    is loaded from a non-default ``res/`` directory.
    """
    return read_member_from(name, bundle_path or BUNDLE_PATH)


@lru_cache(maxsize=4)
def bundle_version(*, bundle_path: str | None = None) -> str:
    """The corpus release this bundle was cut from, e.g. ``2026.08.1``.

    Empty string when the bundle predates the VERSION member or is absent.

    This exists because the package version and the corpus version are
    different facts that used to be conflated. A consumer generating a seed
    from the bundle at build time and pinning the package at runtime has no way
    to assert the two came from the same release unless the corpus says so
    itself; ``mirobody==1.2.2`` did not.
    """
    raw = read_member(VERSION_MEMBER, bundle_path=bundle_path)
    return raw.decode("utf-8").strip() if raw else ""


def is_lfs_pointer(path: str) -> bool:
    """True when the file at *path* is a Git LFS pointer stub, not real data.

    A clone made without git-lfs leaves ~130 bytes of text where the tar.gz
    should be; feeding that to tarfile produced a screenful of traceback before
    the one line that mattered. Naming the stub is the whole fix.
    """
    try:
        with open(path, "rb") as fh:
            return fh.read(24).startswith(b"version https://git-lfs")
    except OSError:
        return False


def read_member_from(name: str, path: str) -> bytes | None:
    if not os.path.isfile(path):
        return None
    if is_lfs_pointer(path):
        log.error("%s is a Git LFS pointer stub, not the data bundle — run `git lfs pull`", path)
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


def read_code_list(name: str, *, bundle_path: str | None = None) -> list[str]:
    """One member that is a list of codes, one per line → the codes.

    Blank lines and ``#`` comments are dropped. Three readers parsed this
    format independently (the resolver's skip set, the semantic tier's, and
    the embedding index's mask builder) and the third did NOT drop comments,
    so a commented line would have reached ``code_to_int`` and raised. The
    member has no comments today, which is why nothing caught it; one
    function is why nothing has to.
    """
    raw = read_member(name, bundle_path=bundle_path)
    if raw is None:
        return []
    lines = (line.strip() for line in raw.decode("utf-8").splitlines())
    return [line for line in lines if line and not line.startswith("#")]


def read_members(names, *, bundle_path: str | None = None) -> dict[str, bytes]:
    """Read several members in ONE pass over the tarball.

    `read_member` opens, gunzips and streams the whole archive per call, so the
    resolver's six-member load cost six full passes over 40 MB: 1.7 s, most of
    it re-inflating the same bytes. One pass is 0.6 s. Missing members are
    simply absent from the result; the caller says what that means.
    """
    path = bundle_path or BUNDLE_PATH
    wanted = set(names)
    out: dict[str, bytes] = {}
    if not os.path.isfile(path):
        return out
    if is_lfs_pointer(path):
        log.error("%s is a Git LFS pointer stub, not the data bundle — run `git lfs pull`", path)
        return out
    try:
        with tarfile.open(path, "r:gz") as tf:
            for m in tf:
                if m.name in wanted and m.isfile():
                    f = tf.extractfile(m)
                    if f is not None:
                        out[m.name] = f.read()
                    if len(out) == len(wanted):
                        break
    except Exception:
        log.exception("failed to read %s", path)
    return out


def list_members(*, bundle_path: str | None = None) -> list[str]:
    """Return all member names in the bundle (or [] if absent)."""
    path = bundle_path or BUNDLE_PATH
    if not os.path.isfile(path) or is_lfs_pointer(path):
        return []
    try:
        with tarfile.open(path, "r:gz") as tf:
            return [m.name for m in tf.getmembers()]
    except Exception:
        log.exception("failed to list %s", path)
        return []


# ── the axis table ───────────────────────────────────────────────────────────

AXIS_BLOB_MEMBER = "axis_fields.bin"
AXIS_INDEX_MEMBER = "axis_index.npz"
#: Field positions inside `axis_fields.bin`, row-major. 7 is the analyte HEAD
#: (COMPONENT with any `^challenge` suffix stripped, folded) and 8 the folded
#: LONG_COMMON_NAME; both are stored rather than derived because the split and
#: the fold have to happen in that order on the RAW value.
AXIS_CODE, AXIS_COMPONENT, AXIS_PROPERTY, AXIS_SCALE = 0, 1, 2, 3
AXIS_SYSTEM, AXIS_METHOD, AXIS_LCN, AXIS_ANALYTE, AXIS_FOLDED_LCN = 4, 5, 6, 7, 8
#: 9 and 10 arrived with the 1.5.0 cut: TIME_ASPCT is the sixth axis a
#: series key needs, CLASS is what the gate reads. Both verbatim.
AXIS_TIME, AXIS_CLASS = 9, 10
AXIS_FIELDS = 11


def load_axis(*, bundle_path: str | None = None, members: dict[str, bytes] | None = None):
    """The LOINC axis table as ``(FieldTable, order_by_code, order_by_name)``.

    Shared by the lexical resolver and the semantic tier so there is one reader
    for one table. It used to be two: both parsed ``loinc_axis.csv`` into their
    own dicts, and when the CSV stopped shipping (superseded by this blob) 
    the semantic tier would have carried on with EMPTY gate tables rather than
    failing. That matters more than the duplication: those gates are what stop
    cosine answering `total cholesterol` with a PhenX survey item, and losing
    them silently is the worst available failure.
    """
    from ._strtab import FieldTable

    # *members* is a batch already read by `read_members`; without it this
    # opens and gunzips the whole tarball twice, which is both slower and (the
    # part that surprised) heavier, because the caller's copy of the same blob
    # becomes garbage the allocator keeps. Measured at +19 MB and +0.3 s.
    if members is not None:
        blob, index = members.get(AXIS_BLOB_MEMBER), members.get(AXIS_INDEX_MEMBER)
    else:
        blob = read_member(AXIS_BLOB_MEMBER, bundle_path=bundle_path)
        index = read_member(AXIS_INDEX_MEMBER, bundle_path=bundle_path)
    if blob is None or index is None:
        raise RuntimeError(
            f"{AXIS_BLOB_MEMBER} / {AXIS_INDEX_MEMBER} not found in "
            f"{bundle_path or BUNDLE_PATH}. Run `git lfs pull` for the data "
            "bundles; if the bundle predates 1.3.0, rebuild the runtime index "
            "with `python -m translate_build.build_bundle --loinc <release>`."
        )
    import io

    import numpy as np

    with np.load(io.BytesIO(index)) as z:
        off, order_code, order_name = z["off"], z["order_code"], z["order_name"]
    return FieldTable(blob, off, AXIS_FIELDS), order_code, order_name


# Alias sources: ``res/loinc/aliases_src/zh.tsv`` (claimed LOINC variant-derived,
# see LICENSE-3RD-PARTY), ``{lang}_curated.tsv`` and
# ``res/loinc/resolver_overrides.tsv``, loose files rather than bundle members.
# Byte-identical copies used to live in the tarball too, and the two drifted:
# four rows added to ``zh_curated.tsv`` were live for the resolver and
# invisible to the build. The tarball members are gone; this is the one
# reader. Other languages resolve through the release's own variants, which
# the alias index is built from.

ALIAS_SRC_DIR = os.path.join(RES_DIR, "loinc", "aliases_src")
OVERRIDES_PATH = os.path.join(RES_DIR, "loinc", "resolver_overrides.tsv")


def alias_source_files(*, include_overrides: bool = True) -> list[str]:
    """Alias TSVs in precedence order; earlier files win.

    Overrides first, then the curated corrections, then the machine-derived
    per-language files. Plain ``sorted()`` put ``zh.tsv`` before
    ``zh_curated.tsv``, which silently discarded curated rows: adding the right
    row changed nothing.
    """
    files = [OVERRIDES_PATH] if include_overrides and os.path.isfile(OVERRIDES_PATH) else []
    if include_overrides and not files:
        # Every row there is a documented wrong answer; without them `HRV`
        # resolves to 40991-2, a rhinovirus RNA test, and nothing said so.
        log.warning("resolver overrides missing (%s): answers fall back to the index alone", OVERRIDES_PATH)
    if os.path.isdir(ALIAS_SRC_DIR):
        names = [fn for fn in os.listdir(ALIAS_SRC_DIR) if fn.endswith(".tsv")]
        files += [
            os.path.join(ALIAS_SRC_DIR, fn)
            for fn in sorted(names, key=lambda n: (0 if "_curated" in n else 1, n))
        ]
    return files


def load_alias_sources(*, include_overrides: bool = True, fold=None) -> dict[str, str]:
    """Merge every alias TSV into one ``term -> target`` dict, first file wins.

    *fold* normalizes the key; the resolver passes
    :func:`mirobody.lexical.index_fold` so the keys match the index, and the
    lexicon build passes nothing when it wants the surfaces as written.
    """
    out: dict[str, str] = {}
    for path in alias_source_files(include_overrides=include_overrides):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) == 2 and parts[0] and parts[1]:
                        out.setdefault(fold(parts[0]) if fold else parts[0], parts[1])
        except OSError:
            log.exception("failed to read alias source %s", path)
    return out

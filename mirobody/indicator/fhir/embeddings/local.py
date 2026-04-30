"""Loader for the local FHIR artifact bundle in ``mirobody/res``.

Layout produced by the ``embeddings`` / ``id-map`` / ``code-names``
subcommands (or one-shot :mod:`migrate_fhir_id`):

    fhir_embeddings.npy   structured (N,)
                          dtype=[('fhir_id','i8'),('emb','f2',(1024,))]
                          fhir_id is canonical packed via code_to_fhir_id
                          (NOT fhir_indicators.id)

    fhir_meta.csv.gz      row-aligned to embeddings.npy; cols: name, code_str
                          name: display string per row (may be empty)
                          code_str: only set for DCM/THETA hash rows
                          OPTIONAL — search works without it (no display)

    fhir_id_map.npy       structured (M,)
                          dtype=[('canonical','i8'),('db_pk','i8')]
                          canonical → fhir_indicators.id translation table
                          OPTIONAL — present in compat mode (DB still keys
                          th_series_data.fhir_id by DB pk); absent in
                          terminal mode (DB has been backfilled to canonical)
"""

from __future__ import annotations

import contextlib
import csv
import gzip
import io
import logging
import os

import numpy as np

from ..common import EMBEDDING_DIM

log = logging.getLogger(__name__)

# This module lives at mirobody/indicator/fhir/embeddings/local.py;
# the bundle dir is mirobody/res/ — three levels up.
RES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "res")
)
# gemini's emb npy keeps the unprefixed name so existing deployments
# (and their virtual-disk mounts) don't need a rename. Other providers
# get a sibling ``fhir_embeddings_<provider>.npy`` — see ``emb_basename``.
EMB_PATH = os.path.join(RES_DIR, "fhir_embeddings.npy")
META_PATH = os.path.join(RES_DIR, "fhir_meta.csv.gz")
ID_MAP_PATH = os.path.join(RES_DIR, "fhir_id_map.npy")


def emb_basename() -> str:
    """Emb npy basename for the active ``DIM_EMBEDDING_PROVIDER``.

    gemini → ``fhir_embeddings.npy`` (default; back-compat — keeps
    existing disk mounts working without rename). Other providers →
    ``fhir_embeddings_<provider>.npy``, sitting alongside.

    ``fhir_meta.csv.gz`` and ``fhir_id_map.npy`` are *not* provider-tagged:
    they are row-aligned to whichever emb npy was just exported, so a
    fresh export overwrites them. Switching provider therefore requires
    re-exporting both providers' emb npys against the same fhir_indicators
    snapshot to keep all bundles consistent.
    """
    from ..common import resolve_fhir_embedding_column
    provider, _ = resolve_fhir_embedding_column()
    return (
        "fhir_embeddings.npy"
        if provider == "gemini"
        else f"fhir_embeddings_{provider}.npy"
    )

# Container deployments mount the 1.4 GB fhir_embeddings.npy on a
# virtual disk to keep it out of pip / git. The application layer
# (services / CLI entrypoints) reads its own config and passes the
# resolved path here via ``bundle_dir``. The active embedding provider
# (which selects the emb npy *file*) is read from config inside
# :func:`emb_basename`.

EMB_DTYPE = np.dtype([("fhir_id", "<i8"), ("emb", "<f2", (EMBEDDING_DIM,))])
# fhir_id_map.npy stores db_pk per row, row-aligned with fhir_embeddings.npy.
# canonical lives in arr['fhir_id'] so storing it again would be redundant.
ID_MAP_DTYPE = np.dtype("<i8")

# Cache key = (bundle_dir, provider): different providers share a dir
# but live in different files, so they get separate entries and don't
# stomp on each other's mmap views. Within the same (dir, provider) the
# cache is a true singleton (~200 MB Python heap each, plus a shared
# mmap).
_caches: dict[tuple[str, str], dict] = {}
_meta_loaded: set[tuple[str, str]] = set()


@contextlib.contextmanager
def open_gz_text_write(disk_path: str):
    """gzip text-mode writer that does **not** bake *disk_path*'s
    basename into the archive header.

    ``gzip.open(path, 'wt')`` records ``os.path.basename(path)`` (minus
    a trailing ``.gz``) in the gz FNAME field. With our transient
    ``.tmp.<name>.gz`` paths that would leak the ``.tmp.`` prefix into
    every consumer's view via ``gunzip -N`` / ``gzip -l`` / ``file``,
    even after we atomic-rename to the final name. Passing
    ``filename=""`` to :class:`gzip.GzipFile` suppresses the FNAME
    field entirely; ``gunzip`` then falls back to stripping ``.gz``
    from the outer disk name, which is what we want.
    """
    with open(disk_path, "wb") as raw:
        gz = gzip.GzipFile(filename="", fileobj=raw, mode="wb")
        try:
            with io.TextIOWrapper(gz, encoding="utf-8", newline="") as txt:
                yield txt
        finally:
            gz.close()


def tmp_path(target_path: str) -> str:
    """Hidden, transient sibling of *target_path*.

    Format: ``<dir>/.tmp.<basename>``. Leading dot keeps it out of
    plain ``ls``; the ``.tmp.`` prefix flags it as transient. The
    target's extension is preserved unchanged at the end, so
    :func:`numpy.save`, :func:`gzip.open`, ``gunzip``, and any tool
    that sniffs by trailing extension all behave normally.
    """
    d, base = os.path.split(target_path)
    return os.path.join(d, f".tmp.{base}")


def atomic_swap_keep_backup(new_path: str, target_path: str) -> None:
    """Atomically replace *target_path* with *new_path*, keeping the
    previous target as ``target_path + ".bak"``.

    Always keeps exactly one backup — the next call atomically overwrites
    the prior ``.bak``. Used for fhir_embeddings.npy where rebuilding
    costs hours of Gemini calls / DB streaming; meta and id_map are
    cheap enough to skip the backup.
    """
    bak_path = target_path + ".bak"
    if os.path.isfile(target_path):
        os.replace(target_path, bak_path)
    os.replace(new_path, target_path)


def load(
    load_meta: bool = True,
    bundle_dir: str | None = None,
) -> dict | None:
    """Lazy-load the local bundle. Returns None if the active provider's
    emb npy is absent.

    *bundle_dir*: explicit override (e.g. test fixture, version pin, or
    app config like ``FHIR_INDICATORS_DIR`` resolved by the caller). If
    None, uses ``RES_DIR``. If given but the directory lacks the active
    provider's emb npy (see :func:`emb_basename`), log a warning and fall
    back to ``RES_DIR``. Caches are keyed on ``(resolved_path, basename)``
    so multiple providers can co-exist without fighting over one slot.

    With ``load_meta=False``, ``names`` and ``code_strs`` stay ``None`` —
    saves ~50 MB of Python heap for callers that only need embeddings +
    id translation (e.g. ``FhirAdapter._search_fhir_local``, which
    re-fetches display strings via a DB JOIN at output time). A later
    ``load(load_meta=True)`` against the same path fills them in-place.

    Returned dict keys:
      arr               : structured npy view, mmap'd
      embs              : strided view of arr['emb'] — (N, 1024) fp16
      canonical         : strided view of arr['fhir_id'] — (N,) int64
      row_by_id         : dict[int, int] — input lookup; accepts canonical
                          and (if id_map sidecar present) DB pk
      to_output_id      : callable(int)->int — translates canonical to DB
                          pk if sidecar present, otherwise identity
      names             : list[str] | None — meta.name column, or None
                          (None when meta missing or load_meta=False)
      code_strs         : dict[int, str] | None — row → original code_str
                          for DCM/THETA hash rows; None when meta missing
                          or load_meta=False
      has_id_map        : bool — whether the sidecar was loaded
    """
    anchor = emb_basename()
    resolved = _resolve_bundle_dir(bundle_dir, anchor)
    cache_key = (resolved, anchor)
    if cache_key not in _caches:
        cache = _load_base(resolved, anchor)
        if cache is None:
            return None
        _caches[cache_key] = cache
    cache = _caches[cache_key]
    if load_meta and cache_key not in _meta_loaded:
        _meta_loaded.add(cache_key)
        _ensure_meta(cache)
    return cache


def _resolve_bundle_dir(override: str | None, anchor: str) -> str:
    """If *override* is given and contains *anchor* (the active provider's
    emb basename), use it; otherwise log a warning and fall back to
    ``RES_DIR`` (pip-bundled). Caller is responsible for sourcing
    *override* from app config.
    """
    if override:
        if os.path.isfile(os.path.join(override, anchor)):
            return override
        log.warning(
            "bundle_dir %s does not contain %s; falling back to %s",
            override, anchor, RES_DIR,
        )
    return RES_DIR


def _load_base(bundle_dir: str, anchor: str) -> dict | None:
    """Load embeddings npy + id_map sidecar; no meta."""
    emb_path = os.path.join(bundle_dir, anchor)
    id_map_path = os.path.join(bundle_dir, os.path.basename(ID_MAP_PATH))

    if not os.path.isfile(emb_path):
        log.info("local fhir bundle not found at %s; using DB path", bundle_dir)
        return None
    try:
        arr = np.load(emb_path, mmap_mode="r")
    except Exception:
        log.exception("failed to load %s", emb_path)
        return None
    if arr.dtype != EMB_DTYPE:
        log.warning(
            "%s has dtype %s, expected %s; ignoring local bundle. "
            "Re-run migrate_fhir_id to convert old-format artifacts.",
            emb_path, arr.dtype, EMB_DTYPE,
        )
        return None

    n = int(arr.shape[0])
    canonical = arr["fhir_id"]
    row_by_id: dict[int, int] = {int(canonical[r]): r for r in range(n)}

    # Optional id_map sidecar (compat mode bridge). Row-aligned with
    # fhir_embeddings.npy: ``db_pks[r]`` is the DB pk of canonical row r.
    has_id_map = False
    db_pks: np.ndarray | None = None
    if os.path.isfile(id_map_path):
        try:
            m = np.load(id_map_path)
            if m.dtype != ID_MAP_DTYPE or m.shape != (n,):
                log.warning(
                    "%s has dtype %s shape %s, expected %s shape (%d,); skipping",
                    id_map_path, m.dtype, m.shape, ID_MAP_DTYPE, n,
                )
            else:
                db_pks = m
                for r in range(n):
                    pk = int(db_pks[r])
                    if pk:
                        row_by_id[pk] = r
                has_id_map = True
        except Exception:
            log.exception("failed to load %s", id_map_path)

    if has_id_map:
        # Capture db_pks (np.ndarray) and row_by_id by closure. row_by_id
        # accepts canonical (always) and db_pk (when sidecar present); we
        # only reach here for canonical inputs (consumer-side outputs are
        # always canonical from arr['fhir_id']).
        _db_pks = db_pks
        _row_by_id = row_by_id

        def to_output_id(canonical_id: int) -> int:
            r = _row_by_id.get(canonical_id)
            if r is None:
                return canonical_id
            pk = int(_db_pks[r])
            return pk if pk else canonical_id
    else:
        def to_output_id(canonical_id: int) -> int:
            return canonical_id

    cache = {
        "arr": arr,
        "embs": arr["emb"],
        "canonical": canonical,
        "row_by_id": row_by_id,
        "to_output_id": to_output_id,
        "names": None,
        "code_strs": None,
        "has_id_map": has_id_map,
        "_bundle_dir": bundle_dir,
    }
    log.info("loaded local fhir bundle (base) from %s: N=%d, id_map=%s",
             bundle_dir, n, "yes" if has_id_map else "no")
    return cache


def _ensure_meta(cache: dict) -> None:
    """Populate cache['names'] / cache['code_strs'] from the meta sidecar
    in cache['_bundle_dir']. Mutates *cache* in place. Idempotency is the
    caller's responsibility — ``load()`` dedups via the ``_meta_loaded``
    set so we don't track per-cache state here."""
    meta_path = os.path.join(cache["_bundle_dir"], os.path.basename(META_PATH))
    if not os.path.isfile(meta_path):
        return
    n = int(cache["arr"].shape[0])
    try:
        ns: list[str] = [""] * n
        cs: dict[int, str] = {}
        with gzip.open(meta_path, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # header
            rows_seen = 0
            for r, row in enumerate(reader):
                if r >= n:
                    rows_seen = r + 1
                    break
                ns[r] = row[0] if len(row) >= 1 else ""
                if len(row) >= 2 and row[1]:
                    cs[r] = row[1]
                rows_seen = r + 1
        if rows_seen != n:
            log.warning("%s has %d rows, expected %d; ignoring",
                        meta_path, rows_seen, n)
            return
        cache["names"] = ns
        cache["code_strs"] = cs
        log.info("loaded fhir meta sidecar: N=%d", n)
    except Exception:
        log.exception("failed to load %s", meta_path)

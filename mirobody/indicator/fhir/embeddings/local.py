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
import re

import numpy as np

from ..common import EMBEDDING_DIM

log = logging.getLogger(__name__)

# This module lives at mirobody/indicator/fhir/embeddings/local.py;
# the bundle dir is mirobody/res/ — three levels up.
RES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "res")
)
# One bundle per deployment — provider is fixed by config at build
# time, so file layout doesn't carry it. Switching provider means
# regenerating all three artifacts together.
EMB_BASENAME = "fhir_embeddings.npy"
META_BASENAME = "fhir_meta.csv.gz"
ID_MAP_BASENAME = "fhir_id_map.npy"

# Container deployments mount the 1.4 GB fhir_embeddings.npy on a
# virtual disk to keep it out of pip / git. The application layer
# (services / CLI entrypoints) reads its own config and passes the
# resolved path here via ``bundle_dir``.

EMB_DTYPE = np.dtype([("fhir_id", "<i8"), ("emb", "<f2", (EMBEDDING_DIM,))])
# fhir_id_map.npy stores db_pk per row, row-aligned with fhir_embeddings.npy.
# canonical lives in arr['fhir_id'] so storing it again would be redundant.
ID_MAP_DTYPE = np.dtype("<i8")

# Cache key = bundle_dir. Within the same dir the cache is a true
# singleton (~200 MB Python heap, plus a shared mmap).
_caches: dict[str, dict] = {}
_meta_loaded: set[str] = set()


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
    """Lazy-load the local bundle. Returns None if the emb npy is absent.

    *bundle_dir*: explicit override (e.g. test fixture, version pin, or
    app config like ``FHIR_INDICATORS_DIR`` resolved by the caller). If
    None, uses ``RES_DIR``. If given but the directory lacks the emb
    npy, log a warning and fall back to ``RES_DIR``.

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
    resolved = _resolve_bundle_dir(bundle_dir)
    if resolved not in _caches:
        cache = _load_base(resolved)
        if cache is None:
            return None
        _caches[resolved] = cache
    cache = _caches[resolved]
    if load_meta and resolved not in _meta_loaded:
        _meta_loaded.add(resolved)
        _ensure_meta(cache)
    return cache


def _resolve_bundle_dir(override: str | None) -> str:
    """If *override* is given and contains the emb npy, use it;
    otherwise log a warning and fall back to ``RES_DIR`` (pip-bundled).
    Caller is responsible for sourcing *override* from app config.
    """
    if override:
        if os.path.isfile(os.path.join(override, EMB_BASENAME)):
            return override
        log.warning(
            "bundle_dir %s does not contain %s; falling back to %s",
            override, EMB_BASENAME, RES_DIR,
        )
    return RES_DIR


def _load_base(bundle_dir: str) -> dict | None:
    """Load embeddings npy + id_map sidecar; no meta."""
    emb_path = os.path.join(bundle_dir, EMB_BASENAME)
    id_map_path = os.path.join(bundle_dir, ID_MAP_BASENAME)

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
        "loinc_skip_mask": None,
        "loinc_demote_mask": None,
        "loinc_rank_bonus": None,
        "loinc_ratio_code_mask": None,
        "alias_index": None,
        "dose_index": None,
        "snomed_body_structure_mask": None,
        "_bundle_dir": bundle_dir,
    }
    _load_loinc_skip(cache)
    _load_loinc_demote(cache)
    _load_loinc_rank(cache)
    _load_alias_index(cache)
    _load_dose_index(cache)
    _load_snomed_body_structure(cache)
    log.info(
        "loaded local fhir bundle (base) from %s: N=%d, id_map=%s, "
        "loinc_skip=%s, loinc_demote=%s, loinc_rank=%s, alias_index=%s, "
        "dose_index=%s, snomed_bs=%s",
        bundle_dir, n, "yes" if has_id_map else "no",
        "yes" if cache["loinc_skip_mask"] is not None else "no",
        "yes" if cache["loinc_demote_mask"] is not None else "no",
        "yes" if cache["loinc_rank_bonus"] is not None else "no",
        f"{len(cache['alias_index'])} aliases" if cache["alias_index"] else "no",
        f"{len(cache['dose_index'])} dose-keys" if cache["dose_index"] else "no",
        f"{int(cache['snomed_body_structure_mask'].sum())} rows"
        if cache["snomed_body_structure_mask"] is not None else "no",
    )
    return cache


def _load_loinc_code_mask(cache: dict, member: str, *, kind: str) -> None:
    """Read a member of LOINC codes (one per line) from the bundle and set
    ``cache[f"loinc_{kind}_mask"]`` to bool[N].

    Shared implementation for ``loinc_skip.txt`` and ``loinc_demote.txt`` —
    both have the same code-list format. *kind* is the cache-key suffix
    (``skip`` or ``demote``).
    """
    from .bundle import BUNDLE_BASENAME, read_member
    bundle_path = os.path.join(cache["_bundle_dir"], BUNDLE_BASENAME)
    raw = read_member(member, bundle_path=bundle_path)
    if raw is None:
        return
    codes = [c for c in (line.strip() for line in raw.decode("utf-8").splitlines()) if c]
    if not codes:
        return
    from ..common import _CODE_BITS, _CODE_MASK, SYSTEM_TO_CODE, code_to_int
    canonical = cache["canonical"]
    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    loinc_idx = SYSTEM_TO_CODE["LOINC"]
    code_ints = np.fromiter(
        (code_to_int(c, "LOINC") for c in codes),
        dtype=np.int64, count=len(codes),
    )
    code_int_arr = canonical & _CODE_MASK
    mask = (sys_arr == loinc_idx) & np.isin(code_int_arr, code_ints)
    cache[f"loinc_{kind}_mask"] = mask
    log.info(
        "loaded loinc %s mask from bundle %s: %d / %d rows %s (%d codes)",
        kind, member, int(mask.sum()), int(cache["arr"].shape[0]),
        "masked" if kind == "skip" else "demoted", len(codes),
    )


def _load_loinc_skip(cache: dict) -> None:
    """Read ``loinc_skip.txt`` from the bundle → bool[N] mask (True =
    exclude). Sets ``cache["loinc_skip_mask"]``."""
    _load_loinc_code_mask(cache, "loinc_skip.txt", kind="skip")


def _load_loinc_demote(cache: dict) -> None:
    """Read ``loinc_demote.txt`` from the bundle → bool[N] mask (True =
    demote in rank-by-cosine; demoted rows are NOT excluded, they
    only fall behind non-demoted peers in the sort). Sets
    ``cache["loinc_demote_mask"]``."""
    _load_loinc_code_mask(cache, "loinc_demote.txt", kind="demote")


def _load_snomed_body_structure(cache: dict) -> None:
    """Read ``snomed_body_structure.txt`` from the SNOMED CT bundle →
    bool[N] mask, True on rows whose canonical (system, code) is a
    SNOMED concept inside the ``123037004 |Body structure|`` subtree.

    Used by :func:`_snomed_picks_topk` in the resolve pipeline as an
    anatomy-bias filter: when a query's top-K full-SNOMED window has
    enough body-structure rows to imply an anatomy intent, the picker
    restricts to this mask. Pure body-structure subtree membership,
    derived upstream from active is_a edges in the SNOMED Snapshot —
    one runtime lookup, no per-query FSN-suffix parsing.

    Returns silently when the SNOMED bundle is absent (e.g. stripped
    deployment that doesn't carry SNOMED data).
    """
    from .bundle import SNOMED_BUNDLE_BASENAME, read_snomed_member
    bundle_path = os.path.join(cache["_bundle_dir"], SNOMED_BUNDLE_BASENAME)
    raw = read_snomed_member("snomed_body_structure.txt", bundle_path=bundle_path)
    if raw is None:
        return
    codes = [c for c in (line.strip() for line in raw.decode("utf-8").splitlines()) if c]
    if not codes:
        return
    from ..common import _CODE_BITS, _CODE_MASK, SYSTEM_TO_CODE, code_to_int
    canonical = cache["canonical"]
    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    snomed_idx = SYSTEM_TO_CODE.get("SNOMED_CT")
    if snomed_idx is None:
        return
    code_ints = np.fromiter(
        (code_to_int(c, "SNOMED_CT") for c in codes),
        dtype=np.int64, count=len(codes),
    )
    code_int_arr = canonical & _CODE_MASK
    mask = (sys_arr == snomed_idx) & np.isin(code_int_arr, code_ints)
    cache["snomed_body_structure_mask"] = mask
    log.info(
        "loaded snomed body-structure mask from bundle: %d / %d rows tagged "
        "(%d concept IDs in subtree)",
        int(mask.sum()), int(cache["arr"].shape[0]), len(codes),
    )


def _load_loinc_rank(cache: dict) -> None:
    """Read ``loinc_rank_bonus.npy`` from the bundle → float32[N] cosine
    bonus (0.0 for non-LOINC and unranked LOINC rows). Sets
    ``cache["loinc_rank_bonus"]``. Silently skipped when the member is
    absent — resolve works without rank tie-breaking."""
    from .bundle import BUNDLE_BASENAME, read_member
    import io as _io
    n = int(cache["arr"].shape[0])
    bundle_path = os.path.join(cache["_bundle_dir"], BUNDLE_BASENAME)
    raw = read_member("loinc_rank_bonus.npy", bundle_path=bundle_path)
    if raw is None:
        return
    try:
        a = np.load(_io.BytesIO(raw))
        if a.dtype != np.float32 or a.shape != (n,):
            log.warning(
                "loinc_rank_bonus.npy has dtype %s shape %s, expected "
                "float32 shape (%d,); ignoring", a.dtype, a.shape, n,
            )
            return
        cache["loinc_rank_bonus"] = a
        log.info(
            "loaded loinc_rank_bonus from bundle: %d / %d rows ranked",
            int((a > 0).sum()), n,
        )
    except Exception:
        log.exception("failed to load loinc_rank_bonus.npy from bundle")


def _load_dose_index(cache: dict) -> None:
    """Read ``fhir_dose_index.npz`` from the bundle and build a
    ``{(value, unit): np.ndarray of row indices}`` lookup in
    ``cache["dose_index"]``. Silently skipped when the member is absent
    — resolve falls back to behavior without dose-match.

    Storage is parallel arrays (row_idx / value / unit_id / unit_table —
    see :mod:`.dose`); the runtime form is dict-of-arrays keyed by the
    canonical ``(value, ucum_unit)`` tuple, which is what the resolver
    actually intersects against per-query dose sets.
    """
    from .bundle import BUNDLE_BASENAME, read_member
    import io as _io
    bundle_path = os.path.join(cache["_bundle_dir"], BUNDLE_BASENAME)
    raw = read_member("fhir_dose_index.npz", bundle_path=bundle_path)
    if raw is None:
        return
    try:
        with np.load(_io.BytesIO(raw), allow_pickle=False) as z:
            row_idx = z["row_idx"]
            value = z["value"]
            unit_id = z["unit_id"]
            unit_table = z["unit_table"]
        # Group row indices by (value, unit) tuple. dict-of-arrays
        # rather than dict-of-lists so the bonus application is a
        # vectorized scores_all[b, idx] += w.
        keys: dict[tuple[float, str], list[int]] = {}
        for i in range(row_idx.shape[0]):
            unit = str(unit_table[int(unit_id[i])])
            key = (float(value[i]), unit)
            keys.setdefault(key, []).append(int(row_idx[i]))
        index: dict[tuple[float, str], np.ndarray] = {
            k: np.asarray(v, dtype=np.int32) for k, v in keys.items()
        }
        cache["dose_index"] = index
        log.info(
            "loaded fhir_dose_index from bundle: %d unique (value, unit) keys, %d total hits",
            len(index), int(row_idx.shape[0]),
        )
    except Exception:
        log.exception("failed to load fhir_dose_index.npz from bundle")


def _load_alias_index(cache: dict) -> None:
    """Read ``loinc_alias_index.npz`` from the bundle and reconstruct the
    alias → corpus-rows dict in ``cache["alias_index"]``. The npz holds
    three arrays (``aliases`` object, ``offsets`` int32, ``rows``
    int32) — see :mod:`.alias`. Silently skipped when the member is
    absent — resolve falls back to embedding-only scoring."""
    from .bundle import BUNDLE_BASENAME, read_member
    import io as _io
    bundle_path = os.path.join(cache["_bundle_dir"], BUNDLE_BASENAME)
    raw = read_member("loinc_alias_index.npz", bundle_path=bundle_path)
    if raw is None:
        return
    try:
        with np.load(_io.BytesIO(raw), allow_pickle=True) as z:
            aliases = z["aliases"]
            offsets = z["offsets"]
            rows = z["rows"]
        idx: dict[str, list[int]] = {
            str(a): rows[offsets[i]:offsets[i + 1]].tolist()
            for i, a in enumerate(aliases)
        }
        cache["alias_index"] = idx
        log.info(
            "loaded loinc_alias_index from bundle: %d aliases",
            len(idx),
        )
    except Exception:
        log.exception("failed to load loinc_alias_index.npz from bundle")


def _ensure_meta(cache: dict) -> None:
    """Populate cache['names'] / cache['code_strs'] from the meta sidecar
    in cache['_bundle_dir']. Mutates *cache* in place. Idempotency is the
    caller's responsibility — ``load()`` dedups via the ``_meta_loaded``
    set so we don't track per-cache state here."""
    meta_path = os.path.join(cache["_bundle_dir"], META_BASENAME)
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
        _augment_demote_with_names(cache)
        _compute_ratio_code_mask(cache)
    except Exception:
        log.exception("failed to load %s", meta_path)


# A LOINC display name is treated as a "ratio/index code" iff it carries
# one of these signals:
#
# - Explicit ratio markers: ``\bRatio\b`` (free or in ``[Mass Ratio]`` /
#   ``[Molar Ratio]``) or ``\bIndex\b`` (AHI, ODI, BMI percentile, …).
# - Analyte/analyte slash ``X/Y`` outside ``[...]`` — analyte ratios
#   look like ``Lipoprotein.beta/total Lipoprotein`` or
#   ``CD4/CD8 [Ratio]``. We exclude bracket-enclosed slashes which are
#   unit forms (``[Mass/volume]``, ``[Mass/time]``) — those mean "the
#   value is measured in mass-per-volume", not "this is a ratio of
#   two analytes". The ``(?<!\[)`` lookbehind catches the bracket case.
# - ``Mass fraction`` / ``Number fraction`` / ``Pure number fraction``
#   — LOINC's value-as-fraction PROPERTY indicators, semantically
#   ratios.
#
# INR-style codes (``38875-1 INR in Platelet poor plasma by Coagulation
# assay``) don't carry an explicit ``Ratio`` token in the LongCommon
# Name and will NOT match — that's fine, INR resolves correctly via
# embedding + alias index already (no bonus needed).
_RATIO_CODE_NAME_RE = re.compile(
    r"\bratio\b"
    r"|\bindex\b"
    r"|(?<!\[)\b[A-Za-z][\w.]{1,30}/[A-Za-z][\w.]{1,30}\b"
    r"|\b(?:mass|number|pure number) fraction\b",
    re.IGNORECASE,
)


def _compute_ratio_code_mask(cache: dict) -> None:
    """Build a bool[N] mask flagging LOINC rows whose name looks like a
    ratio / index / fraction code. Used by the resolve-time ratio guard
    (:class:`FhirAdapter._resolve_local_batch`) to boost ratio-named
    candidates when the query explicitly asks for a ratio/index.
    """
    from ..common import SYSTEM_TO_CODE, _CODE_BITS
    names = cache.get("names")
    if not names:
        return
    canonical = cache["canonical"]
    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    mask = np.zeros(len(names), dtype=bool)
    for r in np.flatnonzero(is_loinc):
        nm = names[r]
        if nm and _RATIO_CODE_NAME_RE.search(nm):
            mask[r] = True
    cache["loinc_ratio_code_mask"] = mask
    log.info(
        "computed loinc ratio code mask: %d / %d LOINC rows flagged",
        int(mask.sum()), int(is_loinc.sum()),
    )


def _augment_demote_with_names(cache: dict) -> None:
    """OR hand-curated name-pattern matches into ``loinc_demote_mask``.

    Tarball-driven demote (status / class) runs at base load. Name-
    pattern demote needs ``cache['names']`` which is meta-sidecar lazy,
    so it piggybacks on :func:`_ensure_meta`. Idempotency: re-OR with
    the same patterns is a no-op since the existing mask already
    covers the matches.
    """
    from ..common import (
        SYSTEM_TO_CODE, _CODE_BITS, _HAND_DEMOTE_NAME_PATTERNS,
    )
    if not _HAND_DEMOTE_NAME_PATTERNS:
        return
    names = cache.get("names")
    if not names:
        return
    canonical = cache["canonical"]
    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    add = np.zeros(len(names), dtype=bool)
    # Only scan LOINC rows — non-LOINC names can't be demoted (the
    # demote mask is LOINC-scoped) and the test on names[r] is the
    # expensive part of the loop.
    for r in np.flatnonzero(is_loinc):
        nm = names[r]
        if nm and any(p.search(nm) for p in _HAND_DEMOTE_NAME_PATTERNS):
            add[r] = True
    existing = cache.get("loinc_demote_mask")
    if existing is None:
        cache["loinc_demote_mask"] = add
    else:
        cache["loinc_demote_mask"] = existing | add
    log.info(
        "hand-demote: %d LOINC rows added from %d name pattern(s)",
        int(add.sum()), len(_HAND_DEMOTE_NAME_PATTERNS),
    )

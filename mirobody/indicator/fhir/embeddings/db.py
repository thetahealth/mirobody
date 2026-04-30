"""Export ``fhir_indicators`` → offline bundle in ``mirobody/res``.

Used when there is an existing populated ``fhir_indicators`` table and
``th_series_data.fhir_id`` rows already key by ``fhir_indicators.id``
(DB pk). Writes:

  fhir_embeddings.npy   structured (N,)
                        dtype = [('fhir_id','i8'),('emb','f2',(1024,))]
                        ``fhir_id`` is canonical packed via
                        :func:`code_to_fhir_id` — NOT the DB pk.

  fhir_id_map.npy       structured (M,)
                        dtype = [('canonical','i8'),('db_pk','i8')]
                        Compat-mode bridge so consumers can translate
                        ``th_series_data.fhir_id`` (DB pk) ↔ canonical.
                        Once that column is backfilled to canonical, the
                        sidecar can be deleted; consumers fall through
                        to terminal mode.

  fhir_meta.csv.gz      row-aligned to ``fhir_embeddings.npy``;
                        cols: ``name`` (empty here — run ``code-names``
                        to fill from ~/ref), ``code_str`` (original
                        string for hash rows DCM/THETA, empty otherwise).

Layout note: ``SYSTEMS`` in :mod:`common` is **append-only** — its
index is bit-packed into every ``canonical`` value. Reordering or
deleting an entry breaks every previously-written ``fhir_id``.

Phase 2 (embedding download) is checkpoint-resumable via memmap
partials and a JSON progress file in ``--output``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from argparse import Namespace

import numpy as np

from ..common import (
    EMBEDDING_DIM,
    SYSTEM_TO_CODE,
    code_to_fhir_id,
    resolve_fhir_embedding_column,
)
from .local import (
    EMB_DTYPE,
    ID_MAP_DTYPE,
    ID_MAP_PATH,
    META_PATH,
    RES_DIR,
    atomic_swap_keep_backup,
    emb_basename,
    open_gz_text_write,
    tmp_path,
)

log = logging.getLogger(__name__)

_BATCH = 10000

# Bumped to v2 when the on-disk format changed (ids partial now stores
# canonical packed values instead of DB pks). Old partials are discarded
# on resume to avoid silently mixing schemas.
_PROGRESS_VERSION = 2

# Systems whose codes go through blake2b in code_to_int — for these
# rows we need to persist the original code string in fhir_meta.csv.gz
# because the hash is one-way.
_HASH_SYSTEMS = frozenset({"DCM", "THETA"})

#-----------------------------------------------------------------------------

async def _get_cursor():
    from mirobody.utils import global_config
    return await global_config().get_postgresql().get_async_client(cursor_factory=None)

#-----------------------------------------------------------------------------

async def build_id_map(out_path: str, emb_path: str | None = None) -> int:
    """Build ``fhir_id_map.npy`` row-aligned with the active provider's
    emb npy (``fhir_embeddings[_<provider>].npy``).

    Reads canonical fhir_ids from *emb_path*, queries ``fhir_indicators``
    for every ``(system, code)``, and writes one ``db_pk`` per
    embedding row. Rows whose canonical isn't present in the DB get
    ``db_pk = 0`` (treated as "no DB pk known" by consumers).

    Requires *emb_path* to exist — id_map without an embeddings npy to
    align against has no meaningful row order. Use this after
    ``embeddings`` (or ``migrate_fhir_id``) has produced the npy.
    """
    if emb_path is None:
        emb_path = os.path.join(RES_DIR, emb_basename())
    if not os.path.isfile(emb_path):
        raise SystemExit(
            f"missing {emb_path}; run `embeddings` first so id_map has "
            f"a row order to align against"
        )
    arr = np.load(emb_path, mmap_mode="r")
    if arr.dtype != EMB_DTYPE:
        raise SystemExit(
            f"{emb_path} has dtype {arr.dtype}, expected {EMB_DTYPE} — "
            f"old-format artifact; re-run `embeddings` or `migrate`"
        )
    canonical = arr["fhir_id"]
    n = int(arr.shape[0])

    conn = await _get_cursor()
    async with conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, indicator_standard, code "
                "FROM fhir_indicators "
                "WHERE indicator_standard IS NOT NULL AND code IS NOT NULL;"
            )
            rows = await cur.fetchall()
    log.info("fetched %d rows from fhir_indicators", len(rows))

    canonical_to_db_pk: dict[int, int] = {}
    skipped = 0
    for row_id, sys_name, code in rows:
        sys_int = SYSTEM_TO_CODE.get(sys_name)
        if sys_int is None:
            # SYSTEMS in common.py is append-only — adding a new vocab
            # there is the right fix; reordering breaks every existing
            # canonical fhir_id.
            skipped += 1
            continue
        canonical_to_db_pk[code_to_fhir_id(sys_int, code)] = row_id
    if skipped:
        log.warning(
            "%d rows had unknown indicator_standard; skipped — append "
            "the vocab to SYSTEMS in common.py to encode them",
            skipped,
        )

    db_pks = np.empty(n, dtype=ID_MAP_DTYPE)
    n_missing = 0
    for r in range(n):
        pk = canonical_to_db_pk.get(int(canonical[r]), 0)
        db_pks[r] = pk
        if pk == 0:
            n_missing += 1
    if n_missing:
        log.warning(
            "%d embedding rows have no matching fhir_indicators row "
            "(canonical present in npy but DB pk unknown); db_pk=0",
            n_missing,
        )

    tmp = tmp_path(out_path)
    np.save(tmp, db_pks)
    os.replace(tmp, out_path)
    log.info("wrote %s (%d rows)", out_path, n)
    return n


async def cmd_id_map(args: Namespace) -> None:
    """Subcommand: id-map — generate fhir_id_map.npy without touching
    embeddings. Cheap recovery path for users whose canonical-keyed
    embeddings already exist (e.g. post-migration) but who lack the
    sidecar.
    """
    res_dir = args.res_dir or RES_DIR
    os.makedirs(res_dir, exist_ok=True)
    out_path = os.path.join(res_dir, os.path.basename(ID_MAP_PATH))
    n = await build_id_map(out_path)
    log.info("id-map: %d entries → %s (%.1f MB)",
             n, out_path, os.path.getsize(out_path) / 1e6)

#-----------------------------------------------------------------------------

async def _export_embeddings_and_meta(
    out_dir: str, emb_path: str, meta_path: str, id_map_path: str,
) -> int:
    """Stream rows with the configured ``embedding_<provider>`` column set,
    compute canonical fhir_id, write structured npy + meta.csv.gz + id_map.npy.

    Provider is selected via :func:`resolve_fhir_embedding_column`
    (``DIM_EMBEDDING_PROVIDER``, default gemini).

    On first run: pre-allocates fp32 emb memmap + canonical memmap +
    db_pk memmap + hash-codes JSON in *out_dir*. Streams via id-paginated
    queries with per-batch fsync. On resume: rebinds all four partials
    if the row count, progress version, and provider match — switching
    provider mid-export discards old partials to avoid mixing dtypes.

    Finalize step (only when the full N is reached):
      1. L2-normalise fp32 embeddings, cast to fp16
      2. Write structured npy: arr['fhir_id']=canonicals, arr['emb']=fp16
      3. Write meta.csv.gz: row-aligned ``name``+``code_str`` (name empty)
      4. Write fhir_id_map.npy: row-aligned db_pk per embedding row
      5. Delete partials.
    """
    from pgvector.psycopg import register_vector_async

    provider, emb_col = resolve_fhir_embedding_column()
    os.makedirs(out_dir, exist_ok=True)
    # Provider in the filename so a partial built from one provider
    # can't be silently resumed against another.
    partial_emb = os.path.join(out_dir, f"fhir_embeddings_db_{provider}.npy.partial")
    partial_ids = os.path.join(out_dir, f"fhir_canonicals_db_{provider}.npy.partial")
    partial_db_pks = os.path.join(out_dir, f"fhir_db_pks_db_{provider}.npy.partial")
    partial_hash = os.path.join(out_dir, f"fhir_meta_hash_db_{provider}.json.partial")
    progress_path = os.path.join(out_dir, f"fhir_embeddings_db_{provider}.progress.json")

    conn = await _get_cursor()
    async with conn:
        await register_vector_async(conn)

        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM fhir_indicators "
                f"WHERE {emb_col} IS NOT NULL "
                f"AND indicator_standard IS NOT NULL "
                f"AND code IS NOT NULL;"
            )
            row = await cur.fetchone()
            n_rows = int(row[0])
        if n_rows == 0:
            raise SystemExit(
                f"no fhir_indicators rows with {emb_col} + indicator_standard + code"
            )
        log.info("target row count: %s (provider=%s, column=%s)",
                 f"{n_rows:,}", provider, emb_col)

        # Resume check — version-gate against stale (v1) partials.
        last_completed = 0
        last_db_id = -1
        hash_codes: dict[int, str] = {}
        resume_ok = (
            os.path.isfile(partial_emb)
            and os.path.isfile(partial_ids)
            and os.path.isfile(progress_path)
        )
        if resume_ok:
            with open(progress_path, "r", encoding="utf-8") as f:
                prog = json.load(f)
            if (
                prog.get("format_version") == _PROGRESS_VERSION
                and prog.get("n_rows") == n_rows
            ):
                last_completed = int(prog.get("last_completed", 0))
                last_db_id = int(prog.get("last_db_id", -1))
                if os.path.isfile(partial_hash):
                    with open(partial_hash, "r", encoding="utf-8") as f:
                        hash_codes = {int(k): v for k, v in json.load(f).items()}
                log.info("resuming from row %s/%s", f"{last_completed:,}", f"{n_rows:,}")
            else:
                log.warning(
                    "progress stale (version=%s n_rows=%s) — restarting from 0",
                    prog.get("format_version"), prog.get("n_rows"),
                )
                resume_ok = False
                last_completed = 0
                last_db_id = -1

        mode = "r+" if (resume_ok and last_completed > 0) else "w+"
        emb_mm = np.memmap(partial_emb, dtype=np.float32, mode=mode,
                           shape=(n_rows, EMBEDDING_DIM))
        ids_mm = np.memmap(partial_ids, dtype=np.int64, mode=mode, shape=(n_rows,))
        db_pks_mm = np.memmap(partial_db_pks, dtype=ID_MAP_DTYPE, mode=mode,
                              shape=(n_rows,))

        pos = last_completed
        async with conn.cursor() as cur:
            while pos < n_rows:
                # Pagination by DB id: stable across crashes, monotonic.
                # We fetch (id, std, code, vec) so canonical can be computed
                # without a second pass.
                await cur.execute(
                    f"SELECT id, indicator_standard, code, {emb_col} "
                    f"FROM fhir_indicators "
                    f"WHERE {emb_col} IS NOT NULL "
                    f"AND indicator_standard IS NOT NULL "
                    f"AND code IS NOT NULL "
                    f"AND id > %s "
                    f"ORDER BY id LIMIT %s;",
                    (last_db_id, _BATCH),
                )
                rows = await cur.fetchall()
                if not rows:
                    break
                for i, (row_id, sys_name, code, vec) in enumerate(rows):
                    if vec.shape[0] != EMBEDDING_DIM:
                        raise ValueError(
                            f"fhir_indicators.id={row_id}: expected {EMBEDDING_DIM} dims, "
                            f"got {vec.shape[0]}"
                        )
                    sys_int = SYSTEM_TO_CODE.get(sys_name)
                    if sys_int is None:
                        # SYSTEMS is append-only because the enum index
                        # is bit-packed into canonical fhir_ids. Adding a
                        # new vocab requires extending the tuple in
                        # common.py without reordering.
                        raise ValueError(
                            f"fhir_indicators.id={row_id}: unknown indicator_standard "
                            f"{sys_name!r}; append to SYSTEMS in common.py"
                        )
                    emb_mm[pos + i] = vec
                    ids_mm[pos + i] = code_to_fhir_id(sys_int, code)
                    db_pks_mm[pos + i] = row_id
                    if sys_name in _HASH_SYSTEMS:
                        hash_codes[pos + i] = code
                emb_mm.flush()
                ids_mm.flush()
                db_pks_mm.flush()
                pos += len(rows)
                last_db_id = int(rows[-1][0])
                with open(progress_path, "w", encoding="utf-8") as f:
                    json.dump({
                        "format_version": _PROGRESS_VERSION,
                        "n_rows": n_rows,
                        "last_completed": pos,
                        "last_db_id": last_db_id,
                    }, f)
                with open(partial_hash, "w", encoding="utf-8") as f:
                    json.dump({str(k): v for k, v in hash_codes.items()}, f)
                log.info("embeddings: %s/%s rows", f"{pos:,}", f"{n_rows:,}")

    if pos != n_rows:
        raise RuntimeError(
            f"incomplete: pos={pos} but n_rows={n_rows} "
            f"(DB returned fewer rows than COUNT; rerun to continue)"
        )

    log.info(
        "finalising: L2-normalise + cast fp32→fp16 + structured-merge (%s rows)",
        f"{n_rows:,}",
    )
    arr_f32 = np.asarray(emb_mm[:])
    norms = np.linalg.norm(arr_f32, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    arr_f32 /= norms

    # Structured npy: write to a hidden sibling tmp, keep previous as
    # .bak before swapping in. Rebuilding this file costs hours, so the
    # backup is worth the 1.4 GB.
    emb_tmp = tmp_path(emb_path)
    out = np.lib.format.open_memmap(
        emb_tmp, mode="w+", dtype=EMB_DTYPE, shape=(n_rows,))
    out["fhir_id"] = ids_mm[:]
    out["emb"] = arr_f32.astype(np.float16)
    out.flush()
    del out
    atomic_swap_keep_backup(emb_tmp, emb_path)

    # Meta: name column intentionally empty (run code-names to fill).
    meta_tmp = tmp_path(meta_path)
    with open_gz_text_write(meta_tmp) as f:
        w = csv.writer(f)
        w.writerow(["name", "code_str"])
        for r in range(n_rows):
            w.writerow(["", hash_codes.get(r, "")])
    os.replace(meta_tmp, meta_path)

    # id_map: row-aligned db_pk per embedding row.
    id_map_tmp = tmp_path(id_map_path)
    np.save(id_map_tmp, np.asarray(db_pks_mm[:], dtype=ID_MAP_DTYPE))
    os.replace(id_map_tmp, id_map_path)

    del emb_mm, ids_mm, db_pks_mm
    for p in (partial_emb, partial_ids, partial_db_pks, partial_hash, progress_path):
        if os.path.isfile(p):
            os.remove(p)
    return n_rows


async def cmd_embeddings_db(args: Namespace) -> None:
    out_dir = args.output or os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "out"
    ))
    res_dir = args.res_dir or RES_DIR
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(res_dir, exist_ok=True)

    emb_path = os.path.join(res_dir, emb_basename())
    id_map_path = os.path.join(res_dir, os.path.basename(ID_MAP_PATH))
    meta_path = os.path.join(res_dir, os.path.basename(META_PATH))

    # All three artifacts are produced in one streaming pass: each fetched
    # fhir_indicators row contributes its embedding (→ npy), canonical
    # fhir_id (→ npy), db_pk (→ id_map row-aligned), and code_str if
    # hashed (→ meta).
    n_embs = await _export_embeddings_and_meta(
        out_dir, emb_path, meta_path, id_map_path,
    )

    log.info(
        "wrote %s (%d rows, %.1f MB)",
        emb_path, n_embs, os.path.getsize(emb_path) / 1e6,
    )
    log.info(
        "wrote %s (%d rows, %.1f MB)",
        id_map_path, n_embs, os.path.getsize(id_map_path) / 1e6,
    )
    log.info(
        "wrote %s (%.1f MB) — name column empty; run `code-names` to populate",
        meta_path, os.path.getsize(meta_path) / 1e6,
    )

#-----------------------------------------------------------------------------

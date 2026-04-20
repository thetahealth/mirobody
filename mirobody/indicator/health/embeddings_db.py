"""Export fhir_indicators lookup & embedding data to in-memory-friendly files.

Two independent groups, bridged by fhir_indicators.id:

  Group 1 — (standard, code) → id  [self-contained lookup table]
    fhir_code_index.csv.gz         columns: standard (int8 enum), code, id
                                   covers ALL fhir_indicators rows where both
                                   indicator_standard and code are set

  Group 2 — row_i → id, embedding  [row-aligned pair for vector search]
    fhir_embedding_ids.npy         int64   (N,)
    fhir_embeddings.npy            float16 (N, 1024), L2-normalised

L2-normalising at export time lets cosine similarity reduce to a single
matmul at query time. ~677k × 1024 fp16 ≈ 1.35 GB; brute-force is faster
than pgvector's HNSW at this N.

Group 2 is checkpoint-resumable via memmap'd partial files in `--output`.
"""

from __future__ import annotations

import csv
import gzip
import json
import logging
import os
from argparse import Namespace

import numpy as np

from .common import EMBEDDING_DIM, STANDARD_TO_CODE

log = logging.getLogger(__name__)

_BATCH = 10000


async def _get_cursor():
    from mirobody.utils import global_config
    return await global_config().get_postgresql().get_async_client(cursor_factory=None)


async def _stream_paginated(sql: str, batch: int):
    """Yield rows from *sql* paginated by id. *sql* must have a `{id_gt}` placeholder
    for the pagination predicate and a `{limit}` placeholder for the batch size,
    and must `ORDER BY id` with id as the first selected column."""
    conn = await _get_cursor()
    last_id = -1
    async with conn:
        async with conn.cursor() as cur:
            while True:
                await cur.execute(
                    sql.format(id_gt=last_id, limit=batch)
                )
                rows = await cur.fetchall()
                if not rows:
                    return
                yield rows
                last_id = rows[-1][0]


async def _export_code_index(path: str) -> int:
    """Group 1: write (standard_enum, code, id) for every fhir_indicators row
    where both indicator_standard and code are set."""
    sql = (
        "SELECT id, indicator_standard, code FROM fhir_indicators "
        "WHERE indicator_standard IS NOT NULL AND code IS NOT NULL "
        "AND id > {id_gt} ORDER BY id LIMIT {limit};"
    )
    total = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["standard", "code", "id"])
        async for rows in _stream_paginated(sql, _BATCH):
            for row_id, std, code in rows:
                std_code = STANDARD_TO_CODE.get(std)
                if std_code is None:
                    raise ValueError(
                        f"fhir_indicators.id={row_id}: unknown indicator_standard {std!r}; "
                        f"add to STANDARDS tuple in common.py"
                    )
                w.writerow([std_code, code, row_id])
            total += len(rows)
            log.info("code index: %s rows", f"{total:,}")
    return total


async def _export_embeddings(out_dir: str, ids_path: str, emb_path: str) -> int:
    """Group 2: pgvector binary protocol + resumable memmap.

    On first run: pre-allocates fp32 memmap files in out_dir, streams vectors
    directly as numpy arrays (no ::text cast), writes into memmap with per-
    batch fsync + progress.json. On restart: resumes at `last_completed` if
    row count matches.

    Finalize: L2-normalise, cast fp16, write final res/ artifacts, delete
    partials.
    """
    from pgvector.psycopg import register_vector_async

    os.makedirs(out_dir, exist_ok=True)
    partial_emb = os.path.join(out_dir, "fhir_embeddings_db.npy.partial")
    partial_ids = os.path.join(out_dir, "fhir_embedding_ids_db.npy.partial")
    progress_path = os.path.join(out_dir, "fhir_embeddings_db.progress.json")

    conn = await _get_cursor()
    async with conn:
        await register_vector_async(conn)

        # Count rows (cheap with index)
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM fhir_indicators WHERE embedding_gemini IS NOT NULL;"
            )
            row = await cur.fetchone()
            n_rows = int(row[0])
        if n_rows == 0:
            raise SystemExit("no fhir_indicators rows with embedding_gemini")
        log.info("target row count: %s", f"{n_rows:,}")

        # Resume check
        last_completed = 0
        resume_ok = (
            os.path.isfile(partial_emb)
            and os.path.isfile(partial_ids)
            and os.path.isfile(progress_path)
        )
        if resume_ok:
            with open(progress_path, "r", encoding="utf-8") as f:
                prog = json.load(f)
            if prog.get("n_rows") == n_rows:
                last_completed = int(prog.get("last_completed", 0))
                log.info("resuming from row %s/%s", f"{last_completed:,}", f"{n_rows:,}")
            else:
                log.warning(
                    "row count changed (%s → %s) — restarting from 0",
                    prog.get("n_rows"), n_rows,
                )
                last_completed = 0

        mode = "r+" if os.path.isfile(partial_emb) else "w+"
        emb_mm = np.memmap(partial_emb, dtype=np.float32, mode=mode, shape=(n_rows, EMBEDDING_DIM))
        ids_mm = np.memmap(partial_ids, dtype=np.int64, mode=mode, shape=(n_rows,))

        pos = last_completed
        last_id = int(ids_mm[pos - 1]) if pos > 0 else -1

        async with conn.cursor() as cur:
            while pos < n_rows:
                await cur.execute(
                    "SELECT id, embedding_gemini FROM fhir_indicators "
                    "WHERE embedding_gemini IS NOT NULL AND id > %s "
                    "ORDER BY id LIMIT %s;",
                    (last_id, _BATCH),
                )
                rows = await cur.fetchall()
                if not rows:
                    break
                for i, (row_id, vec) in enumerate(rows):
                    if vec.shape[0] != EMBEDDING_DIM:
                        raise ValueError(
                            f"fhir_indicators.id={row_id}: expected {EMBEDDING_DIM} dims, got {vec.shape[0]}"
                        )
                    emb_mm[pos + i] = vec
                    ids_mm[pos + i] = row_id
                emb_mm.flush()
                ids_mm.flush()
                pos += len(rows)
                last_id = int(rows[-1][0])
                with open(progress_path, "w", encoding="utf-8") as f:
                    json.dump({"n_rows": n_rows, "last_completed": pos}, f)
                log.info("embeddings: %s/%s rows", f"{pos:,}", f"{n_rows:,}")

    # Finalize (conn closed)
    if pos != n_rows:
        raise RuntimeError(
            f"incomplete: pos={pos} but n_rows={n_rows} "
            f"(DB returned fewer rows than COUNT; rerun to continue)"
        )
    log.info("finalising: L2-normalise + cast fp32 → fp16 (%s rows)", f"{n_rows:,}")
    arr = np.asarray(emb_mm[:])
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    arr /= norms
    np.save(emb_path, arr.astype(np.float16))
    np.save(ids_path, np.asarray(ids_mm[:], dtype=np.int64))

    del emb_mm, ids_mm
    os.remove(partial_emb)
    os.remove(partial_ids)
    os.remove(progress_path)
    return n_rows


async def cmd_embeddings_db(args: Namespace) -> None:
    out_dir = args.output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "out"
    )
    res_dir = args.res_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "res"
    )
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(res_dir, exist_ok=True)
    code_path = os.path.join(res_dir, "fhir_code_index.csv.gz")
    ids_path = os.path.join(res_dir, "fhir_embedding_ids.npy")
    emb_path = os.path.join(res_dir, "fhir_embeddings.npy")

    n_codes = await _export_code_index(code_path)
    n_embs = await _export_embeddings(out_dir, ids_path, emb_path)

    log.info(
        "exported %s code entries → %s (%.1f MB)",
        f"{n_codes:,}", code_path, os.path.getsize(code_path) / 1e6,
    )
    log.info(
        "exported %s embeddings → %s (%.1f MB), %s (%.1f MB)",
        f"{n_embs:,}",
        ids_path, os.path.getsize(ids_path) / 1e6,
        emb_path, os.path.getsize(emb_path) / 1e6,
    )

"""One-shot migration: 4-file artifacts → structured npy + meta sidecar.

Reads the existing on-disk artifacts and rewrites them in the new layout
without re-querying the DB or recomputing embeddings:

    BEFORE                                  AFTER
    ──────                                  ─────
    fhir_embeddings.npy        (N, 1024)    fhir_embeddings.npy
    fhir_embedding_ids.npy     (N,) DB pk     dtype=[('fhir_id','i8'),
    fhir_code_index.csv.gz     id→(std,code)         ('emb','f2',(1024,))]
    fhir_embedding_names.csv.gz (N,) name   fhir_meta.csv.gz
                                              cols: name, code_str
                                              code_str only for hashed rows

The new ``fhir_id`` column is the canonical packed value from
:func:`code_to_fhir_id` (sentinel-aware encoding). Run once after
upgrading common.py; subsequent runs detect the new layout and exit.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import os
import sys

import numpy as np

from ..common import SYSTEMS, code_to_fhir_id
from .local import RES_DIR, atomic_swap_keep_backup, open_gz_text_write, tmp_path

_HASH_SYSTEMS = frozenset({"DCM", "THETA"})

log = logging.getLogger("migrate_fhir_id")


def _default_res_dir() -> str:
    return RES_DIR


def migrate(res_dir: str, *, dry_run: bool = False) -> int:
    old_emb = os.path.join(res_dir, "fhir_embeddings.npy")
    old_ids = os.path.join(res_dir, "fhir_embedding_ids.npy")
    old_idx = os.path.join(res_dir, "fhir_code_index.csv.gz")
    old_names = os.path.join(res_dir, "fhir_embedding_names.csv.gz")
    new_meta = os.path.join(res_dir, "fhir_meta.csv.gz")
    new_id_map = os.path.join(res_dir, "fhir_id_map.npy")
    new_emb_tmp = tmp_path(old_emb)
    new_meta_tmp = tmp_path(new_meta)
    new_id_map_tmp = tmp_path(new_id_map)

    if not os.path.isfile(old_ids) and os.path.isfile(new_meta):
        log.info("already migrated (no %s, %s exists); nothing to do",
                 os.path.basename(old_ids), os.path.basename(new_meta))
        return 0
    for p in (old_emb, old_ids, old_idx, old_names):
        if not os.path.isfile(p):
            log.error("missing input: %s", p)
            return 1

    embs = np.load(old_emb, mmap_mode="r")
    ids = np.load(old_ids)
    if embs.shape[0] != ids.shape[0]:
        log.error("row mismatch: embeddings=%d ids=%d", embs.shape[0], ids.shape[0])
        return 1
    n = int(embs.shape[0])
    log.info("loaded N=%d (emb dtype=%s, dim=%d)", n, embs.dtype, embs.shape[1])

    # 1) old DB pk → (system_int, code_str). CSV column "standard" is
    # the persisted artifact column name; kept as-is for compatibility.
    id_to_pair: dict[int, tuple[int, str]] = {}
    with gzip.open(old_idx, "rt", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            id_to_pair[int(row["id"])] = (int(row["standard"]), row["code"])
    log.info("loaded %d code_index rows", len(id_to_pair))

    # 2) per-row new fhir_id; capture hash rows (DCM/THETA) for meta
    new_ids = np.empty(n, dtype=np.int64)
    hashed: dict[int, str] = {}
    orphans: list[int] = []
    for r in range(n):
        old_id = int(ids[r])
        pair = id_to_pair.get(old_id)
        if pair is None:
            orphans.append(r)
            new_ids[r] = 0
            continue
        sys_int, code_str = pair
        new_ids[r] = code_to_fhir_id(sys_int, code_str)
        if SYSTEMS[sys_int] in _HASH_SYSTEMS:
            hashed[r] = code_str

    if orphans:
        log.error(
            "%d rows have ids absent from code_index (e.g. %s). "
            "Aborting — investigate before re-running.",
            len(orphans), orphans[:5],
        )
        return 1

    log.info("new fhir_ids built: %d hashed rows (%.2f%%)",
             len(hashed), 100.0 * len(hashed) / n)

    if dry_run:
        log.info("--dry-run: skipping writes")
        return 0

    # 3) structured npy: chunked emb copy to keep peak RAM bounded
    dtype = np.dtype([("fhir_id", "<i8"), ("emb", "<f2", (embs.shape[1],))])
    out = np.lib.format.open_memmap(new_emb_tmp, mode="w+", dtype=dtype, shape=(n,))
    out["fhir_id"] = new_ids
    chunk = 1 << 16
    for s in range(0, n, chunk):
        e = min(s + chunk, n)
        out["emb"][s:e] = embs[s:e]
    out.flush()
    del out
    log.info("wrote %s", new_emb_tmp)

    # 4) meta csv (row-aligned; code_str only for hashed rows)
    with gzip.open(old_names, "rt", encoding="utf-8", newline="") as fin, \
         open_gz_text_write(new_meta_tmp) as fout:
        r_in = csv.reader(fin)
        next(r_in)  # skip header
        w = csv.writer(fout)
        w.writerow(["name", "code_str"])
        rows_written = 0
        for r, row in enumerate(r_in):
            name = row[0] if row else ""
            w.writerow([name, hashed.get(r, "")])
            rows_written += 1
    if rows_written != n:
        log.error("names row count %d != N=%d; aborting before swap",
                  rows_written, n)
        os.remove(new_emb_tmp)
        os.remove(new_meta_tmp)
        return 1
    log.info("wrote %s", new_meta_tmp)

    # 5) id_map: row-aligned db_pk per embedding row. The legacy
    # fhir_embedding_ids.npy already holds DB pks aligned to embedding
    # rows — just save it under the new name with the right dtype.
    np.save(new_id_map_tmp, ids.astype(np.int64))
    log.info("wrote %s", new_id_map_tmp)

    # 6) atomic swap-in + remove old artifacts. fhir_embeddings.npy is
    # the expensive one (1.4 GB, hours to rebuild) — keep its previous
    # version as .bak; meta and id_map are cheap so plain replace.
    atomic_swap_keep_backup(new_emb_tmp, old_emb)
    os.replace(new_meta_tmp, new_meta)
    os.replace(new_id_map_tmp, new_id_map)
    os.remove(old_ids)
    os.remove(old_idx)
    os.remove(old_names)
    log.info("done; new layout active in %s", res_dir)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--res-dir", default=_default_res_dir(),
                   help="dir containing the fhir_* artifacts")
    p.add_argument("--dry-run", action="store_true",
                   help="check + log without writing/replacing")
    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return migrate(args.res_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())

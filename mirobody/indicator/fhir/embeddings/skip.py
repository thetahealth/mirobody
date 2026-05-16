"""Build ``fhir_loinc_skip.npy``: a row-aligned bool mask marking LOINC
codes that ``resolve`` should exclude. Two filter axes apply:

* CLASS / CLASSTYPE — surveys, panels, docs, admin (see
  :data:`mirobody.indicator.fhir.common._SKIP_CLASS_PREFIXES` and
  :data:`_SKIP_CLASSTYPES`).
* STATUS — DEPRECATED and DISCOURAGED rows; both have ACTIVE successors
  that LOINC steers callers toward (see :data:`_SKIP_STATUSES`).

The mask is row-aligned to ``fhir_embeddings.npy``. Resolve loads it
eagerly (~700 KB raw) and ANDs it into the per-system topk gate, so
filtered codes never compete for slots. Generating the mask is cheap —
derived from existing meta + LoincTableCore.csv — so it does **not**
require re-running the slow embedding pipeline.

Non-LOINC rows are always ``False`` (not skipped). Out of scope for
this v1: SNOMED inactive, RxNorm obsolete. Add only when concrete
need arises.
"""

from __future__ import annotations

import logging
import os
from argparse import Namespace

import numpy as np

from ..common import (
    SYSTEM_TO_CODE,
    _CODE_BITS,
    _CODE_MASK,
    code_to_int,
    load_loinc_skip_codes,
)
from .local import EMB_BASENAME, EMB_DTYPE, RES_DIR

log = logging.getLogger(__name__)

LOINC_SKIP_BASENAME = "fhir_loinc_skip.npy"
LOINC_SKIP_PATH = os.path.join(RES_DIR, LOINC_SKIP_BASENAME)


def build_loinc_skip_mask(emb_path: str, loinc_core_csv: str) -> np.ndarray:
    """Compute a bool[N] mask, row-aligned to *emb_path*."""
    arr = np.load(emb_path, mmap_mode="r")
    if arr.dtype != EMB_DTYPE:
        raise ValueError(
            f"{emb_path} dtype {arr.dtype} != expected {EMB_DTYPE}"
        )
    canonical = np.asarray(arr["fhir_id"])

    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    loinc_idx = SYSTEM_TO_CODE["LOINC"]
    is_loinc = sys_arr == loinc_idx

    skip_codes = load_loinc_skip_codes(loinc_core_csv)
    skip_ints = np.fromiter(
        (code_to_int(c, "LOINC") for c in skip_codes),
        dtype=np.int64,
        count=len(skip_codes),
    )
    code_int_arr = canonical & _CODE_MASK
    mask = is_loinc & np.isin(code_int_arr, skip_ints)

    log.info(
        "loinc skip mask: %d / %d rows masked (%d skip-codes from %s)",
        int(mask.sum()), int(arr.shape[0]), len(skip_codes), loinc_core_csv,
    )
    return mask


async def cmd_loinc_skip(args: Namespace) -> None:
    """Subcommand: loinc-skip — write ``fhir_loinc_skip.npy``."""
    res_dir = args.res_dir or RES_DIR
    emb_path = os.path.join(res_dir, EMB_BASENAME)
    skip_path = os.path.join(res_dir, LOINC_SKIP_BASENAME)

    if not os.path.isfile(emb_path):
        raise FileNotFoundError(f"{emb_path} missing; run `embeddings` first")

    if not args.loinc_dir or not os.path.isdir(args.loinc_dir):
        raise SystemExit(
            f"--loinc-dir required (got {args.loinc_dir!r}); "
            "needs LoincTableCore.csv to identify class-filtered codes"
        )
    loinc_core = os.path.join(args.loinc_dir, "LoincTableCore", "LoincTableCore.csv")
    if not os.path.isfile(loinc_core):
        raise FileNotFoundError(loinc_core)

    mask = build_loinc_skip_mask(emb_path, loinc_core)
    np.save(skip_path, mask)
    log.info(
        "wrote %s (%d rows, %.1f KB)",
        skip_path, mask.shape[0], os.path.getsize(skip_path) / 1024,
    )

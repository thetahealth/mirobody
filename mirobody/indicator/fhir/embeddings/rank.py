"""Build the ``loinc_rank_bonus.npy`` member of
``fhir_loinc_bundle.tar.gz``: a row-aligned float32 bonus array applied
to LOINC cosines during resolve, derived from LOINC's
``COMMON_TEST_RANK`` column in ``LoincTable/Loinc.csv``.

A code's COMMON_TEST_RANK reflects how often the US LOINC Lab Network
sees it in real lab observations (1 = most common, max ~20000, 0 =
unranked). Top-100 ranks are everyday canonical analytes — Glucose
``2345-7`` at rank 6, Creatinine ``2160-0`` at rank 4 — while the long
tail is specialty / niche. The bonus is a SOFT tie-breaker: small
enough to never override a substantively better cosine, large enough
to flip a near-tie toward the canonical code (which is what callers
nearly always want).

The bonus is NOT a "modern method" signal. Old immunoassay codes
(``Helicobacter pylori IgG Ab``, ``Norovirus Ag``) often rank as high
as their PCR counterparts because labs still run them. For methodology
demote of clinically-obsolete codes, see ``_HAND_DEMOTE_NAME_PATTERNS``
in :mod:`mirobody.indicator.fhir.common`.

Build once per LOINC release; the bonus array is row-aligned to
``fhir_embeddings.npy`` so it re-uses the embedding ordering. Non-LOINC
rows and unranked LOINC rows get bonus 0.0.
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
)
from .local import EMB_BASENAME, EMB_DTYPE, RES_DIR

log = logging.getLogger(__name__)

LOINC_RANK_BONUS_MEMBER = "loinc_rank_bonus.npy"


# Tier curve: rank ∈ [1, threshold] gets bonus. Magnitudes calibrated
# to ~½ of one axis-bonus weight (axis weights are 0.04 each, summing
# to ~0.15) — tie-breaker scale, never a rank override. The first tier
# (top-100) is roughly the LOINC ``Universal Lab Orders`` value set's
# size; subsequent tiers widen by ×10 each.
_RANK_BONUS_TIERS: tuple[tuple[int, float], ...] = (
    (100, 0.020),
    (1000, 0.015),
    (5000, 0.010),
    (20000, 0.005),
)


def _rank_to_bonus(rank: int) -> float:
    """Map COMMON_TEST_RANK integer to bonus magnitude. 0 / unranked → 0.0."""
    if rank <= 0:
        return 0.0
    for threshold, bonus in _RANK_BONUS_TIERS:
        if rank <= threshold:
            return bonus
    return 0.0


def build_loinc_rank_bonus(emb_path: str, loinc_csv: str) -> np.ndarray:
    """Compute a float32 bonus array, row-aligned to *emb_path*.

    *loinc_csv* should be ``LoincTable/Loinc.csv`` (the full table —
    LoincTableCore.csv does NOT carry the rank columns).
    """
    arr = np.load(emb_path, mmap_mode="r")
    if arr.dtype != EMB_DTYPE:
        raise ValueError(
            f"{emb_path} dtype {arr.dtype} != expected {EMB_DTYPE}"
        )
    canonical = np.asarray(arr["fhir_id"])
    n = int(canonical.shape[0])

    sys_arr = ((canonical >> _CODE_BITS) & 0x7).astype(np.int8)
    loinc_idx = SYSTEM_TO_CODE["LOINC"]
    is_loinc = sys_arr == loinc_idx
    code_int_arr = canonical & _CODE_MASK

    import polars as pl
    df = pl.read_csv(
        loinc_csv, columns=["LOINC_NUM", "COMMON_TEST_RANK"],
        infer_schema=False,
    ).with_columns(
        pl.col("COMMON_TEST_RANK").cast(pl.Int32, strict=False).fill_null(0)
    ).filter(pl.col("COMMON_TEST_RANK") > 0)

    code_to_rank: dict[int, int] = {}
    for code_str, rank in zip(df["LOINC_NUM"].to_list(), df["COMMON_TEST_RANK"].to_list()):
        code_to_rank[code_to_int(code_str, "LOINC")] = int(rank)

    bonus = np.zeros(n, dtype=np.float32)
    # Tier-bucket histogram for log diagnostics.
    tier_counts = [0] * (len(_RANK_BONUS_TIERS) + 1)
    for r in np.flatnonzero(is_loinc):
        rank = code_to_rank.get(int(code_int_arr[r]), 0)
        if rank > 0:
            b = _rank_to_bonus(rank)
            bonus[r] = b
            # Find tier index for logging
            for i, (threshold, _) in enumerate(_RANK_BONUS_TIERS):
                if rank <= threshold:
                    tier_counts[i] += 1
                    break

    log.info(
        "loinc rank bonus built: %d rows ranked, tiers=%s, source=%s, embeddings=%s",
        int((bonus > 0).sum()),
        ", ".join(
            f"≤{t}:{c}" for (t, _), c in zip(_RANK_BONUS_TIERS, tier_counts)
        ),
        loinc_csv, emb_path,
    )
    return bonus


async def cmd_loinc_rank(args: Namespace) -> None:
    """Subcommand: loinc-rank — write ``loinc_rank_bonus.npy`` into the
    LOINC bundle (``fhir_loinc_bundle.tar.gz``)."""
    import io
    from .bundle import BUNDLE_BASENAME, write_member

    res_dir = args.res_dir or RES_DIR
    emb_path = os.path.join(res_dir, EMB_BASENAME)
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)

    if not os.path.isfile(emb_path):
        raise FileNotFoundError(f"{emb_path} missing; run `embeddings` first")
    if not args.loinc_dir or not os.path.isdir(args.loinc_dir):
        raise SystemExit(
            f"--loinc-dir required (got {args.loinc_dir!r}); "
            "needs LoincTable/Loinc.csv to read COMMON_TEST_RANK"
        )
    loinc_csv = os.path.join(args.loinc_dir, "LoincTable", "Loinc.csv")
    if not os.path.isfile(loinc_csv):
        raise FileNotFoundError(loinc_csv)

    bonus = build_loinc_rank_bonus(emb_path, loinc_csv)
    buf = io.BytesIO()
    np.save(buf, bonus, allow_pickle=False)
    write_member(LOINC_RANK_BONUS_MEMBER, buf.getvalue(), bundle_path=bundle_path)
    log.info(
        "wrote %s (%d rows) into %s",
        LOINC_RANK_BONUS_MEMBER, bonus.shape[0], bundle_path,
    )

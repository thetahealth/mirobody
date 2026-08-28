"""Build the ``fhir_dose_index.npz`` member of ``fhir_loinc_bundle.tar.gz``.

Corpus-side index of dose value+unit pairs extracted from each row's
display name. The resolver applies a small bonus to candidate rows
whose dose set intersects the query's dose set — catches the recurring
failure where a "OGTT 75g 葡萄糖" query lands on a 100g code or a
no-dose-specified placeholder because cosine alone can't tell the
challenge variants apart.

Spans the whole corpus, not just LOINC: RxNorm drug strengths (``500
mg``) and SNOMED procedure doses match the same way.

Storage (npz):
    row_idx     int32  (M,)     corpus row indices with at least one dose
    value       float32 (M,)    parsed dose value
    unit_id     int16  (M,)     index into unit_table
    unit_table  <U16   (K,)     UCUM canonical unit strings

Parallel arrays sized by the total dose count (one row can carry
multiple doses); the unit table stays small (~10 entries, restricted to
the dose-relevant UCUM families: Mass, Vol, CCnt, MCnt).
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
import os
from argparse import Namespace

import numpy as np

from mirobody.units import scan_value_units
from .local import META_BASENAME, RES_DIR

log = logging.getLogger(__name__)

DOSE_INDEX_MEMBER = "fhir_dose_index.npz"


def build_dose_index(meta_path: str) -> dict:
    """Scan every row's display name in *meta_path* and emit the parallel
    arrays + unit table that get stored as the bundle member.

    Returns a dict with keys ``row_idx``, ``value``, ``unit_id``,
    ``unit_table`` suitable for ``np.savez_compressed``. Empty arrays
    when no row carries a recognized dose (still valid input for the
    loader, which then sets ``cache["dose_index"] = {}`` and the
    resolver becomes a no-op for this layer).
    """
    if not os.path.isfile(meta_path):
        raise FileNotFoundError(meta_path)

    row_idx: list[int] = []
    values: list[float] = []
    unit_strs: list[str] = []
    with gzip.open(meta_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for r, row in enumerate(reader):
            if not row:
                continue
            name = row[0] if row else ""
            if not name:
                continue
            for v, u in scan_value_units(name):
                row_idx.append(r)
                values.append(v)
                unit_strs.append(u)

    # Build the unit table (sorted for reproducibility) and remap each
    # unit string to its table index. int16 is plenty — the dose-family
    # restriction keeps the cardinality in the single digits.
    unique_units = sorted(set(unit_strs))
    unit_to_id = {u: i for i, u in enumerate(unique_units)}
    unit_id_arr = np.fromiter(
        (unit_to_id[u] for u in unit_strs),
        dtype=np.int16, count=len(unit_strs),
    )

    out = {
        "row_idx":    np.asarray(row_idx, dtype=np.int32),
        "value":      np.asarray(values, dtype=np.float32),
        "unit_id":    unit_id_arr,
        "unit_table": np.asarray(unique_units, dtype="<U16"),
    }

    # Per-unit histogram for log diagnostics.
    counts = {u: 0 for u in unique_units}
    for u in unit_strs:
        counts[u] += 1
    by_count = ", ".join(
        f"{u}:{c}" for u, c in sorted(counts.items(), key=lambda x: -x[1])[:8]
    )
    log.info(
        "dose index built: %d hits across %d rows (out of %d); top units: %s",
        len(row_idx), len(set(row_idx)), r + 1, by_count,
    )
    return out


async def cmd_dose_index(args: Namespace) -> None:
    """Subcommand: dose-index — write ``fhir_dose_index.npz`` into the
    LOINC bundle (``fhir_loinc_bundle.tar.gz``).

    Source is the existing ``fhir_meta.csv.gz`` (row-aligned to
    embeddings) — no external ref-data dependency, unlike ``loinc-rank``
    / ``loinc-alias``. Rebuild after ``embeddings`` or after a units
    module change that alters ``scan_value_units``'s output.
    """
    from .bundle import BUNDLE_BASENAME, write_member

    res_dir = args.res_dir or RES_DIR
    meta_path = os.path.join(res_dir, META_BASENAME)
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)

    if not os.path.isfile(meta_path):
        raise FileNotFoundError(
            f"{meta_path} missing; run `embeddings` first to produce the meta sidecar"
        )

    data = build_dose_index(meta_path)
    buf = io.BytesIO()
    np.savez_compressed(buf, **data)
    write_member(DOSE_INDEX_MEMBER, buf.getvalue(), bundle_path=bundle_path)
    log.info(
        "wrote %s (%d hits, %d units) into %s",
        DOSE_INDEX_MEMBER, data["row_idx"].shape[0],
        data["unit_table"].shape[0], bundle_path,
    )

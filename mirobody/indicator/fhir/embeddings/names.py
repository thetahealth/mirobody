"""Display-name parsers for ~/ref source files.

Both ``embeddings`` modes (db / ref) call :func:`load_name_sources` and
:func:`_fill_meta_names` inline to populate the ``name`` column, so the
``code-names`` subcommand is a recovery path — only needed when a bundle
arrives with an empty ``name`` column (e.g. produced on a machine
without ``~/ref``) and the user later wants to fill it.

DCM rows look up by the original code stored in ``code_str`` (the int
form is one-way hashed); THETA rows are skipped — user-defined codes
have no canonical name source.
"""

from __future__ import annotations

import csv
import gzip
import logging
import os
from argparse import Namespace

import numpy as np

from ..common import (
    SYSTEMS,
    SYSTEM_TO_CODE,
    _CODE_BITS,
    _CODE_MASK,
    csv_field_size_limit,
    int_to_code,
)
from .local import EMB_BASENAME, EMB_DTYPE, META_BASENAME, RES_DIR, open_gz_text_write, tmp_path

log = logging.getLogger(__name__)

# SNOMED Fully Specified Name typeId — disambiguating (tag-suffixed) and
# always present for active concepts, so preferable to PT lookup which
# needs the language refset.
_SNOMED_FSN_TYPE = "900000000000003001"

# RxNorm term-type priority for picking one display STR per RXCUI.
# Lower index = higher priority. Covers drug concepts (SCD/SBD/PSN),
# ingredients (IN/MIN/PIN), dose forms, and brand names.
_RXNORM_TTY_PRIORITY = [
    "PSN", "SCD", "SBD", "BN", "IN", "MIN", "PIN",
    "DF", "SCDF", "SBDF", "SCDC", "SBDC", "BPCK", "GPCK", "SY",
]


# ─── Source parsers ─────────────────────────────────────────────────


def _load_loinc_names(loinc_dir: str) -> dict[str, str]:
    """LOINC code → preferred display (LCN, then SHORTNAME, then DisplayName)."""
    path = os.path.join(loinc_dir, "LoincTable", "Loinc.csv")
    out: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row["LOINC_NUM"]
            name = row["LONG_COMMON_NAME"] or row["SHORTNAME"] or row["DisplayName"]
            if code and name:
                out[code] = name
    return out


def _load_snomed_names(snomed_dir: str) -> dict[str, str]:
    """SCTID → active FSN (always present, tag-suffixed)."""
    snap_dir = os.path.join(snomed_dir, "Snapshot", "Terminology")
    desc_file = next(
        (os.path.join(snap_dir, f) for f in sorted(os.listdir(snap_dir))
         if f.startswith("sct2_Description_Snapshot-en")),
        None,
    )
    if desc_file is None:
        raise FileNotFoundError(f"SNOMED en-description snapshot not found under {snap_dir}")
    out: dict[str, str] = {}
    with csv_field_size_limit(), open(desc_file, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter="\t")
        for row in r:
            if row["active"] != "1" or row["typeId"] != _SNOMED_FSN_TYPE:
                continue
            out[row["conceptId"]] = row["term"]
    return out


def _load_rxnorm_names(rxnorm_dir: str) -> dict[str, str]:
    """RxCUI → best STR by TTY priority (RXNORM source only, suppressed=N)."""
    path = os.path.join(rxnorm_dir, "rrf", "RXNCONSO.RRF")
    pri = {t: i for i, t in enumerate(_RXNORM_TTY_PRIORITY)}
    best: dict[str, tuple[int, str]] = {}  # rxcui -> (rank, str)
    with open(path, encoding="utf-8") as f:
        for line in f:
            # Layout: RXCUI|LAT|TS|LUI|STT|SUI|ISPREF|RXAUI|SAUI|SCUI|
            #         SDUI|SAB|TTY|CODE|STR|SRL|SUPPRESS|CVF|
            parts = line.rstrip("\n").split("|")
            if len(parts) < 17 or parts[11] != "RXNORM" or parts[16] == "Y":
                continue
            tty, code, s = parts[12], parts[13], parts[14]
            if not code or not s:
                continue
            rank = pri.get(tty, 999)
            existing = best.get(code)
            if existing is None or rank < existing[0]:
                best[code] = (rank, s)
    return {k: v[1] for k, v in best.items()}


def _load_cvx_names(ref_dir: str) -> dict[str, str]:
    """CVX code → full name (or short_name fallback)."""
    path = os.path.join(ref_dir, "cvx.txt")
    out: dict[str, str] = {}
    # utf-8-sig strips the leading BOM.
    with open(path, encoding="utf-8-sig") as f:
        for line in f:
            # Layout: code|short_name|full_name|notes|status|...|date
            parts = line.rstrip("\n").split("|")
            if len(parts) < 3:
                continue
            code = parts[0].strip()
            name = parts[2].strip() or parts[1].strip()
            if code and name:
                out[code] = name
    return out


def _load_dcm_names(dicom_dir: str) -> dict[str, str]:
    """DCM code → canonical display name.

    Sources (in priority order):
      1. **Part 16 Annex D, ``table_D-1``** — authoritative
         "DICOM Controlled Terminology Definitions" master list,
         one canonical name per code.
      2. **CID tables** — fall-back for codes Annex D omits, using
         the first non-empty Code Meaning encountered.
    """
    # Deferred imports to avoid module-level cycle: ref.py imports
    # ``load_name_sources`` from this module at top level.
    from .ref import _iter_dcm_terminology, _iter_dicom_cid_rows
    path = os.path.join(dicom_dir, "part16.xml")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DICOM part16.xml not found: {path}")
    out: dict[str, str] = {}
    for code, name in _iter_dcm_terminology(path):
        out[code] = name
    for _cid, scheme, code, meaning in _iter_dicom_cid_rows(path):
        if scheme == "DCM" and meaning and code not in out:
            out[code] = meaning
    return out


def load_name_sources(args: Namespace) -> dict[int, dict[str, str]]:
    """Load whichever per-vocab name dicts the args point at.

    Keys are the ``SYSTEMS`` enum int. THETA / DCM are intentionally
    absent — no curated source. Vocabs without a corresponding ``--*-dir``
    arg are silently skipped (caller can decide whether that's an error).
    """
    sources: dict[int, dict[str, str]] = {}
    if getattr(args, "snomed_dir", None) and os.path.isdir(args.snomed_dir):
        log.info("loading SNOMED FSN from %s", args.snomed_dir)
        d = _load_snomed_names(args.snomed_dir)
        log.info("  %d SNOMED FSN entries", len(d))
        sources[SYSTEM_TO_CODE["SNOMED_CT"]] = d
    if getattr(args, "loinc_dir", None) and os.path.isdir(args.loinc_dir):
        log.info("loading LOINC LCN from %s", args.loinc_dir)
        d = _load_loinc_names(args.loinc_dir)
        log.info("  %d LOINC entries", len(d))
        sources[SYSTEM_TO_CODE["LOINC"]] = d
    if getattr(args, "rxnorm_dir", None) and os.path.isdir(args.rxnorm_dir):
        log.info("loading RxNorm from %s", args.rxnorm_dir)
        d = _load_rxnorm_names(args.rxnorm_dir)
        log.info("  %d RxNorm RXCUIs", len(d))
        sources[SYSTEM_TO_CODE["RXNORM"]] = d
    cvx_path = (
        os.path.join(args.ref_dir, "cvx.txt")
        if getattr(args, "ref_dir", None) else ""
    )
    if cvx_path and os.path.isfile(cvx_path):
        log.info("loading CVX from %s", cvx_path)
        d = _load_cvx_names(args.ref_dir)
        log.info("  %d CVX codes", len(d))
        sources[SYSTEM_TO_CODE["CVX"]] = d
    if getattr(args, "dicom_dir", None) and os.path.isdir(args.dicom_dir):
        log.info("loading DCM Code Meaning from %s", args.dicom_dir)
        d = _load_dcm_names(args.dicom_dir)
        log.info("  %d DCM codes", len(d))
        sources[SYSTEM_TO_CODE["DCM"]] = d
    return sources


# ─── Subcommand ─────────────────────────────────────────────────────


def _fill_meta_names(
    emb_path: str, meta_path: str, sources: dict[int, dict[str, str]],
) -> None:
    """Rewrite *meta_path* with the ``name`` column filled from *sources*.

    Reads canonical fhir_ids from *emb_path*, decodes ``(system, code)``,
    looks up the display name in *sources*, and writes a new meta gz
    preserving the existing ``code_str`` column. Rows for vocabs absent
    from *sources* (e.g. THETA) keep ``name`` empty.
    """
    arr = np.load(emb_path, mmap_mode="r")
    if arr.dtype != EMB_DTYPE:
        raise ValueError(
            f"{emb_path} has dtype {arr.dtype}, expected {EMB_DTYPE} — "
            f"re-run `embeddings` to produce the new format"
        )
    n_rows = int(arr.shape[0])

    code_strs: list[str] = [""] * n_rows
    if os.path.isfile(meta_path):
        with gzip.open(meta_path, "rt", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for i, row in enumerate(r):
                if i >= n_rows:
                    break
                code_strs[i] = row.get("code_str", "")

    canonical = arr["fhir_id"]
    names: list[str] = [""] * n_rows
    hit = {sys_int: 0 for sys_int in sources}
    miss = {sys_int: 0 for sys_int in sources}
    for r in range(n_rows):
        fid = int(canonical[r])
        sys_int = fid >> _CODE_BITS
        nd = sources.get(sys_int)
        if nd is None:
            continue
        sys_name = SYSTEMS[sys_int]
        if sys_name in ("DCM", "THETA"):
            # Hash row: int_to_code can't reverse, original code lives
            # in meta.code_str (preserved earlier in this function).
            code = code_strs[r]
            if not code:
                miss[sys_int] += 1
                continue
        else:
            code = int_to_code(fid & _CODE_MASK, sys_name)
        nm = nd.get(code, "")
        names[r] = nm
        if nm:
            hit[sys_int] += 1
        else:
            miss[sys_int] += 1

    for sys_int in sources:
        total = hit[sys_int] + miss[sys_int]
        if total:
            log.info("  %s: %d/%d hit (%.1f%%)",
                     SYSTEMS[sys_int], hit[sys_int], total,
                     100.0 * hit[sys_int] / total)

    tmp = tmp_path(meta_path)
    with open_gz_text_write(tmp) as f:
        w = csv.writer(f)
        w.writerow(["name", "code_str"])
        for r in range(n_rows):
            w.writerow([names[r], code_strs[r]])
    os.replace(tmp, meta_path)
    log.info("wrote %s (%d rows, %.1f MB)",
             meta_path, n_rows, os.path.getsize(meta_path) / 1e6)


async def cmd_code_names(args: Namespace) -> None:
    """Subcommand: code-names — recovery path that fills the ``name``
    column of an existing ``fhir_meta.csv.gz``.

    Both ``embeddings`` modes already do this inline when ``~/ref`` is
    available; use this only to repair a bundle whose ``name`` column
    was left empty (e.g. produced on a machine without ``~/ref``).
    """
    res_dir = args.res_dir or RES_DIR
    emb_path = os.path.join(res_dir, EMB_BASENAME)
    meta_path = os.path.join(res_dir, META_BASENAME)

    if not os.path.isfile(emb_path):
        raise FileNotFoundError(f"{emb_path} missing; run `embeddings` first")

    sources = load_name_sources(args)
    if not sources:
        raise SystemExit(
            "no name source dirs provided; pass --snomed-dir / --loinc-dir / "
            "--rxnorm-dir or set MIROBODY_REF_DIR"
        )
    _fill_meta_names(emb_path, meta_path, sources)

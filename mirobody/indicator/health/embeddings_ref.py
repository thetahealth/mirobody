"""Bootstrap fhir_code_index + fhir_embeddings from ~/ref source files.

For users with an empty fhir_indicators table (e.g. fresh GitHub clones).
Enumerates (standard, code, text) triples from SNOMED / LOINC / RxNorm raw
files, calls Gemini embedding in resumable batches.

Output schema matches the DB path (see embeddings.py), but the data is
NOT interchangeable:
  • text source differs (raw English descriptions vs DB's llm_description)
    → numerically different vectors, slightly different recall
  • id namespace differs (synthetic row index vs fhir_indicators.id)
    → a user who picks this path can never mix in DB-derived artifacts

Two phases; Phase 2 is checkpoint-resumable (500k+ API calls, will be
interrupted at least once in practice):

  Phase 1 (fast, deterministic, no API):
    parse ~/ref → out/fhir_ref_texts.csv
                + res/fhir_code_index.csv.gz
                + res/fhir_embedding_ids.npy

  Phase 2 (slow, resumable):
    out/fhir_ref_texts.csv → out/fhir_embeddings.npy.partial (memmap fp32)
                           + out/fhir_embeddings.progress.json
    on completion: finalize → res/fhir_embeddings.npy (fp16, L2-normalised)
"""

from __future__ import annotations

import csv
import glob
import gzip
import hashlib
import json
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict
from collections.abc import Iterator
from xml.etree import ElementTree as ET

import numpy as np

from .common import EMBEDDING_DIM, STANDARD_TO_CODE

log = logging.getLogger(__name__)

_EMBED_BATCH = 256  # rows per Gemini call + checkpoint granularity

# SNOMED RF2 description typeIds
_SNOMED_FSN = "900000000000003001"
_SNOMED_SYNONYM = "900000000000013009"

# DocBook namespace used by DICOM PS3.16 XML
_DOCBOOK_NS = "http://docbook.org/ns/docbook"
_XML_NS = "http://www.w3.org/XML/1998/namespace"


# ─── Source parsers ─────────────────────────────────────────────────


def _find_rf2(snomed_dir: str, basename_glob: str) -> str:
    matches = glob.glob(os.path.join(snomed_dir, "Snapshot", "Terminology", basename_glob))
    if not matches:
        matches = glob.glob(os.path.join(snomed_dir, "**", basename_glob), recursive=True)
    if not matches:
        raise FileNotFoundError(f"RF2 file not found: {basename_glob} in {snomed_dir}")
    return matches[0]


def _iter_snomed(snomed_dir: str) -> Iterator[tuple[str, str]]:
    """Yield (sctid, text) for every active SNOMED concept.

    Text = "FSN; synonym1; synonym2; ..." — FSN first, all active descriptions
    joined with "; " (deterministic order: FSN, then synonyms sorted).
    """
    path = _find_rf2(snomed_dir, "sct2_Description_Snapshot-en*.txt")
    log.info("Parsing SNOMED descriptions: %s", path)

    fsn: dict[str, str] = {}
    synonyms: dict[str, set[str]] = defaultdict(set)
    with open(path, "r", encoding="utf-8") as f:
        next(f)  # header
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 8 or fields[2] != "1":  # inactive
                continue
            sctid = fields[4]
            type_id = fields[6]
            term = fields[7]
            if type_id == _SNOMED_FSN:
                fsn.setdefault(sctid, term)
            elif type_id == _SNOMED_SYNONYM:
                synonyms[sctid].add(term)

    n = 0
    for sctid in sorted(fsn):
        parts = [fsn[sctid]]
        extras = sorted(s for s in synonyms.get(sctid, ()) if s != fsn[sctid])
        parts.extend(extras)
        yield sctid, "; ".join(parts)
        n += 1
    log.info("SNOMED concepts enumerated: %s", f"{n:,}")


def _iter_loinc(loinc_dir: str) -> Iterator[tuple[str, str]]:
    """Yield (loinc_num, text) for every non-skipped LOINC code.

    Text = "{LCN}. Component: {C}. System: {S}. Method: {M}."
    Empty axes are omitted. Skip prefixes (LP/LA/MTHU/LG) + non-lab CLASS
    filters match the existing pipeline (common.py).
    """
    from .common import TARGET_SYSTEMS, load_loinc_skip_codes

    core_csv = os.path.join(loinc_dir, "LoincTable", "LoincTableCore.csv")
    full_csv = os.path.join(loinc_dir, "LoincTable", "Loinc.csv")
    if not os.path.isfile(core_csv):
        raise FileNotFoundError(f"LoincTableCore.csv not found: {core_csv}")
    if not os.path.isfile(full_csv):
        full_csv = core_csv  # fall back; LCN is in core anyway

    skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]
    skip_codes = load_loinc_skip_codes(core_csv)
    log.info("Parsing LOINC: %s (%s skip-codes)", full_csv, f"{len(skip_codes):,}")

    n = 0
    rows: list[tuple[str, str]] = []
    with open(full_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row["LOINC_NUM"]
            if code.startswith(skip_prefixes) or code in skip_codes:
                continue
            lcn = (row.get("LONG_COMMON_NAME") or "").strip()
            comp = (row.get("COMPONENT") or "").strip()
            system = (row.get("SYSTEM") or "").strip()
            method = (row.get("METHOD_TYP") or "").strip()
            segments: list[str] = []
            if lcn:
                segments.append(lcn)
            if comp:
                segments.append(f"Component: {comp}")
            if system:
                segments.append(f"System: {system}")
            if method:
                segments.append(f"Method: {method}")
            if not segments:
                continue
            rows.append((code, ". ".join(segments) + "."))
            n += 1
    rows.sort(key=lambda r: r[0])
    yield from rows
    log.info("LOINC codes enumerated: %s", f"{n:,}")


def _iter_rxnorm(rxnorm_dir: str) -> Iterator[tuple[str, str]]:
    """Yield (rxcui, text) for every active RxNorm concept with at least one name.

    Text = "name1; name2; ..." — dedup'd IN/PIN/BN names for that RXCUI
    plus brand tradenames joined via RXNREL tradename_of (matches the
    existing siblings pipeline).
    """
    from .common import read_rrf

    try:
        import polars as pl
    except ImportError as e:
        raise RuntimeError("polars required for RxNorm parsing") from e

    rxnconso = os.path.join(rxnorm_dir, "rrf", "RXNCONSO.RRF")
    rxnrel = os.path.join(rxnorm_dir, "rrf", "RXNREL.RRF")
    if not os.path.isfile(rxnconso):
        raise FileNotFoundError(f"RXNCONSO.RRF not found: {rxnconso}")
    log.info("Parsing RxNorm: %s", rxnconso)

    conso_df = read_rrf(rxnconso, ["SAB", "TTY", "CODE", "STR"])
    rxn_df = conso_df.filter(pl.col("SAB") == "RXNORM")

    names: dict[str, set[str]] = defaultdict(set)
    bn_name: dict[str, str] = {}
    for row in rxn_df.iter_rows(named=True):
        code, tty, name = row["CODE"], row["TTY"], row["STR"]
        if tty in ("IN", "PIN"):
            names[code].add(name)
        elif tty == "BN":
            bn_name[code] = name
            names[code].add(name)

    if os.path.isfile(rxnrel):
        with open(rxnrel, "r", encoding="utf-8") as f:
            for line in f:
                fields = line.split("|")
                if len(fields) > 7 and fields[7] == "tradename_of":
                    in_rxcui, bn_rxcui = fields[0], fields[4]
                    bn = bn_name.get(bn_rxcui)
                    if bn and in_rxcui in names:
                        names[in_rxcui].add(bn)

    n = 0
    for code in sorted(names):
        if not names[code]:
            continue
        yield code, "; ".join(sorted(names[code]))
        n += 1
    log.info("RxNorm codes enumerated: %s", f"{n:,}")


def _dicom_cell_text(td: ET.Element) -> str:
    """Flatten text inside a DocBook <td>, collapsing whitespace."""
    return re.sub(r"\s+", " ", "".join(td.itertext())).strip()


def _iter_dicom_cid_rows(part16_xml: str) -> Iterator[tuple[str, str, str, str]]:
    """Yield ``(cid, scheme_designator, code_value, code_meaning)`` for every
    row across every CID table in DICOM PS3.16.

    Scheme values encountered include ``DCM`` (DICOM-native), ``SCT`` (SNOMED
    CT), ``LN`` (LOINC), ``MDC``, ``UCUM``, ``RADLEX``, etc.
    """
    tree = ET.parse(part16_xml)
    q_table = f"{{{_DOCBOOK_NS}}}table"
    q_tbody = f"{{{_DOCBOOK_NS}}}tbody"
    q_tr = f"{{{_DOCBOOK_NS}}}tr"
    q_td = f"{{{_DOCBOOK_NS}}}td"
    q_xml_id = f"{{{_XML_NS}}}id"

    for table in tree.getroot().iter(q_table):
        tid = table.get(q_xml_id, "")
        if not tid.startswith("table_CID_"):
            continue
        cid = tid[len("table_CID_"):]
        tbody = table.find(q_tbody)
        if tbody is None:
            continue
        for tr in tbody.findall(q_tr):
            tds = tr.findall(q_td)
            if len(tds) < 3:
                continue
            scheme = _dicom_cell_text(tds[0])
            code = _dicom_cell_text(tds[1])
            meaning = _dicom_cell_text(tds[2])
            if scheme and code:
                yield cid, scheme, code, meaning


def _iter_dcm(dicom_dir: str) -> Iterator[tuple[str, str]]:
    """Yield ``(dcm_code, text)`` for every DCM code across Part 16 CIDs.

    Text = ``"meaning1; meaning2; ..."`` — dedup'd Code Meanings for that
    code across every CID it appears in (a single DCM code can be reused
    across multiple Context Groups with the same or slightly-varied meaning).
    """
    path = os.path.join(dicom_dir, "part16.xml")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DICOM part16.xml not found: {path}")
    log.info("Parsing DICOM Part 16: %s", path)

    meanings: dict[str, set[str]] = defaultdict(set)
    for _cid, scheme, code, meaning in _iter_dicom_cid_rows(path):
        if scheme == "DCM" and meaning:
            meanings[code].add(meaning)

    n = 0
    for code in sorted(meanings):
        yield code, "; ".join(sorted(meanings[code]))
        n += 1
    log.info("DCM codes enumerated: %s", f"{n:,}")


def _write_dcm_sct_bridge(dicom_dir: str, out_path: str) -> int:
    """Write a ``(dcm_code, sct_code, cid)`` co-occurrence table.

    For every CID, emit the cartesian product of DCM codes × SCT codes
    appearing in that CID. Acts as an alias lookup: a SNOMED search hit
    can surface the DICOM concept covering the same meaning in an imaging
    context, and vice versa.
    """
    path = os.path.join(dicom_dir, "part16.xml")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DICOM part16.xml not found: {path}")

    per_cid: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for cid, scheme, code, _m in _iter_dicom_cid_rows(path):
        per_cid[cid][scheme].add(code)

    pairs: set[tuple[str, str, str]] = set()
    for cid, by_scheme in per_cid.items():
        for d in by_scheme.get("DCM", ()):
            for s in by_scheme.get("SCT", ()):
                pairs.add((d, s, cid))

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dcm_code", "sct_code", "cid"])
        for row in sorted(pairs):
            w.writerow(row)
    log.info("DCM↔SCT bridge: %s pairs → %s", f"{len(pairs):,}", out_path)
    return len(pairs)


# ─── Phase 1 ────────────────────────────────────────────────────────


def _phase1_enumerate(args: Namespace, out_dir: str, res_dir: str) -> int:
    """Parse ~/ref, write fhir_ref_texts.csv + Group 1 artifacts.

    Returns the total row count N (same for all three output files).
    """
    texts_path = os.path.join(out_dir, "fhir_ref_texts.csv")
    code_path = os.path.join(res_dir, "fhir_code_index.csv.gz")
    ids_path = os.path.join(res_dir, "fhir_embedding_ids.npy")

    sources: list[tuple[str, Iterator[tuple[str, str]]]] = []
    if args.snomed_dir and os.path.isdir(args.snomed_dir):
        sources.append(("SNOMED_CT", _iter_snomed(args.snomed_dir)))
    if args.loinc_dir and os.path.isdir(args.loinc_dir):
        sources.append(("LOINC", _iter_loinc(args.loinc_dir)))
    if args.rxnorm_dir and os.path.isdir(args.rxnorm_dir):
        sources.append(("RXNORM", _iter_rxnorm(args.rxnorm_dir)))
    if getattr(args, "dicom_dir", None) and os.path.isdir(args.dicom_dir):
        sources.append(("DCM", _iter_dcm(args.dicom_dir)))
    if not sources:
        raise SystemExit(
            "no reference directories found. Pass --snomed-dir / --loinc-dir / "
            "--rxnorm-dir / --dicom-dir or set MIROBODY_REF_DIR."
        )

    row_idx = 0
    with open(texts_path, "w", encoding="utf-8", newline="") as ft, \
         gzip.open(code_path, "wt", encoding="utf-8", newline="") as fc:
        wt = csv.writer(ft)
        wc = csv.writer(fc)
        wt.writerow(["row_idx", "standard", "code", "text"])
        wc.writerow(["standard", "code", "id"])
        for std_name, it in sources:
            std_code = STANDARD_TO_CODE[std_name]
            cnt = 0
            for code, text in it:
                wt.writerow([row_idx, std_code, code, text])
                wc.writerow([std_code, code, row_idx])
                row_idx += 1
                cnt += 1
            log.info("  %s: %s rows", std_name, f"{cnt:,}")

    np.save(ids_path, np.arange(row_idx, dtype=np.int64))
    log.info(
        "Phase 1 done: N=%s → %s (%.1f MB), %s, %s",
        f"{row_idx:,}",
        texts_path, os.path.getsize(texts_path) / 1e6,
        code_path, ids_path,
    )
    return row_idx


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Phase 2 ────────────────────────────────────────────────────────


async def _phase2_embed(args: Namespace, out_dir: str, res_dir: str, n_rows: int) -> None:
    """Resumable Gemini embedding over out/fhir_ref_texts.csv."""
    from mirobody.utils.embedding import text_embedding

    texts_path = os.path.join(out_dir, "fhir_ref_texts.csv")
    partial_path = os.path.join(out_dir, "fhir_embeddings.npy.partial")
    progress_path = os.path.join(out_dir, "fhir_embeddings.progress.json")
    final_path = os.path.join(res_dir, "fhir_embeddings.npy")

    texts_md5 = _file_md5(texts_path)
    resume_from = 0
    if os.path.isfile(partial_path) and os.path.isfile(progress_path):
        with open(progress_path, "r", encoding="utf-8") as f:
            prog = json.load(f)
        if prog.get("texts_md5") == texts_md5 and prog.get("n_rows") == n_rows:
            resume_from = int(prog.get("last_completed", 0))
            log.info("resuming Phase 2 from row %s/%s", f"{resume_from:,}", f"{n_rows:,}")
        else:
            log.warning(
                "progress file stale (md5/n_rows mismatch) — restarting Phase 2 from 0"
            )
            resume_from = 0

    # Pre-allocate memmap (stays fp32 during embed; cast+normalise at finalize)
    mm = np.memmap(partial_path, dtype=np.float32, mode="r+" if os.path.isfile(partial_path) else "w+",
                   shape=(n_rows, EMBEDDING_DIM))

    # Stream texts, skipping already-done prefix
    buf_idx: list[int] = []
    buf_text: list[str] = []

    async def flush():
        if not buf_text:
            return
        embs = await text_embedding(buf_text, provider="gemini")
        for i, emb in zip(buf_idx, embs):
            if emb is None:
                raise RuntimeError(
                    f"row {i}: text_embedding returned None; text likely empty/invalid"
                )
            if len(emb) != EMBEDDING_DIM:
                raise RuntimeError(
                    f"row {i}: expected {EMBEDDING_DIM} dims, got {len(emb)}"
                )
            mm[i] = np.asarray(emb, dtype=np.float32)
        mm.flush()
        last = buf_idx[-1]
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump(
                {"texts_md5": texts_md5, "n_rows": n_rows, "last_completed": last + 1},
                f,
            )
        log.info("embedded %s/%s rows", f"{last + 1:,}", f"{n_rows:,}")
        buf_idx.clear()
        buf_text.clear()

    with open(texts_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            idx = int(row["row_idx"])
            if idx < resume_from:
                continue
            buf_idx.append(idx)
            buf_text.append(row["text"])
            if len(buf_text) >= _EMBED_BATCH:
                await flush()
    await flush()

    # Finalize: L2-normalise, cast to fp16, write res/fhir_embeddings.npy
    log.info("finalizing: normalising + casting fp32 → fp16 (%s rows)", f"{n_rows:,}")
    arr = np.asarray(mm[:])  # read whole thing (≈2 GB fp32; OK in RAM)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    arr /= norms
    np.save(final_path, arr.astype(np.float16))

    del mm
    os.remove(partial_path)
    os.remove(progress_path)
    log.info(
        "Phase 2 done: %s (%.1f MB)",
        final_path, os.path.getsize(final_path) / 1e6,
    )


# ─── Entry point ────────────────────────────────────────────────────


async def cmd_embeddings_ref(args: Namespace) -> None:
    out_dir = args.output or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "out"
    )
    res_dir = args.res_dir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "res"
    )
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(res_dir, exist_ok=True)

    # Phase 1 is cheap and deterministic — always re-run (also refreshes
    # texts_md5 for Phase 2's stale-checkpoint detection).
    n = _phase1_enumerate(args, out_dir, res_dir)

    # Side artifact: DCM ↔ SCT co-occurrence bridge (requires part16.xml)
    if getattr(args, "dicom_dir", None) and os.path.isdir(args.dicom_dir):
        _write_dcm_sct_bridge(
            args.dicom_dir,
            os.path.join(res_dir, "fhir_dcm_sct_bridge.csv"),
        )

    await _phase2_embed(args, out_dir, res_dir, n)

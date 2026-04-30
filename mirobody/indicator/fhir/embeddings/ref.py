"""Bootstrap the offline bundle from ~/ref source files (no DB needed).

Used when ``fhir_indicators`` is empty / does not exist (fresh GitHub
clone, fresh deployment). Enumerates ``(system, code, text)`` triples
from SNOMED / LOINC / RxNorm / DCM raw files, calls Gemini embedding in
resumable batches, and writes:

  fhir_embeddings.npy   structured (N,)
                        dtype = [('fhir_id','i8'),('emb','f2',(1024,))]
                        ``fhir_id`` is canonical packed via
                        :func:`code_to_fhir_id`.

  fhir_meta.csv.gz      row-aligned to ``fhir_embeddings.npy``;
                        cols: ``name`` (filled inline from ~/ref —
                        :mod:`names` parsers), ``code_str`` (original
                        string for DCM hash rows; empty otherwise).
                        No post-step ``code-names`` needed for ref mode.

**No** ``fhir_id_map.npy`` is produced — there is no DB pk to bridge.
This is the **terminal mode** layout: consumers will use canonical ids
directly. Upstream **must** populate ``th_series_data.fhir_id`` with
canonical values (``code_to_fhir_id(system, code)``) for the local
search path to find anything.

Layout note: ``SYSTEMS`` in :mod:`common` is **append-only** — the
enum index is bit-packed into every ``canonical`` value. Reordering or
deleting an entry breaks every previously-written ``fhir_id``.

Two phases; Phase 2 is checkpoint-resumable (500k+ Gemini calls — will
be interrupted at least once in practice):

  Phase 1 (fast, deterministic, no API):
    parse ~/ref → out/fhir_ref_texts.csv
                  cols: canonical, standard, code, text, name
                  (CSV column "standard" kept for artifact compatibility)

  Phase 2 (slow, resumable):
    out/fhir_ref_texts.csv → out/fhir_embeddings.npy.partial (fp32 memmap)
                           + out/fhir_embeddings.progress.json
    on completion: finalize → res/fhir_embeddings.npy + res/fhir_meta.csv.gz
"""

from __future__ import annotations

import csv
import glob
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

from ..common import EMBEDDING_DIM, SYSTEMS, SYSTEM_TO_CODE, code_to_fhir_id
from .local import (
    EMB_DTYPE,
    EMB_PATH,
    META_PATH,
    RES_DIR,
    atomic_swap_keep_backup,
    open_gz_text_write,
    tmp_path,
)
from .names import load_name_sources

log = logging.getLogger(__name__)

_EMBED_BATCH = 256  # rows per Gemini call + checkpoint granularity

# Systems whose codes go through blake2b in code_to_int — original
# strings must be persisted in fhir_meta.csv.gz for round-trip.
_HASH_SYSTEMS = frozenset({"DCM", "THETA"})

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
    from ..common import TARGET_SYSTEMS, load_loinc_skip_codes

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
    from ..common import read_rrf

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


def _iter_dcm_terminology(part16_xml: str) -> Iterator[tuple[str, str]]:
    """Yield ``(code, canonical_name)`` from DICOM Annex D ``table_D-1``
    (DICOM Controlled Terminology Definitions).

    Authoritative master list — one canonical name per code, contrast
    with :func:`_iter_dicom_cid_rows` which yields per-CID context-
    specific phrasings (the same code can recur across multiple CIDs
    with slightly different meanings).
    """
    tree = ET.parse(part16_xml)
    q_table = f"{{{_DOCBOOK_NS}}}table"
    q_tr = f"{{{_DOCBOOK_NS}}}tr"
    q_td = f"{{{_DOCBOOK_NS}}}td"
    q_xml_id = f"{{{_XML_NS}}}id"

    for table in tree.getroot().iter(q_table):
        if table.get(q_xml_id, "") != "table_D-1":
            continue
        for tr in table.iter(q_tr):
            tds = tr.findall(q_td)
            if len(tds) < 2:
                continue
            code = _dicom_cell_text(tds[0])
            name = _dicom_cell_text(tds[1])
            if code and name:
                yield code, name


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


def _phase1_enumerate(args: Namespace, out_dir: str) -> int:
    """Parse ~/ref, write the intermediate texts file.

    Layout: cols = canonical, standard (enum int — column name kept for
    artifact compatibility), code, text, name — one row per concept,
    ordered by source then by code. ``canonical`` is the final fhir_id
    used in the structured npy; ``name`` is the display string (looked
    up via :mod:`names` while we have the source files in hand — saves
    a second parse later). Computing both here means Phase 2 doesn't
    need to know about :func:`code_to_fhir_id` nor revisit ~/ref.

    Returns N (total rows enumerated).
    """
    texts_path = os.path.join(out_dir, "fhir_ref_texts.csv")

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

    # Display-name dicts per vocab — keyed by SYSTEMS enum int. DCM
    # has no curated name source, so its rows get name="" (same as in
    # the db-mode pipeline). Parsing the source files twice (once here
    # for embedding text, once in load_name_sources for display) costs
    # ~30s extra; cheap relative to phase 2.
    name_sources = load_name_sources(args)

    n = 0
    with open(texts_path, "w", encoding="utf-8", newline="") as ft:
        wt = csv.writer(ft)
        wt.writerow(["canonical", "standard", "code", "text", "name"])
        for sys_name, it in sources:
            sys_int = SYSTEM_TO_CODE[sys_name]
            nd = name_sources.get(sys_int, {})
            cnt = 0
            for code, text in it:
                canonical = code_to_fhir_id(sys_int, code)
                wt.writerow([canonical, sys_int, code, text, nd.get(code, "")])
                n += 1
                cnt += 1
            log.info("  %s: %s rows", sys_name, f"{cnt:,}")

    log.info(
        "Phase 1 done: N=%s → %s (%.1f MB)",
        f"{n:,}", texts_path, os.path.getsize(texts_path) / 1e6,
    )
    return n


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Phase 2 ────────────────────────────────────────────────────────


async def _phase2_embed(
    args: Namespace, out_dir: str, emb_path: str, meta_path: str, n_rows: int,
) -> None:
    """Resumable Gemini embedding over ``out/fhir_ref_texts.csv``.

    Partial state lives in *out_dir*:
      fhir_embeddings.npy.partial   fp32 memmap (n_rows, EMBEDDING_DIM)
      fhir_embeddings.progress.json {texts_md5, n_rows, last_completed}

    Canonical fhir_ids and per-row systems are re-derived from the
    texts CSV at finalize time, so the partial only needs to track the
    embeddings themselves.
    """
    from mirobody.utils.embedding import text_embedding

    texts_path = os.path.join(out_dir, "fhir_ref_texts.csv")
    partial_path = os.path.join(out_dir, "fhir_embeddings.npy.partial")
    progress_path = os.path.join(out_dir, "fhir_embeddings.progress.json")

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
    mm = np.memmap(
        partial_path,
        dtype=np.float32,
        mode="r+" if os.path.isfile(partial_path) else "w+",
        shape=(n_rows, EMBEDDING_DIM),
    )

    # Stream texts, skipping already-done prefix.
    buf_idx: list[int] = []
    buf_text: list[str] = []

    async def flush() -> None:
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

    # Phase 2 also walks in row-position order, matching the CSV's
    # in-file order — finalize relies on this alignment.
    with open(texts_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r, row in enumerate(reader):
            if r < resume_from:
                continue
            buf_idx.append(r)
            buf_text.append(row["text"])
            if len(buf_text) >= _EMBED_BATCH:
                await flush()
    await flush()

    # Finalize: combine partial fp32 embs with canonicals from texts CSV,
    # write structured npy + meta.csv.gz. Read texts once more to recover
    # canonical/system/code per row (cheaper than another disk schema).
    # Note: the CSV column header is "standard" (artifact compatibility).
    log.info(
        "finalising: L2-normalise + cast fp32→fp16 + structured-merge (%s rows)",
        f"{n_rows:,}",
    )
    arr_f32 = np.asarray(mm[:])
    norms = np.linalg.norm(arr_f32, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    arr_f32 /= norms

    canonicals = np.empty(n_rows, dtype=np.int64)
    names: list[str] = [""] * n_rows
    hash_codes: dict[int, str] = {}
    with open(texts_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r, row in enumerate(reader):
            if r >= n_rows:
                raise RuntimeError(
                    f"texts file has more rows ({r + 1}) than expected N={n_rows}"
                )
            canonicals[r] = int(row["canonical"])
            names[r] = row.get("name", "")
            sys_name = SYSTEMS[int(row["standard"])]
            if sys_name in _HASH_SYSTEMS:
                hash_codes[r] = row["code"]
    if r + 1 != n_rows:
        raise RuntimeError(
            f"texts file has {r + 1} rows, expected N={n_rows}"
        )

    # Backup-before-swap: rebuilding this file costs hundreds of
    # thousands of Gemini calls, so we keep the previous version as .bak.
    emb_tmp = tmp_path(emb_path)
    out = np.lib.format.open_memmap(
        emb_tmp, mode="w+", dtype=EMB_DTYPE, shape=(n_rows,))
    out["fhir_id"] = canonicals
    out["emb"] = arr_f32.astype(np.float16)
    out.flush()
    del out
    atomic_swap_keep_backup(emb_tmp, emb_path)

    meta_tmp = tmp_path(meta_path)
    with open_gz_text_write(meta_tmp) as f:
        w = csv.writer(f)
        w.writerow(["name", "code_str"])
        for r in range(n_rows):
            w.writerow([names[r], hash_codes.get(r, "")])
    os.replace(meta_tmp, meta_path)

    del mm
    os.remove(partial_path)
    os.remove(progress_path)
    log.info(
        "Phase 2 done: %s (%.1f MB), %s (%.1f MB)",
        emb_path, os.path.getsize(emb_path) / 1e6,
        meta_path, os.path.getsize(meta_path) / 1e6,
    )


# ─── Entry point ────────────────────────────────────────────────────


async def cmd_embeddings_ref(args: Namespace) -> None:
    out_dir = args.output or os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "out"
    ))
    res_dir = args.res_dir or RES_DIR
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(res_dir, exist_ok=True)

    emb_path = os.path.join(res_dir, os.path.basename(EMB_PATH))
    meta_path = os.path.join(res_dir, os.path.basename(META_PATH))

    # Phase 1 is cheap and deterministic — always re-run (also refreshes
    # texts_md5 for Phase 2's stale-checkpoint detection).
    n = _phase1_enumerate(args, out_dir)

    # Side artifact: DCM ↔ SCT co-occurrence bridge (requires part16.xml).
    if getattr(args, "dicom_dir", None) and os.path.isdir(args.dicom_dir):
        _write_dcm_sct_bridge(
            args.dicom_dir,
            os.path.join(res_dir, "fhir_dcm_sct_bridge.csv"),
        )

    await _phase2_embed(args, out_dir, emb_path, meta_path, n)

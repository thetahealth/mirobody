"""Build the ``loinc_alias_index.npz`` member of
``fhir_loinc_bundle.tar.gz``: a lexical alias → corpus-row inverted
index derived from LOINC's per-language synonym tables.

The embedding pathway picks up semantic similarity but loses precision
on abbreviations and language-specific synonyms (``Glu``, ``CR``,
``肌酐``, ``HPV 11``, ``Creat``). LOINC ships these synonyms officially:

- ``LoincTable/Loinc.csv``: English ``RELATEDNAMES2``, ``SHORTNAME``,
  ``DisplayName``, ``CONSUMER_NAME`` per code.
- ``AccessoryFiles/LinguisticVariants/*LinguisticVariant.csv``: 21
  languages (zh-CN, de, fr, es, ja, ko, ru, ...) with translated
  ``COMPONENT`` and ``RELATEDNAMES2``.

The index is language-agnostic: every alias maps to the corpus rows
that declare it. A lexical hit on the query side yields a small bonus
on top of cosine, helping the resolver pick the right code when the
embedding is ambiguous but the user's spelling matches a known alias.

Bonus calibration: ~one axis bonus per match, capped at two matches
per row. Strong enough to break embedding near-ties, weak enough not
to overpower a substantively better cosine.

Stopword filter: aliases with document frequency > ``_DF_CAP`` codes
are dropped at build time. These are LOINC ScaleType / classification
translations ("随机", "数值型", "化学检验项目") that explode the index
and add no disambiguation signal.
"""

from __future__ import annotations

import glob
import io
import logging
import os
import re
from argparse import Namespace
from collections import defaultdict

import numpy as np

from mirobody.lexical import index_fold

from ..common import (
    SYSTEM_TO_CODE,
    _CODE_BITS,
    _CODE_MASK,
    code_to_int,
)
from ..index import EMB_BASENAME, EMB_DTYPE, RES_DIR

log = logging.getLogger(__name__)

# Member name inside ``fhir_loinc_bundle.tar.gz``. Stored as an .npz
# archive with three arrays (``aliases`` object, ``offsets`` int32,
# ``rows`` int32) — keeps the file numpy-native (no pickle exec
# hazard) while preserving variable-length string aliases.
ALIAS_INDEX_MEMBER = "loinc_alias_index.npz"

# Bonus per matched alias on a corpus row. Calibrated to ~one axis
# bonus (0.04) — tie-breaker / disambiguation scale. The per-row cap
# below prevents an alias-rich row stacking many small matches into a
# large override.
ALIAS_BONUS_PER_MATCH = 0.04
ALIAS_BONUS_ROW_CAP = 0.08

# Drop aliases that appear in more than this many corpus rows at build
# time. Component names like ``Creatinine`` / ``肌酐`` / ``Glucose`` map
# to 50-200 codes each (every specimen/method variant of that
# analyte) and ARE useful disambiguation signal — boosting the whole
# component cluster lets the embedding pick among the variants. Only
# real stopwords ("化学" 20K codes, "Random" 89K codes, "数值型" 42K
# codes) need to die. Set to 500 to admit component names while still
# axing ScaleType/Method category translations.
_DF_CAP = 500

# Min/max alias length (in normalized characters). Latin must be ≥ 3
# to skip "of", "in", "by" type fillers; CJK ≥ 2 since one ideograph
# can still be a meaningful name.
_LATIN_MIN_LEN = 3
_CJK_MIN_LEN = 2
_ALIAS_MAX_LEN = 50

# CJK Unified Ideographs + Extension A. Hiragana / Katakana / Hangul
# are covered by the per-language files (jp / ko); we keep them in too.
_CJK_RANGE = (
    "぀-ゟ"     # Hiragana
    "゠-ヿ"     # Katakana
    "㐀-䶿"     # CJK Ext A
    "一-鿿"     # CJK
    "가-힯"     # Hangul Syllables
)
_TOKEN_RE = re.compile(rf"[A-Za-z][A-Za-z0-9\-]*|[{_CJK_RANGE}]+")
_CJK_CHAR_RE = re.compile(rf"[{_CJK_RANGE}]")


# The key fold, imported rather than defined: this pass WRITES the index keys
# and `engine.OfflineResolver` READS them, so the two must fold identically or
# the resolver silently loses recall. The definition therefore lives on the
# runtime side (`mirobody.lexical.index_fold`), which is the side that ships —
# this module is pruned from the wheel. The local name stays `_normalize`
# because four call sites below and the surrounding prose use it.
_normalize = index_fold


def _is_cjk(s: str) -> bool:
    """True iff the first char is in the CJK range we treat as
    no-whitespace text."""
    return bool(s) and bool(_CJK_CHAR_RE.match(s[0]))


def _explode_relatednames(raw: str) -> set[str]:
    """Split a RELATEDNAMES2 / LinguisticVariant aliases field into
    candidate alias strings.

    LOINC's separator is mostly ``;`` but per-language files sometimes
    embed multiple distinct synonyms within one ``;`` segment as
    space-separated phrases (especially in CJK languages where the
    space-as-separator artifact comes from upstream transliteration).
    We split on ``;`` first; for each segment, if it's CJK-dominated
    we also split on internal whitespace to recover those sub-aliases.
    Latin segments stay intact to preserve multi-word phrases
    (``Blood glucose``, ``Hemoglobin A1c``).
    """
    out: set[str] = set()
    for chunk in raw.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        out.add(chunk)
        # If the chunk has internal spaces AND is CJK-dominated, split.
        if " " in chunk and _is_cjk(chunk):
            for sub in chunk.split():
                sub = sub.strip()
                if sub:
                    out.add(sub)
    return out


def _alias_passes_filters(a: str) -> bool:
    """Length + character-class checks for a candidate alias."""
    n = len(a)
    if n > _ALIAS_MAX_LEN:
        return False
    if _is_cjk(a):
        return n >= _CJK_MIN_LEN
    # Latin: require ≥ _LATIN_MIN_LEN and at least one letter (drop
    # bare numbers like "11" — too generic; the multi-token "HPV 11"
    # form survives via the parent chunk).
    if n < _LATIN_MIN_LEN:
        return False
    if not any(c.isalpha() for c in a):
        return False
    return True


def _collect_aliases_for_code(
    code: str,
    *fields: str,
) -> set[str]:
    """Gather normalized alias set for one LOINC code from any number
    of source fields (each is a free-text string from a CSV column)."""
    out: set[str] = set()
    for f in fields:
        if not f:
            continue
        # Each field may be one alias (COMPONENT, SHORTNAME) or a
        # multi-alias ; / space string (RELATEDNAMES2). The exploder
        # is safe in both cases — a no-separator string yields a
        # single-element set.
        for cand in _explode_relatednames(f):
            n = _normalize(cand)
            if _alias_passes_filters(n):
                out.add(n)
    return out


def _read_aliases_main_loinc(loinc_csv: str) -> dict[str, set[str]]:
    """Read main Loinc.csv → {code: aliases}.

    Sources English synonyms from the columns LOINC publishes per code.
    """
    import polars as pl
    # COMPONENT is the bare-word concept name ("Creatinine", "Glucose"),
    # which never appears as its own ;-segment in RELATEDNAMES2 (those
    # list synonyms/abbreviations only — CR, Crea, Creat — but not the
    # canonical noun itself). Read it explicitly so single-word queries
    # like "creatinine" or "glucose" hit.
    cols = [
        "LOINC_NUM", "COMPONENT", "SHORTNAME", "LONG_COMMON_NAME",
        "RELATEDNAMES2", "DisplayName", "CONSUMER_NAME",
    ]
    df = pl.read_csv(loinc_csv, columns=cols, infer_schema=False)
    out: dict[str, set[str]] = {}
    for row in df.iter_rows(named=True):
        code = row["LOINC_NUM"]
        aliases = _collect_aliases_for_code(
            code,
            row["COMPONENT"] or "",
            row["SHORTNAME"] or "",
            row["LONG_COMMON_NAME"] or "",
            row["RELATEDNAMES2"] or "",
            row["DisplayName"] or "",
            row["CONSUMER_NAME"] or "",
        )
        if aliases:
            out[code] = aliases
    return out


def _read_aliases_linguistic_variant(csv_path: str) -> dict[str, set[str]]:
    """Read one LinguisticVariants/*.csv → {code: aliases}.

    Per-language ``LONG_COMMON_NAME`` and ``SHORTNAME`` are mostly
    empty for many languages (verified for zh-CN: 0% LONG, 0% SHORT,
    100% COMPONENT, 100% RELATEDNAMES2). Take whatever is populated.
    """
    import polars as pl
    df = pl.read_csv(csv_path, infer_schema=False)
    out: dict[str, set[str]] = {}
    cols = df.columns
    has = lambda c: c in cols  # noqa: E731
    for row in df.iter_rows(named=True):
        code = row.get("LOINC_NUM") or ""
        if not code:
            continue
        fields: list[str] = []
        for col in ("COMPONENT", "SHORTNAME", "LONG_COMMON_NAME",
                    "RELATEDNAMES2", "LinguisticVariantDisplayName"):
            if has(col):
                fields.append(row.get(col) or "")
        aliases = _collect_aliases_for_code(code, *fields)
        if aliases:
            out[code] = aliases
    return out


def build_alias_index(
    emb_path: str,
    loinc_csv: str,
    linguistic_variants_dir: str | None,
) -> dict[str, list[int]]:
    """Build the alias → corpus-row index.

    *emb_path* gives the canonical id-to-row mapping (the index is
    aligned to embedding rows so resolve can apply the bonus directly).

    *loinc_csv* is the main ``LoincTable/Loinc.csv`` (English synonyms).

    *linguistic_variants_dir* is the per-language directory; if missing
    or None, the index is built from main Loinc.csv only (English-only
    aliases).
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

    # code_str → row index. Cheaper to invert via int-keyed dict than
    # to format every code back to "NNNN-N" form.
    code_int_to_row: dict[int, int] = {}
    for r in np.flatnonzero(is_loinc):
        code_int_to_row[int(code_int_arr[r])] = int(r)
    log.info("alias build: %d LOINC rows in corpus", len(code_int_to_row))

    # Per-code alias sets from each source.
    log.info("alias build: reading main Loinc.csv (%s)", loinc_csv)
    src_main = _read_aliases_main_loinc(loinc_csv)
    log.info("  main Loinc.csv: %d codes with aliases", len(src_main))

    per_lang_counts: dict[str, int] = {}
    src_langs: list[dict[str, set[str]]] = []
    if linguistic_variants_dir and os.path.isdir(linguistic_variants_dir):
        for path in sorted(glob.glob(
            os.path.join(linguistic_variants_dir, "*LinguisticVariant.csv")
        )):
            d = _read_aliases_linguistic_variant(path)
            src_langs.append(d)
            per_lang_counts[os.path.basename(path)] = len(d)
        log.info("  linguistic variants: %d languages, codes=%s",
                 len(src_langs),
                 ", ".join(f"{k.split('LinguisticVariant')[0]}:{v}"
                           for k, v in per_lang_counts.items()))

    # Merge → alias → set(row_index)
    alias_to_rows: dict[str, set[int]] = defaultdict(set)
    for src in (src_main, *src_langs):
        for code, aliases in src.items():
            code_int = code_to_int(code, "LOINC") if code else 0
            row = code_int_to_row.get(code_int)
            if row is None:
                continue
            for a in aliases:
                alias_to_rows[a].add(row)

    # DF filter: drop ultra-frequent aliases (stopwords).
    total_aliases = len(alias_to_rows)
    keep: dict[str, list[int]] = {}
    df_dropped = 0
    df_hist = [0, 0, 0, 0, 0]   # [1, 2-5, 6-50, 51-500, 501+]
    for a, rows in alias_to_rows.items():
        df = len(rows)
        if df == 1:        df_hist[0] += 1
        elif df <= 5:      df_hist[1] += 1
        elif df <= 50:     df_hist[2] += 1
        elif df <= 500:    df_hist[3] += 1
        else:              df_hist[4] += 1
        if df > _DF_CAP:
            df_dropped += 1
            continue
        keep[a] = sorted(rows)
    log.info(
        "alias build: %d aliases total, %d kept (df≤%d), %d dropped (df>%d). "
        "df histogram [1, 2-5, 6-50, 51-500, 501+] = %s",
        total_aliases, len(keep), _DF_CAP, df_dropped, _DF_CAP, df_hist,
    )
    return keep


def _alias_candidates(text: str) -> set[str]:
    """Extract candidate alias strings from a free-text query.

    Generates tokens that could match alias-index keys:
    - Latin word tokens (≥ 3 chars), word bigrams, and word trigrams.
    - CJK character n-grams of length 2-5 within each contiguous CJK run.

    Caller normalizes the text the same way the build pipeline does
    via :func:`_normalize`.
    """
    text = _normalize(text)
    raw = _TOKEN_RE.findall(text)
    if not raw:
        return set()
    out: set[str] = set()
    # Walk the token stream, accumulating Latin n-grams and CJK char
    # n-grams. CJK runs interleave with Latin tokens, so we can't just
    # blindly bigram across them.
    latin_run: list[str] = []
    def _flush_latin() -> None:
        for i, t in enumerate(latin_run):
            if len(t) >= _LATIN_MIN_LEN and any(c.isalpha() for c in t):
                out.add(t)
            if i + 1 < len(latin_run):
                out.add(f"{t} {latin_run[i+1]}")
            if i + 2 < len(latin_run):
                out.add(f"{t} {latin_run[i+1]} {latin_run[i+2]}")
        latin_run.clear()

    for tok in raw:
        if _is_cjk(tok):
            _flush_latin()
            # Whole token + n-grams of length 2 to min(5, len(tok)).
            if len(tok) >= _CJK_MIN_LEN:
                out.add(tok)
            for n in (2, 3, 4, 5):
                if n > len(tok):
                    break
                for i in range(len(tok) - n + 1):
                    out.add(tok[i:i+n])
        else:
            latin_run.append(tok)
    _flush_latin()
    return out


def alias_bonus_row_vector(
    query_text: str,
    alias_index: dict[str, list[int]],
    n_rows: int,
) -> np.ndarray | None:
    """Return a (n_rows,) float32 bonus vector for *query_text*.

    For each candidate substring of the query that hits the alias
    index, add :data:`ALIAS_BONUS_PER_MATCH` to every row the alias
    points to. Per-row cap at :data:`ALIAS_BONUS_ROW_CAP` keeps a
    highly-multi-aliased corpus row from accumulating a large total.

    Returns None when no candidates hit — caller can skip the add and
    save a vector op.
    """
    cands = _alias_candidates(query_text)
    if not cands:
        return None
    # Pull row hits via the small per-alias lists. Most queries hit
    # ≪ 100 alias entries; the inner sum is cheap.
    bonus = np.zeros(n_rows, dtype=np.float32)
    hit = False
    for c in cands:
        rows = alias_index.get(c)
        if not rows:
            continue
        hit = True
        for r in rows:
            bonus[r] += ALIAS_BONUS_PER_MATCH
    if not hit:
        return None
    np.minimum(bonus, ALIAS_BONUS_ROW_CAP, out=bonus)
    return bonus


def _serialize_alias_index(index: dict[str, list[int]]) -> bytes:
    """Pack the alias dict into a compressed ``.npz`` blob.

    Three arrays: ``aliases`` (object, sorted), ``offsets`` (int32,
    cumulative row-list start positions, length N+1), ``rows`` (int32,
    flat concatenation of per-alias row index lists). Lookup at load
    time reconstructs the dict via slicing rows[offsets[i]:offsets[i+1]]
    per alias. Numpy-native — no pickle execution hazard.
    """
    aliases = sorted(index.keys())
    offsets = np.empty(len(aliases) + 1, dtype=np.int32)
    offsets[0] = 0
    flat: list[int] = []
    for i, a in enumerate(aliases):
        rs = index[a]
        flat.extend(rs)
        offsets[i + 1] = len(flat)
    rows = np.asarray(flat, dtype=np.int32)
    aliases_arr = np.asarray(aliases, dtype=object)
    buf = io.BytesIO()
    np.savez_compressed(buf, aliases=aliases_arr, offsets=offsets, rows=rows)
    return buf.getvalue()


async def cmd_loinc_alias(args: Namespace) -> None:
    """Subcommand: loinc-alias — write ``loinc_alias_index.npz`` into the
    LOINC bundle (``fhir_loinc_bundle.tar.gz``)."""
    from .bundle import BUNDLE_BASENAME, write_member

    res_dir = args.res_dir or RES_DIR
    emb_path = os.path.join(res_dir, EMB_BASENAME)
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)

    if not os.path.isfile(emb_path):
        raise FileNotFoundError(f"{emb_path} missing; run `embeddings` first")
    if not args.loinc_dir or not os.path.isdir(args.loinc_dir):
        raise SystemExit(
            f"--loinc-dir required (got {args.loinc_dir!r}); "
            "needs LoincTable/Loinc.csv and AccessoryFiles/LinguisticVariants/"
        )
    loinc_csv = os.path.join(args.loinc_dir, "LoincTable", "Loinc.csv")
    if not os.path.isfile(loinc_csv):
        raise FileNotFoundError(loinc_csv)
    ling_dir = os.path.join(
        args.loinc_dir, "AccessoryFiles", "LinguisticVariants",
    )

    index = build_alias_index(emb_path, loinc_csv, ling_dir)
    payload = _serialize_alias_index(index)
    write_member(ALIAS_INDEX_MEMBER, payload, bundle_path=bundle_path)
    log.info(
        "wrote %s (%d aliases, %.1f MB) into %s",
        ALIAS_INDEX_MEMBER, len(index), len(payload) / (1024 * 1024),
        bundle_path,
    )

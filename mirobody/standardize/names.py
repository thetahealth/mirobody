"""What a coded reading is called in the reader's language.

A coded reading's English name is LOINC's LONG_COMMON_NAME, stored once per code
(`th_concept.display`). For a Chinese reader, three sources answer, first hit wins:

1. `res/loinc/zh_CN_names.tsv`: a display name (LOINC's LinguisticVariantDisplayName
   column) and a long name (LONG_COMMON_NAME) for every code LOINC ranks as commonly
   ordered. LOINC's own zh-CN variant (ID 5) translates only the six Parts and leaves
   those columns empty, so this file fills them, working from Parts that were reviewed
   one by one against that variant (`standardize_build/zh_CN/`).
2. The catalogue's Chinese label (`res/catalog/labels/zh.tsv`) of the member whose
   `loinc` code is in the same series, when the catalogue names that series one way
   only: `每日深度睡眠` and `深度睡眠` share 93831-6, so neither may speak for it.
3. The six reviewed Chinese Parts (`res/loinc/zh_CN_parts.tsv.gz`), composed the way
   LOINC composes a long name: component [property], timing, specimen, method.
   Formal, but never a different test: the component alone is not enough, since
   MCH and haemoglobin share `血红蛋白`.

No hit: `None`, and the caller shows the English name. Nothing here is stored: a
better translation in a later release names every past reading, the same rule as
`kernel.meds.slot_state`.

Licence. A translation of LOINC is a derivative work assigned to Regenstrief and
must be sent back to them (LOINC License 5.8, section 12; `res/loinc/zh_CN.NOTICE`).
These files ship under the LOINC licence, not this project's.
"""

from __future__ import annotations

import csv
import gzip
import logging
import os
import re
from dataclasses import dataclass
from functools import lru_cache

from mirobody._bundle import RES_DIR, is_lfs_pointer

logger = logging.getLogger(__name__)

NAMES_PATH = os.path.join(RES_DIR, "loinc", "zh_CN_names.tsv")
PARTS_PATH = os.path.join(RES_DIR, "loinc", "zh_CN_parts.tsv.gz")

#: The English axes a LOINC long name leaves out: a point in time, and the two
#: placeholder systems. Matched on the English axis, never on its translation.
_OMIT_TIME = frozenset({"Pt", ""})
_OMIT_SYSTEM = frozenset({"XXX", "^Patient", ""})
_PART_COLUMNS = ("LOINC_NUM", "COMPONENT", "PROPERTY", "TIME_ASPCT", "SYSTEM", "SCALE_TYP", "METHOD_TYP")


@dataclass(frozen=True)
class Names:
    """`display` for a list of results, `long` where the precise test matters."""

    display: str
    long: str


def is_chinese(language: str) -> bool:
    """`zh`, `zh-CN`, `zh_TW`, `zh-Hant`: every Chinese reader gets the Simplified
    names, as the server's other strings do (`utils.i18n.LANGUAGE_CODES`). The
    Parts cannot be converted to Traditional: LOINC's licence (section 2) forbids
    changing a variant's text."""
    return re.split(r"[-_]", (language or "").strip().lower(), maxsplit=1)[0] == "zh"


def names_for(code: str, language: str) -> Names | None:
    """The names of LOINC `code` in `language`, or `None` (show the English)."""
    if not code or not is_chinese(language):
        return None
    return chinese_names(code)


@lru_cache(maxsize=8192)
def chinese_names(code: str) -> Names | None:
    hit = _translated().get(code)
    if hit is not None:
        return hit
    composed = _composed(code)
    label = _catalogue_by_series().get(_series(code))
    if label:
        return Names(label, composed or label)
    if composed:
        return Names(composed, composed)
    return None


def _unreadable(path: str, e: Exception) -> None:
    # Once per file (the callers are cached): a reader without the file gets the
    # English names, which is a degraded answer, not a failed request.
    file_kind = "names" if path == NAMES_PATH else "parts"
    if is_lfs_pointer(path):
        logger.warning("Chinese LOINC names unavailable: file_kind=%s is a Git LFS pointer; run `git lfs pull`",
                       file_kind)
    else:
        logger.warning("Chinese LOINC names unavailable: file_kind=%s error_type=%s", file_kind, type(e).__name__)


@lru_cache(maxsize=1)
def _translated() -> dict[str, Names]:
    try:
        with open(NAMES_PATH, encoding="utf-8", newline="") as f:
            return {
                r["LOINC_NUM"]: Names(r["LinguisticVariantDisplayName"], r["LONG_COMMON_NAME"])
                for r in csv.DictReader(f, delimiter="\t")
            }
    except (OSError, KeyError, csv.Error) as e:
        _unreadable(NAMES_PATH, e)
        return {}


def _series(code: str) -> str:
    from .code import series_of

    return series_of(code)


@lru_cache(maxsize=1)
def _catalogue_by_series() -> dict[str, str]:
    from mirobody.kernel import metrics

    metrics.load_labels_resource("zh")
    seen: dict[str, set[str]] = {}
    for m in metrics.ROWS:
        label = metrics.label(m.member, "zh")
        for loinc in (m.loinc or "").replace(",", " ").split():
            series = _series(loinc)
            if label and series:
                seen.setdefault(series, set()).add(label)
    return {series: next(iter(labels)) for series, labels in seen.items() if len(labels) == 1}


@lru_cache(maxsize=1)
def _parts():
    """The six reviewed Chinese Parts of every code, as one `_strtab.FieldTable`
    (`LOINC_NUM` first, rows in code order). About 61,000 rows: as a dict of tuples
    they cost ~28 MB of objects, as a blob ~7 MB and nothing per entry, the shape
    the resolver's own tables use."""
    import numpy as np

    from mirobody._strtab import FieldTable

    try:
        with open(PARTS_PATH, "rb") as f:
            lines = gzip.decompress(f.read()).split(b"\n")
    except OSError as e:  # missing, or the LFS pointer text (BadGzipFile is an OSError)
        _unreadable(PARTS_PATH, e)
        return FieldTable(b"", np.zeros(1, dtype=np.int32), len(_PART_COLUMNS)), np.zeros(0, dtype=np.int32)
    header = tuple(lines[0].decode("utf-8").split("\t"))
    if header != _PART_COLUMNS:
        raise ValueError(f"{PARTS_PATH}: unexpected header {header}")
    cells: list[bytes] = []
    previous = b""
    for line in lines[1:]:
        if not line:
            continue
        row = line.split(b"\t")
        if len(row) != len(_PART_COLUMNS) or row[0] <= previous:
            # A bisect over an unsorted table answers with another code's Parts.
            raise ValueError(f"{PARTS_PATH}: malformed or unsorted at {row[0]!r}")
        previous = row[0]
        cells.extend(row)
    off = np.zeros(len(cells) + 1, dtype=np.int32)
    np.cumsum(np.fromiter(map(len, cells), dtype=np.int32, count=len(cells)), out=off[1:])
    table = FieldTable(b"".join(cells), off, len(_PART_COLUMNS))
    return table, np.arange(len(table), dtype=np.int32)


def _composed(code: str) -> str:
    table, order = _parts()
    row = table.find_field(code.encode("utf-8"), order, 0)
    if row < 0:
        return ""
    parts = table.row(row, len(_PART_COLUMNS))[1:]
    from .code import axes_for

    found = axes_for(code)
    if found is None:
        return ""
    axes = found[0]
    component, prop, time, system, _scale, method = parts
    head = f"{component} [{prop}]" if prop else component
    tail = []
    if axes.time.strip() not in _OMIT_TIME and time:
        tail.append(time)
    if axes.system.strip() not in _OMIT_SYSTEM and system:
        tail.append(system)
    if method:
        tail.append(method)
    return "，".join([head, *tail])


__all__ = ["NAMES_PATH", "PARTS_PATH", "Names", "chinese_names", "is_chinese", "names_for"]

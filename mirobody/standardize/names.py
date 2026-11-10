"""What a coded reading is called in the reader's language, and what it measures.

A coded reading's English name is LOINC's LONG_COMMON_NAME, stored once per code
(`th_concept.display`): `Hemoglobin [Mass/volume] in Blood`. That is a key for
systems, not a name for people, and a Chinese rendering of it (`血红蛋白 [质量/体积]，
全血`) is no better. So a reader gets two things instead (`Names`):

- `display`, the one name a list shows: 血红蛋白, 空腹血糖, `Hemoglobin, Blood`;
- `facets`, what the test measures, one axis per line, for a detail view: specimen
  全血, property 质量浓度, plus timing and method when the code has them. These are
  LOINC's own Parts, so they say exactly what the long name says, without its syntax.

A Chinese `display` comes from the first of these that answers:

1. `res/loinc/zh_CN_names.tsv`: a display name for every code LOINC ranks as
   commonly ordered (its LinguisticVariantDisplayName column; the file's long-name
   column is the translation sent back to LOINC and is not shown). LOINC's own zh-CN
   variant (ID 5) translates only the six Parts, so this file fills the names,
   working from Parts reviewed one by one against that variant
   (`standardize_build/zh_CN/`).
2. The display name of a translated code in the same series with the same method:
   a code that differs from it only in mass against molar units. LOINC ranks by US
   practice, so the molar code a Chinese report in mmol/L codes to is often unranked
   while its mass sibling is ranked: 14771-0 (fasting glucose, moles) reads as its
   sibling 1558-6 does, 空腹血糖. Only when every such sibling agrees on the name.
3. The catalogue's Chinese label (`res/catalog/labels/zh.tsv`) of the member whose
   `loinc` code is in the same series, when the catalogue names that series one way
   only: `每日深度睡眠` and `深度睡眠` share 93831-6, so neither may speak for it.
4. The Chinese consumer name, when the code's English ConsumerName is one translated
   for a ranked code: LOINC gives many codes one consumer name by design, so
   `Albumin, Urine` names an unranked albumin code as 白蛋白，尿液.
5. The reviewed Chinese component, a qualifier in brackets (心率（静息）), and the
   specimen when it is not blood: what the test is, in its own Parts.

Any other reader gets LOINC's English ConsumerName, verbatim, when the code has one,
and English facets (each Part's PartDisplayName). No name: `display` is empty and
the caller shows the long name. Nothing here is stored: a better translation in a
later release names every past reading, the same rule as `kernel.meds.slot_state`.

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
EN_PARTS_PATH = os.path.join(RES_DIR, "loinc", "en_parts.tsv.gz")

_PART_COLUMNS = ("LOINC_NUM", "COMPONENT", "PROPERTY", "TIME_ASPCT", "SYSTEM", "SCALE_TYP", "METHOD_TYP",
                 "CONSUMER_NAME")
_COMPONENT, _PROPERTY, _TIME, _SYSTEM, _SCALE, _METHOD, _CONSUMER = range(7)

#: The English axes a reader is not told: a point in time, and the placeholders
#: ("not specified", "the patient"). Matched on the English axis, never on its
#: translation.
_OMIT_TIME = frozenset({"Pt", "XXX", ""})
_OMIT_SYSTEM = frozenset({"XXX", "^Patient", "*", "*^Patient", ""})
#: Specimens a Chinese report leaves unsaid: 血红蛋白 is a blood test without saying so.
_BLOOD = frozenset({"Bld", "BldA", "BldV", "BldC", "Ser", "Plas", "Ser/Plas", "Ser/Plas/Bld", "Ser/Bld", "Plas/Bld"})

#: `facets` axes, in the order a detail view lists them, with the Part each reads.
FACET_AXES = (("specimen", _SYSTEM), ("property", _PROPERTY), ("timing", _TIME), ("method", _METHOD))


@dataclass(frozen=True)
class Names:
    """`display`: the name a list shows ("" when there is none in that language).
    `facets`: `(axis, text)` pairs, axes from `FACET_AXES`."""

    display: str
    facets: tuple[tuple[str, str], ...]


def is_chinese(language: str) -> bool:
    """`zh`, `zh-CN`, `zh_TW`, `zh-Hant`: every Chinese reader gets the Simplified
    names, as the server's other strings do (`utils.i18n.LANGUAGE_CODES`). The
    Parts cannot be converted to Traditional: LOINC's licence (section 2) forbids
    changing a variant's text."""
    return re.split(r"[-_]", (language or "").strip().lower(), maxsplit=1)[0] == "zh"


def names_for(code: str, language: str) -> Names | None:
    """The names of LOINC `code` for a reader of `language`, or `None` for a code
    the shipped tables do not hold."""
    if not code:
        return None
    return chinese_names(code) if is_chinese(language) else english_names(code)


@lru_cache(maxsize=8192)
def chinese_names(code: str) -> Names | None:
    english = _axes(code)
    row = _row(_parts(), code)
    if english is None or row is None:
        return None
    display = (_translated().get(code)
               or _display_by_series_and_method().get(_series_and_method(code))
               or _catalogue_by_series().get(_series(code))
               or row[_CONSUMER]
               or _from_parts(row, english))
    return Names(display, _facets(row, english, "zh"))


@lru_cache(maxsize=8192)
def english_names(code: str) -> Names | None:
    english = _axes(code)
    row = _row(_english_parts(), code)
    if english is None or row is None:
        return None
    return Names(row[_CONSUMER], _facets(row, english, "en"))


def _readable(part: str, language: str) -> str:
    """A Part with its qualifiers in brackets: LOINC writes `Heart rate^resting`
    and `RBC^Donor`, a person writes 心率（静息）."""
    head, *rest = [p.strip() for p in part.split("^")]
    rest = [p for p in rest if p]
    if not rest:
        return head
    if language == "zh":
        return f"{head}（{'，'.join(rest)}）"
    return f"{head} ({', '.join(rest)})"


def _facets(row: tuple[str, ...], english: tuple[str, ...], language: str) -> tuple[tuple[str, str], ...]:
    out = []
    for axis, i in FACET_AXES:
        if not row[i]:
            continue
        if (i == _SYSTEM and english[i] in _OMIT_SYSTEM) or (i == _TIME and english[i] in _OMIT_TIME):
            continue
        out.append((axis, _readable(row[i], language)))
    return tuple(out)


def _from_parts(row: tuple[str, ...], english: tuple[str, ...]) -> str:
    """A Chinese name from the Parts: the component, and the specimen unless blood."""
    name = _readable(row[_COMPONENT], "zh")
    system = english[_SYSTEM]
    if row[_SYSTEM] and system not in _OMIT_SYSTEM and system not in _BLOOD:
        name = f"{name}，{_readable(row[_SYSTEM], 'zh')}"
    return name


def _axes(code: str) -> tuple[str, ...] | None:
    """The code's six English axes in `_PART_COLUMNS` order, stripped."""
    from .code import axes_for

    found = axes_for(code)
    if found is None:
        return None
    a = found[0]
    return tuple(v.strip() for v in (a.component, a.property, a.time, a.system, a.scale, a.method))


def _series_and_method(code: str) -> tuple[str, str]:
    from .code import axes_for
    from .series import series_id

    found = axes_for(code)
    if found is None:
        return "", ""
    return series_id(found[0]), found[0].method.strip()


@lru_cache(maxsize=1)
def _display_by_series_and_method() -> dict[tuple[str, str], str]:
    """`(series, method)` -> the display name every translated code there shares.
    The series already folds mass and molar together; keeping the method apart is
    what stops a thromboelastography clotting time from naming a Lee-White one."""
    seen: dict[tuple[str, str], set[str]] = {}
    for code, display in _translated().items():
        key = _series_and_method(code)
        if key[0]:
            seen.setdefault(key, set()).add(display)
    return {key: next(iter(found)) for key, found in seen.items() if len(found) == 1}


def _unreadable(path: str, e: Exception) -> None:
    # Once per file (the callers are cached): a reader without the file gets the
    # English long names, which is a degraded answer, not a failed request.
    file_kind = {NAMES_PATH: "names", PARTS_PATH: "parts"}.get(path, "english_parts")
    if is_lfs_pointer(path):
        logger.warning("LOINC display names unavailable: file_kind=%s is a Git LFS pointer; run `git lfs pull`",
                       file_kind)
    else:
        logger.warning("LOINC display names unavailable: file_kind=%s error_type=%s", file_kind, type(e).__name__)


@lru_cache(maxsize=1)
def _translated() -> dict[str, str]:
    """Code -> its translated Chinese display name."""
    try:
        with open(NAMES_PATH, encoding="utf-8", newline="") as f:
            return {r["LOINC_NUM"]: r["LinguisticVariantDisplayName"] for r in csv.DictReader(f, delimiter="\t")}
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


def _load(path: str):
    """A per-code Parts table as one `_strtab.FieldTable` (`LOINC_NUM` first, rows in
    code order). About 63,000 rows: as a dict of tuples they cost ~30 MB of objects,
    as a blob ~7 MB and nothing per entry, the shape the resolver's own tables use."""
    import numpy as np

    from mirobody._strtab import FieldTable

    try:
        with open(path, "rb") as f:
            lines = gzip.decompress(f.read()).split(b"\n")
    except OSError as e:  # missing, or the LFS pointer text (BadGzipFile is an OSError)
        _unreadable(path, e)
        return FieldTable(b"", np.zeros(1, dtype=np.int32), len(_PART_COLUMNS)), np.zeros(0, dtype=np.int32)
    header = tuple(lines[0].decode("utf-8").split("\t"))
    if header != _PART_COLUMNS:
        raise ValueError(f"{path}: unexpected header {header}")
    cells: list[bytes] = []
    previous = b""
    for line in lines[1:]:
        if not line:
            continue
        row = line.split(b"\t")
        if len(row) != len(_PART_COLUMNS) or row[0] <= previous:
            # A bisect over an unsorted table answers with another code's Parts.
            raise ValueError(f"{path}: malformed or unsorted at {row[0]!r}")
        previous = row[0]
        cells.extend(row)
    off = np.zeros(len(cells) + 1, dtype=np.int32)
    np.cumsum(np.fromiter(map(len, cells), dtype=np.int32, count=len(cells)), out=off[1:])
    table = FieldTable(b"".join(cells), off, len(_PART_COLUMNS))
    return table, np.arange(len(table), dtype=np.int32)


@lru_cache(maxsize=1)
def _parts():
    """The reviewed Chinese Parts and consumer name of every code."""
    return _load(PARTS_PATH)


@lru_cache(maxsize=1)
def _english_parts():
    """LOINC's display name of every Part of every code, and its ConsumerName."""
    return _load(EN_PARTS_PATH)


def _row(loaded, code: str) -> tuple[str, ...] | None:
    """`code`'s cells after LOINC_NUM, or `None`."""
    table, order = loaded
    row = table.find_field(code.encode("utf-8"), order, 0)
    if row < 0:
        return None
    return tuple(table.row(row, len(_PART_COLUMNS))[1:])


__all__ = ["EN_PARTS_PATH", "FACET_AXES", "NAMES_PATH", "PARTS_PATH", "Names", "chinese_names",
           "english_names", "is_chinese", "names_for"]

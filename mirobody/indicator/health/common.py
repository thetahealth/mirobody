"""Shared types, constants, and utilities for the mapper package."""

from __future__ import annotations

import contextlib
import csv
import logging
import os
from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING, NamedTuple, TypedDict

if TYPE_CHECKING:
    import polars as pl

log = logging.getLogger(__name__)


# ─── CSV helpers ──────────────────────────────────────────────────────

@contextlib.contextmanager
def csv_field_size_limit(limit: int = 1 << 20) -> Iterator[None]:
    """Temporarily raise csv.field_size_limit, restoring the original on exit."""
    old = csv.field_size_limit(limit)
    try:
        yield
    finally:
        csv.field_size_limit(old)


# ─── Code ↔ int conversion helpers ───────────────────────────────────

def code_to_int(code: str, standard: str = "") -> int:
    """Convert a vocabulary code string to int for faster hashing/comparison."""
    if standard == "LOINC" or "-" in code:
        return int(code.replace("-", ""))
    return int(code)


def int_to_code(n: int, standard: str = "") -> str:
    """Convert int back to code string."""
    if standard == "LOINC":
        s = str(n)
        return s[:-1] + "-" + s[-1]
    return str(n)


# ─── Target vocabulary config ───────────────────────────────────────

TARGET_SYSTEMS = {
    "LOINC":  {"sab"            : "LNC",
               "preferred_tty"  : ("LN", "LC", "DN", "OSN"),
               "skip_prefixes"  : ("LP", "LA", "MTHU", "LG"),
               "mrrel_expand"   : True},
    "CVX":    {"sab"            : "CVX",
               "preferred_tty"  : ("PT", "AB"),
               "skip_prefixes"  : (),
               "mrrel_expand"   : False},
    "RXNORM": {"sab"            : "RXNORM",
               "preferred_tty"  : ("IN", "PIN", "BN", "SBD", "SCD", "MIN"),
               "skip_prefixes"  : (),
               "mrrel_expand"   : False},
}

# ICD-10-CM SABs used as bridge vocabulary
ICD_BRIDGE_SABS = {"ICD10CM", "ICD10"}

# LOINC CLASS/CLASSTYPE values to exclude (surveys, docs, admin, etc.)
_SKIP_CLASSTYPES = {"3", "4"}
_SKIP_CLASS_PREFIXES = (
    "SURVEY.", "PHENX", "PANEL.SURVEY.", "PANEL.PHENX",
    "ATTACH", "PANEL.ATTACH", "DOC.", "PANEL.DOC",
    "DOCUMENT.", "ADMIN", "PANEL.ADMIN",
    "PUBLICHEALTH",
)


def load_loinc_skip_codes(loinc_core_csv: str) -> set[str]:
    """Return LOINC code strings that should be excluded (non-lab/non-clinical)."""
    import polars as pl
    ct_df = pl.read_csv(loinc_core_csv, columns=["LOINC_NUM", "CLASS", "CLASSTYPE"])
    is_skip = pl.col("CLASSTYPE").cast(str).is_in(list(_SKIP_CLASSTYPES))
    for prefix in _SKIP_CLASS_PREFIXES:
        is_skip = is_skip | pl.col("CLASS").str.starts_with(prefix)
    return set(ct_df.filter(is_skip)["LOINC_NUM"].to_list())


# ─── RRF reader ──────────────────────────────────────────────────────

# UMLS RRF column layout (MRCONSO / RXNCONSO)
_RRF_COLUMNS = [
    "CUI", "LAT", "TS", "LUI", "STT", "SUI", "ISPREF",
    "AUI", "SAUI", "SCUI", "SDUI", "SAB", "TTY", "CODE", "STR",
    "SRL", "SUPPRESS", "CVF", "_trailing",
]


def read_rrf(path: str, columns: list[str] | None = None) -> pl.DataFrame:
    """Read a UMLS RRF file via polars.

    RRF uses ``|`` as separator with a trailing ``|`` per line (creating an
    empty last field) and no quote-escaping.  ``quote_char=None`` prevents
    polars from misinterpreting embedded quotes in medical terms, and
    ``truncate_ragged_lines=True`` handles the trailing delimiter.
    """
    import polars as pl
    df = pl.read_csv(
        path, separator="|", has_header=False,
        new_columns=_RRF_COLUMNS, infer_schema=False,
        quote_char=None, truncate_ragged_lines=True,
    )
    if columns:
        df = df.select(columns)
    return df


# ─── Shared types ──────────────────────────────────────────────────

class MappingRow(TypedDict):
    snomed_code: str
    snomed_name: str
    cui: str
    target_system: str
    target_code: str
    target_name: str
    target_tty: str
    path: str
    distance: int


class LoincAxisData(NamedTuple):
    """LOINC axis info parsed from LN (Long Name) format: COMPONENT:PROPERTY:TIME:SYSTEM:SCALE:METHOD."""
    code_to_component: dict[int, str]       # loinc_code_int -> COMPONENT string
    component_to_codes: dict[str, set[int]] # COMPONENT string -> {loinc_code_ints}
    code_to_system: dict[int, str]          # loinc_code_int -> SYSTEM string
    code_to_method: dict[int, str]          # loinc_code_int -> METHOD string


# ─── LOINC axis parser ─────────────────────────────────────────────

def parse_loinc_axes(
    targets_by_cui: dict[str, list] | None = None,
    loinc_csv_path: str | None = None,
) -> LoincAxisData | None:
    """Parse LOINC axis info (COMPONENT, SYSTEM, METHOD).

    Provide either:
    - targets_by_cui: from parse_mrconso (extracts axes from LN names)
    - loinc_csv_path: path to LoincTableCore.csv (direct, faster)
    """
    code_to_component: dict[int, str] = {}
    component_to_codes: dict[str, set[int]] = defaultdict(set)
    code_to_system: dict[int, str] = {}
    code_to_method: dict[int, str] = {}

    skip_prefixes = TARGET_SYSTEMS["LOINC"]["skip_prefixes"]

    if loinc_csv_path:
        log.info("Parsing LOINC axes from: %s", loinc_csv_path)
        with open(loinc_csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                code_str = row["LOINC_NUM"]
                if code_str.startswith(skip_prefixes):
                    continue
                code_int = code_to_int(code_str, "LOINC")
                component = row.get("COMPONENT", "").strip()
                system = row.get("SYSTEM", "").strip()
                method = row.get("METHOD_TYP", "").strip()
                if component:
                    code_to_component[code_int] = component
                    component_to_codes[component].add(code_int)
                if system:
                    code_to_system[code_int] = system
                if method:
                    code_to_method[code_int] = method
    elif targets_by_cui is not None:
        code_ln: dict[int, str] = {}
        for cui, entries in targets_by_cui.items():
            for code_int, tty, name in entries:
                if tty == "LN" and name and ":" in name:
                    code_ln[code_int] = name
        for code_int, ln in code_ln.items():
            parts = ln.split(":")
            if len(parts) < 4:
                continue
            component = parts[0].strip()
            system = parts[3].strip() if len(parts) > 3 else ""
            method = parts[5].strip() if len(parts) > 5 else ""
            if component:
                code_to_component[code_int] = component
                component_to_codes[component].add(code_int)
            if system:
                code_to_system[code_int] = system
            if method:
                code_to_method[code_int] = method
    else:
        log.error("parse_loinc_axes: provide either targets_by_cui or loinc_csv_path")
        return None

    log.info("LOINC axes: %s codes, %s unique components, %s systems, %s methods",
             f"{len(code_to_component):,}", f"{len(component_to_codes):,}",
             f"{len(code_to_system):,}", f"{len(code_to_method):,}")
    return LoincAxisData(
        code_to_component=code_to_component,
        component_to_codes=dict(component_to_codes),
        code_to_system=code_to_system,
        code_to_method=code_to_method,
    )

"""Shared types, constants, and utilities for the mapper package."""

from __future__ import annotations

import contextlib
import csv
import hashlib
import logging
import re

from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    import polars as pl

log = logging.getLogger(__name__)


# ─── Code system enum ───────────────────────────────────────────────

# Enum mapping for the DB column ``fhir_indicators.indicator_standard``
# (the column keeps the legacy name; in code we use FHIR's ``system``
# vocabulary, matching ``Coding.system``). Append-only: the index is
# persisted in fhir_code_index.csv.gz and in packed fhir_id values, so
# never reorder or delete entries.
SYSTEMS: tuple[str, ...] = (
    "SNOMED_CT",
    "LOINC",
    "RXNORM",
    "CVX",
    "DCM",
    "THETA",
)
SYSTEM_TO_CODE: dict[str, int] = {s: i for i, s in enumerate(SYSTEMS)}

# Gemini text-embedding-004 / -qwen output dimensionality. Shared by
# the DB and ~/ref embedding export paths so artifacts stay compatible.
EMBEDDING_DIM = 1024

# Output filename for the FHIR-vocabulary concept graph binary. Lives
# under ``mirobody/res/`` at runtime; uses the ``fhir_`` content prefix
# (matches ``fhir_embeddings.npy`` / ``fhir_id_map.npy`` /
# ``fhir_meta.csv.gz`` — the file's contents are FHIR concept relations
# indexed by canonical fhir_id). Other domains (e.g. finance) name their
# graphs after their own content scheme.
FHIR_GRAPH_BIN = "fhir_concept_graph.bin"


# ─── Embedding provider → fhir_indicators column ────────────────────

# fhir_indicators uses model-version-specific column names (qwen3,
# gemma3) — diverges from th_series_dim's family-only convention
# (embedding_qwen). Both columns already exist with HNSW indexes, so
# rename isn't free; we map explicitly here instead.
#: provider → the `fhir_indicators` vector column holding its embeddings.
FHIR_EMBEDDING_COLUMN: dict[str, str] = {
    "gemini": "embedding_gemini",
    # DashScope's text-embedding-v4 — the fallback path for networks where
    # openrouter.ai is unreachable. (The column
    # name says "qwen3" because v4 is the productized Qwen3-Embedding; the
    # openrouter entry below names the model precisely to avoid repeating
    # that ambiguity.)
    "qwen": "embedding_qwen3",
    # Open-weights qwen/qwen3-embedding-8b via OpenRouter (or self-hosted —
    # same weights, same column), one OPENROUTER_API_KEY for agent + embeddings.
    "openrouter": "embedding_qwen3_8b",
}

#: provider → the `th_series_dim` vector column. **A separate map, because the
#: two tables genuinely disagree**: `fhir_indicators` names the model version
#: (`embedding_qwen3`) while `th_series_dim` names the family
#: (`embedding_qwen`). Three call sites used to build this name as
#: `f"embedding_{provider}"`, which happened to be right for both current
#: providers and is not a convention — it is a coincidence that breaks the
#: moment a provider's name is not its column's name.
#:
#: It also has to be a whitelist rather than a format string because the result
#: is interpolated into SQL.
DIM_EMBEDDING_COLUMN: dict[str, str] = {
    "gemini": "embedding_gemini",
    "qwen": "embedding_qwen",
    "openrouter": "embedding_qwen3_8b",
}


def resolve_dim_embedding_column() -> tuple[str, str]:
    """``EMBEDDING_PROVIDER`` → ``(provider, th_series_dim column)``.

    Providers with no column raise here rather than composing SQL against one
    that does not exist. (That decision was taken deliberately for openrouter
    when it became the shipped default: the model is settled —
    qwen/qwen3-embedding-8b — and both tables carry `embedding_qwen3_8b`.)
    """
    from mirobody.utils.embedding import EMBEDDING_PROVIDERS, resolve_embedding_provider

    provider = resolve_embedding_provider()
    if provider not in EMBEDDING_PROVIDERS:
        raise ValueError(
            f"EMBEDDING_PROVIDER invalid: {provider!r} "
            f"(available: {sorted(EMBEDDING_PROVIDERS)})"
        )
    if provider not in DIM_EMBEDDING_COLUMN:
        raise ValueError(
            f"provider {provider!r} has no th_series_dim vector column. "
            f"Database vector search supports {sorted(DIM_EMBEDDING_COLUMN)}; "
            f"{provider!r} is for the file-based semantic tier and for "
            f"text_embedding() callers."
        )
    return provider, DIM_EMBEDDING_COLUMN[provider]


def resolve_fhir_embedding_column() -> tuple[str, str]:
    """Resolve ``EMBEDDING_PROVIDER`` to ``(provider, fhir_indicators column)``.

    Read from a single config key — ``EMBEDDING_PROVIDER`` (default
    ``openrouter``). Validated against both :data:`FHIR_EMBEDDING_COLUMN` and the
    embedding-API provider registry, since the column name is interpolated
    into SQL.
    """
    from mirobody.utils.embedding import EMBEDDING_PROVIDERS, resolve_embedding_provider

    provider = resolve_embedding_provider()
    if provider not in EMBEDDING_PROVIDERS:
        raise ValueError(
            f"EMBEDDING_PROVIDER invalid: {provider!r} "
            f"(available: {sorted(EMBEDDING_PROVIDERS)})"
        )
    if provider not in FHIR_EMBEDDING_COLUMN:
        # Every registered provider now has a column (openrouter got
        # embedding_qwen3_8b when it became the shipped default), so this
        # branch only fires for a provider added to the API registry without
        # its schema column + map entry — refuse rather than compose SQL
        # against a column that does not exist.
        raise ValueError(
            f"provider {provider!r} has no fhir_indicators vector column. "
            f"Database vector search supports {sorted(FHIR_EMBEDDING_COLUMN)}; "
            f"{provider!r} is for the file-based semantic tier and for "
            f"text_embedding() callers. Either set EMBEDDING_PROVIDER to one of "
            f"the former, or use the file matrix."
        )
    return provider, FHIR_EMBEDDING_COLUMN[provider]


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

def code_to_int(code: str, system: str = "") -> int:
    """Convert a vocabulary code string to int.

    DCM and THETA go through a 60-bit blake2b digest (one-way: original
    string must be looked up in the meta sidecar via :func:`int_to_code`
    raising). All other vocabs parse decimally; LOINC's dash is stripped.
    """
    if system in ("THETA", "DCM"):
        # 8-byte digest >> 4 → top 60 bits, fits the _CODE_BITS budget.
        return int.from_bytes(
            hashlib.blake2b(code.encode(), digest_size=8).digest(), "big"
        ) >> 4
    if system == "LOINC" or "-" in code:
        return int(code.replace("-", ""))
    return int(code)


def int_to_code(n: int, system: str = "") -> str:
    """Inverse of :func:`code_to_int` for non-hashed systems.

    Raises ``NotImplementedError`` for THETA/DCM — those went through
    a one-way blake2b digest, the original string lives in the meta
    sidecar.
    """
    if system in ("THETA", "DCM"):
        raise NotImplementedError(
            f"int_to_code for {system!r} is not supported: "
            f"code_to_int uses a one-way blake2b hash"
        )
    if system == "LOINC":
        s = str(n)
        return s[:-1] + "-" + s[-1]
    return str(n)


# ─── (system, code) ↔ bigint fhir_id packing ────────────────────────

# Layout: [3-bit system | 60-bit code], total 63 bits → fits in signed
# int64 / PG bigint positive range. Chosen so th_series_data.fhir_id
# (bigint) can hold (system, code) directly without a schema change.
#
# Budget rationale:
#   - code: SNOMED spec caps SCTID at 18 decimal digits (<10^18 < 2^60)
#   - system: 3 bits = 8 vocabs; currently 6 used (SNOMED/LOINC/RXNORM/
#     CVX/DCM/THETA), leaving 2 slots before a layout change is needed
#
# Changing either constant is a breaking change: historical fhir_id
# values become unparseable.
_SYS_BITS = 3
_CODE_BITS = 60
_CODE_MASK = (1 << _CODE_BITS) - 1


def code_to_fhir_id(system: str | int, code: str) -> int:
    """Pack ``(system, code_str)`` into a single bigint fhir_id.

    ``system`` accepts either the name (``"LOINC"``) or its enum int,
    so rows loaded directly from ``fhir_code_index.csv.gz`` (where the
    column is the int form) can be passed through unchanged.
    """
    if isinstance(system, str):
        sys_int = SYSTEM_TO_CODE[system]
        sys_name = system
    else:
        sys_int = system
        sys_name = SYSTEMS[sys_int]  # IndexError if out of range
    if sys_int >> _SYS_BITS:
        raise ValueError(
            f"system enum {sys_int} ({sys_name!r}) exceeds {_SYS_BITS}-bit budget"
        )
    n = code_to_int(code, sys_name)
    if n >> _CODE_BITS:
        raise ValueError(
            f"code {code!r} (int {n}) exceeds {_CODE_BITS}-bit budget"
        )
    return (sys_int << _CODE_BITS) | n


def fhir_id_to_code(fhir_id: int) -> tuple[str, str]:
    """Inverse of :func:`code_to_fhir_id`."""
    system = SYSTEMS[fhir_id >> _CODE_BITS]
    return system, int_to_code(fhir_id & _CODE_MASK, system)


# ─── Legacy DB fhir_id remap ────────────────────────────────────────

# Deployments that already populated ``th_series_data.fhir_id`` with the
# synthetic ``fhir_indicators.id`` (not the packed value above) can load
# a CSV to translate packed → DB id. Without a map, ``resolve_fhir_id``
# returns the packed bigint — which is what new deployments should use.





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
# DEPRECATED is superseded outright; DISCOURAGED has a MAP_TO replacement
# and LOINC explicitly tells callers not to use it for new submissions.
# TRIAL stays in — those are provisional codes that may become ACTIVE.
_SKIP_STATUSES = {"DEPRECATED", "DISCOURAGED"}

# TRIAL codes (incl. all 2.5k LABORDERS.ONTOLOGY abstract placeholders
# like 108689-1 "Urea nitrogen [Measurement]") rank as soft-demote rather
# than skip: empirically (loinc-mapping-verify v3, 19 of 64 Partials) they
# outrank ACTIVE peers for BUN/CRP/TG/LDH/T4/FSH/TIBC etc when raw cosine
# is close, because their "[Measurement]" names trigger generic-analyte
# matches. Keep them findable when no ACTIVE alternative exists; demote
# globally otherwise.
_DEMOTE_STATUSES = {"TRIAL"}
_DEMOTE_CLASSES = {"LABORDERS.ONTOLOGY"}

# Hand-curated demote patterns over LOINC display names. Apply
# RUNTIME (see ``_augment_demote_with_names`` in
# :mod:`mirobody.indicator.fhir.index`) on top of the
# tarball-shipped status/class-driven mask. Use this layer for codes
# that LOINC still flags STATUS=ACTIVE but are clinically obsolete or
# superseded by a same-vocabulary modern peer that should win top-1.
#
# Demote (not skip): historical data still surfaces when a query
# explicitly invokes the obsolete method (``antigen``) and no modern
# peer competes.
_HAND_DEMOTE_NAME_PATTERNS: tuple[re.Pattern[str], ...] = (
    # HPV antigen tests. Cervical HPV detection is uniformly DNA/RNA
    # probe in modern practice; the LOINC 17xxx ``HPV NN Ag [Presence]``
    # codes are 1990s serology kept ACTIVE for legacy interoperability.
    # Without demote, queries whose Ag code embeds slightly closer than
    # the DNA peer (HPV 11/16/33/35/43/44/51/...) returned a mix of
    # methodologies for what should be a uniform column shape. The
    # 61xxx / 95xxx DNA family now wins whenever it exists; Ag survives
    # only when the type has no DNA code at all (e.g. rare types LOINC
    # never molecularized).
    re.compile(r"\bHuman papilloma virus \d+ Ag \["),
    # Clinician-set treatment targets — LDL goal, Vit D goal, INR goal,
    # BP goal, etc. (CLASS=CLIN, COMPONENT ends in ``goal``). These are
    # values the clinician wants the patient to reach, not measurements,
    # but their long name embeds close to the measurement peer
    # (``25-Hydroxyvitamin D3+D2 goal`` vs the measurement 62292-8).
    # 26 codes match in current bundle; 8 are already skipped via
    # CLASSTYPE=4, the remaining 18 demote behind their measurement peer.
    re.compile(r"\bgoal(\s+\[|\s+in\s|$)"),
    # Dietary intake estimates — 24-hour recall of consumption, not a
    # blood/serum measurement. 104 codes (CLASS=NUTRITION&DIETETICS,
    # IO_IN_*); without demote ``维生素B2/B7/B9`` queries pick the
    # ``Vitamin B9 (Folate) intake 24 hour Estimated`` form over
    # ``Folate [Mass/volume] in Blood`` (1989-3 / 2282-2).
    re.compile(r"\bintake \d+ hour Estimated\b"),
)


def load_loinc_skip_codes(loinc_core_csv: str) -> set[str]:
    """Return LOINC code strings that should be excluded (non-lab/non-clinical
    classes, plus superseded statuses)."""
    import polars as pl
    ct_df = pl.read_csv(
        loinc_core_csv, columns=["LOINC_NUM", "CLASS", "CLASSTYPE", "STATUS"],
    )
    is_skip = pl.col("CLASSTYPE").cast(str).is_in(list(_SKIP_CLASSTYPES))
    for prefix in _SKIP_CLASS_PREFIXES:
        is_skip = is_skip | pl.col("CLASS").str.starts_with(prefix)
    is_skip = is_skip | pl.col("STATUS").is_in(list(_SKIP_STATUSES))
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
        with open(loinc_csv_path, encoding="utf-8") as f:
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
        for entries in targets_by_cui.values():
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

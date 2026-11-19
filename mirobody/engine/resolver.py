"""Lexical indicator-name resolution against the shipped LOINC bundle.

Offline and key-free: `resolve` answers a name, `resolve_reading` a
measurement (the unit picks the LOINC PROPERTY), and a term the tables do not
hold comes back unresolved rather than guessed. See the package docstring for
what the bundle holds.
"""


from __future__ import annotations

import io
import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import TYPE_CHECKING

from mirobody._bundle import BUNDLE_PATH as _BUNDLE
from mirobody._bundle import (
    AXIS_ANALYTE as _ANALYTE,
    AXIS_CODE as _CODE,
    AXIS_COMPONENT as _COMPONENT,
    AXIS_LCN as _LCN,
    AXIS_SYSTEM as _SYSTEM,
    AXIS_TIME as _TIME,
    bundle_version,
    load_alias_sources,
    load_axis,
    read_members,
)
from mirobody._strtab import StringTable
from mirobody.lexical import (
    fold_plural, index_fold, is_component_suffix, measure_stems, split_camel, split_trailing_hyphen,
    split_trailing_latin, split_trailing_parenthetical, split_trailing_space_acronym, strip_report_marks,
    surface_variants,
)

if TYPE_CHECKING:  # `_posting` names np.ndarray in its annotation; numpy itself
    import numpy as np  # is imported lazily so `import mirobody.engine` stays cheap

logger = logging.getLogger(__name__)


#: Everything `OfflineResolver.__init__` reads, fetched in one tar pass. The
#: axis field positions live in `_bundle` beside the loader, which
#: `mirobody.bundle` also offers to build-time tools.
_RUNTIME_MEMBERS = (
    "alias_keys.bin", "alias_index.npz",
    "corpus_names.bin", "corpus_names.npz",
    "axis_fields.bin", "axis_index.npz",
    "loinc_rank_bonus.npy",
)

# Sentinel target in resolver_overrides.tsv meaning "deliberately unresolved".
_BLOCK_SENTINEL = "!unresolved"

# Bare abbreviations the raw index ties between two different analytes at
# the SAME commonness rank, where neither reading dominates enough for a
# CA/PT/MG-style "answer the widespread one" row: `resolve()` checks this
# before the index, `resolve_reading` arbitrates by the unit.
_KNOWN_TIES: dict[str, tuple[str, str]] = {
    "pct": ("33959-8", "51637-7"),  # Procalcitonin, Plateletcrit
}

# R3-lite. SYSTEM axis values that name a specimen obtained ONLY by a
# dedicated, invasive procedure (lumbar puncture, amniocentesis,
# thoracentesis, paracentesis, arthrocentesis, lavage, pericardiocentesis,
# a skin or marrow biopsy), never assumed the way a venipuncture is. Fixed
# and small: a general abstain-on-unstated-specimen rule was tried and
# reverted (1,418 correct answers lost for 427 catches on the maintained
# eval set, mostly `Urine`/`Isolate`/`Air`/`Ventilator`/`Bld.dot`, none of
# which is here). This rule only SWITCHES to a same-analyte sibling.
_PROCEDURE_SYSTEMS = frozenset({
    "CSF", "Amnio fld", "Plr fld", "Periton fld", "Synv fld", "Body fld",
    "BAL", "Pericard fld", "Fibroblasts", "Tiss", "Bone mar",
})
# The switch target: the ordinary venipuncture a lab report's whole
# vocabulary already assumes, smaller than R3's reverted person-level set
# (no `XXX`/`Arterial system`/compound-building). `PPP` is the one
# addition: centrifuged from the SAME draw, not a second procedure, and
# where D-dimer's own plasma code lives -- without it the switch lands on
# a METHOD-qualified `Bld` row, a narrower claim than the bare term makes.
_PERSON_LEVEL_SYSTEMS_LITE = frozenset({"Bld", "BldV", "BldA", "BldC", "Ser", "Plas", "Ser/Plas", "PPP"})

# Generic axis-SYSTEM words that name no specimen by themselves (a side, a
# donor, a comparison group): excluded so "left"/"control" in a term is
# never mistaken for stating the organ or fluid that carries it.
_SYSTEM_WORD_STOPWORDS = frozenset({
    "left", "right", "upper", "lower", "interest", "region", "specimen",
    "system", "device", "donor", "patient", "fetus", "control", "normal",
    "mother", "father", "newborn", "the",
})
_SYSTEM_WORD_SPLIT = re.compile(r"[^a-zA-Z]+")

# A term (or its curated alias TARGET) names one of `_PROCEDURE_SYSTEMS` in
# a word the axis table's own abbreviated SYSTEM text does not share:
# Chinese, or English natural language for a LOINC abbreviation written in
# lab jargon. Scoped to the eleven systems above, not every specimen in
# LOINC -- R3's full table (`尿`, `breath`, `stone`, ...) answered for
# systems this rule never touches.
_SPECIMEN_SYNONYMS: dict[str, tuple[str, ...]] = {
    "脑脊液": ("csf",),
    "cerebrospinal": ("csf",),
    "羊水": ("amnio",),
    "amniotic": ("amnio",),
    "胸水": ("plr", "pleural"),
    "胸腔积液": ("plr", "pleural"),
    "pleural": ("plr",),
    "腹水": ("periton", "peritoneal"),
    "腹腔积液": ("periton", "peritoneal"),
    "peritoneal": ("periton",),
    "滑液": ("synv", "synovial"),
    "关节液": ("synv", "synovial"),
    "synovial": ("synv",),
    "体液": ("body",),
    "肺泡灌洗液": ("bal",),
    "支气管肺泡灌洗液": ("bal",),
    "bronchoalveolar": ("bal",),
    "心包积液": ("pericard",),
    "pericardial": ("pericard",),
    "成纤维细胞": ("fibroblasts",),
    "fibroblast": ("fibroblasts",),
    "组织": ("tiss",),
    "tissue": ("tiss",),
    "骨髓": ("bone mar", "marrow"),
    "marrow": ("bone mar",),
}

# Prefer the everyday specimen variants when the commonness prior ties. Bytes,
# not str: `_pick` matches these against slices of the corpus-name blob, so
# decoding is paid only for the row that wins.
_PLAIN_SPECIMEN = re.compile(rb"in (Serum or Plasma|Blood)\b", re.I)
_SPECIAL_SPECIMEN = re.compile(rb"\b(cord|capillary|venous|arterial|dialysis)\b", re.I)
# A LOINC challenge that names one draw of a series: a time after the load
# (`2h post 75 g glucose po`, `30m post xxx challenge`, `2d post dose
# dexamethasone`) or an ordinal specimen (`3rd specimen post xxx
# challenge`). `post cfst` (fasting), `trough` and `peak` name a state.
_TIMED_CHALLENGE = re.compile(r"\b\d+(?:\.\d+)?[mhd]\b|\b\d+(?:st|nd|rd|th) specimen\b", re.I)


def _specimen_tokens(system: str) -> frozenset[str]:
    """The specimens a LOINC SYSTEM names: ``Ser/Plas`` is {Ser, Plas}."""
    return frozenset(t for t in re.split(r"[/^+]", system or "") if t)


#: Preparations of ONE venipuncture: whole blood, serum, plasma, and
#: plasma centrifuged for (or stripped of) platelets. `Thrombin time`'s
#: own Interpretation row is axis SYSTEM `Bld`; its Qn sibling's is `PPP`
#: -- the same draw, recorded at a different specimen GRANULARITY, not a
#: different specimen, the way `Ser`/`Ser/Plas` already cross below.
_VENIPUNCTURE_SPECIMENS = frozenset({"Bld", "Ser", "Plas", "PPP", "PRP"})


def _is_venipuncture_draw(system: str) -> bool:
    """Whether every specimen `_specimen_tokens` finds in `system` is one
    of `_VENIPUNCTURE_SPECIMENS`."""
    tokens = _specimen_tokens(system)
    return bool(tokens) and tokens <= _VENIPUNCTURE_SPECIMENS


def _method_identity(method: str) -> str:
    """The identity-bearing prefix of a dotted METHOD qualifier chain, up
    to its SECOND segment. Measured: COMPONENT "coagulation" (133 rows)
    is the one case in the shipped axis table where many rows share a
    COMPONENT that names no real analyte on its own -- PT, aPTT, TT and
    Recalcification time are told apart ONLY by what induced the clot,
    `Coag.tissue factor induced`/`Coag.surface induced`/`Coag.thrombin
    induced`/`Coag.calcium ion induced` -- so `variant_for_reading`'s
    sibling search must not cross between them on COMPONENT alone. A
    method under two segments (plain, or a bare technique like `IFCC` or
    `Automated count`) carries no such chain and returns `""`, matching
    anything, same as an empty one always has: the existing preference
    for a method-LESS sibling (`... by Automated count` is a narrower
    claim than the report supports) is unaffected."""
    parts = (method or "").split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else ""


#: Scales a free-prose value maps to. `scales_for_value` answers (`Nar`, `Doc`)
#: for anything it cannot read as a number, an ordinal or a comparator, so this
#: set is "the value column holds a sentence" rather than a constraint on the
#: analyte. See `_value_scales`.
_NARRATIVE_SCALES = frozenset({"Nar", "Doc"})
#: The five leukocyte types of a differential, as the axis table spells their
#: COMPONENT. Their percentage code is the count component over
#: `/leukocytes`, and nothing else in LOINC pairs that regularly.
_LEUKOCYTE_TYPES = frozenset({"neutrophils", "lymphocytes", "monocytes", "eosinophils", "basophils"})
_FRACTION_PROPERTIES = frozenset({"NFr", "MFr", "VFr", "AFr", "SFr", "CFr"})


def _value_scales(value: str | None) -> frozenset[str]:
    """The ``SCALE_TYP`` values a reading's value admits; empty when the value
    places no constraint.

    Free prose in the value column (`见报告`, `clear yellow fluid`) is the
    ABSENCE of a measurement, not a claim about the analyte's scale: the
    report wrote a sentence where a result goes. As a constraint it made
    `尿蛋白` + `见报告` an axis-conflict against its own `PrThr/Ord` code; as
    evidence it claimed the value had confirmed a scale.
    """
    from mirobody.value_scale import scales_for_value

    scales = scales_for_value(value) or frozenset()
    return frozenset() if scales <= _NARRATIVE_SCALES else scales


@dataclass(frozen=True)
class Resolution:
    """One resolved indicator name."""

    term: str                       # the input, as given
    canonical: str = ""             # LONG_COMMON_NAME of the answer
    loinc: str = ""                 # LOINC_NUM of the answer
    candidates: int = 0             # how many corpus rows matched the alias
    resolved: bool = False
    #: How the answer was reached, or why there is none. ``"lexical"`` from the
    #: shipped vocabularies is the only kind :func:`resolve` returns;
    #: ``"refused"`` is a decision not to answer (a panel name, a string naming
    #: two tests) and unlike ``""`` must not be overturned by a second opinion.
    #: A caller using a code as an IDENTITY accepts only ``"lexical"``.
    method: str = ""
    #: Always 0.0: it held the cosine of the semantic tier, which 1.5.0 removed,
    #: and nothing lexical scores an answer.
    score: float = 0.0

    #: Which axes corroborated this answer, in order: ``("name",)`` the alias
    #: table alone, ``("name", "property")`` the printed unit agreed with or
    #: selected the code, ``("name", "property", "scale")`` the value's kind
    #: too. A unit that confirmed the code, one that did not parse and no unit
    #: at all used to give byte-identical results; this is how they differ.
    evidence: tuple[str, ...] = ()
    #: ``True``/``False`` when a unit was printed and did/does not normalize to
    #: UCUM; ``None`` when the reading carried no unit at all. A `False` here is
    #: the caller's signal that `loinc` rests on the name alone, and is usually
    #: a gap in our unit tables rather than a fault in the report.
    unit_recognized: bool | None = None
    #: PROPERTY, SCALE, SYSTEM of `loinc`, so a caller can judge the answer
    #: without a second lookup. Empty when there is no code.
    axes: tuple[str, str, str] = ("", "", "")
    #: A code that WAS reachable from the name but was set aside, and why. Never
    #: an identity; it is shown so the decision is auditable and so a wrong
    #: alias row can be found from the outside. `hemoglobin 14 %` carries
    #: `rejected_code="718-7"` (a mass concentration, for a reading printed as
    #: a percentage) and no code; `neutrophils 62 %` carries `751-8` (the
    #: count the name alone gave) beside the ratio code the unit selected.
    rejected_code: str = ""
    rejected_reason: str = ""
    #: The two codes a name-only disagreement found (stem's, then label's),
    #: for `resolve_reading` to arbitrate with the measurement (#306):
    #: `肌酐(Cre)` names creatinine and a carbapenem-resistant culture by
    #: name alone, and only a unit or a value's kind can say which one the
    #: report meant. Empty unless `method == "refused"` for exactly this
    #: reason; `rejected_reason` already says so in prose, this is the same
    #: fact in a field a caller can act on without parsing text.
    ambiguous: tuple[str, str] = ()

    @property
    def code(self) -> str:
        return self.loinc


@dataclass(frozen=True)
class UnitVerdict:
    """What a reading's printed unit and value said about a candidate code.

    `variant_for_reading` used to return a bare code, which collapsed four
    outcomes into one string: agreement, a switch, a unit that did not parse,
    and a unit that CONTRADICTED the code. The last is the dangerous one and
    was indistinguishable from the first.
    """

    #: The code to use. Empty only on ``"axis-conflict"``, where every reachable
    #: code disagrees with what the report printed.
    code: str
    #: ``no-signal``: neither a usable unit nor a value whose kind constrains
    #: SCALE. ``agreed``: the name's code satisfies unit and value.
    #: ``switched``: a sibling of the same analyte matches; `code` is it.
    #: ``unit-unrecognized``: a unit was printed and is not in our UCUM tables;
    #: the name-only code is returned (our gap, not the report's) but
    #: `evidence` will not claim the unit corroborated it. ``axis-conflict``:
    #: the unit parsed and no code of this analyte carries that property or
    #: scale; the name and the unit describe different measurements.
    outcome: str
    unit_ucum: str = ""
    axes: tuple[str, str, str] = ("", "", "")
    rejected_code: str = ""
    rejected_reason: str = ""


class OfflineResolver:
    """Lexical indicator-name resolution against the shipped bundles.

    Construction is one pass over the bundle, a few tenths of a second. Use
    :func:`get_resolver` for the cached singleton.

    It took 1.09 s and 514 MB when the artifacts were a pickled object array
    and two CSVs: reading those allocates ~1.6 million `str` for tables that
    answer a few hundred lookups per call. Storage shape, not data volume
    (77 MB of text either way). They are byte blobs plus offset arrays
    now, cut by `standardize_build/build_bundle.py`, and nothing allocates per
    entry: `_posting` bisects the alias blob, `_pick` matches regexes against
    slices of the corpus-name blob, and only the winning row is decoded.
    """

    def __init__(self) -> None:
        import numpy as np

        self._normalize = index_fold
        blobs = read_members(_RUNTIME_MEMBERS, bundle_path=_BUNDLE)

        self._alias, alias_idx = self._table(blobs, "alias_keys.bin", "alias_index.npz")
        self._alias_off = alias_idx["offsets"]
        self._alias_rows = alias_idx["rows"]

        self._names, _ = self._table(blobs, "corpus_names.bin", "corpus_names.npz")

        rank_raw = blobs.get("loinc_rank_bonus.npy")
        self._rank = (
            np.load(io.BytesIO(rank_raw))
            if rank_raw is not None
            else np.zeros(len(self._names), dtype=np.float32)
        )

        self._axis, self._order_code, _by_name = load_axis(bundle_path=_BUNDLE, members=blobs)
        self._by_component: dict[bytes, list[int]] | None = None

        # term -> a target the index resolves, from two sources in precedence
        # order (`_bundle.alias_source_files` has the ordering rule):
        #   1. res/loinc/resolver_overrides.tsv, whose targets are index keys.
        #   2. res/loinc/aliases_src/*.tsv: zh.tsv and the curated corrections,
        #      ~23k terms since 1.5.0 dropped ja and the five machine-derived
        #      files. Their targets are phrases written for the 1.4.x index
        #      build, so they resolve only sometimes; hence (1).
        self._system_values: set[str] | None = None
        self._system_keys: set[str] | None = None
        self._src = load_alias_sources(fold=index_fold)

        version = bundle_version() or "unversioned"
        logger.info(
            "OfflineResolver ready: %d aliases, %d corpus names, %d LOINC axis rows (corpus %s)",
            len(self._alias), len(self._names), len(self._axis), version,
        )

    @staticmethod
    def _table(blobs: dict[str, bytes], blob_member: str, index_member: str) -> tuple[StringTable, dict[str, np.ndarray]]:
        """Load one blob member plus its offset/order arrays.

        The blob is a plain tar member and arrives as `bytes` in one
        allocation. Inside the .npz it would cost more than twice its size
        resident: `np.load` decompresses to an ndarray, `.tobytes()` copies
        it, and the allocator keeps both arenas (75 MB versus 2 MB, measured
        on a 30 MB alias blob), so offsets go in an .npz and the text does not.
        """
        import numpy as np

        blob = blobs.get(blob_member)
        index = blobs.get(index_member)
        if blob is None or index is None:
            raise RuntimeError(
                f"{blob_member} / {index_member} not found in {_BUNDLE}. Run "
                "`git lfs pull` for the data bundles; if the bundle predates "
                "1.3.0, rebuild the runtime index with "
                "`python -m standardize_build.build_bundle --loinc <release>`."
            )
        with np.load(io.BytesIO(index)) as z:
            arrays = {k: z[k] for k in z.files}
        # Two members, two names for the same thing: the corpus-name index
        # calls it `off`, the alias index `keys_off` (it also carries the CSR
        # arrays, which the caller keeps).
        offsets = arrays.pop("off", None)
        if offsets is None:
            offsets = arrays.pop("keys_off")
        return StringTable(blob, offsets), arrays

    # -- the axis table --------------------------------------------------------

    def _axis_row(self, row: int) -> tuple[str, str, str, str, str, str, str]:
        """One axis row as the 7-tuple the callers below expect."""
        return self._axis.row(row, 7)

    def _row_for_code(self, code: str) -> int:
        return self._axis.find_field(code.encode("utf-8"), self._order_code, _CODE)

    def _analyte_of(self, code: str) -> str:
        """LOINC_NUM -> folded analyte head, or "" when unknown."""
        row = self._row_for_code(code)
        return self._axis.field(row, _ANALYTE) if row >= 0 else ""

    def _challenge_of(self, code: str) -> str:
        """LOINC_NUM -> the challenge after `^` in its COMPONENT (`post CFst`,
        `2H post 75 g glucose PO`), or "" when it has none or is unknown."""
        row = self._row_for_code(code)
        if row < 0:
            return ""
        return self._axis.field_raw(row, _COMPONENT).decode("utf-8").partition("^")[2]

    def _name_for(self, code: str) -> str:
        """code -> LONG_COMMON_NAME, for reporting a switched variant."""
        row = self._row_for_code(code)
        return self._axis.field(row, _LCN) if row >= 0 else ""

    def axes_of(self, code: str) -> tuple[str, str, str, str, str, str, str] | None:
        """`(component, property, scale, system, method, time, long_common_name)`
        of a code in the bundle, or `None`. The public face of the axis
        table, for `mirobody.standardize` to build a series key from.

        TIME_ASPCT arrived with the 1.5.0 cut. Without it a fasting glucose and
        a 2-hour post-load glucose share one key, which is the axis LOINC uses
        to tell them apart."""
        row = self._row_for_code(code)
        if row < 0:
            return None
        _code, component, prop, scale, system, method, lcn = self._axis_row(row)
        return component, prop, scale, system, method, self._axis.field(row, _TIME), lcn

    # -- lookup ----------------------------------------------------------------

    def _posting(self, key: str) -> np.ndarray | None:
        """Corpus rows for one already-folded alias key, or None.

        Bisects the key blob rather than consulting a dict built from it: the
        shipped table is already sorted, and materialising it as Python strings
        plus a dict cost 285 MB (921k keys, 1.4.x) to turn a 0.9 us bisect into
        a 0.02 us hash, inside a `resolve()` that takes tens of microseconds
        either way.
        """
        i = self._alias.find(key.encode("utf-8"))
        if i < 0:
            return None
        return self._alias_rows[self._alias_off[i]:self._alias_off[i + 1]]

    def _candidate_keys(self, term: str) -> Iterator[str]:
        """The lookup keys to try, most-specific first, each once.

        The alias-table hop goes FIRST. A row in ``self._src`` is one person's
        statement that one term means one concept; a hit in the big alias index
        is every corpus row sharing a surface string, which ``_pick`` then
        guesses among by commonness.

        Index-first got this wrong: ``血红蛋白`` matches hundreds of index rows,
        of which the commonness prior likes *Hemoglobin A1c* best, so "hemoglobin"
        resolved to a different test entirely. The alias table says
        ``血红蛋白 -> Hemoglobin``. test_engine_coverage.py guards the class.

        Both hops then repeat for each surface variant from
        :func:`mirobody.lexical.surface_variants`. The bundle normalizer is
        NFKC + casefold and nothing more (it folded the index keys at build
        time), so a full-width ``ＦＢＧ``, an en-dashed ``LDL–C`` and a
        snake_case ``fasting_glucose`` each sat one codepoint away from an
        existing key. snake_case is the convention the platform API documents
        in every ``POST /data`` example. Variants come last, after the term as
        written has missed, so they can only turn a miss into a hit.

        A generator, because `_lookup` stops at the first key that hits and
        the trailing-token test below costs two lookups of its own: built as a
        list, it ran on every call, including the ones the first key answered.
        """
        seen: set[str] = set()

        def fresh(keys: Iterable[str]) -> Iterator[str]:
            for key in keys:
                if key not in seen:
                    seen.add(key)
                    yield key

        surfaces = surface_variants(term)
        for surface in surfaces:
            yield from fresh(self._keys_for(self._normalize(surface)))

        # "monocyte count", "中性粒细胞计数": the analyte plus the word for how
        # it was counted. The index knows the analyte and the unit then picks
        # its count or fraction code (`variant_for_reading`). Appended last,
        # so it can only turn a miss into a hit.
        for stem in measure_stems(term):
            yield from fresh(self._keys_for(self._normalize(stem)))

    def _is_specimen_token(self, label: str) -> bool:
        """Whether `label` is itself a LOINC SYSTEM axis value -- `CSF`,
        the one that matters, but also `WBC`: LOINC uses leukocytes as the
        SPECIMEN for a handful of cell-derived molecular tests, the same
        three letters #305 needs as the ABBREVIATION of 白细胞/Leukocytes
        itself. One table, two readings, so `_resolve_seam` only treats
        this as a veto when `label` does NOT independently name the
        stem's own analyte -- `白细胞WBC` passes (both sides agree), `Protein
        CSF` does not (CSF carries no code of its own, so nothing to
        agree with, and dropping it would be a guess)."""
        if self._system_keys is None:
            self._system_keys = {self._normalize(system) for system in self._systems()}
        return self._normalize(label) in self._system_keys

    def _systems(self) -> set[str]:
        """Every SYSTEM axis value: the specimens a trailing token could name.

        Built on first use and only ever from the trailing-token test, which
        is itself a cold path in `resolve`: it runs once every key of the
        term as written has missed.
        """
        if self._system_values is None:
            self._system_values = {
                v
                for v in (
                    self._axis.field(i, _SYSTEM) for i in range(len(self._order_code))
                )
                if v
            }
        return self._system_values

    def _states_specimen(self, text: str, system: str) -> bool:
        """Whether `text` names the SPECIMEN `system` draws from -- a word
        from the axis table's own SYSTEM text (`Amnio fld` -> "amnio"), or a
        synonym for it in `_SPECIMEN_SYNONYMS` (R3-lite, #306's own gate on
        switching away from a dedicated-procedure system: a term that
        already says CSF must keep its CSF code, not be switched to a
        serum sibling it never asked for). Checked against both the raw
        input and, when the hit came through an alias, the curated TARGET
        text, so a row whose own name already says "Cerebrospinal Fluid"
        is never switched away from for not repeating the table's own
        abbreviation of it."""
        if not text:
            return False
        folded = self._normalize(text)
        words = {w for w in _SYSTEM_WORD_SPLIT.split(system.lower()) if len(w) >= 3} - _SYSTEM_WORD_STOPWORDS
        if any(word in folded for word in words):
            return True
        # A synonym is a WHOLE TOKEN for ASCII (`ur` must not match "your"
        # or "nurse" as a bare substring), a plain substring for CJK, which
        # has no word boundaries a split would find.
        tokens = set(_SYSTEM_WORD_SPLIT.split(folded))
        system_lower = system.lower()
        for synonym, meanings in _SPECIMEN_SYNONYMS.items():
            matched = synonym in tokens if synonym.isascii() else synonym in folded
            if matched and any(w in system_lower for w in meanings):
                return True
        return False

    def _is_blocked(self, term: str) -> bool:
        """True when ANY surface variant of the term is a deliberate
        non-answer (target ``!unresolved`` in resolver_overrides.tsv).

        Every variant, not only the term as written: variant lookup is a new
        route into the index, so a block that knew one spelling would simply be
        walked around. ``lipid_panel`` must refuse because ``lipid panel`` does,
        and it only does because the check flattens too.

        Found the hard way on blood pressure, which is no longer blocked (it has
        a panel code, see resolver_overrides.tsv): ``blood_pressure`` skipped
        the block written for ``blood pressure``, tokenized to it anyway, hit
        183 index rows and came back 8462-4, the DIASTOLIC code. Same back door,
        and the terms still on the block list use the same spellings.
        """
        return any(
            self._src.get(self._normalize(surface)) == _BLOCK_SENTINEL
            for surface in surface_variants(term)
        )

    def _keys_for(self, norm: str) -> list[str]:
        """Alias-table hop then the raw key, for one already-normalized term.

        No keys at all for a blocked term. `resolve` refuses a blocked term
        as written, but the stems derived from it come here without that
        check, and the raw key of a category word reaches some narrow assay:
        `电解质计数` answered an electrolytes panel and `Stool OB` a
        budgerigar-droppings IgE, while `电解质` and `Stool` refuse.
        """
        keys: list[str] = []
        eng = self._src.get(norm)
        if eng == _BLOCK_SENTINEL:
            return keys
        if eng:
            keys.append(self._normalize(eng))
            # The same split on the alias table's TARGET: 429 of 48,366
            # targets end in an acronym, and the stem is sometimes the better
            # index key. Its example used to be an INPUT ("Fasting plasma
            # glucose FPG"), which is not what this line can see -- that case
            # is `_find_seam`'s now. Both are real: deleting this one costs
            # 0.003 coverage and RAISES the wrong-rate 0.032 -> 0.034.
            stripped, _label = split_trailing_space_acronym(eng)
            if stripped and stripped != eng:
                keys.append(self._normalize(stripped))
        keys.append(norm)
        return keys

    def _pick(self, rows: np.ndarray, key: str) -> int:
        """Prefer the named component without changing a patient into a specimen.

        `globulin` otherwise selects thyroxine bound to globulin, and `urea`
        selects urea nitrogen. An exact COMPONENT corrects these, but cannot
        override a patient-level measurement with an organ or device: `weight`
        became stone weight and `temperature` became humidifier temperature.
        A singular/plural spelling cannot break an existing commonness tie:
        `platelet` otherwise prefers mean platelet volume to platelet count.

        Names stay raw blob slices; only exact component candidates need
        decoding, not the hundreds of long names an alias can post.
        """
        names = self._names
        rank = self._rank
        axis = self._axis
        exact = key.encode("utf-8")
        # Seeded from the first row rather than left at -1: a comparison
        # against -inf fails only on a NaN score, and a NaN rank must not turn
        # a hit into a miss. No shipped rank is NaN; the answer should not
        # depend on that.
        best_row, best_score = -1, float("-inf")
        exact_matches: list[tuple[int, float]] = []
        for r in rows:
            r = int(r)
            if best_row < 0:
                best_row = r
            name = names.raw(r)
            score = float(rank[r])
            if _PLAIN_SPECIMEN.search(name):
                score += 0.25
            if _SPECIAL_SPECIMEN.search(name):
                score -= 0.25
            if axis.field_raw(r, _COMPONENT) == exact:
                exact_matches.append((r, score))
            if score > best_score:
                best_row, best_score = r, score
        baseline = self._axis_row(best_row)
        baseline_score = best_score
        if (
            baseline[1] != key and fold_plural(baseline[1]) == fold_plural(key)
            and any(score == baseline_score for _row, score in exact_matches)
        ):
            return best_row
        for r, score in exact_matches:
            candidate = self._axis_row(r)
            if baseline[4] == "^Patient" and candidate[4] != "^Patient":
                continue
            score += 0.5
            if score > best_score:
                best_row, best_score = r, score
        return best_row

    def resolve(self, term: str) -> Resolution:
        """Resolve one free-text indicator name. Never raises on a miss."""
        term = (term or "").strip()
        if not term:
            return Resolution(term=term)

        # Deliberate non-answers (target `!unresolved` in the overrides file).
        # Some terms name a CATEGORY with no code of its own: "血脂" is four
        # analytes and LOINC's lipid panels differ by which children they carry,
        # so any single code assumes what was ordered. A panel term that HAS a
        # real panel code ("blood pressure" -> 85354-9) still resolves, because
        # a panel code says "expect components". All surface variants are
        # checked, not just the term as written; `_is_blocked` has the back door
        # that requires.
        if self._is_blocked(term):
            return Resolution(term=term, method="refused")

        # `PCT` ties plateletcrit against procalcitonin at the SAME
        # commonness rank, and row order alone picked the sepsis marker
        # every time. A general `_pick` tie detector cost unrelated terms
        # (`HDL-C`, `FT4`, `Lp(a)`...) their own answer (measured), so this
        # is a named exception instead, the CA/PT/MG rows' opposite case:
        # neither reading dominates, so it refuses by name and lets
        # `resolve_reading` arbitrate by the unit.
        key = self._normalize(term)
        if key in _KNOWN_TIES:
            code, tied = _KNOWN_TIES[key]
            return Resolution(
                term=term,
                method="refused",
                ambiguous=(code, tied),
                rejected_reason=f"{term!r} ties {code} and {tied} at the same rank",
            )

        hit = self._lookup(term)
        if hit is not None:
            return hit

        # `尿酸(UA)测定`, `γ-谷氨酰基转移酶*`: the name with what the report
        # printed after it. Answered as the bare name would be, blocks and the
        # seam rule below included; only reached after a miss.
        bare = strip_report_marks(term)
        if bare:
            return replace(self.resolve(bare), term=term)

        return self._resolve_seam(term)

    def _find_seam(self, term: str) -> tuple[str, str, bool]:
        """``(stem, label, strict)`` for the first shape that matches a name
        run into its own abbreviation; ``("", "", False)`` when none does.

        #305 unifies five shapes a report glues the two with behind one
        split: bracket ``名(缩写)``, hyphen ``Name-ABBR``, a bare space
        before an acronym ``Name ABBR``, and a CJK run straight into Latin
        ``白细胞WBC`` -- the last four tried again on the CamelCase-split
        surface, so ``FastingPlasmaGlucoseFPG`` is the same shape as
        ``Fasting Plasma Glucose FPG``. `strict` is True only for the CJK
        shape: see `_resolve_seam` for why only it drops the
        one-half-missing fallback the other four keep.
        """
        for candidate in (term, split_camel(term)):
            stem, label = split_trailing_latin(candidate)
            if label:
                return stem, label, True
            for finder in (split_trailing_parenthetical, split_trailing_hyphen, split_trailing_space_acronym):
                stem, label = finder(candidate)
                if label:
                    return stem, label, False
        return "", "", False

    def _resolve_seam(self, term: str) -> Resolution:
        """A name and its own abbreviation, in any of the shapes `_find_seam`
        covers (#305). Which half is the answer is not decidable by
        position: ``空腹血糖(GLU)`` means the stem, ``血糖(HbA1c)`` means the
        label, and answering the second with glucose would file an HbA1c
        reading into the glucose series. Both halves are resolved and
        compared by LOINC analyte (COMPONENT), not by exact code: a
        disagreement there leaves the term unresolved.
        """
        stem, label, strict = self._find_seam(term)
        if not stem and not label:
            return Resolution(term=term)

        # A component suffix (`MB`, `BB`, an isoenzyme number, a bare digit
        # run) names a FRACTION of the stem's analyte, never a spelling of
        # it, whatever it would otherwise resolve to: `Creatine Kinase-MB`
        # answered total CK before this guard existed.
        if label and is_component_suffix(label):
            return Resolution(term=term)
        # Both halves, and the whole name without its measure words: `Serum
        # HRV (RMSSD)` is the blocked `HRV (RMSSD)`, and its stem half alone
        # answers the SDNN code that block exists to refuse.
        if (
            (stem and self._is_blocked(stem))
            or (label and self._is_blocked(label))
            or any(self._is_blocked(measured) for measured in measure_stems(term))
        ):
            return Resolution(term=term, method="refused")

        stem_hit = self._lookup(stem) if stem else None
        label_hit = self._lookup(label) if label else None
        both_resolved = stem_hit is not None and label_hit is not None

        if stem_hit and label_hit and stem_hit.loinc != label_hit.loinc:
            # Different codes, so ask LOINC's COMPONENT whether they are even
            # the same substance.
            #   空腹血糖(GLU)   Glucose^post CFst  vs Glucose
            #     same analyte: the label only labels the stem, so the stem
            #     wins
            #   血糖(HbA1c)    Glucose      vs Hemoglobin A1c/Hemoglobin.total
            #   胆固醇(HDL-C)  Cholesterol  vs Cholesterol.in HDL
            #     two tests in one string, so it stays unresolved
            stem_analyte = self._analyte_of(stem_hit.loinc)
            label_analyte = self._analyte_of(label_hit.loinc)
            if not stem_analyte or stem_analyte != label_analyte:
                # Still `refused`, but with the two answers named -- in
                # `ambiguous`, a field, for `resolve_reading` to arbitrate
                # with the measurement (#306), and in `rejected_reason` for
                # a human; a caller can also put the choice to a person,
                # which a category word's refusal never offers.
                return Resolution(
                    term=term,
                    method="refused",
                    candidates=stem_hit.candidates + label_hit.candidates,
                    ambiguous=(stem_hit.loinc, label_hit.loinc),
                    rejected_reason=(
                        f"{stem!r} gives {stem_hit.loinc} and {label!r} gives {label_hit.loinc}: "
                        "two analytes in one name"
                    ),
                )
            # Same analyte head; the challenge decides which half answers. A
            # label carrying a state the stem lacks is the measurement
            # (`血糖(FPG)`, 1558-6). A timed one (`血糖(OGTT)`, 2h post 75 g)
            # is one draw of a series the name does not identify: the stem
            # filed that draw into the plain glucose series (1.6.1 for the
            # bracket, #305 for `Glucose OGTT` and the other shapes).
            label_challenge = self._challenge_of(label_hit.loinc)
            if not label_challenge or label_challenge == self._challenge_of(stem_hit.loinc):
                label_hit = None
            elif _TIMED_CHALLENGE.search(label_challenge):
                return Resolution(
                    term=term,
                    method="refused",
                    candidates=stem_hit.candidates + label_hit.candidates,
                    ambiguous=(stem_hit.loinc, label_hit.loinc),
                    rejected_reason=(
                        f"{label!r} gives {label_hit.loinc}, one draw of a timed challenge "
                        f"({label_challenge}), and the name does not say which draw"
                    ),
                )
            else:
                stem_hit = None

        # Two guards on the ONE-SIDED fallback below, reached only when
        # label did NOT resolve (or was narrowed away above as a repeat):
        # a specimen token (`CSF`; `WBC` too, so checked here rather than
        # as a pre-filter, since `白细胞WBC` already agreed above and never
        # reaches this line) names WHERE the stem was drawn from; a label
        # with a digit that resolves to nothing (`SomeAnalyte-D7`) is a
        # SERIES MEMBER, not an unknown abbreviation.
        if stem_hit and label_hit is None and label and (
            self._is_specimen_token(label) or any(ch.isdigit() for ch in label)
        ):
            return Resolution(term=term)

        # The CJK-glued shape drops the "one half is missing, the other
        # still answers" fallback the other four keep: `乙肝病毒DNA` must not
        # become an HBV antibody code through its stem alone just because
        # `DNA` carries no code of its own (#305).
        if strict and not both_resolved:
            return Resolution(term=term)
        chosen = stem_hit or label_hit
        if chosen is None:
            return Resolution(term=term)
        return replace(chosen, term=term)

    def _component_index(self) -> dict[bytes, list[int]]:
        """component -> its rows, built on first use and then cached.

        Lazy because `resolve()` never needs it: a caller with no value and no
        unit has nothing to pick a variant with, and paying for this index on
        every import would tax the offline path that is the whole point of the
        library.
        """
        if self._by_component is None:
            index: dict[bytes, list[int]] = {}
            axis = self._axis
            for i in range(len(axis)):
                component = axis.field_raw(i, _COMPONENT)
                if component:
                    index.setdefault(component, []).append(i)
            self._by_component = index
        return self._by_component

    def unit_variants(self, loinc: str) -> tuple[str, ...]:
        """The codes of the same measurement in the other of mass and moles:
        same component, scale, specimen, method and time, property `M*` against
        `S*` (1558-6 MCnc against 14771-0 SCnc). Only a unit tells them apart,
        so a reading printed without one cannot pick. Empty when the property
        is not a mass or substance one, or the analyte has only one."""
        row = self._row_for_code(loinc) if loinc else -1
        if row < 0:
            return ()
        _code, component, prop, scale, system, method, _lcn = self._axis_row(row)
        if prop[:1] not in ("M", "S") or len(prop) < 2:
            return ()
        other = ("S" if prop[0] == "M" else "M") + prop[1:]
        time = self._axis.field(row, _TIME)
        return tuple(
            r[0]
            for i in self._component_index().get(component.encode("utf-8"), [])
            for r in (self._axis_row(i),)
            if r[2] == other and r[3] == scale and r[4] == system and r[5] == method
            and self._axis.field(i, _TIME) == time
        )

    def variant_for_reading(self, loinc: str, value: str | None, unit: str | None) -> UnitVerdict:
        """The code for the SAME measurement, in the form this reading took.

        LOINC gives one code per (analyte, property, scale, specimen, method),
        so one measurement has many codes and the reading itself says which:

        * the UNIT picks the ``PROPERTY``. Total cholesterol is 2093-3 in mg/dL
          and 14647-2 in mmol/L. The alias table answers with whichever one it
          points at, so a mmol/L reading routinely landed on the
          mass-concentration code and everything downstream believed a
          two-unit series was one unit.
        * the VALUE'S KIND picks the ``SCALE_TYP``. `尿糖 阴性` is not a number,
          and answering it with *Glucose [Mass/volume] in Urine* files a
          dipstick result into a quantitative assay. Measured on the everyday
          qualitative panel, ten of thirty indicators did exactly that:
          尿糖, 尿酮体, 类风湿因子, 抗核抗体, 妊娠试验 and their English forms.
          Over two-fifths of the shipped axis table is not ``Qn``, so this is
          not an edge.

        Both constraints are applied to the sibling with the same **full**
        COMPONENT: `Glucose^post CFst`, not `Glucose`, so a fasting reading
        cannot decay into plain glucose. Prefers the same SYSTEM and a
        method-less variant.

        Deterministic: no embedding, no scoring, still `method="lexical"`,
        because the analyte came from the alias table and the variant from a
        table lookup. The code comes back unchanged when the reading says
        nothing or already agrees, and with ``unit-unrecognized`` when the
        unit is not in our tables. When the unit or the value fits no code of
        this analyte in this specimen, the verdict is ``axis-conflict`` and
        carries no code: the name's own code would file the reading under a
        measurement the report did not make.
        """
        if not loinc:
            return UnitVerdict(code=loinc, outcome="no-signal")
        from mirobody.units import normalize_unit, parse_value_unit, unit_families

        # `20%` and `("20", "%")` are the same reading written two ways, and a
        # stored value routinely carries its unit inline: `th_series_data.value`
        # holds "3.9 mmol/L". Without this the unit gate did not fire at all on
        # half the shapes real data arrives in.
        if not unit and value:
            unit = parse_value_unit(value).unit or ""
        printed_unit = (unit or "").strip()
        ucum = normalize_unit(printed_unit) if printed_unit else None
        families = frozenset(unit_families(ucum) or ()) if ucum else frozenset()
        scales = _value_scales(value)

        row = self._row_for_code(loinc)
        axes = ("", "", "")
        current = None
        if row >= 0:
            current = self._axis_row(row)
            axes = (current[2], current[3], current[4])

        # A unit was printed and our tables do not know it. Answer from the name
        # and SAY SO, rather than either withholding the code or, as before,
        # returning it as though the unit had agreed. Withholding would be the
        # larger error: an unrecognized unit is usually OUR gap, not a bad
        # report. `Thousand/uL`, `uIU/mL`, `mm/hr` and `个/HP` were all printed
        # on real reports before the alias table learned them.
        if printed_unit and ucum is None:
            return UnitVerdict(
                code=loinc,
                outcome="unit-unrecognized",
                axes=axes,
                rejected_reason=f"unit {printed_unit!r} is not in the UCUM tables",
            )

        if not families and not scales:
            return UnitVerdict(code=loinc, outcome="no-signal", unit_ucum=ucum or "", axes=axes)
        if current is None:
            return UnitVerdict(code=loinc, outcome="no-signal", unit_ucum=ucum or "", axes=axes)

        prop_ok = (not families) or current[2] in families
        scale_ok = (not scales) or current[3] in scales
        if prop_ok and scale_ok:
            return UnitVerdict(
                code=loinc,
                outcome="agreed",
                unit_ucum=ucum or "",
                axes=axes,
            )

        identity = _method_identity(current[5])

        def _matching(component: bytes, *, same_test: bool = False) -> list:
            # METHOD identity binds siblings of the SAME component: it is
            # what tells PT, aPTT and TT apart under "coagulation". A
            # fallback to another component (a ratio's numerator, a
            # differential's percentage) only PREFERS it: required there,
            # `前列腺特异性抗原 0.55 ng/mL` could not drop 15324-7's
            # `Calculated.PLN` to reach PSA itself (2857-1), while a cardiac
            # index by angiography keeps its technique when its numerator
            # has one.
            fitting = [
                r
                for r in (
                    self._axis_row(i) for i in self._component_index().get(component, [])
                )
                if ((not families) or r[2] in families)
                and ((not scales) or r[3] in scales)
            ]
            if not identity:
                return fitting
            kept = [r for r in fitting if _method_identity(r[5]) == identity]
            return kept if (same_test or kept) else fitting

        siblings = _matching(current[1].encode("utf-8"), same_test=True)
        same_test = bool(siblings)
        if not siblings:
            # A differential percentage and a differential count are two
            # COMPONENTs, not two properties of one: `neutrophils/leukocytes`
            # (NFr) against `neutrophils` (NCnc). So `中性粒细胞` reported as
            # `4.2 10*9/L` could not reach its own count code. Dropping the
            # denominator is well-determined; adding one is not, since
            # `neutrophils` has three NFr children and only clinical knowledge
            # picks the CBC one. This crosses ratio -> count ONLY; the other
            # direction stays curated in `resolver_overrides.tsv`.
            numerator, sep, _ = current[1].partition("/")
            if sep:
                siblings = _matching(numerator.encode("utf-8"))
        if not siblings and current[1] in _LEUKOCYTE_TYPES and families & _FRACTION_PROPERTIES:
            # Count -> ratio, for the differential alone: `Monocytes 7.1 %`
            # names the count component and the percentage lives under
            # `monocytes/leukocytes`. Five components, one denominator.
            siblings = _matching(f"{current[1]}/leukocytes".encode())
        if not siblings:
            # The unit parsed and nothing this analyte can be carries that
            # property or scale: the name and the unit describe two different
            # measurements. Returning `loinc` here is what made
            # `neutrophils 62 %` answer `751-8` (*Neutrophils [#/volume] … by
            # Automated count*): an absolute count, for a reading printed as a
            # percentage, and carrying a METHOD nothing in the input specified.
            # That answer is indistinguishable from a correct one, which is the
            # whole reason this branch now refuses instead.
            want = "/".join(sorted(families)) if families else "/".join(sorted(scales))
            return UnitVerdict(
                code="",
                outcome="axis-conflict",
                unit_ucum=ucum or "",
                axes=axes,
                rejected_code=loinc,
                rejected_reason=(
                    f"{loinc} is {current[2]}/{current[3]}; the reading needs {want}, "
                    "and no code for this analyte carries it"
                ),
            )
        same_system = [r for r in siblings if r[4] == current[4]]
        if not same_system:
            # `Ser` and `Ser/Plas` are one draw; `rheumatoid factor negative`
            # moves from 11572-5 (Ser/Plas) to its Ql code in Ser.
            own = _specimen_tokens(current[4])
            same_system = [r for r in siblings if _specimen_tokens(r[4]) & own]
        if not same_system and identity and same_test and _is_venipuncture_draw(current[4]):
            # Scoped to `identity` on same-component siblings -- METHOD
            # already agreed the sibling is the SAME test -- so this never substitutes
            # for the specimen check on an ordinary analyte: it only
            # widens what "one draw" already means for a component like
            # `coagulation`, whose rows record it at varying granularity.
            same_system = [r for r in siblings if _is_venipuncture_draw(r[4])]
        if not same_system:
            # The unit fits a sibling only in another specimen: `albumin 30
            # mg/24h` reached the URINE excretion code from a serum name. A
            # unit may pick the property, never the specimen.
            elsewhere = sorted({r[4] for r in siblings})
            return UnitVerdict(
                code="",
                outcome="axis-conflict",
                unit_ucum=ucum or "",
                axes=axes,
                rejected_code=loinc,
                rejected_reason=(
                    f"{loinc} is {current[2]}/{current[3]} in {current[4]}; the unit fits this analyte "
                    f"only in {', '.join(elsewhere)}"
                ),
            )
        # A method-less variant first: `... by Automated count` is a narrower
        # claim than the report supports.
        same_system.sort(key=lambda r: (r[5] != "", len(r[6])))
        chosen = same_system[0]
        return UnitVerdict(
            code=chosen[0],
            outcome="switched",
            unit_ucum=ucum or "",
            axes=(chosen[2], chosen[3], chosen[4]),
            rejected_code=loinc,
            rejected_reason=f"the name alone gives {loinc} ({current[2]}/{current[3]})",
        )

    def _switch_from_procedure_specimen(self, term: str, key: str, best_row: int) -> int:
        """R3-lite (#306): when `_pick`'s choice carries a dedicated-
        procedure SYSTEM (`_PROCEDURE_SYSTEMS`) that nothing states, switch
        to the best-ranked SAME-COMPONENT row whose SYSTEM is an ordinary
        venipuncture (`_PERSON_LEVEL_SYSTEMS_LITE`); `best_row` unchanged if
        none exists. `D-dimer` (English, bare) answers 42499-4, D-dimer in
        CSF, on main -- a lumbar puncture nobody asked for -- while 48065-7,
        the plasma code, sits one surface variant away (`d-dimer`'s own
        posting is the lone CSF row; `d dimer`'s, from the SAME term, has
        both), so every candidate KEY is searched, not only the one that
        hit first. Never a different analyte: COMPONENT is held fixed."""
        system = self._axis.field(best_row, _SYSTEM)
        if system not in _PROCEDURE_SYSTEMS:
            return best_row
        override = self._src.get(self._normalize(term))
        if (
            self._states_specimen(term, system)
            or self._states_specimen(key, system)
            or self._states_specimen(override, system)
        ):
            return best_row
        component = self._axis.field_raw(best_row, _COMPONENT)
        best_sibling, best_score = -1, float("-inf")
        seen: set[int] = {best_row}
        for candidate_key in self._candidate_keys(term):
            rows = self._posting(candidate_key)
            if rows is None:
                continue
            for r in rows:
                r = int(r)
                if r in seen:
                    continue
                seen.add(r)
                if self._axis.field_raw(r, _COMPONENT) != component:
                    continue
                if self._axis.field(r, _SYSTEM) not in _PERSON_LEVEL_SYSTEMS_LITE:
                    continue
                score = float(self._rank[r])
                if score > best_score:
                    best_sibling, best_score = r, score
        return best_sibling if best_sibling >= 0 else best_row

    def _lookup(self, term: str) -> Resolution | None:
        """First candidate key that hits the alias index, or None on a miss.

        A posting row IS an axis row: the 1.5.0 cut writes one corpus row per
        code, in axis order, and only for codes it keeps, so every candidate
        has a LOINC code and none is in `loinc_skip.txt` (the ACTIVE codes the
        cut left out). `test_engine_coverage.py` pins both.
        """
        for key in self._candidate_keys(term):
            rows = self._posting(key)
            if rows is None or not len(rows):
                continue
            chosen = self._switch_from_procedure_specimen(term, key, self._pick(rows, key))
            code, _component, prop, scale, system, _method, lcn = self._axis_row(chosen)
            return Resolution(
                term=term,
                canonical=lcn,
                loinc=code,
                candidates=int(len(rows)),
                resolved=True,
                method="lexical",
                # `("name",)`, not `()`: the alias table chose this code and
                # nothing corroborated it. Left empty, `"name" in evidence`
                # was False from `resolve()` and True from
                # `resolve_reading()` for the same term and code.
                evidence=("name",),
                axes=(prop, scale, system),
            )
        return None


@lru_cache(maxsize=1)
def get_resolver() -> OfflineResolver:
    return OfflineResolver()


def resolve(term: str) -> Resolution:
    """Module-level convenience: offline-resolve one indicator name."""
    return get_resolver().resolve(term)


def _arbitrate_by_measurement(resolver: OfflineResolver, hit: Resolution, value: str | None, unit: str | None) -> Resolution | None:
    """Settle a name-only "two analytes" refusal with the measurement, or
    hand back `None` to leave it refused (#306).

    `resolve()` cannot see the unit, so it refuses before `variant_for_
    reading` ever gets a say -- but the measurement routinely answers the
    question the name alone cannot: 肌酐(Cre) names both creatinine and a
    carbapenem-resistant culture, and only 78 µmol/L's PROPERTY says which
    one. Both candidate codes are checked independently; exactly one
    compatible with what was printed is the answer. Both compatible (e.g.
    `胆固醇(HDL-C)` with any lipid unit) or neither (no unit at all, or a
    unit that fits nothing) stays refused, same as today -- a measurement
    that cannot decide is not evidence for either side.
    """
    verdicts = [(code, resolver.variant_for_reading(code, value, unit)) for code in hit.ambiguous]
    compatible = [(code, v) for code, v in verdicts if v.outcome in ("agreed", "switched")]
    if len(compatible) != 1:
        return None
    named_code, verdict = compatible[0]
    loser = next(code for code in hit.ambiguous if code != named_code)
    return Resolution(
        term=hit.term,
        canonical=resolver._name_for(verdict.code),
        loinc=verdict.code,
        candidates=hit.candidates,
        resolved=True,
        method="lexical",
        evidence=("name", "property"),
        unit_recognized=True,
        rejected_code=loser,
        axes=verdict.axes,
        rejected_reason=f"{hit.rejected_reason}; the unit decided for {verdict.code}",
    )


def resolve_reading(name: str, value: str | None = None, unit: str | None = None) -> Resolution:
    """Resolve a reading, in the unit it was actually reported in.

    `resolve()` answers a NAME; this answers a MEASUREMENT, and the difference
    matters because LOINC codes the unit into the identity:

        resolve("total cholesterol")                    -> 2093-3  [Mass/volume]
        resolve_reading("total cholesterol", "5.0", "mmol/L") -> 14647-2 [Moles/volume]
        resolve_reading("total cholesterol", "193", "mg/dL")  -> 2093-3  (unchanged)

    Two lexical steps, no embedding and no guessing: the alias table picks the
    analyte, then the axis table picks the variant whose PROPERTY matches the
    unit. A reading whose unit does not parse, or already agrees, comes back
    exactly as `resolve()` would answer it.

    This is where the value and unit earn their keep. Upstream, an LLM
    extraction pass has already decided the content is health data and emitted
    `{indicator, value, unit}`, so by the time a name reaches the resolver it
    is a measurement with a magnitude, and throwing that away to match on the
    name alone discards the strongest disambiguator available.
    """
    resolver = get_resolver()
    hit = resolver.resolve(name)
    if not hit.resolved or not hit.loinc:
        if hit.ambiguous:
            arbitrated = _arbitrate_by_measurement(resolver, hit, value, unit)
            if arbitrated is not None:
                return arbitrated
        return hit
    verdict = resolver.variant_for_reading(hit.loinc, value, unit)

    # `evidence` records which axes actually corroborated the answer. It is
    # built here rather than inside the verdict because only this function knows
    # that the analyte itself came from the alias table.
    if verdict.outcome == "axis-conflict":
        return Resolution(
            term=hit.term,
            canonical="",
            loinc="",
            candidates=hit.candidates,
            resolved=False,
            method="refused",
            evidence=("name",),
            unit_recognized=True,
            rejected_code=verdict.rejected_code,
            rejected_reason=verdict.rejected_reason,
        )

    unit_recognized: bool | None = None
    if verdict.outcome == "unit-unrecognized":
        unit_recognized = False
    elif verdict.unit_ucum:
        unit_recognized = True

    evidence: tuple[str, ...] = ("name",)
    if verdict.outcome in ("agreed", "switched"):
        if verdict.unit_ucum:
            evidence += ("property",)
        if _value_scales(value):
            evidence += ("scale",)

    if verdict.code == hit.loinc:
        return Resolution(
            term=hit.term,
            canonical=hit.canonical,
            loinc=hit.loinc,
            candidates=hit.candidates,
            resolved=hit.resolved,
            method=hit.method,
            score=hit.score,
            evidence=evidence,
            unit_recognized=unit_recognized,
            axes=verdict.axes,
            rejected_reason=verdict.rejected_reason,
        )
    return Resolution(
        term=hit.term,
        # No `or hit.canonical` fallback: `switched` is a code read out of the
        # axis table, so the lookup cannot miss, and the fallback was not
        # inert, it was the mask. A duplicate docstring-only `_name_for` left
        # behind by a refactor shadowed the real one and returned None for
        # every code, so EVERY switched reading reported the pre-switch name:
        # 14647-2 labelled "Cholesterol [Mass/volume]", the exact case this
        # function's own docstring uses as its example. A fallback that yields
        # a name contradicting the code is worse than no fallback.
        canonical=resolver._name_for(verdict.code),
        loinc=verdict.code,
        candidates=hit.candidates,
        resolved=True,
        method="lexical",
        evidence=evidence,
        unit_recognized=unit_recognized,
        axes=verdict.axes,
        rejected_code=verdict.rejected_code,
        rejected_reason=verdict.rejected_reason,
    )


__all__ = ["OfflineResolver", "Resolution", "UnitVerdict", "get_resolver", "resolve", "resolve_reading"]

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
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from mirobody._bundle import BUNDLE_PATH as _BUNDLE
from mirobody._bundle import (
    AXIS_ANALYTE as _ANALYTE,
    AXIS_CODE as _CODE,
    AXIS_COMPONENT as _COMPONENT,
    AXIS_FOLDED_LCN as _FOLDED_LCN,
    AXIS_LCN as _LCN,
    AXIS_SYSTEM as _SYSTEM,
    AXIS_TIME as _TIME,
    bundle_version,
    load_alias_sources,
    load_axis,
    read_code_list,
    read_members,
)
from mirobody._strtab import StringTable
from mirobody.lexical import index_fold, is_component_suffix, measure_stems, split_trailing_parenthetical, surface_variants

if TYPE_CHECKING:  # `_posting` names np.ndarray in its annotation; numpy itself
    import numpy as np  # is imported lazily so `import mirobody.engine` stays cheap

logger = logging.getLogger(__name__)


#: Everything `OfflineResolver.__init__` reads, fetched in one tar pass. The
#: axis field positions live in `_bundle` beside the loader, because the
#: semantic tier reads the same table.
_RUNTIME_MEMBERS = (
    "alias_keys.bin", "alias_index.npz",
    "corpus_names.bin", "corpus_names.npz",
    "axis_fields.bin", "axis_index.npz",
    "loinc_rank_bonus.npy",
)

# Sentinel target in resolver_overrides.tsv meaning "deliberately unresolved".
_BLOCK_SENTINEL = "!unresolved"

# Prefer the everyday specimen variants when the commonness prior ties. Bytes,
# not str: `_pick` matches these against slices of the corpus-name blob, so
# decoding is paid only for the row that wins.
_PLAIN_SPECIMEN = re.compile(rb"in (Serum or Plasma|Blood)\b", re.I)
_SPECIAL_SPECIMEN = re.compile(rb"\b(cord|capillary|venous|arterial|dialysis)\b", re.I)
# "Fasting plasma glucose FPG" -> "Fasting plasma glucose" (trailing acronym).
def _specimen_tokens(system: str) -> frozenset[str]:
    """The specimens a LOINC SYSTEM names: ``Ser/Plas`` is {Ser, Plas}."""
    return frozenset(t for t in re.split(r"[/^+]", system or "") if t)


_TRAILING_ACRONYM = re.compile(r"\s+[A-Z][A-Z0-9-]{1,7}$")

#: Scales a free-prose value maps to. `scales_for_value` answers (`Nar`, `Doc`)
#: for anything it cannot read as a number, an ordinal or a comparator, so this
#: set is "the value column holds a sentence" rather than a constraint on the
#: analyte. See `variant_for_reading`.
_NARRATIVE_SCALES = frozenset({"Nar", "Doc"})
#: The five leukocyte types of a differential, as the axis table spells their
#: COMPONENT. Their percentage code is the count component over
#: `/leukocytes`, and nothing else in LOINC pairs that regularly.
_LEUKOCYTE_TYPES = frozenset({"neutrophils", "lymphocytes", "monocytes", "eosinophils", "basophils"})
_FRACTION_PROPERTIES = frozenset({"NFr", "MFr", "VFr", "AFr", "SFr", "CFr"})


@dataclass(frozen=True)
class Resolution:
    """One resolved indicator name."""

    term: str                       # the input, as given
    canonical: str = ""             # canonical long name from the corpus
    loinc: str = ""                 # LOINC_NUM when the canonical name is LOINC
    candidates: int = 0             # how many corpus rows matched the alias
    resolved: bool = False
    #: How the answer was reached, or why there is none. ``"lexical"`` from the
    #: shipped vocabularies is the only kind :func:`resolve` returns;
    #: ``"refused"`` is a decision not to answer (a panel name, a string naming
    #: two tests) and unlike ``""`` must not be overturned by a second opinion.
    #: A caller using a code as an IDENTITY must accept only ``"lexical"``:
    #: semantic recall cannot abstain, nonsense scoring 0.78 on the LOINC matrix
    #: where real terms went as low as 0.56, so no threshold separates them.
    method: str = ""
    score: float = 0.0              # cosine, semantic answers only

    #: Which axes corroborated this answer, in order: ``("name",)`` the alias
    #: table alone, ``("name", "property")`` the printed unit agreed with or
    #: selected the code, ``("name", "property", "scale")`` the value's kind
    #: too. A unit that confirmed the code, one that did not parse and no unit
    #: at all used to give byte-identical results; this is how they differ.
    evidence: tuple[str, ...] = ()
    #: ``True``/``False`` when a unit was printed and did/does not normalize to
    #: UCUM; ``None`` when the reading carried no unit at all. A `False` here is
    #: the caller's signal that `loinc` rests on the name alone: 11 of 32 real
    #: printed unit spellings measured do not normalize today, so this is common
    #: and is a gap in our tables rather than a fault in the report.
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

    Construction is one pass over the bundle: about 0.33 s and 156 MB
    resident. Use :func:`get_resolver` for the cached singleton.

    Both numbers were 1.09 s and 514 MB when the artifacts were a pickled
    object array and two CSVs: reading those allocates ~1.6 million `str` for
    tables that answer a few hundred lookups per call. Storage shape, not data
    volume (77 MB of text either way). They are byte blobs plus offset arrays
    now, cut by `translate_build/build_bundle.py`, and nothing allocates per
    entry: `_posting` bisects the alias blob, `_pick` matches regexes against
    slices of the corpus-name blob, and only the winning row is decoded.
    """

    def __init__(self) -> None:
        import numpy as np

        self._normalize = index_fold
        # One pass over the tarball for all five members the resolver needs;
        # `loinc_skip.txt` stays out because `_component_index` is lazy.
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

        self._axis, self._order_code, self._order_name = load_axis(
            bundle_path=_BUNDLE, members=blobs
        )
        self._by_component: dict[bytes, list[int]] | None = None

        # term -> a target the index resolves, from two sources in precedence
        # order (`_bundle.alias_source_files` has the ordering rule):
        #   1. res/loinc/resolver_overrides.tsv, whose targets are index keys.
        #   2. res/loinc/aliases_src/*.tsv: zh.tsv and the curated corrections,
        #      ~23k terms since 1.5.0 dropped ja and the five machine-derived
        #      files. Their targets are phrases meant for the index build, so
        #      they resolve only sometimes; hence (1).
        self._skip: set[bytes] | None = None
        self._system_values: set[str] | None = None
        self._src = load_alias_sources(fold=index_fold)

        logger.info(
            "OfflineResolver ready: %d aliases, %d corpus names, %d LOINC axis rows (corpus %s)",
            len(self._alias), len(self._names), len(self._axis),
            bundle_version() or "unversioned",
        )

    @staticmethod
    def _table(blobs: dict, blob_member: str, index_member: str, *, raw: bool = False):
        """Load one blob member plus its offset/order arrays.

        The blob is a plain tar member and arrives as `bytes` in one
        allocation. Inside the .npz it would cost more than twice its size
        resident: `np.load` decompresses to an ndarray, `.tobytes()` copies
        it, and the allocator keeps both arenas. 75 MB versus 2 MB for the
        30 MB alias blob, so offsets go in an .npz and the text does not.
        """
        import numpy as np

        blob = blobs.get(blob_member)
        index = blobs.get(index_member)
        if blob is None or index is None:
            raise RuntimeError(
                f"{blob_member} / {index_member} not found in {_BUNDLE}. Run "
                "`git lfs pull` for the data bundles; if the bundle predates "
                "1.3.0, rebuild the runtime index with "
                "`python -m translate_build.build_bundle --loinc <release>`."
            )
        with np.load(io.BytesIO(index)) as z:
            arrays = {k: z[k] for k in z.files}
        if raw:
            return blob, arrays
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

    def _name_for(self, code: str) -> str:
        """code -> LONG_COMMON_NAME, for reporting a switched variant."""
        row = self._row_for_code(code)
        return self._axis.field(row, _LCN) if row >= 0 else ""

    def axes_of(self, code: str) -> tuple[str, str, str, str, str, str, str] | None:
        """`(component, property, scale, system, method, time, long_common_name)`
        of a code in the bundle, or `None`. The public face of the axis
        table, for `mirobody.translate` to build a series key from.

        TIME_ASPCT arrived with the 1.5.0 cut. Without it a fasting glucose and
        a 2-hour post-load glucose share one key, which is the axis LOINC uses
        to tell them apart."""
        row = self._row_for_code(code)
        if row < 0:
            return None
        _code, component, prop, scale, system, method, lcn = self._axis_row(row)
        return component, prop, scale, system, method, self._axis.field(row, _TIME), lcn

    def _loinc_for_name(self, name: str) -> str:
        """Corpus long name -> LOINC_NUM, "" when the name is not a LOINC row."""
        needle = self._normalize(name).encode("utf-8")
        row = self._axis.find_field(needle, self._order_name, _FOLDED_LCN)
        return self._axis.field(row, _CODE) if row >= 0 else ""

    # -- lookup ----------------------------------------------------------------

    def _posting(self, key: str) -> np.ndarray | None:
        """Corpus rows for one already-folded alias key, or None.

        Bisects the key blob rather than consulting a dict built from it: the
        shipped table is already sorted, and materialising it as 921k Python
        strings plus a dict cost 285 MB to turn a 0.9 us bisect into a 0.02 us
        hash, inside a `resolve()` that takes tens of microseconds either way.
        """
        i = self._alias.find(key.encode("utf-8"))
        if i < 0:
            return None
        return self._alias_rows[self._alias_off[i]:self._alias_off[i + 1]]

    def _candidate_keys(self, term: str) -> list[str]:
        """The lookup keys to try, most-specific first.

        The alias-table hop goes FIRST. A row in ``self._src`` is one person's
        statement that one term means one concept; a hit in the big alias index
        is every corpus row sharing a surface string, which ``_pick`` then
        guesses among by commonness.

        Index-first got this wrong: ``血红蛋白`` matches 301 index rows, of which
        the commonness prior likes *Hemoglobin A1c* best, so "hemoglobin"
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
        """
        keys: list[str] = []
        for surface in surface_variants(term):
            for key in self._keys_for(self._normalize(surface)):
                if key not in keys:
                    keys.append(key)

        # "Total cholesterol TC" -> "Total cholesterol". A lab report prints the
        # analyte beside its abbreviation constantly, and none of those strings
        # resolved: `Fasting plasma glucose FPG`, `总胆固醇 TC`, `甘油三酯 TG` all
        # returned nothing while their bare stems answered. The strip used to be
        # applied to the alias table's target inside `_keys_for`, where it could
        # only fire on inputs that already resolved. Tested on the RAW term
        # because the pattern is a case test and `normalize` lowercases;
        # appended last, so it can only turn a miss into a hit.
        stem = _TRAILING_ACRONYM.sub("", term).strip()
        if stem and stem != term.strip():
            if self._trailing_token_is_an_abbreviation(stem, term.strip()[len(stem):].strip()):
                for key in self._keys_for(self._normalize(stem)):
                    if key not in keys:
                        keys.append(key)
        # "monocyte count", "中性粒细胞计数": the analyte plus the word for how
        # it was counted. The index knows the analyte and the unit then picks
        # its count or fraction code (`variant_for_reading`). Appended last,
        # so it can only turn a miss into a hit.
        for stem in measure_stems(term):
            for key in self._keys_for(self._normalize(stem)):
                if key not in keys:
                    keys.append(key)
        return keys

    def _trailing_token_is_an_abbreviation(self, stem: str, token: str) -> bool:
        """Is the trailing ALL-CAPS token a repeat of `stem`, or does it add to it?

        `Total cholesterol TC` and `Protein CSF` are the same SHAPE and opposite
        meanings. Stripping the first is lossless; stripping the second answers a
        serum protein for a spinal-fluid one, and the answer looks confident.
        Four such wrong answers came out of a LIS-style `analyte + qualifier`
        export: `Protein CSF`, `Calcium ION`, `胆固醇 HDL`, `Glucose OGTT`.

        Two questions separate them, and neither is a word list:

        * **does the token name a SPECIMEN?** `CSF` is a SYSTEM axis value, so
          dropping it changes what was measured, not how it was spelled. `TC`,
          `TG` and `FPG` are not, which is why "is this token anywhere in the
          axis table" is too coarse a test, it fires on the abbreviations too.
        * **does the token mean something else on its own?** `HDL` answers
          2085-9 against `胆固醇`'s 2093-3, `ION` answers ionized calcium
          against total, `OGTT` answers a tolerance-test glucose against a
          plain one. A token that resolves to a DIFFERENT code than the stem is
          carrying information the stem does not have. `TC` and `TG` resolve to
          exactly their stem's code, which is what makes them redundant.

        A token that resolves to nothing and is no specimen is treated as an
        abbreviation: `FPG` reaches its stem through the alias table, and
        refusing the strip on "unknown" would give back the misses this exists
        to fix. Recursion is not a concern: the pattern needs whitespace before
        the token, and neither argument here has any.
        """
        if not token or token in self._systems() or is_component_suffix(token):
            return False
        if any(ch.isdigit() for ch in token):
            # `Vitamin D-3`, `Apolipoprotein B-100`: a numbered tail is a
            # member of a series, never a spelling of the stem.
            return False
        own = self._lookup(token)
        if own is None or not own.loinc:
            return True
        base = self._lookup(stem)
        return base is None or not base.loinc or base.loinc == own.loinc

    def _systems(self) -> set[str]:
        """Every SYSTEM axis value: the specimens a trailing token could name.

        2,467 of them over 97k rows. Built on first use and only ever from the
        trailing-token test, which is itself the coldest path in `resolve`:
        it runs after every other candidate key has already missed.
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
        """Alias-table hop then the raw key, for one already-normalized term."""
        keys: list[str] = []
        eng = self._src.get(norm)
        if eng:
            keys.append(self._normalize(eng))
            # The same strip on the alias table's TARGET: 429 of 48,366 targets
            # end in an acronym, and the stem is sometimes the better index key.
            # Its example used to be an INPUT ("Fasting plasma glucose FPG"),
            # which is not what this line can see, that case is handled in
            # `_candidate_keys`. Both are real: deleting this one costs 0.003
            # coverage and RAISES the wrong-rate 0.032 -> 0.034.
            stripped = _TRAILING_ACRONYM.sub("", eng).strip()
            if stripped and stripped != eng:
                keys.append(self._normalize(stripped))
        keys.append(norm)
        return keys

    def _axes_of(self, code: str) -> tuple[str, str, str]:
        """PROPERTY, SCALE, SYSTEM for a code, or ("", "", "") when unknown."""
        row = self._row_for_code(code)
        if row < 0:
            return ("", "", "")
        r = self._axis_row(row)
        return (r[2], r[3], r[4])

    def _skipped(self) -> set[bytes]:
        """LOINC codes the bundle says not to answer with.

        Non-clinical CLASS (SURVEY, PHENX, DOC, ADMIN, the PANEL.SURVEY.*
        family) plus DEPRECATED and DISCOURAGED status. Loaded on first use:
        `resolve()` needs it on every call, but importing the module should not
        open the bundle. Kept as bytes, because `_component_index` compares it
        against slices of the axis blob and decoding 97k codes to match a set
        of strings would cost more than the check saves.

        **It was built for `resolve()` and `resolve()` never consulted it.**
        Only `_component_index` did, so the list gated which sibling a
        unit-aware lookup could switch TO while leaving the first answer
        ungated: `呼吸次数` came back as *First Respiration rate Set*, a nursing
        documentation item, and 52 of the 7,354 eval cases answered with a code
        LOINC has since retired.

        A CLASS gate used to ride alongside this, `res/loinc_class_gated.tsv`,
        10,045 codes from disciplines a lab report never prints (`癌胚抗原`
        answered 17188-4 CLASS=CELLMARK instead of 2039-6 CLASS=CHEM). The
        1.5.0 cut drops those families at build time, so all 10,045 are now
        outside the bundle and the file gated nothing: measured, then deleted.
        """
        if self._skip is None:
            self._skip = {
                code.encode("ascii")
                for code in read_code_list("loinc_skip.txt", bundle_path=_BUNDLE)
            }
        return self._skip

    def _pick(self, rows, exclude: frozenset[int] = frozenset()) -> int:
        """Best corpus row: commonness prior, nudged toward plain specimens.

        Matches the two specimen patterns against raw blob slices. A hit like
        `血红蛋白` has 301 candidate rows, and decoding all of them to run a
        regex that only ever looks at ASCII would be 301 throwaway strings per
        call: the loser rows are never needed as text.
        """
        names = self._names
        rank = self._rank
        # -1 must mean "every candidate was excluded" and nothing else. Seeding
        # `best_row` from the first surviving row rather than leaving it at -1
        # is what keeps that true: a comparison against -inf can only fail on a
        # NaN score, and then the caller would read a miss where the old code
        # returned `rows[0]`. No shipped rank is NaN; the invariant should not
        # depend on that.
        best_row, best_score = -1, float("-inf")
        for r in rows:
            r = int(r)
            if r in exclude:
                continue
            if best_row < 0:
                best_row = r
            name = names.raw(r)
            score = float(rank[r])
            if _PLAIN_SPECIMEN.search(name):
                score += 0.25
            if _SPECIAL_SPECIMEN.search(name):
                score -= 0.25
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

        hit = self._lookup(term)
        if hit is not None:
            return hit

        # "名称(缩写)", the shape a lab report prints more often than not: on
        # production data 147 of 868 distinct indicator names have it and 70 of
        # those carried no code. Which half is the answer is not decidable by
        # position. ``空腹血糖(GLU)`` means the stem, ``血糖(HbA1c)`` means the
        # parenthetical, and answering the second with glucose would file an
        # HbA1c reading into the glucose series. So both halves are resolved and
        # a disagreement leaves the term unresolved.
        stem, inside = split_trailing_parenthetical(term)
        if not stem and not inside:
            return Resolution(term=term)
        if (stem and self._is_blocked(stem)) or (inside and self._is_blocked(inside)):
            return Resolution(term=term, method="refused")

        stem_hit = self._lookup(stem) if stem else None
        inside_hit = self._lookup(inside) if inside else None
        if stem_hit and inside_hit and stem_hit.loinc != inside_hit.loinc:
            # Different codes, so ask LOINC's COMPONENT whether they are even
            # the same substance.
            #   空腹血糖(GLU)   Glucose^post CFst  vs Glucose
            #     same analyte: the parenthetical only labels the stem, so the
            #     stem wins
            #   血糖(HbA1c)    Glucose      vs Hemoglobin A1c/Hemoglobin.total
            #   胆固醇(HDL-C)  Cholesterol  vs Cholesterol.in HDL
            #     two tests in one string, so it stays unresolved
            stem_analyte = self._analyte_of(stem_hit.loinc)
            inside_analyte = self._analyte_of(inside_hit.loinc)
            if not stem_analyte or stem_analyte != inside_analyte:
                # Still `refused`, but with the two answers named: a caller
                # can put the choice to a person, which a category word's
                # refusal never offers.
                return Resolution(
                    term=term,
                    method="refused",
                    candidates=stem_hit.candidates + inside_hit.candidates,
                    rejected_reason=(
                        f"{stem!r} gives {stem_hit.loinc} and {inside!r} gives {inside_hit.loinc}: "
                        "two analytes in one name"
                    ),
                )
            inside_hit = None

        chosen = stem_hit or inside_hit
        if chosen is None:
            return Resolution(term=term)
        return Resolution(
            term=term,
            canonical=chosen.canonical,
            loinc=chosen.loinc,
            candidates=chosen.candidates,
            resolved=True,
            method="lexical",
        )

    def _component_index(self) -> dict[bytes, list[int]]:
        """component -> its rows, built on first use and then cached.

        Lazy because `resolve()` never needs it: a caller with no value and no
        unit has nothing to pick a variant with, and paying for this index on
        every import would tax the offline path that is the whole point of the
        library.
        """
        if self._by_component is None:
            skip = self._skipped()
            index: dict[bytes, list[int]] = {}
            axis = self._axis
            for i in range(len(axis)):
                component = axis.field_raw(i, _COMPONENT)
                if component and axis.field_raw(i, _CODE) not in skip:
                    index.setdefault(component, []).append(i)
            self._by_component = index
        return self._by_component

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
          Half the shipped corpus is non-``Qn`` (38,687 rows), so this is not
          an edge.

        Both constraints are applied to the sibling with the same **full**
        COMPONENT: `Glucose^post CFst`, not `Glucose`, so a fasting reading
        cannot decay into plain glucose. Prefers the same SYSTEM and a
        method-less variant.

        Deterministic and reversible: no embedding, no scoring, still
        `method="lexical"`, because the analyte came from the alias table and
        the variant from a table lookup. Returns `loinc` unchanged whenever the
        reading says nothing, already agrees, or has no sibling: "leave it
        alone" is always available and always safe.
        """
        if not loinc:
            return UnitVerdict(code=loinc, outcome="no-signal")
        from mirobody.units import normalize_unit, parse_value_unit, unit_families
        from mirobody.value_scale import scales_for_value

        # `20%` and `("20", "%")` are the same reading written two ways, and a
        # stored value routinely carries its unit inline: `th_series_data.value`
        # holds "3.9 mmol/L". Without this the unit gate did not fire at all on
        # half the shapes real data arrives in.
        if not unit and value:
            unit = parse_value_unit(value).unit or ""
        printed_unit = (unit or "").strip()
        ucum = normalize_unit(printed_unit) if printed_unit else None
        families = frozenset(unit_families(ucum) or ()) if ucum else frozenset()
        scales = scales_for_value(value) or frozenset()
        # Free prose in the value column (`见报告`, `clear yellow fluid`) maps
        # to (`Nar`, `Doc`). That is the ABSENCE of a measurement, not a claim
        # about this analyte's scale: the report wrote a sentence where a result
        # goes. Treating it as a constraint made `尿蛋白` + `见报告` an
        # axis-conflict against its own correct `PrThr/Ord` code.
        if scales and scales <= _NARRATIVE_SCALES:
            scales = frozenset()

        row = self._row_for_code(loinc)
        axes = ("", "", "")
        current = None
        if row >= 0:
            current = self._axis_row(row)
            axes = (current[2], current[3], current[4])

        # A unit was printed and our tables do not know it. Answer from the name
        # and SAY SO, rather than either withholding the code or, as before,
        # returning it as though the unit had agreed. Withholding would be the
        # larger error: measured across real printed spellings, 11 of 32 fail to
        # normalize today (`Thousand/uL`, `uIU/mL`, `mm/hr`, `个/HP` …), so an
        # unrecognized unit is usually OUR gap, not a bad report.
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

        def _matching(component: bytes) -> list:
            return [
                r
                for r in (
                    self._axis_row(i) for i in self._component_index().get(component, [])
                )
                if ((not families) or r[2] in families)
                and ((not scales) or r[3] in scales)
            ]

        siblings = _matching(current[1].encode("utf-8"))
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

    def _lookup(self, term: str) -> Resolution | None:
        """First candidate key that hits the alias index, or None on a miss."""
        for key in self._candidate_keys(term):
            rows = self._posting(key)
            if rows is None or not len(rows):
                continue
            # Take the best candidate whose code the bundle does not tell us
            # to avoid. Re-picking rather than giving up matters: an alias with
            # 300 candidates usually has a good one behind the skipped one, and
            # refusing the whole term would trade far more coverage than the
            # one wrong answer is worth. Bounded by the candidate count, and in
            # practice it runs once.
            skipped = self._skipped()
            exclude: set[int] = set()
            while True:
                row = self._pick(rows, frozenset(exclude))
                if row < 0:
                    break
                name = self._names.get(row)
                code = self._loinc_for_name(name)
                # Two ways a candidate cannot be an identity, both meaning
                # "try the next one": the bundle says not to answer with this
                # code, or the row has no LOINC code at all. The corpus spans
                # six vocabularies and carries 4,991 `Deprecated ...` names, so
                # a tenth of alias hits came back `resolved=True,
                # method="lexical", loinc=""`, and a caller following this
                # module's identity rule got `""` as a grouping key, merging
                # every such reading into one bucket.
                if not code or code.encode("ascii") in skipped:
                    exclude.add(row)
                    continue
                return Resolution(
                    term=term,
                    canonical=name,
                    loinc=code,
                    candidates=int(len(rows)),
                    resolved=True,
                    method="lexical",
                    # `("name",)`, not `()`: the alias table chose this code and
                    # nothing corroborated it. Left empty, `"name" in evidence`
                    # was False from `resolve()` and True from
                    # `resolve_reading()` for the same term and code.
                    evidence=("name",),
                    axes=self._axes_of(code),
                )
        return None


@lru_cache(maxsize=1)
def get_resolver() -> OfflineResolver:
    return OfflineResolver()


def resolve(term: str) -> Resolution:
    """Module-level convenience: offline-resolve one indicator name."""
    return get_resolver().resolve(term)


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

    unit_recognized = None if verdict.outcome == "no-signal" and not verdict.unit_ucum else None
    if verdict.outcome == "unit-unrecognized":
        unit_recognized = False
    elif verdict.unit_ucum:
        unit_recognized = True

    evidence: tuple[str, ...] = ("name",)
    if verdict.outcome in ("agreed", "switched") and verdict.unit_ucum:
        evidence += ("property",)
    if verdict.outcome in ("agreed", "switched") and value:
        from mirobody.value_scale import scales_for_value

        if scales_for_value(value):
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

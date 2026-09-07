"""The engine's front door — parse a health document, resolve to standard codes.

Zero infrastructure: no PostgreSQL, no Redis, no server. ``resolve`` needs no
credentials at all (pure offline lookup against the shipped data bundles);
``parse`` needs exactly one LLM key (any of the providers
:func:`mirobody.utils.llm.unified_file_extract` auto-detects).

    from mirobody.engine import resolve, parse_file

    resolve("血红蛋白").loinc          # -> "718-7", offline
    await parse_file("labs.pdf")       # -> readings + resolutions, one LLM call

This is the deliberate small door into the first two engine stages (① Collect,
② Standardize) — the same machinery the full platform uses, minus its persistence:

* **Lexical resolution** rides the shipped LOINC bundle: the 921k-entry
  multilingual alias index (``loinc_alias_index.npz``), the 677k-name corpus
  sidecar (``fhir_meta.csv.gz``), the per-row commonness prior
  (``loinc_rank_bonus.npy``), and the LOINC axis table for the final
  name → LOINC_NUM hop. All official build artifacts, loaded read-only —
  plus ``res/resolver_overrides.tsv``, the hand-written corrections for terms
  the index gets wrong or misses (see that file's header, and
  ``test_engine_coverage.py`` for what it is measured against).
* **What this deliberately is NOT**: the full v2 semantic pipeline
  (:func:`mirobody.indicator.fhir.resolve.pipeline.resolve_many`), which adds
  embedding recall + family rerank but requires the multi-GB embedding matrix
  that does not ship in git. When a term misses here, the honest answer is
  ``unresolved`` — never a guess.
* **Unit normalization** via :mod:`mirobody.units` (offline).

The candidate picker is a lite heuristic (commonness prior, then a small
preference for the plain Serum/Plasma/Blood variants over cord/capillary
specials). It exists to give ONE good default answer; ``candidates`` carries
the count so callers know when a term was ambiguous.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from ._bundle import BUNDLE_PATH as _BUNDLE
from ._bundle import (
    AXIS_ANALYTE as _ANALYTE,
    AXIS_CODE as _CODE,
    AXIS_COMPONENT as _COMPONENT,
    AXIS_FOLDED_LCN as _FOLDED_LCN,
    AXIS_LCN as _LCN,
    AXIS_SYSTEM as _SYSTEM,
    bundle_version,
    load_alias_sources,
    load_axis,
    read_code_list,
    read_members,
)
from ._strtab import StringTable
from .lexical import index_fold, split_trailing_parenthetical, surface_variants

if TYPE_CHECKING:  # `_posting` names np.ndarray in its annotation; numpy itself
    import numpy as np  # is imported lazily so `import mirobody.engine` stays cheap

logger = logging.getLogger(__name__)

#: The stable surface of this module. `mirobody/__init__.py` re-exports all of
#: it lazily, so `from mirobody import resolve` and `from mirobody.engine
#: import resolve` are the same function; the short spelling is the documented
#: one. Anything not listed here is internal — `OfflineResolver`'s underscore
#: attributes especially, which are the loaded index and change shape freely.
__all__ = [
    "OfflineResolver",
    "Reading",
    "Resolution",
    "get_resolver",
    "parse_file",
    "resolve",
    "resolve_reading",
    "resolve_with_semantic_fallback",
]

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
_TRAILING_ACRONYM = re.compile(r"\s+[A-Z][A-Z0-9-]{1,7}$")


@dataclass(frozen=True)
class Resolution:
    """One resolved indicator name."""

    term: str                       # the input, as given
    canonical: str = ""             # canonical long name from the corpus
    loinc: str = ""                 # LOINC_NUM when the canonical name is LOINC
    candidates: int = 0             # how many corpus rows matched the alias
    resolved: bool = False
    #: How the answer was reached, or why there is none:
    #:
    #:   ``"lexical"``   shipped vocabularies — the only kind :func:`resolve`
    #:                   returns
    #:   ``"semantic"``  embedding recall, via
    #:                   :func:`resolve_with_semantic_fallback`
    #:   ``"refused"``   a DECISION not to answer: a panel name, or a string
    #:                   naming two different tests. Distinct from ``""``, which
    #:                   means simply not found, because the two want opposite
    #:                   treatment — a gap is worth a second opinion, a refusal
    #:                   is the answer and must not be overturned by one.
    #:   ``""``          not found
    #:
    #: **A caller that uses a code as an IDENTITY** — a grouping key, a decision
    #: that two readings are the same series, a FHIR mirror — **must accept only
    #: ``"lexical"``.** Semantic recall cannot abstain: measured on the LOINC
    #: matrix, nonsense scored 0.78 while real terms went as low as 0.56, so no
    #: threshold separates them. It is a suggestion to confirm, not an identity.
    method: str = ""
    score: float = 0.0              # cosine, semantic answers only

    @property
    def code(self) -> str:
        return self.loinc


@dataclass(frozen=True)
class Reading:
    """One extracted observation from a document."""

    name: str
    value: str = ""
    unit: str = ""
    reference_range: str = ""
    resolution: Resolution | None = None


class OfflineResolver:
    """Lexical indicator-name resolution against the shipped bundles.

    Construction is one pass over the bundle: about 0.33 s and 156 MB
    resident. Use :func:`get_resolver` for the cached singleton.

    Both numbers were 1.09 s and 514 MB, and neither was about the amount of
    data — 77 MB of text. They were about its SHAPE. The artifacts stored the
    tables as things that become Python objects when you read them (a pickled
    object array, two CSVs), so a load allocated roughly 1.6 million `str`
    for tables that answer a few hundred lookups per call. They are byte blobs
    plus offset arrays now (`scripts/build_runtime_index.py` cuts them, on
    disk it is a wash), and nothing here allocates per entry: `_posting`
    bisects the alias blob, `_pick` matches its regexes against slices of the
    corpus-name blob, and only the row that wins is ever decoded.
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

        # term -> a target the index resolves. Two sources, in precedence order
        # (see `_bundle.alias_source_files` for the ordering rule and the bug
        # that motivates it):
        #
        #   1. res/resolver_overrides.tsv — this resolver's own corrections,
        #      where the target is guaranteed to be an index key.
        #   2. res/aliases_src/*.tsv — the bundle build's curated inputs,
        #      reused here as a broad fallback (~48k foreign-language terms).
        #      Their targets are descriptive phrases meant for the index BUILD,
        #      so they resolve only sometimes; that is why (1) exists.
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
        allocation. Putting it inside the .npz instead would cost more than
        twice its size in resident memory — `np.load` decompresses to an
        ndarray and `.tobytes()` copies it, and the allocator keeps both
        arenas. Measured at 75 MB versus 2 MB for the 30 MB alias blob, which
        is why the offsets go in an .npz and the text does not.
        """
        import numpy as np

        blob = blobs.get(blob_member)
        index = blobs.get(index_member)
        if blob is None or index is None:
            raise RuntimeError(
                f"{blob_member} / {index_member} not found in {_BUNDLE}. Run "
                "`git lfs pull` for the data bundles; if the bundle predates "
                "1.3.0, rebuild the runtime index with "
                "`python scripts/build_runtime_index.py`."
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
        hash — inside a `resolve()` that takes tens of microseconds either way.
        """
        i = self._alias.find(key.encode("utf-8"))
        if i < 0:
            return None
        return self._alias_rows[self._alias_off[i]:self._alias_off[i + 1]]

    def _candidate_keys(self, term: str) -> list[str]:
        """The lookup keys to try, most-specific first.

        The alias-table hop goes FIRST, ahead of the raw index lookup. A row in
        ``self._src`` is a statement about what one term *means* — one term,
        one concept, written by a person. A hit in the big alias index is a bag
        of every corpus row sharing a surface string, which ``_pick`` then
        guesses among by commonness. When both are available the written
        statement wins, because it is the one carrying human intent.

        This ordering is not cosmetic. ``血红蛋白`` matches 301 rows in the index,
        of which the commonness prior likes *Hemoglobin A1c* best — so the
        index-first order answered "hemoglobin" with the code for a completely
        different test, silently and confidently. The alias table says plainly
        ``血红蛋白 -> Hemoglobin``. See test_engine_coverage.py, which exists
        largely to keep this class of near-miss from coming back.

        Both hops are then repeated for each surface variant from
        :func:`mirobody.lexical.surface_variants`. The bundle's own
        normalizer is NFKC + casefold and nothing more — it has to be, it folded
        the index keys at build time — so a full-width ``ＦＢＧ``, an en-dashed
        ``LDL–C`` and a snake_case ``fasting_glucose`` each sat one invisible
        codepoint away from a key that already exists. (snake_case is not a
        corner case: it is the convention the platform API documents in every
        ``POST /data`` example.) Variants come LAST, after the term as written
        has missed, so they can only turn a miss into a hit — never overrule an
        answer that was already correct.
        """
        keys: list[str] = []
        for surface in surface_variants(term):
            for key in self._keys_for(self._normalize(surface)):
                if key not in keys:
                    keys.append(key)

        # "Total cholesterol TC" -> "Total cholesterol". A lab report prints the
        # analyte and its abbreviation side by side constantly, and none of those
        # strings resolved: `Fasting plasma glucose FPG`, `总胆固醇 TC`,
        # `甘油三酯 TG` all returned nothing while their bare stems answered.
        #
        # The strip used to be applied to the alias table's TARGET value, inside
        # `_keys_for` — so it could only fire on inputs that were already alias
        # keys, which are exactly the inputs that already resolved. The comment
        # there gave an input-side example for target-side code; this is that
        # example, on the input, where it was always meant to be.
        #
        # Tested on the RAW term because the pattern is a case test and
        # `normalize` lowercases. Appended LAST, after every other key has
        # missed, so like the surface variants above it can only turn a miss
        # into a hit — never overrule an answer that was already right.
        stem = _TRAILING_ACRONYM.sub("", term).strip()
        if stem and stem != term.strip():
            if self._trailing_token_is_an_abbreviation(stem, term.strip()[len(stem):].strip()):
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
          axis table" is too coarse a test — it fires on the abbreviations too.
        * **does the token mean something else on its own?** `HDL` answers
          2085-9 against `胆固醇`'s 2093-3, `ION` answers ionized calcium
          against total, `OGTT` answers a tolerance-test glucose against a
          plain one. A token that resolves to a DIFFERENT code than the stem is
          carrying information the stem does not have. `TC` and `TG` resolve to
          exactly their stem's code, which is what makes them redundant.

        A token that resolves to nothing and is no specimen is treated as an
        abbreviation — `FPG` reaches its stem through the alias table, and
        refusing the strip on "unknown" would give back the misses this exists
        to fix. Recursion is not a concern: the pattern needs whitespace before
        the token, and neither argument here has any.
        """
        if not token or token in self._systems():
            return False
        own = self._lookup(token)
        if own is None or not own.loinc:
            return True
        base = self._lookup(stem)
        return base is None or not base.loinc or base.loinc == own.loinc

    def _systems(self) -> set[str]:
        """Every SYSTEM axis value — the specimens a trailing token could name.

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
        a panel code — see resolver_overrides.tsv): ``blood_pressure`` skipped
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
            # which is not what this line can see — that case is handled in
            # `_candidate_keys`. Both are real: deleting this one costs 0.003
            # coverage and RAISES the wrong-rate 0.032 -> 0.034.
            stripped = _TRAILING_ACRONYM.sub("", eng).strip()
            if stripped and stripped != eng:
                keys.append(self._normalize(stripped))
        keys.append(norm)
        return keys

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
        ungated — `呼吸次数` came back as *First Respiration rate Set*, a nursing
        documentation item, and 52 of the 7,354 eval cases answered with a code
        LOINC has since retired.
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
        call — the loser rows are never needed as text.
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
        # analytes and LOINC's lipid panels differ by which children they
        # include, so any single code encodes an assumption about what was
        # ordered. A confident wrong code is worse than an honest miss, so these
        # resolve to nothing and the caller has to say which measurement.
        #
        # Note the case this is NOT: a panel term that has a real panel code
        # ("blood pressure" -> 85354-9) resolves, because a panel code says
        # "expect components", which is the very thing a refusal would only be
        # gesturing at. See resolver_overrides.tsv.
        #
        # All surface variants are checked, not just the term as written — see
        # `_is_blocked` for the back door that requires.
        if self._is_blocked(term):
            return Resolution(term=term, method="refused")

        hit = self._lookup(term)
        if hit is not None:
            return hit

        # "名称(缩写)" — the shape a lab report prints more often than not. On
        # the hosted platform's production data, 147 of 868 distinct indicator
        # names are this shape and 70 of them carried no code at all.
        #
        # Which half is the answer is NOT decidable by position, so it is not
        # guessed. ``空腹血糖(GLU)`` means the stem; ``血糖(HbA1c)`` means the
        # parenthetical, and answering that one with glucose would file an HbA1c
        # reading into the glucose series. So both halves are resolved, and when
        # they disagree the term stays unresolved — the same trade the
        # ``!unresolved`` sentinel makes, applied to a shape rather than a word.
        stem, inside = split_trailing_parenthetical(term)
        if not stem and not inside:
            return Resolution(term=term)
        if (stem and self._is_blocked(stem)) or (inside and self._is_blocked(inside)):
            return Resolution(term=term, method="refused")

        stem_hit = self._lookup(stem) if stem else None
        inside_hit = self._lookup(inside) if inside else None
        if stem_hit and inside_hit and stem_hit.loinc != inside_hit.loinc:
            # Different codes, so ask the axis table whether they are even the
            # same substance. LOINC's COMPONENT answers it:
            #
            #   空腹血糖(GLU)   Glucose^post CFst  vs Glucose
            #                  -> same analyte, the parenthetical is just
            #                     labelling the stem, so the stem (the more
            #                     specific framing, and the written head of the
            #                     term) wins.
            #   血糖(HbA1c)    Glucose            vs Hemoglobin A1c/Hemoglobin.total
            #   胆固醇(HDL-C)  Cholesterol        vs Cholesterol.in HDL
            #                  -> different analytes: two tests in one string,
            #                     and picking either files the reading into the
            #                     wrong series. Stays unresolved.
            stem_analyte = self._analyte_of(stem_hit.loinc)
            inside_analyte = self._analyte_of(inside_hit.loinc)
            if not stem_analyte or stem_analyte != inside_analyte:
                return Resolution(term=term, method="refused")
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

    def variant_for_reading(self, loinc: str, value: str | None, unit: str | None) -> str:
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
          qualitative panel, ten of thirty indicators did exactly that —
          尿糖, 尿酮体, 类风湿因子, 抗核抗体, 妊娠试验 and their English forms.
          Half the shipped corpus is non-``Qn`` (38,687 rows), so this is not
          an edge.

        Both constraints are applied to the sibling with the same **full**
        COMPONENT — `Glucose^post CFst`, not `Glucose`, so a fasting reading
        cannot decay into plain glucose. Prefers the same SYSTEM and a
        method-less variant.

        Deterministic and reversible: no embedding, no scoring, still
        `method="lexical"`, because the analyte came from the alias table and
        the variant from a table lookup. Returns `loinc` unchanged whenever the
        reading says nothing, already agrees, or has no sibling — "leave it
        alone" is always available and always safe.
        """
        if not loinc:
            return loinc
        from .units import normalize_unit, parse_value_unit, unit_families
        from .value_scale import scales_for_value

        # `20%` and `("20", "%")` are the same reading written two ways, and a
        # stored value routinely carries its unit inline — `th_series_data.value`
        # holds "3.9 mmol/L". Without this the unit gate did not fire at all on
        # half the shapes real data arrives in.
        if not unit and value:
            unit = parse_value_unit(value).unit or ""
        ucum = normalize_unit(unit) if unit else None
        families = frozenset(unit_families(ucum) or ()) if ucum else frozenset()
        scales = scales_for_value(value) or frozenset()
        if not families and not scales:
            return loinc

        row = self._row_for_code(loinc)
        if row < 0:
            return loinc
        current = self._axis_row(row)
        prop_ok = (not families) or current[2] in families
        scale_ok = (not scales) or current[3] in scales
        if prop_ok and scale_ok:
            return loinc

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
            # (NFr) and `neutrophils` (NCnc). So `中性粒细胞` reported as
            # `4.2 10*9/L` could not reach its own count code — the analyte
            # resolves to the ratio, which is what a bare differential term
            # means on a CBC, and the unit had no way to say otherwise.
            #
            # Dropping the denominator is well-determined; adding one is not.
            # `neutrophils` has three NFr children (`/cells`, `/leukocytes`,
            # `/round cells`) and only clinical knowledge picks the CBC one, so
            # this crosses in the ratio -> count direction ONLY. The other
            # direction stays curated, in `resolver_overrides.tsv`.
            numerator, sep, _ = current[1].partition("/")
            if sep:
                siblings = _matching(numerator.encode("utf-8"))
        if not siblings:
            return loinc
        same_system = [r for r in siblings if r[4] == current[4]] or siblings
        # A method-less variant first: `... by Automated count` is a narrower
        # claim than the report supports.
        same_system.sort(key=lambda r: (r[5] != "", len(r[6])))
        return same_system[0][0]

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
                # Two ways a candidate cannot be an identity, and both mean
                # "try the next one" rather than "answer with it":
                #
                #   - the bundle tells us not to answer with this code;
                #   - the row has no LOINC code at all. The corpus spans six
                #     vocabularies and carries 4,991 `Deprecated …` names, so a
                #     tenth of all alias hits came back `resolved=True,
                #     method="lexical", loinc=""`. A caller following this
                #     module's own identity rule — accept only
                #     `method == "lexical"` — got `""` as a grouping key and
                #     merged every such reading into one bucket. Two consumers
                #     in this repo read that state opposite ways:
                #     `resolve_with_semantic_fallback` treated it as answered
                #     and withheld the second tier, `mirobody/evals/run_eval.py` scored
                #     it as unanswered.
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
    `{indicator, value, unit}` — so by the time a name reaches the resolver it
    is a measurement with a magnitude, and throwing that away to match on the
    name alone discards the strongest disambiguator available.
    """
    resolver = get_resolver()
    hit = resolver.resolve(name)
    if not hit.resolved or not hit.loinc:
        return hit
    switched = resolver.variant_for_reading(hit.loinc, value, unit)
    if switched == hit.loinc:
        return hit
    return Resolution(
        term=hit.term,
        # No `or hit.canonical` fallback: `switched` is a code read out of the
        # axis table, so the lookup cannot miss — and the fallback was not
        # inert, it was the mask. A duplicate docstring-only `_name_for` left
        # behind by a refactor shadowed the real one and returned None for
        # every code, so EVERY switched reading reported the pre-switch name:
        # 14647-2 labelled "Cholesterol [Mass/volume]", the exact case this
        # function's own docstring uses as its example. A fallback that yields
        # a name contradicting the code is worse than no fallback.
        canonical=resolver._name_for(switched),
        loinc=switched,
        candidates=hit.candidates,
        resolved=True,
        method="lexical",
    )


async def resolve_with_semantic_fallback(
    terms: list[str],
    *,
    index_path: str | None = None,
    min_score: float | None = None,
) -> list[Resolution]:
    """Lexical first; embedding recall only for the terms that missed.

    **Opt-in on purpose, and the opposite of a drop-in upgrade.** Semantic
    recall raises coverage and lowers trust at the same time: it answers terms
    the alias tables never heard of, and it also answers `绝对不存在的指标名xyzzy`
    with a confident code, because it has no way to say "I don't know". Measured
    on the LOINC matrix, nonsense scored 0.78 while genuine indicator names went
    as low as 0.56 — the ranges overlap, so `min_score` cannot make it honest.
    It is exposed anyway because a suggested code a human or a model can confirm
    beats a blank, but every one of them comes back marked ``method="semantic"``
    and must not be used as an identity. :func:`resolve` never returns one.

    Falls back silently to the lexical answer when no matrix is installed —
    that is the normal state of a `pip install`, not a failure.

    `min_score` is offered for callers who want a floor anyway (e.g. to cut the
    obviously-hopeless tail before showing suggestions); it is None by default
    because presenting one as a correctness threshold would be a lie.
    """
    resolver = get_resolver()
    out = [resolver.resolve(t) for t in terms]

    # `method="refused"` is a decision, not a gap. `血脂` is four analytes with
    # no single panel code; `血糖(HbA1c)` names two different tests in one
    # string. Neither has a right answer, and
    # the embedding tier will supply one anyway — measured, it answered all nine
    # refusals in the eval set and got all nine wrong. Letting the second tier
    # overturn the first tier's refusal is the one thing this design must not
    # do, so only genuine misses go on.
    missed = [i for i, r in enumerate(out) if not r.resolved and r.method != "refused"]
    if not missed:
        return out

    from .indicator.semantic import get_index

    index = get_index(index_path)
    if index is None:
        return out

    ranked = await index.search([terms[i] for i in missed], top_k=1)
    for i, candidates in zip(missed, ranked, strict=False):
        if not candidates:
            continue
        best = candidates[0]
        if min_score is not None and best.score < min_score:
            continue
        out[i] = Resolution(
            term=terms[i],
            canonical=best.canonical,
            loinc=best.loinc,
            candidates=1,
            resolved=True,
            method="semantic",
            score=best.score,
        )
    return out


# ── parse: document -> readings (one LLM call) ────────────────────────────────

_EXTRACT_PROMPT = """You are a medical lab-report extraction engine.
Extract EVERY health indicator measurement from this document.
Return ONLY a JSON array, no prose. Each element:
{"name": "<indicator name exactly as printed>", "value": "<numeric or textual result>", "unit": "<unit as printed, empty if none>", "reference_range": "<as printed, empty if none>"}
Rules: keep the original language of names; do not translate; do not invent
values; skip section headers and non-measurements."""



async def parse_text(document: str, *, resolve_names: bool = True) -> list[Reading]:
    """Parse report TEXT into readings — the same one LLM call as
    :func:`parse_file`, without a file.

    Split out of ``parse_file`` for ``POST /api/standardize``, which is handed
    raw text by the caller and had no way in short of writing a temp file.
    """
    from .utils import Config
    from .utils.llm import async_get_text_completion

    await Config.init()
    raw = await async_get_text_completion(
        [
            {"role": "system", "content": _EXTRACT_PROMPT},
            {"role": "user", "content": document},
        ]
    ) or ""
    return _readings_from_json(raw, resolve_names=resolve_names)


async def parse_file(path: str, *, resolve_names: bool = True) -> list[Reading]:
    """Parse a lab report / health document into readings, optionally resolving
    each indicator name to its canonical LOINC identity (offline).

    The document becomes TEXT first (`mirobody.documents.extract`): a PDF's
    embedded text layer page by page, a spreadsheet or Word file as a table,
    and only a scanned page or a photo through the vision provider — one image
    at a time, never the whole file. Then the same one extraction call as
    :func:`parse_text`. A born-digital PDF therefore needs a text model key
    only. Raises RuntimeError with a plain message when no provider key is
    configured or nothing readable was found.
    """
    # The provider auto-detection reads keys through the config system (which
    # also loads .env); standalone callers — the CLI, a bare library user —
    # haven't initialized it. Init is idempotent and works with zero yaml files.
    from .utils import Config

    await Config.init()

    from .documents import extract as documents
    from .documents.ocr import vision_ocr

    with open(path, "rb") as f:
        data = f.read()
    text = await documents.extract_text(os.path.basename(path), None, data, ocr=vision_ocr)
    if not text.strip():
        raise RuntimeError(f"no readable text could be extracted from {os.path.basename(path)}")
    return await parse_text(text, resolve_names=resolve_names)


def _readings_from_json(raw: str, *, resolve_names: bool) -> list[Reading]:
    """The extraction model's JSON array -> Readings. Shared by both parsers."""
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|```$", "", text).strip()
    try:
        items = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"extraction returned non-JSON output: {text[:200]}") from exc
    if not isinstance(items, list):
        raise RuntimeError(f"extraction returned {type(items).__name__}, expected a JSON array")

    resolver = get_resolver() if resolve_names else None
    readings: list[Reading] = []
    for it in items:
        if not isinstance(it, dict) or not it.get("name"):
            continue
        name = str(it["name"]).strip()
        value = str(it.get("value") or "").strip()
        unit = str(it.get("unit") or "").strip()
        readings.append(
            Reading(
                name=name,
                value=value,
                unit=unit,
                reference_range=str(it.get("reference_range") or "").strip(),
                # The unit is right here, and LOINC codes the unit into the
                # identity — resolving on the name alone would file a mmol/L
                # reading under the mg/dL code.
                resolution=(
                    resolve_reading(name, value, unit) if resolver else None
                ),
            )
        )
    return readings

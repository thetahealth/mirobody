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
* **Unit normalization** via :mod:`mirobody.indicator.fhir.units` (offline).

The candidate picker is a lite heuristic (commonness prior, then a small
preference for the plain Serum/Plasma/Blood variants over cord/capillary
specials). It exists to give ONE good default answer; ``candidates`` carries
the count so callers know when a term was ambiguous.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional

from .indicator.lexical import split_trailing_parenthetical, surface_variants

logger = logging.getLogger(__name__)

_RES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "res")
_BUNDLE = os.path.join(_RES_DIR, "fhir_loinc_bundle.tar.gz")
_META = os.path.join(_RES_DIR, "fhir_meta.csv.gz")
_ALIAS_SRC_DIR = os.path.join(_RES_DIR, "aliases_src")
_OVERRIDES = os.path.join(_RES_DIR, "resolver_overrides.tsv")

# Sentinel target in resolver_overrides.tsv meaning "deliberately unresolved".
_BLOCK_SENTINEL = "!unresolved"

# Prefer the everyday specimen variants when the commonness prior ties.
_PLAIN_SPECIMEN = re.compile(r"in (Serum or Plasma|Blood)\b", re.I)
_SPECIAL_SPECIMEN = re.compile(r"\b(cord|capillary|venous|arterial|dialysis)\b", re.I)
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
    resolution: Optional[Resolution] = None


class OfflineResolver:
    """Lexical indicator-name resolution against the shipped bundles.

    Construction cost is a few seconds (rebuilding the 921k-alias dict);
    use :func:`get_resolver` for the cached singleton.
    """

    def __init__(self) -> None:
        import numpy as np

        from .indicator.fhir.embeddings.alias import _normalize
        from .indicator.fhir.embeddings.bundle import read_member

        self._normalize = _normalize

        raw = read_member("loinc_alias_index.npz", bundle_path=_BUNDLE)
        if raw is None:
            raise RuntimeError(
                f"loinc_alias_index.npz not found in {_BUNDLE} — run `git lfs pull` "
                "to fetch the data bundles (see README prerequisites)"
            )
        with np.load(io.BytesIO(raw), allow_pickle=True) as z:
            aliases, offsets, rows = z["aliases"], z["offsets"], z["rows"]
        self._alias: dict[str, "np.ndarray"] = {
            str(a): rows[offsets[i]:offsets[i + 1]] for i, a in enumerate(aliases)
        }

        self._names: list[str] = []
        with gzip.open(_META, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                self._names.append(row[0])

        rank_raw = read_member("loinc_rank_bonus.npy", bundle_path=_BUNDLE)
        self._rank = (
            np.load(io.BytesIO(rank_raw))
            if rank_raw is not None
            else np.zeros(len(self._names), dtype=np.float32)
        )

        # name -> LOINC_NUM, and LOINC_NUM -> analyte head, from the axis
        # table inside the bundle.
        self._loinc_by_name: dict[str, str] = {}
        self._analyte: dict[str, str] = {}
        self._axis_rows: list[tuple[str, str, str, str, str, str, str]] = []
        self._by_component: dict[str, list[tuple]] | None = None
        axis_raw = read_member("loinc_axis.csv", bundle_path=_BUNDLE)
        if axis_raw is not None:
            for row in csv.DictReader(io.StringIO(axis_raw.decode("utf-8"))):
                self._loinc_by_name[_normalize(row["LONG_COMMON_NAME"])] = row["LOINC_NUM"]
                # COMPONENT is the analyte axis, and the part before `^` is the
                # analyte itself with any challenge/timing modifier stripped:
                # 1558-6 is `Glucose^post CFst`, 2339-0 is `Glucose`. Same head =
                # same substance measured differently; different head = a
                # different test. `resolve` uses it to decide whether the two
                # halves of `名称(缩写)` are talking about one thing.
                component = (row.get("COMPONENT") or "").split("^")[0].strip()
                if component:
                    self._analyte[row["LOINC_NUM"]] = _normalize(component)
                # The FULL component (challenge suffix included) plus the two
                # axes `resolve_reading` gates on. Kept from the pass that is
                # already reading this file rather than re-parsing 97k rows.
                self._axis_rows.append((
                    row["LOINC_NUM"],
                    _normalize(row.get("COMPONENT") or ""),
                    row.get("PROPERTY") or "",
                    row.get("SCALE_TYP") or "",
                    row.get("SYSTEM") or "",
                    row.get("METHOD_TYP") or "",
                    row.get("LONG_COMMON_NAME") or "",
                ))

        # term -> a target the index resolves. Two sources, in precedence order:
        #
        #   1. res/resolver_overrides.tsv — this resolver's own corrections,
        #      where the target is guaranteed to be an index key.
        #   2. res/aliases_src/*.tsv — the bundle build's curated inputs,
        #      reused here as a broad fallback (~48k foreign-language terms).
        #      Their targets are descriptive phrases meant for the index BUILD,
        #      so they resolve only sometimes; that is why (1) exists.
        self._src: dict[str, str] = {}
        for path in self._alias_source_files():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) == 2 and parts[0] and parts[1]:
                        self._src.setdefault(_normalize(parts[0]), parts[1])

        logger.info(
            "OfflineResolver ready: %d aliases, %d corpus names, %d LOINC axis rows",
            len(self._alias), len(self._names), len(self._loinc_by_name),
        )

    @staticmethod
    def _alias_source_files() -> list[str]:
        """Alias TSVs in precedence order; earlier files win (setdefault).

        Overrides first, then the bundle-build inputs — and within those, the
        hand-written `*_curated.tsv` ahead of the machine-derived `<lang>.tsv`.
        Plain sorted order put zh.tsv before zh_curated.tsv, which silently
        discarded curated corrections: adding the right row changed nothing.
        """
        files = [_OVERRIDES] if os.path.isfile(_OVERRIDES) else []
        if os.path.isdir(_ALIAS_SRC_DIR):
            names = [fn for fn in os.listdir(_ALIAS_SRC_DIR) if fn.endswith(".tsv")]
            files += [
                os.path.join(_ALIAS_SRC_DIR, fn)
                for fn in sorted(names, key=lambda n: (0 if "_curated" in n else 1, n))
            ]
        return files

    # -- lookup ----------------------------------------------------------------

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
        :func:`mirobody.indicator.lexical.surface_variants`. The bundle's own
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
        return keys

    def _is_blocked(self, term: str) -> bool:
        """True when ANY surface variant of the term is a deliberate
        non-answer (target ``!unresolved`` in resolver_overrides.tsv).

        Every variant, not only the term as written: variant lookup is a new
        route into the index, so a block that knew one spelling would simply be
        walked around. Measured — ``blood_pressure`` skipped a block written for
        ``blood pressure``, tokenized to it anyway, hit those 183 rows and came
        back 8462-4 (diastolic): the exact bug the sentinel exists to prevent,
        reached through the back door.
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
            # "Fasting plasma glucose FPG" -> "fasting plasma glucose"
            stripped = _TRAILING_ACRONYM.sub("", eng).strip()
            if stripped and stripped != eng:
                keys.append(self._normalize(stripped))
        keys.append(norm)
        return keys

    def _pick(self, rows) -> int:
        """Best corpus row: commonness prior, nudged toward plain specimens."""
        best_row, best_score = int(rows[0]), float("-inf")
        for r in rows:
            r = int(r)
            name = self._names[r]
            score = float(self._rank[r])
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
        # Some terms name a PANEL, not one observation: "blood pressure" matched
        # 183 index rows and the commonness prior returned the DIASTOLIC code —
        # so a systolic reading filed under that answer lands in the wrong
        # series entirely. A confident wrong code is worse than an honest miss,
        # so these resolve to nothing and the caller has to ask which
        # measurement was meant.
        #
        # Both spellings are checked, because underscore flattening in
        # `_candidate_keys` reaches the index under the spaced form: with only
        # the as-written check, `blood_pressure` skipped the block, flattened to
        # `blood pressure`, hit those 183 rows and came back 8462-4 —
        # re-creating the very bug this guard was written for, through the back
        # door.
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
            stem_analyte = self._analyte.get(stem_hit.loinc, "")
            inside_analyte = self._analyte.get(inside_hit.loinc, "")
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

    def _component_index(self) -> dict[str, list[tuple]]:
        """component -> its rows, built on first use and then cached.

        Lazy because `resolve()` never needs it: a caller with no value and no
        unit has nothing to pick a variant with, and paying for this index on
        every import would tax the offline path that is the whole point of the
        library.
        """
        if self._by_component is None:
            from .indicator.fhir.embeddings.bundle import read_member

            skip: set[str] = set()
            raw = read_member("loinc_skip.txt", bundle_path=_BUNDLE)
            if raw is not None:
                skip = {
                    line.strip()
                    for line in raw.decode("utf-8").splitlines()
                    if line.strip() and not line.startswith("#")
                }
            index: dict[str, list[tuple]] = {}
            for row in self._axis_rows:
                code, component = row[0], row[1]
                if component and code not in skip:
                    index.setdefault(component, []).append(row)
            self._by_component = index
        return self._by_component

    def _names_by_loinc(self) -> dict[str, str]:
        """code -> LONG_COMMON_NAME, for reporting a switched variant."""
        if not hasattr(self, "_name_by_code"):
            self._name_by_code = {r[0]: r[6] for r in self._axis_rows}
        return self._name_by_code

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
        from .indicator.fhir.units import normalize_unit, unit_families
        from .indicator.value_scale import scales_for_value

        ucum = normalize_unit(unit) if unit else None
        families = frozenset(unit_families(ucum) or ()) if ucum else frozenset()
        scales = scales_for_value(value) or frozenset()
        if not families and not scales:
            return loinc

        current = next((r for r in self._axis_rows if r[0] == loinc), None)
        if current is None:
            return loinc
        prop_ok = (not families) or current[2] in families
        scale_ok = (not scales) or current[3] in scales
        if prop_ok and scale_ok:
            return loinc

        siblings = [
            r for r in self._component_index().get(current[1], [])
            if ((not families) or r[2] in families) and ((not scales) or r[3] in scales)
        ]
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
            rows = self._alias.get(key)
            if rows is None or not len(rows):
                continue
            row = self._pick(rows)
            name = self._names[row]
            return Resolution(
                term=term,
                canonical=name,
                loinc=self._loinc_by_name.get(self._normalize(name), ""),
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
        canonical=resolver._names_by_loinc().get(switched, hit.canonical),
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

    # `method="refused"` is a decision, not a gap. `blood pressure` is a panel;
    # `血糖(HbA1c)` names two different tests. Neither has a right answer, and
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
    for i, candidates in zip(missed, ranked):
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

_MIME = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".txt": "text/plain",
    ".csv": "text/csv",
}


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

    One LLM call via :func:`mirobody.utils.llm.unified_file_extract` (provider
    auto-selected from whichever API key is present). Raises RuntimeError with
    a plain message when no provider key is configured.
    """
    # The provider auto-detection reads keys through the config system (which
    # also loads .env); standalone callers — the CLI, a bare library user —
    # haven't initialized it. Init is idempotent and works with zero yaml files.
    from .utils import Config
    from .utils.llm import unified_file_extract

    await Config.init()

    ext = os.path.splitext(path)[1].lower()
    if ext in (".txt", ".csv", ".md", ".tsv"):
        # Plain-text documents skip the vision path entirely: read the text and
        # ask for the extraction directly (unified_file_extract is built for
        # binary/vision inputs and returns nothing useful for text uploads).
        with open(path, encoding="utf-8", errors="replace") as f:
            document = f.read()
        return await parse_text(document, resolve_names=resolve_names)

    content_type = _MIME.get(ext, "application/pdf")
    raw = await unified_file_extract(path, _EXTRACT_PROMPT, content_type=content_type, json_mode=True)
    return _readings_from_json(raw, resolve_names=resolve_names)


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

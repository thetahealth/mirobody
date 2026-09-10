"""Semantic recall over a LOINC embedding matrix — the opt-in second tier.

The lexical resolver in :mod:`mirobody.engine` answers from shipped vocabularies
and answers *nothing* when it does not know a term. That is the right default,
and it is also a ceiling: a term nobody has written an alias for stays
unresolved no matter how obvious it is. This module is the other half — cosine
recall against an embedding of the LOINC corpus.

**Cosine alone is not usable, and a score threshold cannot rescue it.**
Benchmarked over 32 real indicator terms and 8 non-indicators, on
Qwen3-Embedding-8B and -0.6B, each with and without the query-instruction
wrapper:

    tier                     right + defensible     wrong     abstained
    lexical (this repo)          29/32  (91%)         0           3
    8B  bare / instruct          16 / 18              16 / 14     0
    0.6B bare / instruct         12 / 11              20 / 21     0

Junk scored 0.62-0.74 while genuine terms went down to 0.72, so the ranges
overlap in every configuration. The collisions are not noise either: LOINC ships
an enormous PHQ / FACIT / NIH-Toolbox / PhenX corpus of casual natural-language
items, so `the quick brown fox` matching "Freckles" at 0.741 is the index
working exactly as designed.

**What rescues it is not a better score — it is the two gates the pipeline
already has.** A reading does not arrive here as a bare string. It arrives
having passed an LLM extraction pass whose prompt returns an EMPTY result for
non-health content outright, and it arrives as
``{indicator, value, unit, reference_range}``. So the useful question is never
"is this text health-related" (something upstream already decided) but "is this
candidate code a thing that could have produced THIS value in THIS unit" — and
LOINC answers that itself, on axes that ship in `loinc_axis.csv`:

* a numeric value requires ``SCALE_TYP == "Qn"``. That alone rejects
  *History of Hyperuricemia* (Ord/Hx), *HbA1c device Vendor model code*
  (Nom/ID), a pharmacogenomic gene test, a Hep-C antigen screen, *Freckles*
  and *"Can pronounce quixotic [NIH Toolbox]"*.
* a parseable unit requires the candidate's ``PROPERTY`` to be in that unit's
  family (:func:`~.fhir.units.unit_families`). That rejects
  *Cholesterol in LDL **goal*** for a mmol/L reading (MCnc vs SCnc),
  *PT panel* (PROPERTY ``-``, panels have no single value), and
  *Fenetylline [Mass] of Dose* for a ng/mL reading (Mass vs MCnc).

Measured against the ten wrong answers the benchmark produced, the two gates
reject **nine**. The survivor is `血清铁` → *Ferritin [Mass/volume] in Blood*:
unit-compatible, quantitative, and simply the wrong analyte. That is the honest
residue — a gate built from units and scales cannot know which iron-panel member
was meant, and pretending otherwise is how confident wrong answers get shipped.

So: gates when the caller has a value and unit, and `method="semantic"` always,
because that residue is real. `resolve()` stays lexical, and a caller using a
code as an IDENTITY — a grouping key, a merge decision, a FHIR mirror — should
still prefer ``"lexical"``: it is deterministic and reproducible, while a
semantic hit carries exactly the wrong-analyte residue described above.

**Why this is not** :mod:`mirobody.indicator.fhir.resolve.pipeline`. That is the
full v2 algorithm — embedding recall plus family rerank, SYSTEM centroids, CLASS
routing, multi-span framing — and it needs a full 677k-row multi-vocabulary
corpus matrix plus an axis-centroid bundle. Indicator resolution ranks LOINC and
nothing else, so this tier needs one 96,244-row matrix and numpy. When the
larger artifacts are present the v2 pipeline remains the better answer; this is
the version that fits in a pip install.

The matrix is built by ``scripts/build_loinc_embeddings.py``. Corpus and query
MUST come from the same embedding model — a mismatched pair does not fail, it
silently returns nonsense (a matrix from a different Qwen3-Embedding serving
config answered ``空腹血糖`` with *"Widespread delusions [DI-PAD]"*).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache

logger = logging.getLogger(__name__)

#: Where the matrix is looked for, in order: an explicit argument, then this
#: environment variable / config key, then next to the shipped bundles — so a
#: future wheel that carries a small quantized matrix needs no configuration.
ENV_VAR = "MIROBODY_SEMANTIC_INDEX"
_DEFAULT_BASENAME = "fhir_loinc_embeddings.npy"


@dataclass(frozen=True)
class Gate:
    """What a reading tells us about which codes could have produced it.

    Three independent clauses, each of which may be empty:

    * `scales` — from the value's KIND. A number admits ``Qn``/``SemiQn``/
      ``OrdQn``; `阴性` admits ``Nom``; `++` admits ``Ord``/``OrdQn``/
      ``SemiQn``. Non-numeric is a constraint, not the lack of one.
    * `unit_families` — from the unit's dimension, against ``PROPERTY``.
    * `term_tokens` — the query's own words, against the candidate's
      ``COMPONENT``.
    """

    scales: frozenset[str] | None = None
    unit_families: frozenset[str] | None = None
    term_tokens: frozenset[str] | None = None

    def __bool__(self) -> bool:
        return bool(self.scales or self.unit_families or self.term_tokens)


@dataclass(frozen=True)
class Candidate:
    """One semantic match. `score` is raw cosine — see the module docstring for
    why it is not a confidence and must not be thresholded into one."""

    loinc: str
    canonical: str
    score: float


def default_index_path() -> str | None:
    """The matrix path from the environment/config, or the shipped location."""
    explicit = os.environ.get(ENV_VAR)
    if not explicit:
        try:
            from ..utils.config import safe_read_cfg

            explicit = safe_read_cfg(ENV_VAR, "")
        except Exception:
            explicit = ""
    if explicit:
        return explicit
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "res", _DEFAULT_BASENAME)
    here = os.path.normpath(here)
    return here if os.path.isfile(here) else None


class SemanticIndex:
    """A LOINC embedding matrix, loaded once and searched with a dot product.

    Vectors are stored L2-normalized, so cosine IS the dot product and the
    search is one ``(N, D) @ (D,)`` — no index structure, no extra dependency.
    At 77,482 rows that is single-digit milliseconds.
    """

    def __init__(self, path: str):
        import numpy as np

        from mirobody._bundle import (
            AXIS_CODE,
            BUNDLE_PATH,
            AXIS_COMPONENT,
            AXIS_LCN,
            AXIS_PROPERTY,
            AXIS_SCALE,
            load_axis,
            read_code_list,
        )

        from .fhir.common import fhir_id_to_code

        self._check_identity(path)
        raw = np.load(path, mmap_mode="r")
        self._emb = np.asarray(raw["emb"], dtype=np.float32)
        # Stored normalized, but a truncated (MRL) or re-quantized matrix may
        # not be exactly unit-length any more. Normalizing here costs one pass
        # at load and makes the dot product a cosine whatever produced the file.
        norms = np.linalg.norm(self._emb, axis=1, keepdims=True)
        self._emb /= np.maximum(norms, 1e-9)
        self.dim = int(self._emb.shape[1])

        # The axis table, through the same reader the lexical resolver uses.
        # This used to parse `loinc_axis.csv` into four dicts of its own, and
        # tolerate the file being absent by leaving them EMPTY — which after
        # 1.3.0 stopped shipping that CSV would have quietly disabled every
        # gate below rather than failing. `load_axis` raises instead.
        #
        # COMPONENT arrives folded (NFKC + casefold). That is not a
        # compromise: the only thing done with it is `word_tokens`, which
        # lowercases ASCII itself, and the two agree on all 97,314 rows —
        # asserted in tests/test_resolver_tables.py.
        axis, _order_code, _order_name = load_axis(bundle_path=BUNDLE_PATH)
        names: dict[str, str] = {}
        scales: dict[str, str] = {}
        properties: dict[str, str] = {}
        components: dict[str, str] = {}
        for i in range(len(axis)):
            code = axis.field(i, AXIS_CODE)
            names[code] = axis.field(i, AXIS_LCN)
            scales[code] = axis.field(i, AXIS_SCALE)
            properties[code] = axis.field(i, AXIS_PROPERTY)
            components[code] = axis.field(i, AXIS_COMPONENT)

        # `loinc_skip.txt` ships in the bundle and lists the codes the lexical
        # resolver already refuses to answer with: non-clinical CLASS (SURVEY,
        # PHENX, DOC), plus DEPRECATED and DISCOURAGED status. Applying it here
        # is not tidiness — it removes the single most damaging failure this
        # tier has. LOINC carries an enormous PHQ / FACIT / NIH-Toolbox / PhenX
        # corpus of casual natural-language items, and cosine finds them
        # delightful:
        #
        #   "我今天很开心"          -> 96933-7  "...I was happy yesterday"
        #   "asdfqwerzxcv"          -> 45585-7  "Expresses sadness/anger..."
        #   "请帮我预约挂号"        -> 96746-3  "Appointment type"
        #   "total cholesterol"     -> 105515-1 PhenX SELF-REPORT survey item
        #
        # That last one is the point: all four model/prompt configurations
        # measured answered a plain, correct, English lab term with a survey
        # question, because the survey item's NAME matches better than the
        # assay's does. Every one of those four codes is in the skip list.
        #
        # It does NOT make the tier able to abstain — `the quick brown fox`
        # still reaches "Freckles" — see the module docstring.
        skip: set[str] = set(
            read_code_list("loinc_skip.txt", bundle_path=BUNDLE_PATH)
        )

        keep: list[int] = []
        self._codes: list[str] = []
        self._names: list[str] = []
        row_scales: list[str] = []
        row_props: list[str] = []
        for row, packed in enumerate(raw["fhir_id"]):
            try:
                _, code = fhir_id_to_code(int(packed))
            except Exception:
                code = ""
            if code and code in skip:
                continue
            keep.append(row)
            self._codes.append(code)
            self._names.append(names.get(code, ""))
            row_scales.append(scales.get(code, ""))
            row_props.append(properties.get(code, ""))

        dropped = len(raw) - len(keep)
        if dropped:
            # Dropped rather than masked at query time: a smaller matrix is a
            # faster dot product and there is no case where a caller wants the
            # excluded rows back.
            self._emb = self._emb[keep]

        # The gate axes, as int arrays so a gate is one vectorized comparison:
        # membership over ~77k strings is an order slower than over ints.
        #
        # SCALE_TYP is kept whole rather than reduced to "is it Qn". A
        # non-numeric value is not the absence of a constraint, it is the
        # OPPOSITE one — a 阴性 reading must not be answered with a
        # mass-concentration code any more than 5.6 mmol/L may be answered with
        # a presence code — and 38,687 of the corpus's rows are non-Qn.
        self._scale_ids: dict[str, int] = {}
        scale_codes = np.empty(len(row_scales), dtype=np.int32)
        for i, sc in enumerate(row_scales):
            scale_codes[i] = self._scale_ids.setdefault(sc, len(self._scale_ids))
        self._scale_of_row = scale_codes

        self._property_ids: dict[str, int] = {}
        prop_codes = np.empty(len(row_props), dtype=np.int32)
        for i, prop in enumerate(row_props):
            prop_codes[i] = self._property_ids.setdefault(prop, len(self._property_ids))
        self._property_of_row = prop_codes

        # Analyte tokens per row, for the overlap gate. Built from COMPONENT
        # (the analyte axis) rather than the display name, which carries
        # specimen and method words that would match anything.
        from mirobody.lexical import word_tokens

        self._component_tokens: list[frozenset[str]] = [
            frozenset(word_tokens(components.get(c, ""))) for c in self._codes
        ]

        logger.info(
            "SemanticIndex ready: %d rows (%d non-clinical dropped), %d dims, "
            "%d quantitative, from %s",
            len(self._codes), dropped, self.dim,
            int((scale_codes == self._scale_ids.get("Qn", -1)).sum()), path,
        )

    def _admissible(self, gate: Gate):
        """Row mask for a reading: what could have produced this value+unit.

        None means no constraint at all — nothing was known about the reading.
        Each clause narrows independently, and a clause that would empty the
        mask is dropped rather than allowed to starve the search (see
        `search_vectors`).
        """
        import numpy as np

        mask = None
        if gate.scales:
            allowed = [self._scale_ids[s] for s in gate.scales if s in self._scale_ids]
            if allowed:
                mask = np.isin(self._scale_of_row, allowed)
        if gate.unit_families:
            allowed = [
                self._property_ids[f] for f in gate.unit_families
                if f in self._property_ids
            ]
            if allowed:
                by_unit = np.isin(self._property_of_row, allowed)
                mask = by_unit if mask is None else (mask & by_unit)
        if gate.term_tokens:
            # The ANALYTE gate. The scale and unit gates constrain the axes —
            # what kind of measurement, in what dimension — and cannot tell one
            # iron-panel member from another, which is where their surviving
            # errors live. This asks a different question: does the candidate's
            # COMPONENT share a word with the query at all?
            #
            # Deliberately weak. One shared token, not a score, because the
            # query may be Chinese and the COMPONENT is always English, and a
            # ratio would then be measuring how much English is in the query.
            # It exists to reject the candidates that share NOTHING — the
            # `the quick brown fox` -> "Freckles" shape — not to rank.
            overlap = np.fromiter(
                (bool(gate.term_tokens & toks) for toks in self._component_tokens),
                dtype=bool, count=len(self._component_tokens),
            )
            mask = overlap if mask is None else (mask & overlap)
        return mask

    def __len__(self) -> int:
        return len(self._codes)

    @staticmethod
    def _check_identity(path: str) -> None:
        """Refuse a matrix built by a different (provider, model) pair.

        The failure this prevents is silent: a mismatched matrix does not
        error, it ranks confidently in the wrong space — a matrix from a
        different Qwen3-Embedding serving config once answered ``空腹血糖``
        with *"Widespread delusions"* at a plausible score. The build script
        stamps ``<matrix>.meta.json``; queries here are embedded with the
        configured ``UTILS_EMBEDDING_MODEL``, so the two identities must agree.

        A matrix WITHOUT a sidecar (built before stamping existed) loads with
        a warning: refusing it would brick every existing download, and the
        pre-stamp default build was the same openrouter/8B pair the runtime
        now defaults to.
        """
        import json

        meta_path = f"{path}.meta.json"
        if not os.path.isfile(meta_path):
            logger.warning(
                "semantic matrix %s has no .meta.json identity stamp; cannot "
                "verify it matches UTILS_EMBEDDING_MODEL. Rebuild with "
                "scripts/build_loinc_embeddings.py to silence this.", path,
            )
            return

        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)

        from ..utils.embedding import embedding_model_id, resolve_embedding_provider

        provider = resolve_embedding_provider()
        model = embedding_model_id(provider)
        if meta.get("provider") != provider or meta.get("model") != model:
            raise ValueError(
                f"semantic matrix {path} was built by "
                f"{meta.get('provider')}/{meta.get('model')} but queries are "
                f"embedded by {provider}/{model} (UTILS_EMBEDDING_MODEL). A "
                "cross-model cosine is confident nonsense, not a looser "
                "match — rebuild the matrix with "
                f"scripts/build_loinc_embeddings.py --provider {provider}, "
                "or set UTILS_EMBEDDING_MODEL to match the matrix."
            )

    def search_vectors(
        self,
        query_vectors,
        top_k: int = 1,
        gates: list[Gate] | None = None,
    ) -> list[list[Candidate]]:
        """Rank the corpus against already-embedded queries.

        A gate restricts which rows are eligible BEFORE the top-k, so the answer
        is the best *admissible* candidate rather than the best candidate with a
        veto stapled on — that distinction is the whole point, since the best
        overall match for `uric acid` was "History of Hyperuricemia".

        A gate that admits NOTHING is dropped for that query rather than
        returning an empty result: it means the constraints and the corpus
        disagree, and the honest fallback is the ungated ranking, which the
        caller can still see is a semantic answer and treat accordingly.
        """
        import numpy as np

        Q = np.asarray(query_vectors, dtype=np.float32)
        if Q.ndim == 1:
            Q = Q[None, :]
        # A query embedded at a different width than the matrix is a
        # configuration error, not something to paper over with padding: the
        # dimensions of an MRL prefix only line up if both sides truncated the
        # same way.
        if Q.shape[1] != self.dim:
            raise ValueError(
                f"query vectors are {Q.shape[1]}-dim but the index is {self.dim}-dim; "
                "corpus and query must come from the same model and truncation"
            )
        Q /= np.maximum(np.linalg.norm(Q, axis=1, keepdims=True), 1e-9)

        sims = Q @ self._emb.T
        k = max(1, min(top_k, sims.shape[1]))
        out: list[list[Candidate]] = []
        for qi, row in enumerate(sims):
            if gates and qi < len(gates) and gates[qi]:
                mask = self._admissible(gates[qi])
                if mask is not None and mask.any():
                    # Copy: `sims` rows are views and a later query must not
                    # inherit this one's exclusions.
                    row = np.where(mask, row, -np.inf)
            top = np.argpartition(-row, k - 1)[:k]
            top = top[np.argsort(-row[top])]
            out.append([
                Candidate(loinc=self._codes[i], canonical=self._names[i], score=float(row[i]))
                for i in top
            ])
        return out

    @staticmethod
    def gate_for(
        value: str | None,
        unit: str | None,
        term: str | None = None,
    ) -> Gate:
        """A reading as written -> the constraints it implies.

        Every clause degrades to "says nothing" rather than to a wrong
        constraint: an unclassifiable value, a unit the UCUM table does not
        know, and a query with no word tokens each simply contribute nothing.
        """
        from mirobody.units import normalize_unit, unit_families
        from mirobody.lexical import word_tokens
        from mirobody.value_scale import scales_for_value

        ucum = normalize_unit(unit) if unit else None
        families = unit_families(ucum) if ucum else None
        return Gate(
            scales=scales_for_value(value),
            unit_families=frozenset(families) if families else None,
            term_tokens=frozenset(word_tokens(term)) if term else None,
        )

    async def search(
        self,
        terms: list[str],
        top_k: int = 1,
        gates: list[Gate] | None = None,
    ) -> list[list[Candidate]]:
        """Embed `terms` with the configured provider, then rank.

        The import is local because :mod:`mirobody.utils.embedding` pulls in
        aiohttp and the config system, and the lexical engine's floor is "the
        wheel plus numpy". Nothing here should raise that floor for callers who
        never ask for the semantic tier.
        """
        from ..utils.embedding import text_embedding

        vectors = await text_embedding(terms, cache=True)
        results: list[list[Candidate]] = []
        embedded = [(i, v) for i, v in enumerate(vectors) if v]
        if not embedded:
            return [[] for _ in terms]

        ranked = self.search_vectors(
            [v for _, v in embedded],
            top_k=top_k,
            gates=[gates[i] for i, _ in embedded] if gates else None,
        )
        by_index = dict(zip((i for i, _ in embedded), ranked, strict=False))
        for i in range(len(terms)):
            results.append(by_index.get(i, []))
        return results


@lru_cache(maxsize=4)
def get_index(path: str | None = None) -> SemanticIndex | None:
    """Load (and cache) the matrix, or None when there is none to load.

    None is a normal outcome, not an error: the matrix is an optional download,
    it is absent on every `pip install`, and every caller has a lexical answer
    to fall back on.

    "Absent" and "you pointed me at one and it is not there" are NOT the same
    outcome, though, and both used to return None in silence. A typo in
    ``MIROBODY_SEMANTIC_INDEX`` looked exactly like not configuring it — the
    caller asked for the semantic tier, got lexical-only answers, and nothing
    said why.
    """
    requested = path or default_index_path()
    if requested and not os.path.isfile(requested):
        logger.warning(
            "semantic index %s does not exist; answering from the lexical tier only. "
            "Set %s to the matrix, or drop it at res/%s.",
            requested, ENV_VAR, _DEFAULT_BASENAME,
        )
        return None
    path = requested
    if not path:
        return None
    try:
        return SemanticIndex(path)
    except Exception:
        logger.exception("semantic index at %s could not be loaded; continuing without it", path)
        return None

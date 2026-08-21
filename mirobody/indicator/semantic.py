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
still prefer ``"lexical"``. The hosted platform reached the same rule
independently and enforces it as ``TRUSTED_METHODS = {"lexical"}``.

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

import csv
import io
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

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

        from .fhir.common import fhir_id_to_code
        from .fhir.embeddings.bundle import read_member

        raw = np.load(path, mmap_mode="r")
        self._emb = np.asarray(raw["emb"], dtype=np.float32)
        # Stored normalized, but a truncated (MRL) or re-quantized matrix may
        # not be exactly unit-length any more. Normalizing here costs one pass
        # at load and makes the dot product a cosine whatever produced the file.
        norms = np.linalg.norm(self._emb, axis=1, keepdims=True)
        self._emb /= np.maximum(norms, 1e-9)
        self.dim = int(self._emb.shape[1])

        bundle = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "res", "fhir_loinc_bundle.tar.gz"
        )
        axis_raw = read_member("loinc_axis.csv", bundle_path=os.path.normpath(bundle))
        names: dict[str, str] = {}
        scales: dict[str, str] = {}
        properties: dict[str, str] = {}
        components: dict[str, str] = {}
        if axis_raw is not None:
            for row in csv.DictReader(io.StringIO(axis_raw.decode("utf-8"))):
                code = row["LOINC_NUM"]
                names[code] = row["LONG_COMMON_NAME"]
                scales[code] = row.get("SCALE_TYP") or ""
                properties[code] = row.get("PROPERTY") or ""
                components[code] = row.get("COMPONENT") or ""

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
        skip: set[str] = set()
        skip_raw = read_member("loinc_skip.txt", bundle_path=os.path.normpath(bundle))
        if skip_raw is not None:
            skip = {
                line.strip()
                for line in skip_raw.decode("utf-8").splitlines()
                if line.strip() and not line.startswith("#")
            }

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
        from .lexical import word_tokens

        self._component_tokens: list[frozenset[str]] = [
            frozenset(word_tokens(components.get(c, ""))) for c in self._codes
        ]

        logger.info(
            "SemanticIndex ready: %d rows (%d non-clinical dropped), %d dims, "
            "%d quantitative, from %s",
            len(self._codes), dropped, self.dim,
            int((scale_codes == self._scale_ids.get("Qn", -1)).sum()), path,
        )

    def _admissible(self, gate: "Gate"):
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

    def search_vectors(
        self,
        query_vectors,
        top_k: int = 1,
        gates: list["Gate"] | None = None,
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
    ) -> "Gate":
        """A reading as written -> the constraints it implies.

        Every clause degrades to "says nothing" rather than to a wrong
        constraint: an unclassifiable value, a unit the UCUM table does not
        know, and a query with no word tokens each simply contribute nothing.
        """
        from .fhir.units import normalize_unit, unit_families
        from .lexical import word_tokens
        from .value_scale import scales_for_value

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
        gates: list["Gate"] | None = None,
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
        by_index = dict(zip((i for i, _ in embedded), ranked))
        for i in range(len(terms)):
            results.append(by_index.get(i, []))
        return results


@lru_cache(maxsize=4)
def get_index(path: str | None = None) -> Optional[SemanticIndex]:
    """Load (and cache) the matrix, or None when there is none to load.

    None is a normal outcome, not an error: the matrix is an optional download
    and every caller has a lexical answer to fall back on.
    """
    path = path or default_index_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        return SemanticIndex(path)
    except Exception:
        logger.exception("semantic index at %s could not be loaded; continuing without it", path)
        return None

"""Semantic recall over a LOINC embedding matrix — the opt-in second tier.

The lexical resolver in :mod:`mirobody.engine` answers from shipped vocabularies
and answers *nothing* when it does not know a term. That is the right default,
and it is also a ceiling: a term nobody has written an alias for stays
unresolved no matter how obvious it is. This module is the other half — cosine
recall against an embedding of the LOINC corpus.

**It is off unless asked for, and its answers are marked, and that is not
timidity — it is measured.** On a 96,244-row LOINC matrix, real indicator names
scored 0.56–0.80 against their best match while deliberate nonsense
(``绝对不存在的指标名xyzzy``) scored **0.78**. The ranges overlap, so there is no
confidence threshold that admits real terms and rejects junk: this tier
**cannot abstain**. Left on by default it would answer every typo and every
stray sentence with a plausible-looking code. `resolve()` therefore stays
lexical, every result carries ``method``, and a caller that uses a code as an
IDENTITY — a grouping key, a merge decision, a FHIR mirror — must accept only
``"lexical"``. The hosted platform reached the same conclusion independently and
enforces it as ``TRUSTED_METHODS = {"lexical"}``.

What it is good for is the case it was built for: surfacing a candidate for a
human or a model to confirm, and catching concepts the alias tables miss.

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
    At 96,244 rows that is single-digit milliseconds.
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
        if axis_raw is not None:
            for row in csv.DictReader(io.StringIO(axis_raw.decode("utf-8"))):
                names[row["LOINC_NUM"]] = row["LONG_COMMON_NAME"]

        self._codes: list[str] = []
        self._names: list[str] = []
        for packed in raw["fhir_id"]:
            try:
                _, code = fhir_id_to_code(int(packed))
            except Exception:
                code = ""
            self._codes.append(code)
            self._names.append(names.get(code, ""))

        logger.info("SemanticIndex ready: %d rows, %d dims, from %s", len(self._codes), self.dim, path)

    def __len__(self) -> int:
        return len(self._codes)

    def search_vectors(self, query_vectors, top_k: int = 1) -> list[list[Candidate]]:
        """Rank the corpus against already-embedded queries."""
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
        for row in sims:
            top = np.argpartition(-row, k - 1)[:k]
            top = top[np.argsort(-row[top])]
            out.append([
                Candidate(loinc=self._codes[i], canonical=self._names[i], score=float(row[i]))
                for i in top
            ])
        return out

    async def search(self, terms: list[str], top_k: int = 1) -> list[list[Candidate]]:
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

        ranked = self.search_vectors([v for _, v in embedded], top_k=top_k)
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

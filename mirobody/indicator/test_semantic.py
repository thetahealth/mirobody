"""The semantic tier: it stays off, it marks itself, and it never lies about width.

No matrix and no API key needed — a tiny synthetic index is built on the fly.
What is pinned here is the CONTRACT around the tier, because the tier itself
cannot abstain (nonsense scores 0.78, real terms go down to 0.56) and the whole
safety story is "it is off by default and its answers are labelled".
"""

from __future__ import annotations

import numpy as np
import pytest

from mirobody.engine import Resolution, resolve
from mirobody.indicator.fhir.common import SYSTEM_TO_CODE, code_to_fhir_id
from mirobody.indicator.semantic import Candidate, SemanticIndex, get_index


@pytest.fixture
def tiny_index(tmp_path):
    """Three orthogonal unit vectors, one per LOINC code — so the expected
    winner of a query is arithmetic rather than a judgement call."""
    codes = ["1558-6", "718-7", "2093-3"]
    dim = 8
    emb = np.zeros((len(codes), dim), dtype=np.float16)
    for i in range(len(codes)):
        emb[i, i] = 1.0
    arr = np.zeros(len(codes), dtype=[("fhir_id", "<i8"), ("emb", "<f2", (dim,))])
    loinc = SYSTEM_TO_CODE["LOINC"]
    for i, code in enumerate(codes):
        arr["fhir_id"][i] = code_to_fhir_id(loinc, code)
        arr["emb"][i] = emb[i]
    path = tmp_path / "matrix.npy"
    np.save(path, arr)
    return SemanticIndex(str(path))


def test_index_decodes_codes_and_names(tiny_index):
    assert len(tiny_index) == 3
    assert tiny_index.dim == 8
    hits = tiny_index.search_vectors([[1, 0, 0, 0, 0, 0, 0, 0]], top_k=1)
    assert hits[0][0].loinc == "1558-6"
    # The display name comes from the shipped axis table, not from the matrix,
    # so a matrix stays a matrix and never carries a second copy of the corpus.
    assert "glucose" in hits[0][0].canonical.lower()


def test_ranking_is_by_cosine(tiny_index):
    q = np.array([0.6, 0.8, 0, 0, 0, 0, 0, 0], dtype=np.float32)
    hits = tiny_index.search_vectors([q], top_k=3)[0]
    assert [h.loinc for h in hits] == ["718-7", "1558-6", "2093-3"]
    assert hits[0].score == pytest.approx(0.8, abs=1e-3)
    assert hits[1].score == pytest.approx(0.6, abs=1e-3)


def test_a_width_mismatch_raises_instead_of_padding(tiny_index):
    """Corpus and query must be the same model AND the same MRL truncation.

    Padding or trimming to fit would turn a configuration error into silently
    wrong answers, which is the failure mode this whole tier is dangerous for.
    """
    with pytest.raises(ValueError, match="same model"):
        tiny_index.search_vectors([[1.0] * 16])


def test_unnormalized_vectors_are_normalized_on_load(tmp_path):
    """int8 requantization and MRL truncation both break unit length."""
    arr = np.zeros(1, dtype=[("fhir_id", "<i8"), ("emb", "<f2", (4,))])
    arr["fhir_id"][0] = code_to_fhir_id(SYSTEM_TO_CODE["LOINC"], "718-7")
    arr["emb"][0] = [3.0, 4.0, 0.0, 0.0]          # length 5, not 1
    path = tmp_path / "m.npy"
    np.save(path, arr)
    index = SemanticIndex(str(path))
    hit = index.search_vectors([[3.0, 4.0, 0.0, 0.0]])[0][0]
    assert hit.score == pytest.approx(1.0, abs=1e-3)   # cosine, not a dot of lengths


def test_missing_matrix_is_a_normal_outcome(tmp_path):
    """No matrix installed is the default state of a pip install, not an error."""
    get_index.cache_clear()
    assert get_index(str(tmp_path / "nope.npy")) is None


def test_resolve_never_returns_a_semantic_answer():
    """`resolve()` is the documented front door and stays lexical-only.

    A semantic code must never reach a caller who did not ask for one — it is
    not identity-grade, and `resolve()` is exactly where identities come from.
    """
    for term in ("空腹血糖", "HGB", "绝对不存在的指标名xyzzy", "the quick brown fox"):
        assert resolve(term).method in ("lexical", "")


def test_every_lexical_hit_is_labelled():
    hit = resolve("空腹血糖")
    assert hit.resolved and hit.method == "lexical"
    miss = resolve("绝对不存在的指标名xyzzy")
    assert not miss.resolved and miss.method == ""


def test_a_refusal_is_distinguishable_from_a_miss():
    """Both are unresolved; they want opposite treatment downstream.

    A gap is worth a second opinion. A refusal IS the answer — `blood pressure`
    is a panel and `血糖(HbA1c)` names two tests — and must not be overturned
    by one.
    """
    for term in ("blood pressure", "BP", "lipid panel", "血脂", "血糖(HbA1c)", "胆固醇(HDL-C)"):
        r = resolve(term)
        assert not r.resolved and r.method == "refused", term
    for term in ("绝对不存在的指标名xyzzy", "some unheard-of assay name"):
        r = resolve(term)
        assert not r.resolved and r.method == "", term


def test_score_is_zero_for_lexical_answers():
    """`score` is a cosine and only a semantic answer has one; a lexical hit
    must not present 0.0 as low confidence."""
    assert resolve("HGB").score == 0.0
    assert Resolution(term="x").score == 0.0


@pytest.mark.asyncio
async def test_fallback_only_touches_the_misses(monkeypatch):
    """Terms the lexical layer resolved must not be re-answered semantically,
    however confident the matrix is — a lexical hit outranks recall."""
    from mirobody import engine

    called: list[list[str]] = []

    class FakeIndex:
        async def search(self, terms, top_k=1):
            called.append(list(terms))
            return [[Candidate(loinc="9999-9", canonical="Something Else", score=0.99)]
                    for _ in terms]

    monkeypatch.setattr("mirobody.indicator.semantic.get_index", lambda p=None: FakeIndex())

    out = await engine.resolve_with_semantic_fallback(["空腹血糖", "绝对不存在的指标名xyzzy"])
    assert called == [["绝对不存在的指标名xyzzy"]]          # only the miss was sent
    assert out[0].loinc == "1558-6" and out[0].method == "lexical"
    assert out[1].loinc == "9999-9" and out[1].method == "semantic"
    assert out[1].score == pytest.approx(0.99)


@pytest.mark.asyncio
async def test_fallback_is_a_no_op_without_a_matrix(monkeypatch):
    from mirobody import engine

    monkeypatch.setattr("mirobody.indicator.semantic.get_index", lambda p=None: None)
    out = await engine.resolve_with_semantic_fallback(["绝对不存在的指标名xyzzy"])
    assert out[0].resolved is False and out[0].method == ""


@pytest.mark.asyncio
async def test_min_score_is_opt_in_and_off_by_default(monkeypatch):
    """A floor is offered but never applied by default: on real data the junk
    and the genuine ranges overlap, so presenting one as a correctness
    threshold would be a lie."""
    from mirobody import engine

    class FakeIndex:
        async def search(self, terms, top_k=1):
            return [[Candidate(loinc="9999-9", canonical="X", score=0.60)] for _ in terms]

    monkeypatch.setattr("mirobody.indicator.semantic.get_index", lambda p=None: FakeIndex())

    kept = await engine.resolve_with_semantic_fallback(["zzz nonsense"])
    assert kept[0].method == "semantic"

    cut = await engine.resolve_with_semantic_fallback(["zzz nonsense"], min_score=0.7)
    assert cut[0].method == "" and cut[0].resolved is False


@pytest.mark.asyncio
async def test_a_blocked_term_never_reaches_the_fallback(monkeypatch):
    """`!unresolved` is a decision, not a gap.

    `blood pressure` is a panel and `血糖(HbA1c)` is two tests in one string —
    neither has a right answer, so the second tier must not be allowed to
    overturn the first tier's refusal. Measured before this guard existed: the
    embedding tier answered all nine blocked terms in the eval set, all wrong.
    """
    from mirobody import engine

    sent: list[list[str]] = []

    class FakeIndex:
        async def search(self, terms, top_k=1, gates=None):
            sent.append(list(terms))
            return [[Candidate(loinc="9999-9", canonical="Anything", score=0.99)] for _ in terms]

    monkeypatch.setattr("mirobody.indicator.semantic.get_index", lambda p=None: FakeIndex())

    blocked = ["blood pressure", "血脂", "BP", "lipid panel", "血糖(HbA1c)"]
    out = await engine.resolve_with_semantic_fallback(blocked)
    assert sent == []                                  # nothing was even embedded
    # And they come back marked as refusals, not as gaps — the distinction is
    # what let the guard exist at all.
    assert all(r.method == "refused" and not r.resolved for r in out)


@pytest.mark.asyncio
async def test_an_ordinary_miss_still_reaches_the_fallback(monkeypatch):
    """The guard must not swallow genuine gaps — those are what it is for."""
    from mirobody import engine

    sent: list[list[str]] = []

    class FakeIndex:
        async def search(self, terms, top_k=1, gates=None):
            sent.append(list(terms))
            return [[Candidate(loinc="1234-5", canonical="Something", score=0.8)] for _ in terms]

    monkeypatch.setattr("mirobody.indicator.semantic.get_index", lambda p=None: FakeIndex())

    out = await engine.resolve_with_semantic_fallback(["some unheard-of assay name"])
    assert sent == [["some unheard-of assay name"]]
    assert out[0].method == "semantic" and out[0].loinc == "1234-5"

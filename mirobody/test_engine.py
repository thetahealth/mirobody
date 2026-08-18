"""Golden tests for the engine front door (offline resolution only).

No network, no database, no LLM — exactly the promise `mirobody resolve`
makes. Requires the LFS data bundles; skipped honestly on checkouts that
haven't run `git lfs pull` (e.g. a minimal CI).
"""

import os

import pytest

_BUNDLE = os.path.join(os.path.dirname(__file__), "res", "fhir_loinc_bundle.tar.gz")


def _bundle_available() -> bool:
    # An LFS pointer file is ~130 bytes; the real bundle is ~15 MB.
    try:
        return os.path.getsize(_BUNDLE) > 1_000_000
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _bundle_available(),
    reason="LFS data bundles not fetched (run `git lfs pull`)",
)


@pytest.fixture(scope="module")
def resolver():
    from mirobody.engine import get_resolver

    return get_resolver()


# The textbook codes for everyday chemistry-panel indicators. These pin the
# whole chain: alias index -> commonness prior -> axis table. If a data-bundle
# rebuild shifts one of these, that is a REAL regression to investigate, not a
# test to update casually.
GOLDEN = [
    ("谷丙转氨酶", "1742-6"),        # ALT
    ("谷草转氨酶", "1920-8"),        # AST
    ("肌酐", "2160-0"),              # creatinine, serum/plasma
    ("尿酸", "3084-1"),              # urate, serum/plasma
    ("甘油三酯", "2571-8"),          # triglyceride
    ("血小板计数", "26515-7"),       # platelets
    ("creatinine", "2160-0"),        # english direct hit
    ("ALT", "1742-6"),               # bare acronym
]


@pytest.mark.parametrize("term,loinc", GOLDEN)
def test_golden_loinc_codes(resolver, term, loinc):
    r = resolver.resolve(term)
    assert r.resolved, f"{term} failed to resolve at all"
    assert r.loinc == loinc, f"{term}: got {r.loinc} ({r.canonical})"


def test_resolution_carries_provenance(resolver):
    r = resolver.resolve("肌酐")
    assert r.term == "肌酐"
    assert r.canonical  # a human-readable canonical long name
    assert r.candidates >= 1  # ambiguity is surfaced, not hidden


def test_miss_is_honest(resolver):
    r = resolver.resolve("绝对不存在的指标名xyzzy")
    assert not r.resolved
    assert r.loinc == "" and r.canonical == ""


def test_blank_never_raises(resolver):
    for bad in ("", "   ", None):
        r = resolver.resolve(bad or "")
        assert not r.resolved


def test_module_level_resolve_shortcut():
    from mirobody.engine import resolve

    assert resolve("肌酐").loinc == "2160-0"

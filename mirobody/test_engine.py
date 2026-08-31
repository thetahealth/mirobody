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


# ── the CBC differential: a percentage and a count are two analytes ──────────
#
# Reported by a downstream consumer. LOINC models a differential percentage as
# COMPONENT `neutrophils/leukocytes` (PROPERTY NFr), not as a second property of
# `neutrophils` — so the two must resolve to two codes, and a report printing
# `62 %` must not land on the series a report printing `4.2 10*9/L` lands on.

#: The report's evidence table, verbatim: five white-cell lines, of which four
#: mapped to `X/Leukocytes` ratio codes and ONE did not. Pinned as its own case
#: because the bug was never "this code is wrong" — it was that neutrophils sat
#: on a different AXIS from its four siblings, and a consumer grouping by LOINC
#: would plot 4.2 10*9/L and 62 % on one series. The derived spellings below
#: are worth testing too, but they are not what was reported; these five are.
F3_PANEL = [
    ("嗜酸性粒细胞", "26450-7"),
    ("淋巴细胞", "26478-8"),
    ("单核细胞", "26485-3"),
    ("嗜碱性粒细胞", "30180-4"),
    ("中性粒细胞", "26511-6"),  # was 751-8 — the outlier
]


@pytest.mark.parametrize("term,loinc", F3_PANEL)
def test_the_five_differential_lines_share_one_axis(resolver, term, loinc):
    r = resolver.resolve(term)
    assert r.loinc == loinc, f"{term}: got {r.loinc} ({r.canonical})"


def test_no_differential_line_is_an_axis_outlier(resolver):
    """The property the report actually asserted: all five agree, whatever they
    answer. A future retarget that moved all five together would keep this
    green; one that moved four is the bug that was filed."""
    props = {t: resolver._axis_row(resolver._row_for_code(resolver.resolve(t).loinc))[2]
             for t, _ in F3_PANEL}
    assert len(set(props.values())) == 1, props


#: The same distinction written in words rather than inferred from the panel.
DIFFERENTIAL_RATIO = F3_PANEL + [
    ("中性粒细胞百分比", "26511-6"),
    ("中性粒细胞比例", "26511-6"),
    ("中性粒细胞%", "26511-6"),
    ("中性粒細胞百分比", "26511-6"),  # zh-Hant folds to the curated zh-Hans row
    ("嗜酸性粒细胞百分比", "26450-7"),
    ("淋巴细胞百分比", "26478-8"),
    ("单核细胞百分比", "26485-3"),
    ("嗜碱性粒细胞百分比", "30180-4"),
]

#: The METHOD-LESS count codes, deliberately. Pointing these rows at bare
#: `Neutrophils` lands on 751-8 `… by Automated count` while the unit-driven
#: crossing prefers 26499-4 — one measurement with two identities depending on
#: whether the report spelled out 绝对值 or printed a bare name beside 10*9/L.
#: That is the same split these rows exist to prevent, so both routes end here.
DIFFERENTIAL_COUNT = [
    ("中性粒细胞绝对值", "26499-4"),
    ("嗜酸性粒细胞绝对值", "26449-9"),
    ("淋巴细胞绝对值", "26474-7"),
    ("单核细胞绝对值", "26484-6"),
    ("嗜碱性粒细胞绝对值", "26444-0"),
]

#: Explicit pairs. This used to be `zip(RATIO[:1] + RATIO[5:], COUNT)`, which
#: happened to line up and would have gone on passing while comparing the wrong
#: two terms the moment either list was reordered.
PERCENT_VS_ABSOLUTE = [
    ("中性粒细胞百分比", "中性粒细胞绝对值"),
    ("嗜酸性粒细胞百分比", "嗜酸性粒细胞绝对值"),
    ("淋巴细胞百分比", "淋巴细胞绝对值"),
    ("单核细胞百分比", "单核细胞绝对值"),
    ("嗜碱性粒细胞百分比", "嗜碱性粒细胞绝对值"),
]


@pytest.mark.parametrize("term,loinc", DIFFERENTIAL_RATIO + DIFFERENTIAL_COUNT)
def test_differential_percent_and_count_are_distinct_codes(resolver, term, loinc):
    r = resolver.resolve(term)
    assert r.resolved, f"{term} failed to resolve at all"
    assert r.loinc == loinc, f"{term}: got {r.loinc} ({r.canonical})"


@pytest.mark.parametrize("pct,absolute", PERCENT_VS_ABSOLUTE)
def test_percent_and_absolute_never_collide(resolver, pct, absolute):
    """Different codes, and — the point of the report — different PROPERTIES.

    Two codes that differed only by method would still be two series; these
    have to differ by dimension, NFr against NCnc, or the grouping is wrong
    even when the codes are not equal.
    """
    a, b = resolver.resolve(pct), resolver.resolve(absolute)
    assert a.loinc != b.loinc, pct
    pa = resolver._axis_row(resolver._row_for_code(a.loinc))[2]
    pb = resolver._axis_row(resolver._row_for_code(b.loinc))[2]
    assert pa != pb, f"{pct}/{absolute}: both {pa}"


def test_hba1c_spellings_agree(resolver):
    """`Hemoglobin A1c` answered 41995-2 while its three synonyms answered 4548-4.

    The outlier was the spelling an English report is most likely to print.
    """
    codes = {t: resolver.resolve(t).loinc
             for t in ("HbA1c", "A1c", "糖化血红蛋白", "Hemoglobin A1c")}
    assert set(codes.values()) == {"4548-4"}, codes


# ── a trailing acronym on the INPUT ─────────────────────────────────────────
#
# Reports print the analyte and its abbreviation side by side constantly. The
# strip existed but was applied to the alias table's target, so it could only
# fire on inputs that already resolved. Both halves are live now.
@pytest.mark.parametrize("term,loinc", [
    ("Fasting plasma glucose FPG", "1558-6"),
    ("Total cholesterol TC", "2093-3"),
    ("总胆固醇 TC", "2093-3"),
    ("甘油三酯 TG", "2571-8"),
])
def test_trailing_acronym_on_input(resolver, term, loinc):
    assert resolver.resolve(term).loinc == loinc


def test_trailing_acronym_does_not_invent_answers(resolver):
    """The strip runs LAST, so it can only turn a miss into a hit."""
    assert not resolver.resolve("血脂").resolved          # panel term, still refused
    assert not resolver.resolve("血糖(HbA1c)").resolved   # no space: nothing to strip


@pytest.mark.parametrize("term", [
    "Protein CSF",      # CSF is a SYSTEM axis value — a specimen, not a spelling
    "Calcium ION",      # ION answers ionized calcium, not total
    "胆固醇 HDL",         # HDL answers 2085-9, 胆固醇 answers 2093-3
    "Glucose OGTT",     # OGTT answers a tolerance-test glucose
])
def test_a_trailing_qualifier_is_not_stripped(resolver, term):
    """`Total cholesterol TC` and `Protein CSF` are the same shape and opposite
    meanings. Stripping the first is lossless; stripping the second answered a
    SERUM protein for a spinal-fluid one, with a confident-looking code.

    Abstaining is the right answer here: the term names something real that this
    resolver cannot place, and a wrong code is worse than none.
    """
    assert not resolver.resolve(term).resolved, resolver.resolve(term).canonical


def test_alias_hit_without_a_loinc_code_falls_through(resolver):
    """`resolved=True, loinc=''` was reachable and is not an answer.

    The corpus spans six vocabularies and carries 4,991 `Deprecated …` names, so
    a candidate row can match lexically and carry no LOINC code at all. Such a
    row must be skipped in favour of the next candidate, not returned.
    """
    for term, _ in DIFFERENTIAL_RATIO + DIFFERENTIAL_COUNT + GOLDEN:
        r = resolver.resolve(term)
        assert not (r.resolved and not r.loinc), f"{term}: resolved with no code"


# ── a switched variant must report ITS OWN name ─────────────────────────────
@pytest.mark.parametrize("name,value,unit,loinc,in_name", [
    ("total cholesterol", "5.0", "mmol/L", "14647-2", "Moles/volume"),
    ("total cholesterol", "193", "mg/dL", "2093-3", "Mass/volume"),
    ("中性粒细胞", "4.2", "10*9/L", "26499-4", "#/volume"),
    ("中性粒细胞", "62 %", None, "26511-6", "Neutrophils/Leukocytes"),
    ("尿糖", "阴性", None, "2349-9", "Presence"),
])
def test_switched_variant_reports_its_own_name(name, value, unit, loinc, in_name):
    """The canonical used to be the PRE-switch name on every switched reading.

    A docstring-only `_name_for` left behind by a refactor shadowed the real
    one and returned None for every code; `or hit.canonical` then supplied a
    name that contradicted the code it shipped beside.
    """
    from mirobody.engine import resolve_reading

    r = resolve_reading(name, value, unit)
    assert r.loinc == loinc, f"{name} {value} {unit}: got {r.loinc}"
    assert in_name in r.canonical, f"{r.loinc} reported as {r.canonical!r}"


def test_the_unit_picks_percent_or_count(resolver):
    """A differential percentage and a differential count are two COMPONENTs.

    `中性粒细胞` alone means the CBC percentage; the same term reported in
    `10*9/L` means the count, and the unit is the only thing that says so.
    Dropping a ratio's denominator is well-determined; adding one is not, so
    the crossing runs in that direction only.
    """
    from mirobody.engine import resolve_reading

    for term in ("中性粒细胞", "淋巴细胞", "单核细胞"):
        pct = resolve_reading(term, "32 %", None)
        cnt = resolve_reading(term, "4.2", "10*9/L")
        assert pct.loinc != cnt.loinc, term
        assert "/Leukocytes" in pct.canonical, f"{term}: {pct.canonical}"
        assert "#/volume" in cnt.canonical, f"{term}: {cnt.canonical}"

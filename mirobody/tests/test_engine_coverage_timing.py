"""When a sample was drawn, read from its name: fasting, after a meal, a
challenge's timed draws, a drug's trough and peak.

LOINC keeps the draw in the COMPONENT (`insulin^post CFst`, `glucose^2H post
XXX challenge`), and the resolver used to know a timed name only if the whole
string was a row or an index key. Fasting glucose resolved because about
thirty rows spelled it out in four languages; on the same bundle, of the 13
fasting analytes LOINC 2.83 codes, only glucose did, and none of the five
2-hour post-meal ones. Worse than the misses, `胰岛素(60min)`, `(120min)` and
`(180min)` with mU/L all became plain insulin (20448-7): the bracket split
off `120min`, the index answered that alone with a glucose draw, the two
halves were "two analytes", and the unit picked insulin. One release test
put three values into the plain insulin series on one day.

`_resolve_timed` reads the draw (`lexical.split_timing`), resolves the analyte
on its own, and takes the analyte's sibling for that draw; without one it
refuses with `reason_kind == "timing"` and never falls back to the plain code.

The cases below come from the axis table rather than from reports: every
analyte in the bundle that LOINC codes with one of these draws, written the
way a report would. Run with ``-s`` to see the score.
"""

from __future__ import annotations

import csv
import os

import pytest

import mirobody
from mirobody.lexical import Timing, split_timing

_BUNDLE = os.path.join(os.path.dirname(os.path.abspath(mirobody.__file__)), "res", "loinc", "fhir_loinc_bundle.tar.gz")
pytestmark = pytest.mark.skipif(
    not (os.path.exists(_BUNDLE) and os.path.getsize(_BUNDLE) > 1_000_000),
    reason="needs the LOINC bundle (git lfs pull)",
)


@pytest.fixture(scope="module")
def resolver():
    from mirobody.engine import get_resolver

    return get_resolver()


def _component(resolver, code: str) -> str:
    axes = resolver.axes_of(code) if code else None
    return axes[0] if axes else ""


# ── the grammar ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(("term", "stem", "timing"), [
    ("空腹C肽", "C肽", ("fasting", None)),
    ("Insulin, Fasting", "Insulin", ("fasting", None)),
    ("胰岛素(空腹)", "胰岛素", ("fasting", None)),
    ("胰岛素(0min)", "胰岛素", ("baseline", 0)),
    ("胰岛素(120min)", "胰岛素", ("after_challenge", 120)),
    ("胰岛素(30′)", "胰岛素", ("after_challenge", 30)),
    ("C肽2小时", "C肽", ("after_challenge", 120)),
    ("2h胰岛素", "胰岛素", ("after_challenge", 120)),
    ("OGTT2小时血糖", "血糖", ("after_challenge", 120)),
    ("Glucose 120 min", "Glucose", ("after_challenge", 120)),
    ("餐后2小时C肽", "C肽", ("post_meal", 120)),
    ("2-hour postprandial glucose", "glucose", ("post_meal", 120)),
    ("餐后血糖", "血糖", ("post_meal", None)),
    ("万古霉素谷浓度", "万古霉素", ("trough", None)),
    ("Vancomycin peak", "Vancomycin", ("peak", None)),
])
def test_split_timing_reads_the_draw(term, stem, timing):
    found = split_timing(term)
    assert found is not None
    assert (found[0], found[1].kind, found[1].minutes) == (stem, *timing)


@pytest.mark.parametrize("term", [
    "5-HT", "5-HIAA", "17-OHP", "25-hydroxyvitamin D",   # `5-H` is not five hours
    "2hPG",                      # one word, not 2 h + PG
    "24小时尿蛋白", "24h尿蛋白",    # a collection's length, not a draw
    "Peak expiratory flow",      # the analyte's own word
    "凝血酶峰值", "血糖(OGTT)",     # no draw, or no time
    "空腹", "Fasting", "HbA1c", "PM2.5", "Lp(a)",
])
def test_split_timing_leaves_other_names_alone(term):
    assert split_timing(term) is None


# ── the reported failures ─────────────────────────────────────────────────

RELEASE_TEST = [
    # name, value, unit, the code, its component
    ("空腹胰岛素", "8.1", "mU/L", "27873-9", "insulin^post cfst"),
    ("胰岛素(0min)", "8.1", "mU/L", "56482-3", "insulin^baseline"),
    ("胰岛素(30min)", "45.3", "mU/L", "27827-5", "insulin^30m post xxx challenge"),
    ("胰岛素(60min)", "80.2", "mU/L", "27379-7", "insulin^1h post xxx challenge"),
    ("胰岛素(120min)", "62.0", "mU/L", "27826-7", "insulin^2h post xxx challenge"),
    ("胰岛素(180min)", "20.4", "mU/L", "27828-3", "insulin^3h post xxx challenge"),
    ("C肽(0min)", "0.6", "ng/mL", "57376-6", "c peptide^baseline"),
    ("C肽(120min)", "2.9", "ng/mL", "13041-9", "c peptide^2h post xxx challenge"),
    ("血糖(0min)", "5.1", "mmol/L", "14768-6", "glucose^baseline"),
    ("血糖(120min)", "7.4", "mmol/L", "25668-5", "glucose^2h post xxx challenge"),
]


@pytest.mark.parametrize(("name", "value", "unit", "code", "component"), RELEASE_TEST)
def test_each_draw_of_a_release_test_is_its_own_code(resolver, name, value, unit, code, component):
    from mirobody.engine.resolver import resolve_reading

    found = resolve_reading(name, value, unit)
    assert (found.loinc, _component(resolver, found.loinc)) == (code, component)


def test_no_draw_lands_in_the_plain_series(resolver):
    from mirobody.engine.resolver import resolve_reading

    plain = {"insulin", "c peptide", "glucose"}
    landed = [(n, resolve_reading(n, v, u).loinc) for n, v, u, _c, _k in RELEASE_TEST]
    assert not [x for x in landed if _component(resolver, x[1]) in plain], landed
    assert len({code for _, code in landed}) == len(landed)


@pytest.mark.parametrize(("name", "code"), [
    ("空腹C肽", "13037-7"),
    ("C-Peptide, Fasting", "13037-7"),
    ("空腹甘油三酯", "3048-6"),
    ("Fasting triglycerides", "3048-6"),
    ("餐后2小时胰岛素", "95114-5"),
    ("Vancomycin trough", "4092-3"),
    ("万古霉素谷浓度", "4092-3"),
    ("他克莫司谷浓度", "77348-1"),
    # A drug's bare name is a presence screen (13579-8); its trough is a level.
    ("舍曲林谷浓度", "74911-9"),
    ("庆大霉素峰浓度", "3663-2"),
    # What thirty curated rows answered still holds, through the rule.
    ("空腹葡萄糖", "1558-6"),
    ("空腹胰岛素", "27873-9"),
    # A row, because `血清胰岛素` alone resolves to nothing; dropped once
    # with the rows the rule replaced, and nothing else noticed.
    ("空腹血清胰岛素", "27873-9"),
    ("餐后2h血糖", "1521-4"),
])
def test_a_drawn_name_answers_its_sibling(resolver, name, code):
    assert resolver.resolve(name).loinc == code


def test_the_unit_still_picks_the_property_of_the_draw():
    from mirobody.engine.resolver import resolve_reading

    assert resolve_reading("空腹胰岛素", "58", "pmol/L").loinc == "59179-2"
    assert resolve_reading("空腹C肽", "0.6", "nmol/L").loinc == "42180-0"


def test_a_draw_with_no_code_refuses_and_keeps_the_plain_code_out(resolver):
    found = resolver.resolve("空腹同型半胱氨酸")
    assert not found.resolved and found.reason_kind == "timing"
    assert found.rejected_code == "13965-9"


def test_a_time_alone_is_never_read_as_an_analyte(resolver):
    """`120min` alone posts a glucose draw; behind an unknown name it used to
    answer that glucose code through the bracket."""
    assert not resolver.resolve("XYZ(120min)").resolved
    assert not resolver.resolve("胰岛素释放试验(120min)").resolved


def test_a_name_that_owns_the_word_keeps_its_answer(resolver):
    assert resolver.resolve("Thrombin peak").loinc == "97912-0"
    assert resolver.resolve("凝血酶峰值").loinc == "97912-0"
    assert resolver.resolve("空腹血脂").reason_kind == "category"


def test_the_rule_never_disagrees_with_a_curated_row(resolver):
    """Thirty-odd alias rows spell out a draw (`空腹血糖`, `Glukoz (açlık)`). A
    row wins over the rule, so two answers for one name never meet; but if the
    rule would answer a row's name differently, the same draw written another
    way (`空腹葡萄糖`) would land in another series. With each row taken out in
    turn, the rule gives the row's code or no code at all."""
    from mirobody._bundle import alias_source_files
    from mirobody.engine.resolver import OfflineResolver
    from mirobody.lexical import index_fold

    fresh = OfflineResolver()  # its alias table is edited below; the shared one must not be
    rows = []
    for path in alias_source_files():
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if not line.startswith("#") and len(parts) == 2 and all(parts) and split_timing(parts[0]):
                    rows.append(parts[0])
    assert len(rows) >= 30
    conflicts = []
    for term in rows:
        with_row = fresh.resolve(term).loinc
        key = index_fold(term)
        saved = fresh._src.pop(key)
        try:
            by_rule = fresh.resolve(term).loinc
        finally:
            fresh._src[key] = saved
        if by_rule and by_rule != with_row:
            conflicts.append((term, with_row, by_rule))
    assert not conflicts, conflicts


# ── every analyte LOINC codes with one of these draws ─────────────────────

#: (timing, English names, Chinese names) for each draw the grammar reads.
#: `{en}` is the analyte's own LOINC component, `{zh}` its shortest Chinese
#: alias.
_DRAWS: list[tuple[Timing, tuple[str, ...], tuple[str, ...]]] = [
    (Timing("fasting", None, ""), ("Fasting {en}", "{en}, fasting"), ("空腹{zh}", "{zh}(空腹)")),
    (Timing("post_meal", None, ""), ("Postprandial {en}",), ("餐后{zh}",)),
    (Timing("post_meal", 120, ""), ("2-hour postprandial {en}", "{en} 2h post meal"), ("餐后2小时{zh}",)),
    (Timing("baseline", 0, ""), ("{en} 0 min",), ("{zh}(0min)",)),
    (Timing("after_challenge", 30, ""), ("{en} 30 min",), ("{zh}(30min)",)),
    (Timing("after_challenge", 60, ""), ("{en} 60 min",), ("{zh}(60min)",)),
    (Timing("after_challenge", 120, ""), ("{en} 2 hour",), ("{zh}(120min)", "{zh}2小时")),
    (Timing("after_challenge", 180, ""), ("{en} 180 min",), ("{zh}(180min)",)),
    (Timing("trough", None, ""), ("{en} trough",), ("{zh}谷浓度",)),
    (Timing("peak", None, ""), ("{en} peak",), ("{zh}峰浓度",)),
]

#: 734 of 766 when this was written. Raise it when more answer; never lower it
#: to make a red build green. The other 32 refuse as `timing`, rightly: LOINC
#: codes those draws in another specimen or property than the analyte's own
#: name reaches (fasting ketones only in urine, `肾上腺素` answers urine
#: epinephrine and its challenge draws are plasma), and answering across that
#: would file a reading under a specimen nobody printed.
DRAW_COVERAGE_FLOOR = 734 / 766


def _chinese_names(resolver) -> dict[str, str]:
    from mirobody._bundle import ALIAS_SRC_DIR

    out: dict[str, str] = {}
    with open(os.path.join(ALIAS_SRC_DIR, "zh.tsv"), encoding="utf-8") as f:
        for row in csv.reader(f, delimiter="\t"):
            if len(row) < 2 or not row[0] or row[0].startswith("#"):
                continue
            found = resolver.resolve(row[0])
            component = _component(resolver, found.loinc)
            if found.resolved and "^" not in component and (component not in out or len(row[0]) < len(out[component])):
                out[component] = row[0]
    return out


def _generated(resolver):
    """`(name, analyte, the components that are right)` for every analyte whose
    own name resolves to it and that LOINC codes with one of `_DRAWS`."""
    from mirobody.engine.resolver import _loinc_challenges

    index = resolver._component_index()
    zh = _chinese_names(resolver)
    for timing, en_templates, zh_templates in _DRAWS:
        right_challenges = _loinc_challenges(timing)
        heads = {
            component.decode().partition("^")[0]
            for component in index
            if component.decode().partition("^")[2] in right_challenges
        }
        for head in sorted(heads):
            if _component(resolver, resolver.resolve(head).loinc) != head:
                continue
            right = {f"{head}^{c}" for c in right_challenges}
            names = [t.format(en=head) for t in en_templates]
            if head in zh:
                names += [t.format(zh=zh[head]) for t in zh_templates]
            for name in names:
                yield name, head, right


def test_a_drawn_name_never_answers_another_series(resolver):
    """The invariant: the right draw, or a `timing` refusal. Never the plain
    analyte, never another one."""
    answered, wrong, total = 0, [], 0
    for name, head, right in _generated(resolver):
        total += 1
        found = resolver.resolve(name)
        component = _component(resolver, found.loinc)
        if component in right:
            answered += 1
        elif found.loinc or found.reason_kind != "timing":
            wrong.append((name, found.loinc or found.reason_kind, component or found.rejected_reason[:60]))
    print(f"\ndraws: {answered}/{total} answered, {total - answered - len(wrong)} refused as timing")
    assert total > 500
    assert not wrong, wrong[:20]
    assert answered / total >= DRAW_COVERAGE_FLOOR

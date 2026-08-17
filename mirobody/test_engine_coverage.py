"""The public coverage benchmark for offline indicator resolution.

`test_engine.py` pins a handful of golden codes. This file asks the harder,
more honest question: **of the tests an ordinary person actually finds on a
lab report, how many does `mirobody resolve` get right?**

The panels below are the everyday ones — lipid, CBC, metabolic, thyroid,
common vitamins — written the way a report prints them, in the languages the
engine claims to cover. That is deliberately the least flattering possible
test set, because it is the one every new user runs in their first minute.

Ground truth is expressed as *clinical intent*, not as a single LOINC code:
each case names a pattern the canonical result must match, and optionally one
it must not. This is stricter than it looks — it is what catches
``血红蛋白 -> Hemoglobin A1c``, a hit that a resolution-rate metric happily
counts as a success — while staying reviewable by a clinician who does not
have the LOINC table memorised, and staying stable across specimen and method
variants (Serum or Plasma vs Blood, calculated vs direct) where more than one
code is legitimately correct.

Run just this benchmark, with the score printed:

    pytest mirobody/test_engine_coverage.py -v -s

Contributing: a term that misses here is a one-line fix in
``mirobody/res/aliases_src/*_curated.tsv``. Add the row, add the case, watch
COVERAGE_FLOOR go up.
"""

from __future__ import annotations

import os
import re

import pytest

_BUNDLE = os.path.join(os.path.dirname(__file__), "res", "fhir_loinc_bundle.tar.gz")


def _bundle_available() -> bool:
    try:
        return os.path.getsize(_BUNDLE) > 1_000_000
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _bundle_available(),
    reason="LFS data bundles not fetched (run `git lfs pull`)",
)


# (term, must-match, must-not-match) — patterns are case-insensitive.
#
# `must_not` exists for the near-miss traps, where a wrong answer is worse
# than no answer: hemoglobin vs HbA1c, glucose vs a glucose tolerance
# challenge, total vs LDL/HDL cholesterol.
CASES: list[tuple[str, str, str]] = [
    # ── lipid panel ──────────────────────────────────────────────────────────
    ("total cholesterol",           r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("cholesterol",                 r"cholesterol",                  r""),
    ("LDL cholesterol",             r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("LDL-C",                       r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("HDL cholesterol",             r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("triglycerides",               r"triglyceride",                 r""),
    # ── complete blood count ─────────────────────────────────────────────────
    ("hemoglobin",                  r"hemoglobin",                   r"A1c|glycated"),
    ("hematocrit",                  r"hematocrit",                   r""),
    ("white blood cell count",      r"leukocyte|white blood cell",   r""),
    ("red blood cell count",        r"erythrocyte|red blood cell",   r""),
    ("platelet count",              r"platelet",                     r""),
    # ── metabolic panel ──────────────────────────────────────────────────────
    ("glucose",                     r"glucose",                      r"tolerance|challenge"),
    ("fasting glucose",             r"glucose",                      r"tolerance|challenge"),
    ("creatinine",                  r"creatinine",                   r"clearance|urine"),
    ("blood urea nitrogen",         r"urea nitrogen",                r""),
    ("sodium",                      r"sodium",                       r""),
    ("potassium",                   r"potassium",                    r""),
    ("albumin",                     r"albumin",                      r"urine|globulin ratio"),
    ("total bilirubin",             r"bilirubin",                    r"direct|conjugated"),
    ("uric acid",                   r"urate|uric acid",              r""),
    # ── liver enzymes ────────────────────────────────────────────────────────
    ("ALT",                         r"alanine aminotransferase",     r""),
    ("AST",                         r"aspartate aminotransferase",   r""),
    ("alkaline phosphatase",        r"alkaline phosphatase",         r""),
    # ── endocrine & vitamins ─────────────────────────────────────────────────
    ("HbA1c",                       r"hemoglobin a1c",               r""),
    ("TSH",                         r"thyrotropin|thyroid stimulating", r""),
    ("free T4",                     r"thyroxine",                    r""),
    ("vitamin D",                   r"vitamin d|calcidiol|hydroxyvitamin", r""),
    ("vitamin B12",                 r"cobalamin|vitamin b12",        r""),
    ("ferritin",                    r"ferritin",                     r""),
    ("CRP",                         r"c reactive protein",           r""),
    # ── 中文（简体）─────────────────────────────────────────────────────────
    ("血红蛋白",                     r"hemoglobin",                   r"A1c|glycated"),
    ("血糖",                         r"glucose",                      r"tolerance|challenge"),
    ("空腹血糖",                     r"glucose",                      r"tolerance|challenge"),
    ("总胆固醇",                     r"cholesterol",                  r"LDL|HDL"),
    ("低密度脂蛋白胆固醇",             r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("高密度脂蛋白胆固醇",             r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("甘油三酯",                     r"triglyceride",                 r""),
    ("肌酐",                         r"creatinine",                   r"clearance|urine"),
    ("尿酸",                         r"urate|uric acid",              r""),
    ("白细胞计数",                   r"leukocyte|white blood cell",   r""),
    ("红细胞计数",                   r"erythrocyte|red blood cell",   r""),
    ("血小板计数",                   r"platelet",                     r""),
    ("谷丙转氨酶",                   r"alanine aminotransferase",     r""),
    ("谷草转氨酶",                   r"aspartate aminotransferase",   r""),
    ("promoted:糖化血红蛋白",         r"hemoglobin a1c",               r""),
    ("促甲状腺激素",                 r"thyrotropin|thyroid stimulating", r""),
    # ── 日本語 ───────────────────────────────────────────────────────────────
    ("ヘモグロビン",                  r"hemoglobin",                   r"A1c|glycated"),
    ("総コレステロール",              r"cholesterol",                  r"LDL|HDL"),
    ("中性脂肪",                     r"triglyceride",                 r""),
    ("クレアチニン",                  r"creatinine",                   r"clearance|urine"),
    ("尿酸値",                       r"urate|uric acid",              r""),
    # ── second sweep: the wider panel a physical actually orders ─────────────
    # English extended
    ("total protein",               r"^protein \[",                  r"urine|LDL"),
    ("direct bilirubin",            r"bilirubin.*(conjugated|direct)", r"indirect|non-glucuron"),
    ("indirect bilirubin",          r"bilirubin.*indirect",          r""),
    ("lipoprotein(a)",              r"lipoprotein a",                r""),
    ("non-HDL cholesterol",         r"cholesterol non hdl",          r""),
    ("reticulocyte count",          r"reticulocyte",                 r""),
    ("creatinine clearance",        r"creatinine renal clearance",   r""),
    ("T3",                          r"triiodothyronine",             r"free"),
    ("T4",                          r"thyroxine",                    r"free"),
    ("anti-TPO",                    r"thyroperoxidase",              r""),
    ("LH",                          r"lutropin|luteinizing",         r""),
    ("CA-125",                      r"cancer ag 125",                r""),
    ("CA19-9",                      r"cancer ag 19-9",               r""),
    ("free PSA",                    r"prostate specific ag free",    r""),
    ("urine glucose",               r"glucose.*urine",               r""),
    ("urine protein",               r"protein.*urine",               r""),
    ("urine pH",                    r"ph of urine",                  r""),
    ("specific gravity",            r"specific gravity",             r""),
    # 中文 extended — electrolytes and vitals, the single-character names
    ("钾",                          r"potassium",                    r"urine"),
    ("钠",                          r"sodium",                       r"urine"),
    ("氯",                          r"chloride",                     r"urine"),
    ("钙",                          r"calcium",                      r"urine|ionized"),
    ("镁",                          r"magnesium",                    r"urine"),
    ("磷",                          r"phosphate",                    r"urine"),
    ("总蛋白",                       r"^protein \[",                  r"urine"),
    ("糖化血红蛋白",                  r"hemoglobin a1c",               r""),
    ("收缩压",                       r"systolic blood pressure",      r""),
    ("舒张压",                       r"diastolic blood pressure",     r""),
    ("血氧饱和度",                    r"oxygen saturation",            r""),
    ("尿蛋白",                       r"protein.*urine",               r""),
    # 日本語 extended — the katakana and short-kanji forms a 健康診断 prints
    ("白血球数",                     r"leukocyte|white blood cell",   r""),
    ("赤血球数",                     r"erythrocyte|red blood cell",   r""),
    ("血小板",                       r"platelet",                     r""),
    ("総蛋白",                       r"^protein \[",                  r"urine"),
    ("アルブミン",                    r"albumin",                      r"urine|globulin ratio"),
    ("尿素窒素",                     r"urea nitrogen",                r""),
    ("ナトリウム",                    r"sodium",                       r"urine"),
    ("カリウム",                      r"potassium",                    r"urine"),
    ("カルシウム",                    r"calcium",                      r"urine|ionized"),
    ("糖化ヘモグロビン",               r"hemoglobin a1c",               r""),
    ("コレステロール",                 r"cholesterol",                  r"LDL|HDL"),
]

# Terms that must stay UNRESOLVED. A confident wrong code is worse than an
# honest miss, so "answers nothing" is a behaviour worth pinning too.
MUST_NOT_RESOLVE: list[tuple[str, str]] = [
    # Panel / category names. No single code can be right for these, and the
    # index's answer was actively harmful: "blood pressure" matched 183 rows and
    # the commonness prior returned 8462-4 — the DIASTOLIC code — so a systolic
    # reading filed under it lands in the wrong series. Blocked with the
    # `!unresolved` sentinel in resolver_overrides.tsv.
    #
    # Note what is deliberately NOT blocked: 血压 resolves to 18684-1 "Blood
    # pressure Set", a panel code for a panel term — a correct answer.
    ("血圧", "blood pressure is a panel; the index would answer 'diastolic'"),
    ("blood pressure", "panel; answered 8462-4 (diastolic) before it was blocked"),
    ("BP", "the same panel, abbreviated"),
    ("lipid panel", "four analytes, not one observation"),
    ("血脂", "the same lipid panel in Chinese"),
    ("绝对不存在的指标名xyzzy", "pure nonsense must never resolve"),
]

# Ratchet. Every case above is expected to pass, so the floor is 1.0; raise the
# case count, never lower this number to make a red build green. A drop here
# means real users started getting worse answers than they did yesterday.
#
# Baseline for the record: this set scored 34% (32/94) against the resolver as
# it stood before resolver_overrides.tsv and the lookup-order fix existed.
COVERAGE_FLOOR = 1.0


def _strip_marker(term: str) -> str:
    return term.split(":", 1)[1] if term.startswith("promoted:") else term


@pytest.fixture(scope="module")
def resolver():
    from mirobody.engine import get_resolver

    return get_resolver()


def _miss_reason(r, must: str, must_not: str) -> str:
    """Why this case failed, or "" when it passed."""
    if not r.resolved:
        return "unresolved"
    if not re.search(must, r.canonical, re.I):
        return f"expected /{must}/, got {r.loinc} {r.canonical!r}"
    if must_not and re.search(must_not, r.canonical, re.I):
        return f"matched forbidden /{must_not}/: {r.loinc} {r.canonical!r}"
    return ""


def test_coverage(resolver):
    """Score every case, print the number, and list every miss.

    One assertion rather than one-per-case on purpose: when a data-bundle
    rebuild shifts things, the useful output is the whole list of what broke in
    one place, not ninety separate red tests.
    """
    misses = []
    for term, must, must_not in CASES:
        reason = _miss_reason(resolver.resolve(_strip_marker(term)), must, must_not)
        if reason:
            misses.append((term, reason))
    for term, why in MUST_NOT_RESOLVE:
        r = resolver.resolve(term)
        if r.resolved:
            misses.append((term, f"should NOT resolve ({why}), got {r.loinc} {r.canonical!r}"))

    total = len(CASES) + len(MUST_NOT_RESOLVE)
    coverage = 1 - len(misses) / total
    print(f"\n  offline resolver coverage: {total - len(misses)}/{total} = {coverage:.0%}")
    for term, reason in misses:
        print(f"    MISS  {term:<24} {reason}")

    assert coverage >= COVERAGE_FLOOR, (
        f"coverage {coverage:.0%} is below the {COVERAGE_FLOOR:.0%} floor; "
        f"{len(misses)} miss(es): " + "; ".join(f"{t} — {why}" for t, why in misses)
    )

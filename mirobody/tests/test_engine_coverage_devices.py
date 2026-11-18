"""The device-vocabulary coverage benchmark for offline indicator resolution.

`test_engine_coverage.py` asks how many tests from an ordinary LAB REPORT
resolve, and its answer (347/347 before #301, 356/356 after) is a claim
about lab reports specifically — a printed row with the name, the value and
the unit together. The people behind GitHub issue #301 are not reading a lab
report. They manage a chronic condition or a weight-loss program from:

* a body-composition scale, when losing weight;
* a watch or a band, when training;
* a glucometer, a CGM and a blood-pressure cuff, with a chronic condition.

Those apps print the SAME analyte with none of a lab report's scaffolding —
no specimen, no "body" qualifier, often no unit at all (a visceral-fat scale
LEVEL) — which is exactly the shape the lexical resolver's specimen and
commonness heuristics get wrong. Folding these into the lab benchmark would
make "347/347" a claim it never was; this file keeps the two apart.

#301's second comment reports 92 names in all, checked against LOINC 2.83,
grouped the way the issue grouped them: weight-loss/body-composition,
fitness, and chronic-condition devices. This file holds the names the issue
gives CONCRETE printed-name evidence for — its "main gives"/"Expected" table
rows, plus the three already-correct names its lead table spells out by
name. Most of each category's remaining "N right" count is reported only as
a number, never spelled out by name in the issue, so this file does not
invent those names.

Run just this benchmark, with the score printed::

    pytest tests/test_engine_coverage_devices.py -v -s

Unlike the lab benchmark's 1.0 ratchet, `DEVICE_COVERAGE_FLOOR` is set to
today's honestly-achieved score, not to 100%: five names (`去脂体重`'s
family, see `resolver_overrides.tsv`) hit a resolver-level ambiguity no
override row can fix (four LOINC codes share one component at equal
commonness rank) and two (`活动消耗`'s family) have no alias at all without
risking the same ambiguity if one were added. Both stay in CASES, miss on
purpose, and are visible in the printed list — "not fixed yet" is not the
same thing as "not tracked". Fixing either is a `_pick`/build-time change,
out of scope for a `resolver_overrides.tsv`-only pass; raise the floor when
one lands, never lower it to paper over a new regression.
"""

from __future__ import annotations

import os
import re

import pytest

import mirobody

#: Same bundle-location guard as test_engine_coverage.py — see its comment.
_BUNDLE = os.path.join(os.path.dirname(os.path.abspath(mirobody.__file__)), "res", "loinc", "fhir_loinc_bundle.tar.gz")


def _bundle_available() -> bool:
    try:
        return os.path.getsize(_BUNDLE) > 1_000_000
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _bundle_available(),
    reason="LFS data bundles not fetched (run `git lfs pull`)",
)


# (term, must-match, must-not-match) — patterns are case-insensitive, checked
# against the resolved LONG_COMMON_NAME. A term expected to MISS today (see
# the module docstring) is still listed here, with the code it SHOULD reach,
# so the printed miss list says exactly what is still owed.
CASES: list[tuple[str, str, str]] = [
    # ── weight-loss and body composition (scale, InBody) ─────────────────────
    # Already right on main, from #301's lead table — regression cases, not
    # fixes. `脂肪`/`肌肉` below are the two the issue's cause-1 table counts
    # as confidently wrong: a bare fat/muscle MASS reading has no specimen in
    # the name, and the lexical index's only bare-"fat" specimen is stool
    # (16853-4) and its only bare-"muscle" hit is a MuSK antibody (51763-1).
    ("体脂肪量",                      r"body fat",                     r"stool"),
    ("体脂量",                       r"body fat",                     r"stool"),
    ("Body fat mass",               r"body fat",                     r"stool"),
    ("脂肪",                         r"body fat",                     r"stool"),
    ("Fat",                         r"body fat",                     r"stool"),
    ("脂肪量",                        r"body fat",                     r"stool"),
    ("脂肪重量",                       r"body fat",                     r"stool"),
    ("体脂肪",                        r"body fat",                     r"stool"),
    ("Fat Mass",                    r"body fat",                     r"stool"),
    ("体脂肪率",                       r"percentage of body fat",       r""),
    ("脂肪率",                        r"percentage of body fat",       r""),
    ("肌肉",                         r"body muscle mass",             r"antibody|titer"),
    ("肌肉量",                        r"body muscle mass",             r"antibody|titer"),
    ("Muscle Mass",                 r"body muscle mass",             r"antibody|titer"),
    ("肌肉率",                        r"body muscle mass.*body weight", r""),
    # `去脂体重`/`瘦体重` now abstain rather than answer the immersion-method
    # code (`resolver_overrides.tsv` has the four-way commonness tie this
    # exposed); `Lean Body Mass`/`去脂肪体重`/`Fat-Free Mass` have no alias of
    # their own and are left that way rather than risk the same tie. All five
    # are expected MISSES — see the module docstring.
    ("去脂体重",                       r"^lean body weight$",           r"immersion"),
    ("瘦体重",                        r"^lean body weight$",           r"immersion"),
    ("Lean Body Mass",               r"^lean body weight$",           r"immersion"),
    ("去脂肪体重",                      r"^lean body weight$",           r"immersion"),
    ("Fat-Free Mass",                r"^lean body weight$",           r"immersion"),
    ("体水分",                        r"body water mass",              r"percentage"),
    ("总体水",                        r"body water mass",              r"percentage"),
    ("Total Body Water",            r"body water mass",              r"percentage"),
    ("水分率",                        r"percentage of body water",     r""),
    ("体水分率",                       r"percentage of body water",     r""),
    ("骨量",                         r"body bone mass",               r""),
    ("骨盐量",                        r"body bone mass",               r""),
    ("内脏脂肪面积",                    r"visceral fat",                 r""),
    ("Visceral Fat Area",           r"visceral fat",                 r""),
    # NOT a fix: `内脏脂肪` (bare) is left exactly as main already resolves
    # it, 73707-2. #301 proposed abstaining (a scale prints a dimensionless
    # LEVEL, catalogue `bodyVisFatLevel`, no LOINC), but a report that prints
    # 内脏脂肪/內臟脂肪 with cm² means the area, the common clinical shorthand;
    # a scale's unitless level is told apart by the unit gate on a reading,
    # not by redirecting the bare name. Pinned so this stays a decision, not
    # a drift.
    ("内脏脂肪",                       r"visceral fat",                 r""),
    ("标准体重",                       r"ideal body weight",            r""),
    ("当前体重",                       r"^body weight$",                r""),
    ("腹围",                         r"waist circumference",          r""),

    # ── fitness (watch, band) ─────────────────────────────────────────────
    ("安静心率",                       r"heart rate.*resting",          r""),
    ("最大心率",                       r"maximum heart rate",           r""),
    ("最高心率",                       r"maximum heart rate",           r""),
    ("最大摄氧量",                      r"oxygen consumption",           r""),
    ("VO2max",                      r"oxygen consumption",           r""),
    ("每日步数",                       r"number of steps",              r""),
    # `活动消耗`/`消耗热量` have no alias; `Calories burned` (the catalogue's
    # own LCN for 41981-2) posts FIVE rows tied at the same commonness rank
    # — `in 24 hour`, `in 1 week`, bare, `Machine estimate`, `Pedometer` —
    # the exact shape that made `去脂体重` unsafe to redirect. Expected MISSES.
    ("活动消耗",                       r"^calories burned$",            r""),
    ("消耗热量",                       r"^calories burned$",            r""),
    ("运动时长",                       r"exercise duration",            r""),
    # `血氧`/`SpO2` named the generic 20564-1 `Oxygen saturation in Blood`
    # (a 75-way commonness tie the `oxygen saturation` override target fed
    # straight into); both now target the pulse-oximetry LCN the device
    # catalogue's `BLOOD_OXYGEN` (59408-5) ships.
    ("血氧",                         r"pulse oximetry",               r""),
    ("SpO2",                        r"pulse oximetry",               r""),

    # ── chronic conditions (glucometer, CGM, BP cuff, follow-up labs) ────────
    ("指尖血糖",                       r"capillary blood",              r"interstitial"),
    ("末梢血糖",                       r"capillary blood",              r"interstitial"),
    ("毛细血管血糖",                    r"capillary blood",              r"interstitial"),
    ("动态血糖",                       r"interstitial fluid",           r""),
    ("组织间液葡萄糖",                   r"interstitial fluid",           r""),
    ("目标范围内时间",                   r"glucose measurements in range", r""),
    ("平均动脉压",                      r"mean blood pressure",          r""),
    ("非HDL-C",                      r"non hdl",                      r""),
    ("尿白蛋白肌酐比",                   r"albumin/creatinine",           r"microalbumin"),
    ("UACR",                        r"albumin/creatinine",           r"microalbumin"),
    ("尿微量白蛋白/肌酐",                 r"microalbumin/creatinine",      r""),
]

# Terms that must stay UNRESOLVED — a device reading filed under the wrong
# analyte is worse than an honest miss. Each used to land on a specific wrong
# code; the catalogue's own device field (`metrics.tsv`) carries no LOINC for
# any of these, so there is no correct code to redirect to.
MUST_NOT_RESOLVE: list[tuple[str, str]] = [
    ("骨骼肌", "named 13931-1, a striated-muscle antibody test, for a mass in kg; bodySkeletalMuscleMass carries no LOINC"),
    ("骨骼肌量", "same shape as 骨骼肌, with a measure word"),
    ("Skeletal Muscle Mass", "the English spelling of 骨骼肌量"),
    ("基础代谢", "named 24321-2, a basic metabolic PANEL, for a single kcal reading; basalMetabolicRate carries no LOINC"),
    ("基础代谢率", "named 82259-3, a MET-and-RMR PANEL, sharing a row with BMR"),
    ("BMR", "the English abbreviation of 基础代谢率, same panel code"),
    ("压力", "a watch's 0-100 stress score named 61011-3, Umbilical vein blood pressure — no specimen or PROPERTY in common"),
]

# Ratchet, but not at 1.0 like the lab benchmark's: seven of the 62 cases
# above are KNOWN misses (see the module docstring), so 100% is not an
# honest target yet. Raise this when a case starts passing; never lower it
# to make a red build green.
DEVICE_COVERAGE_FLOOR = 55 / 62


def _miss_reason(r, must: str, must_not: str) -> str:
    """Why this case failed, or "" when it passed. Same rule as the lab
    benchmark's `_miss_reason` — kept local rather than imported, since the
    two files are meant to stay independently readable and independently
    scored."""
    if not r.resolved:
        return "unresolved"
    if not re.search(must, r.canonical, re.I):
        return f"expected /{must}/, got {r.loinc} {r.canonical!r}"
    if must_not and re.search(must_not, r.canonical, re.I):
        return f"matched forbidden /{must_not}/: {r.loinc} {r.canonical!r}"
    return ""


def test_device_coverage():
    """Score every device-vocabulary case, print the number, and list every
    miss — including the ones named as known in the module docstring, so
    they stay visible instead of silently passing or silently disappearing."""
    from mirobody.engine import resolve

    misses = []
    for term, must, must_not in CASES:
        reason = _miss_reason(resolve(term), must, must_not)
        if reason:
            misses.append((term, reason))
    for term, why in MUST_NOT_RESOLVE:
        r = resolve(term)
        if r.resolved:
            misses.append((term, f"should NOT resolve ({why}), got {r.loinc} {r.canonical!r}"))

    total = len(CASES) + len(MUST_NOT_RESOLVE)
    coverage = 1 - len(misses) / total
    print(f"\n  device-vocabulary coverage: {total - len(misses)}/{total} = {coverage:.0%}")
    for term, reason in misses:
        print(f"    MISS  {term:<20} {reason}")

    assert coverage >= DEVICE_COVERAGE_FLOOR, (
        f"device coverage {coverage:.0%} is below the {DEVICE_COVERAGE_FLOOR:.0%} floor; "
        f"{len(misses)} miss(es): " + "; ".join(f"{t} — {why}" for t, why in misses)
    )

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
``mirobody/res/resolver_overrides.tsv`` — that file, not
``aliases_src/*_curated.tsv``, is the one this resolver reads at runtime (the
curated files are inputs to the bundle BUILD; see the overrides header for why
the two are separate). Add the row, add the case here, and the score goes up.
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
    # Bare abbreviations and the spelled-out lipoprotein names are near-miss
    # magnets — the index holds neighbours that INVERT the reading: `HDL` sits
    # next to "Cholesterol NON HDL" (the opposite analyte), `LDL` and
    # `低密度脂蛋白` next to "Cholesterol in LDL/Cholesterol in HDL [Mass
    # Ratio]" (a ratio, not a concentration), `高密度脂蛋白` next to
    # "Lipoprotein.alpha/total Lipoprotein". The must-not patterns forbid
    # exactly those.
    ("HDL",                         r"cholesterol.*HDL|HDL.*cholesterol", r"non.?HDL|LDL|ratio"),
    ("LDL",                         r"cholesterol.*LDL|LDL.*cholesterol", r"HDL|ratio"),
    ("High-Density Lipoprotein",    r"cholesterol.*HDL|HDL.*cholesterol", r"non.?HDL|LDL|ratio"),
    ("Low-Density Lipoprotein",     r"cholesterol.*LDL|LDL.*cholesterol", r"HDL|ratio"),
    ("高密度脂蛋白",                  r"cholesterol.*HDL|HDL.*cholesterol", r"non.?HDL|LDL|ratio"),
    ("低密度脂蛋白",                  r"cholesterol.*LDL|LDL.*cholesterol", r"HDL|ratio"),
    ("triglycerides",               r"triglyceride",                 r""),
    # The names the shipped demo report actually prints. Ratios must land on a
    # RATIO code and never on either component.
    ("Blood Glucose",               r"glucose",                      r"fasting|tolerance|A1c|urine"),
    ("Cholesterol/HDL Ratio",       r"ratio",                        r"^(?!.*ratio)"),
    ("LDL/HDL Ratio",               r"LDL.*HDL.*ratio",              r""),
    ("Lipid-Free Fatty Acids",      r"fatty acid",                   r"cholesterol"),
    ("Lipid-Phospholipids",         r"phospholipid",                 r"cholesterol"),
    ("Non-HDL Cholesterol-Non-HDL", r"non HDL",                    r"in HDL \[|in LDL"),
    ("Non-HDL Cholesterol-Non-HDL-C", r"non HDL",                    r"in HDL \[|in LDL"),
    ("Lipid-Low-Density Lipoprotein Calculated", r"cholesterol.*LDL|LDL.*cholesterol", r"HDL|ratio"),
    # ── complete blood count ─────────────────────────────────────────────────
    ("hemoglobin",                  r"hemoglobin",                   r"A1c|glycated"),
    ("hematocrit",                  r"hematocrit",                   r""),
    ("white blood cell count",      r"leukocyte|white blood cell",   r""),
    ("red blood cell count",        r"erythrocyte|red blood cell",   r""),
    ("platelet count",              r"platelet",                     r""),
    # ── metabolic panel ──────────────────────────────────────────────────────
    ("glucose",                     r"glucose",                      r"tolerance|challenge"),
    ("fasting glucose",             r"^fasting glucose",             r"tolerance"),
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
    # ── Simplified Chinese ────────────────────────────────────────────────────
    ("血红蛋白",                     r"hemoglobin",                   r"A1c|glycated"),
    ("血糖",                         r"glucose",                      r"tolerance|challenge"),
    ("空腹血糖",                     r"^fasting glucose",             r"tolerance"),
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
    # ── Japanese ──────────────────────────────────────────────────────────────
    ("ヘモグロビン",                  r"hemoglobin",                   r"A1c|glycated"),
    ("総コレステロール",              r"cholesterol",                  r"LDL|HDL"),
    ("中性脂肪",                     r"triglyceride",                 r""),
    ("クレアチニン",                  r"creatinine",                   r"clearance|urine"),
    ("尿酸値",                       r"urate|uric acid",              r""),
    # The spelled-out ホルモン names a 健康診断結果表 prints — the bare form
    # must resolve, not only the parenthetical `甲状腺刺激ホルモン(TSH)`.
    ("甲状腺刺激ホルモン",             r"thyrotropin|thyroid stimulating", r""),
    ("チロトロピン",                  r"thyrotropin",                  r""),
    # ── Traditional Chinese (Taiwan) ──────────────────────────────────────────
    # Two distinct problems live here and only one of them is script.
    #
    # SCRIPT: 白細胞 / 總膽固醇 / 穀丙轉氨酶 are the same words in different
    # glyphs. `lexical.surface_variants` folds zh-Hant to zh-Hans on the way in
    # (see zh_fold.py), which is the symmetric half of what the
    # lexicon build already does to the corpus.
    #
    # VOCABULARY: Taiwan clinical usage picks DIFFERENT WORDS, and folding
    # those is worse than not folding them. 血紅素 folds to 血红素, which the
    # index answers 4548-4 — HbA1c — while in Taiwan 血紅素 *is* haemoglobin:
    # a character-level fold has haemoglobin and HbA1c backwards for every
    # Taiwanese report. Hence the `must_not` on the first two rows.
    ("血紅素",                       r"^hemoglobin \[",               r"A1c|glycated"),
    ("糖化血色素",                    r"hemoglobin a1c",               r""),
    ("白血球",                       r"leukocyte|white blood cell",   r""),
    ("紅血球",                       r"erythrocyte|red blood cell",   r""),
    ("血小板",                       r"platelet",                     r""),
    ("總膽固醇",                      r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("三酸甘油酯",                    r"triglyceride",                 r""),
    ("低密度脂蛋白膽固醇",             r"cholesterol in LDL",           r"HDL|VLDL"),
    ("高密度脂蛋白膽固醇",             r"cholesterol in HDL",           r"LDL|VLDL"),
    ("肌酸酐",                       r"creatinine",                   r"clearance|urine"),
    ("血中尿素氮",                    r"urea nitrogen",                r""),
    ("甲狀腺刺激素",                  r"thyrotropin|thyroid stimulating", r""),
    ("鹼性磷酸酶",                    r"alkaline phosphatase",         r""),
    ("穀丙轉氨酶",                    r"alanine aminotransferase",     r""),
    ("尿蛋白質",                      r"protein.*urine",               r""),
    ("收縮壓",                       r"systolic blood pressure",      r""),
    ("血壓",                         r"blood pressure panel",         r"systolic|diastolic|attach"),
    ("飯前血糖",                      r"fasting glucose",              r"tolerance|challenge"),

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
    # Blood pressure written as the panel, in every spelling a report or an API
    # caller uses. All five must land on the SAME code and it must be the panel,
    # never one of the two numbers — and never 18684-1, an ED attachment code
    # (CLASS=ATTACH.ED) that merely reads "First Blood pressure Set" yet sits
    # closest in the alias index. The forbidden pattern is what makes this bite.
    ("血压",                         r"blood pressure panel",         r"systolic|diastolic|attach"),
    ("血圧",                         r"blood pressure panel",         r"systolic|diastolic|attach"),
    ("blood pressure",              r"blood pressure panel",         r"systolic|diastolic|attach"),
    ("blood_pressure",              r"blood pressure panel",         r"systolic|diastolic|attach"),
    ("BP",                          r"blood pressure panel",         r"systolic|diastolic|attach"),
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
    # ── third sweep: Chinese as a report PRINTS it, specimen prefix and all ───
    # Written the way a 体检报告 PDF prints them: a Chinese lab prints 血清肌酐,
    # not the bare noun 肌酐 the lexicon's aliases carry. The specimen prefix
    # invites confidently wrong neighbours — 血清肌酐 sits near a MELD score,
    # 血清白蛋白 near a chicken-allergen component — so the negative patterns
    # pin that resolving is not enough: they must not resolve to those.
    ("血清肌酐",                     r"creatinine",                   r"clearance|urine|end.stage|MELD"),
    ("血清尿酸",                     r"urate|uric acid",              r"urine"),
    ("血清总胆固醇",                  r"cholesterol",                  r"LDL|HDL"),
    ("血清甘油三酯",                  r"triglyceride",                 r""),
    ("血清葡萄糖",                    r"glucose",                      r"tolerance|urine"),
    ("全血葡萄糖",                    r"glucose",                      r"tolerance|urine"),
    ("血清白蛋白",                    r"^albumin \[",                  r"chicken|gal d|urine|globulin ratio"),
    ("血清钾",                       r"potassium",                    r"urine"),
    ("血清钠",                       r"sodium",                       r"urine"),
    ("血清谷丙转氨酶",                 r"alanine aminotransferase",     r""),
    ("血清谷草转氨酶",                 r"aspartate aminotransferase",   r""),
    ("血清总胆红素",                  r"bilirubin.total",              r"direct|indirect|urine"),
    ("尿白细胞",                     r"leukocyte",                    r"blood|serum"),
    ("尿比重",                       r"specific gravity",             r""),
    # CBC spelled out in 中文 — the words a report prints, not only the
    # abbreviations (MCV -> 30428-7).
    ("平均红细胞体积",                 r"MCV|mean corpuscular volume",  r""),
    ("平均血红蛋白含量",               r"MCH \[|mean corpuscular hemoglobin", r"concentration|MCHC"),
    ("平均血红蛋白浓度",               r"MCHC",                         r""),
    ("超敏C反应蛋白",                 r"c reactive protein",           r"titer"),
    # Abbreviations that name more than one test must resolve to the widespread
    # reading. `HRV` means heart-rate variability to every wearable, yet sits
    # one alias away from 40991-2, a Rhinovirus+Enterovirus RNA PCR panel.
    ("HRV",                         r"heart rate variability|r-r interval", r"rhinovirus|enterovirus"),
    ("CA",                          r"^calcium",                     r"cancer|antigen"),
    ("PT",                          r"prothrombin time",             r"^inr|substitution"),
    ("MG",                          r"^magnesium",                   r""),
    ("凝血酶原时间",                  r"prothrombin time",             r"^inr"),
    # ── fourth sweep: device & wearable vocabulary, incl. the snake_case an
    # API caller sends ──────────────────────────────────────────────────────
    # `POST /v1/data` in the platform docs calls device data the main form
    # structured records take, and names indicators the way an SDK does:
    # snake_case. Both spellings are cased for each indicator, because `_`
    # normalization is the only thing keeping the documented spelling and the
    # spaced one on the same code.
    ("steps",                       r"steps",                        r"walk 10-meters"),
    ("step count",                  r"steps",                        r"walk 10-meters"),
    ("resting heart rate",          r"heart rate.*resting",          r"variability"),
    ("resting_heart_rate",          r"heart rate.*resting",          r"variability"),
    ("静息心率",                     r"heart rate.*resting",          r"variability"),
    ("heart rate",                  r"^heart rate",                  r"resting|variability|fetal"),
    ("heart_rate",                  r"^heart rate",                  r"resting|variability|fetal"),
    ("心率变异性",                    r"heart rate variability|r-r interval", r""),
    ("sleep duration",              r"sleep duration",               r""),
    ("sleep_duration",              r"sleep duration",               r""),
    ("睡眠时长",                     r"sleep duration",               r""),
    ("body weight",                 r"body weight",                  r"birth|ideal|estimated"),
    ("body_weight",                 r"body weight",                  r"birth|ideal|estimated"),
    ("body fat percentage",         r"body fat",                     r""),
    ("体脂率",                       r"body fat",                     r""),
    # SpO2's nearest alias row is a DEPRECATED "Fractional oxyhemoglobin
    # ... Preductal" entry whose LOINC is empty — resolved=True with no code —
    # which the must-not forbids.
    ("SpO2",                        r"oxygen saturation",            r"deprecated|mixed venous|cord"),
    ("血氧",                         r"oxygen saturation",            r"deprecated|mixed venous|cord"),
    # Fasting glucose in the three spellings the docs and a 中文 report use.
    ("fasting_glucose",             r"^fasting glucose",             r"tolerance|urine"),
    ("FBG",                         r"^fasting glucose",             r"tolerance|urine"),
    ("glucose, fasting",            r"^fasting glucose",             r"tolerance|urine"),
    ("血糖(空腹)",                    r"^fasting glucose",             r"tolerance|urine"),
    ("systolic_blood_pressure",     r"systolic blood pressure",      r""),
    ("total_cholesterol",           r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    # ── fifth sweep: the surfaces a report actually prints ───────────────────
    #
    # (a) "名称(缩写)" — the single commonest shape on a Chinese report.
    #     Handled as a CLASS in engine.py, not row by row: strip the trailing
    #     parenthetical, resolve both halves, and refuse when they disagree
    #     (see MUST_NOT_RESOLVE for the refusals).
    ("空腹血糖(GLU)",                 r"glucose",                      r"tolerance|urine"),
    # The two halves disagree on code but share the COMPONENT analyte head
    # (`Glucose^post CFst` vs `Glucose`), so the stem — the more specific
    # framing — wins instead of the term being refused.
    ("总胆固醇(TC)",                  r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("甘油三酯(TG)",                  r"triglyceride",                 r""),
    ("低密度脂蛋白胆固醇(LDL-C)",         r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("丙氨酸氨基转移酶(ALT)",            r"alanine aminotransferase",     r""),
    ("血小板计数（PLT）",               r"platelet",                     r""),
    ("糖化血红蛋白(HbA1c)",            r"hemoglobin a1c",               r""),
    ("尿素氮(BUN)",                  r"urea nitrogen",                r""),
    # (b) The abbreviation column itself. HGB sits one alias away from 4548-4
    #     (HbA1c) and HCT from 1992-7 (Calcitonin) — the 血红蛋白->HbA1c
    #     near-miss wearing the short code instead of the word.
    ("HGB",                         r"^hemoglobin \[",               r"a1c|glycated"),
    ("Hb",                          r"^hemoglobin \[",               r"a1c|glycated"),
    ("HCT",                         r"hematocrit",                   r"calcitonin"),
    ("PLT",                         r"platelet",                     r""),
    ("RBC",                         r"erythrocyte|red blood cell",   r""),
    ("WBC",                         r"leukocyte|white blood cell",   r""),
    ("TC",                          r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("TG",                          r"triglyceride",                 r""),
    ("GLU",                         r"glucose",                      r"tolerance|urine"),
    ("Cr",                          r"creatinine",                   r"clearance|urine"),
    ("UA",                          r"urate|uric acid",              r"urine"),
    ("TP",                          r"^protein \[",                  r"urine"),
    ("CK",                          r"creatine kinase",              r""),
    # (c) Codepoints that look identical on screen — each is one invisible
    #     character away from a key that exists.
    ("ＦＢＧ",                        r"glucose",                      r"tolerance|urine"),
    ("LDL–C",                       r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    # (d) 日本語 健康診断 names the ja.tsv sweep does not reach — it carries
    #     diseases and organisms, not observations (3% of its targets are keys
    #     the observation index can look up).
    ("LDLコレステロール",               r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("HDLコレステロール",               r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("血清鉄",                       r"^iron \[",                     r"binding|saturation"),
    ("フェリチン",                    r"ferritin",                     r""),
]

# Terms that must stay UNRESOLVED. A confident wrong code is worse than an
# honest miss, so "answers nothing" is a behaviour worth pinning too.
MUST_NOT_RESOLVE: list[tuple[str, str]] = [
    # Category names with no panel code of their own. LOINC's lipid panels
    # differ by which children they include, so picking one is picking an
    # assumption about what was ordered.
    #
    # Note what is deliberately NOT here: blood pressure, in any of its four
    # spellings. It reads like the same case and is the opposite one — it HAS a
    # panel code (85354-9, the one FHIR R4's vital-signs profile mandates), and
    # a panel code is how you tell a caller "expect components", which is the
    # very thing a refusal would be trying to say. The four spellings are pinned
    # as positive cases above.
    ("lipid panel", "four analytes, not one observation"),
    ("血脂", "the same lipid panel in Chinese"),
    ("血脂肪", "the same lipid panel, 台灣 wording — needs its own refusal row: "
               "the zh-Hant fold reaches nothing here, and without the row the "
               "semantic tier would answer it"),
    ("绝对不存在的指标名xyzzy", "pure nonsense must never resolve"),
    # "名称(缩写)" where the two halves mean DIFFERENT tests. The parenthetical
    # strip must not silently prefer the stem: filing an HbA1c reading into the
    # glucose series is the same class of harm as 血红蛋白 -> HbA1c.
    ("血糖(HbA1c)", "stem is glucose, parenthetical is HbA1c — they disagree"),
    ("胆固醇(HDL-C)", "stem is total cholesterol, parenthetical is HDL"),
    # Stem is the BP panel, parenthetical is systolic. Unlike the two above, the
    # halves do not contradict — the parenthetical NARROWS the stem, and 8480-6
    # would be a defensible answer. Refusing anyway, because proving "narrows"
    # needs LOINC panel membership, which the shipped axis table does not carry;
    # without it the rule would be "prefer the parenthetical", which is exactly
    # what breaks 血糖(HbA1c). Written up in roadmap.md.
    ("血压(收缩压)", "stem is the BP panel, parenthetical is one of its members"),
    # NOTE what is deliberately NOT here: CA / PT / MG. An ambiguous
    # abbreviation is not the same case as a panel name. A panel has no correct
    # single observation code; an abbreviation has a reading that dominates
    # ordinary use, and refusing it helps nobody. They are pinned as positive
    # cases above instead.
]

# Ratchet. Every case above is expected to pass, so the floor is 1.0; raise the
# case count, never lower this number to make a red build green. A drop here
# means real users started getting worse answers than they did yesterday.
#
# Baseline for the record: this set scored 34% (32/94) against the bare alias
# index, before `resolver_overrides.tsv` existed.
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


# ── unit-consistent codes: resolve_reading vs resolve ────────────────────────
# LOINC codes the unit into the identity, so total cholesterol is 2093-3 in
# mg/dL and 14647-2 in mmol/L — different codes for the same measurement. The
# alias table answers with whichever one it points at, so without the axis walk
# a mmol/L reading lands on the mass-concentration code and every consumer
# downstream believes a two-unit series is one unit. `resolve_reading` walks
# the axis table to the sibling with the same FULL component and a PROPERTY in
# the unit's family.
#
# (term, value, unit, expected code, why)
READING_CASES: list[tuple[str, str, str, str, str]] = [
    ("total cholesterol", "5.0", "mmol/L", "14647-2", "switches to [Moles/volume]"),
    ("total cholesterol", "193", "mg/dL",  "2093-3",  "already consistent, untouched"),
    # The challenge suffix must survive: `Glucose^post CFst`, not `Glucose`, or
    # a fasting reading decays into plain glucose on the way to mmol/L.
    ("空腹血糖",          "5.6", "mmol/L", "14771-0", "fasting qualifier preserved"),
    ("空腹血糖",          "100", "mg/dL",  "1558-6",  "already consistent"),
    ("肌酐",             "95",  "umol/L", "14682-9", "creatinine to substance conc"),
    ("尿酸",             "420", "umol/L", "14933-6", "urate to substance conc"),
    ("总胆红素",          "17",  "umol/L", "14631-6", "bilirubin.total to substance conc"),
    ("甘油三酯",          "1.7", "mmol/L", "14927-8", "triglyceride to substance conc"),
    # No unit, an unparseable unit, or a unit already in the right family must
    # all leave the answer exactly where `resolve` put it.
    ("HGB",             "140", "g/L",    "718-7",   "g/L is already MCnc"),
    ("hematocrit",      "42",  "%",      "4544-3",  "% has no substance sibling"),
    ("白细胞计数",         "6.5", "10*9/L", "26464-8", "count already NCnc"),
]


@pytest.mark.parametrize("term, value, unit, expected, why", READING_CASES)
def test_resolve_reading_is_unit_consistent(term, value, unit, expected, why):
    from mirobody.engine import resolve_reading

    got = resolve_reading(term, value, unit)
    assert got.loinc == expected, f"{term} @ {unit}: {why} — got {got.loinc} {got.canonical!r}"
    # A variant switch is a table lookup, not a guess: it stays identity-grade.
    assert got.method == "lexical"


def test_resolve_reading_without_a_unit_matches_resolve():
    """No unit means no constraint, not a different answer."""
    from mirobody.engine import resolve, resolve_reading

    for term in ("total cholesterol", "空腹血糖", "HGB", "绝对不存在的指标名xyzzy"):
        assert resolve_reading(term).loinc == resolve(term).loinc
        assert resolve_reading(term, "5.0", "").loinc == resolve(term).loinc
        assert resolve_reading(term, "5.0", "not-a-unit").loinc == resolve(term).loinc


# ── non-numeric readings: the value's KIND picks the scale ────────────────────
# Half of what a report prints is not a number, and 38,687 of the shipped axis
# rows are not Qn (25,156 Ord · 7,859 Nom · 4,258 SemiQn · 1,414 OrdQn). A
# 阴性/阳性/++ result belongs on a [Presence]/[Type] code; answering it with a
# mass-concentration code files a dipstick into a quantitative assay.
#
# (term, value, expected code, note)
QUALITATIVE_READINGS: list[tuple[str, str, str, str]] = [
    ("尿糖",           "阴性", "2349-9",  "dipstick negative -> [Presence], not [Mass/volume]"),
    ("尿蛋白",         "阴性", "2887-8",  "already a Presence code, untouched"),
    ("尿蛋白",         "++",   "2887-8",  "graded ordinal, same code"),
    ("尿酮体",         "阴性", "33903-6", "not 49779-2 [Mass/volume]"),
    ("尿隐血",         "阴性", "5794-3",  "dipstick presence code"),
    ("尿亚硝酸盐",     "阴性", "32710-6", "dipstick presence code"),
    ("尿白细胞酯酶",   "阴性", "5799-2",  "dipstick presence code"),
    ("便隐血",         "阴性", "2335-8",  "already a Presence code"),
    ("乙肝表面抗原",   "阴性", "5196-1",  "serology screen"),
    ("丙肝抗体",       "阴性", "13955-0", "serology screen"),
    ("类风湿因子",     "阴性", "33910-1", "not 11572-5 [Units/volume], the numeric form's code"),
    ("妊娠试验",       "阳性", "2118-8",  "not 19080-1 [Units/volume]"),
    ("血型",           "O",    "883-9",   "not 50962-0, an antibody TITRE"),
    ("Rh血型",         "阳性", "10331-7", "Rh [Type], not a titre"),
    ("urine glucose",  "negative", "2349-9",  "same rule, English"),
    ("rheumatoid factor", "negative", "33910-1", "same rule, English"),
    ("blood type",     "O+",   "883-9",   "same rule, English"),
]


@pytest.mark.parametrize("term, value, expected, note", QUALITATIVE_READINGS)
def test_a_non_numeric_value_picks_a_non_numeric_code(term, value, expected, note):
    from mirobody.engine import resolve_reading

    got = resolve_reading(term, value, None)
    assert got.loinc == expected, f"{term} = {value}: {note} — got {got.loinc} {got.canonical!r}"
    assert got.method == "lexical"          # a table lookup, still identity-grade


@pytest.mark.parametrize(
    "term, value, unit, expected",
    [
        # The SAME indicator reported as a number goes the other way. The value
        # decides, not the name — which is the whole point.
        ("尿糖", "5.6", "mmol/L", "15076-3"),
        ("类风湿因子", "12", "IU/mL", "11572-5"),
    ],
)
def test_the_same_indicator_as_a_number_stays_quantitative(term, value, unit, expected):
    from mirobody.engine import resolve_reading

    assert resolve_reading(term, value, unit).loinc == expected


def test_a_value_of_an_unknown_kind_places_no_constraint():
    """Narrative or empty values must not silently move the answer."""
    from mirobody.engine import resolve, resolve_reading

    for value in (None, "", "见报告", "clear yellow fluid"):
        assert resolve_reading("尿蛋白", value, None).loinc == resolve("尿蛋白").loinc

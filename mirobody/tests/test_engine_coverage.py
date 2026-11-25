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

    pytest tests/test_engine_coverage.py -v -s

Contributing: a term that misses here is a one-line fix in
``mirobody/res/loinc/resolver_overrides.tsv`` — that file, not
``aliases_src/*_curated.tsv``, is the one this resolver reads at runtime (the
curated files are inputs to the bundle BUILD; see the overrides header for why
the two are separate). Add the row, add the case here, and the score goes up.
"""

from __future__ import annotations

import os
import pathlib
import re

import pytest

import mirobody

#: The bundle inside the PACKAGE, found through `mirobody.__file__`: this
#: module has moved once and a `dirname(dirname(__file__))` walk pointed
#: at `mirobody/tests/res/` afterwards — every case then SKIPPED, green
#: and meaningless.
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
    # `CHOL` answered calculated LDL and `总胆固醇(CHOL)` was refused as two
    # analytes. The lipoprotein spellings keep their own codes.
    ("CHOL",                        r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("总胆固醇(CHOL)",                r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("LDL-CHOL",                    r"cholesterol.*LDL|LDL.*cholesterol", r"HDL|ratio"),
    ("HDL-CHOL",                    r"cholesterol.*HDL|HDL.*cholesterol", r"non.?HDL|LDL|ratio"),
    # ── complete blood count ─────────────────────────────────────────────────
    ("hemoglobin",                  r"hemoglobin",                   r"A1c|glycated"),
    # The British spelling answered 4548-4 Hemoglobin A1c, and `Haemoglobin
    # (Hb)` was refused as two analytes.
    ("haemoglobin",                 r"hemoglobin",                   r"A1c|glycated"),
    ("Haemoglobin (Hb)",            r"hemoglobin",                   r"A1c|glycated"),
    ("hematocrit",                  r"hematocrit",                   r""),
    ("white blood cell count",      r"leukocyte|white blood cell",   r""),
    ("red blood cell count",        r"erythrocyte|red blood cell",   r""),
    ("platelet count",              r"platelet",                     r""),
    # ── metabolic panel ──────────────────────────────────────────────────────
    ("glucose",                     r"glucose",                      r"tolerance|challenge"),
    ("fasting glucose",             r"^fasting glucose",             r"tolerance"),
    # A post-meal reading is its own code. `餐后血糖` answered plain glucose
    # (2339-0) and the English long form off a real report answered nothing.
    ("postprandial blood glucose",  r"post meal",                    r"2 hours|1 hour|tolerance"),
    ("Postprandial Blood Glucose-PBG", r"post meal",                 r"2 hours|1 hour|tolerance"),
    ("2-hour postprandial glucose",  r"2 hours post meal",           r""),
    ("餐后血糖",                      r"post meal",                    r"2 hours|1 hour|tolerance"),
    ("creatinine",                  r"creatinine",                   r"clearance|urine"),
    # A printed English panel writes the specimen into the name. Both of these
    # answered 44760-7 "Model for end-stage liver disease score" until
    # 2026-09-14 — the MELD long name names creatinine, bilirubin and INR, so
    # containment matched, and the two analytes MERGED onto one code.
    ("Serum Creatinine",            r"creatinine",                   r"clearance|urine|end-stage"),
    ("blood urea nitrogen",         r"urea nitrogen",                r""),
    ("sodium",                      r"sodium",                       r""),
    ("potassium",                   r"potassium",                    r""),
    ("albumin",                     r"albumin",                      r"urine|globulin ratio"),
    ("transthyretin",                r"prealbumin",                   r"binding|mass\]"),
    # Each answered a binding protein: 14016-0 (thyroxine bound to TBG) or,
    # for `GLB`, 13967-5 (SHBG). A liver panel's globulin joined a thyroid series.
    ("球蛋白",                        r"^globulin \[",                 r"binding|albumin"),
    ("Globulin",                    r"^globulin \[",                 r"binding|albumin"),
    ("GLB",                         r"^globulin \[",                 r"binding|albumin"),
    # `Glycated albumin` was unresolved in English while `糖化白蛋白` resolved
    # (a tester's report, F010): the corpus's own English synonym for
    # 13873-5 was never itself a corpus key, only pointed at by the zh.tsv
    # redirect. Both languages now name the same row.
    ("Glycated albumin",            r"albumin.*glycated|glycated.*albumin", r""),
    ("Glycosylated albumin",        r"albumin.*glycated|glycated.*albumin", r""),
    ("糖化白蛋白",                    r"albumin.*glycated|glycated.*albumin", r""),
    ("total bilirubin",             r"bilirubin",                    r"direct|conjugated"),
    ("Serum Total Bilirubin",       r"bilirubin",                    r"direct|conjugated|end-stage"),
    ("uric acid",                   r"urate|uric acid",              r""),
    # ── liver enzymes ────────────────────────────────────────────────────────
    ("ALT",                         r"alanine aminotransferase",     r""),
    ("AST",                         r"aspartate aminotransferase",   r""),
    # Report spellings that resolved to nothing: a doubled `氨基转氨酶`, and
    # the GGT Greek letter printed as `r`.
    ("丙氨酸氨基转氨酶",                r"alanine aminotransferase",     r""),
    ("天门冬氨酸氨基转氨酶",              r"aspartate aminotransferase",   r""),
    ("γ-谷氨酰基转肽酶",               r"gamma glutamyl transferase",   r""),
    ("r-GT",                        r"gamma glutamyl transferase",   r""),
    # Kept exactly as on main (F011 pin): `γ` is Greek, not Latin, so the
    # hyphen here is never touched by `lexical.fold_script_boundary`.
    ("γ-GTP",                       r"gamma glutamyl transferase",   r""),
    # A hyphenated tail that names a COMPONENT of the parent analyte. The
    # hyphen rule used to strip it and answer the parent: total CK for the
    # MB isoenzyme, total LDH for LDH1, total ALP for bone ALP.
    ("Creatine Kinase-MB",          r"creatine kinase\.MB",          r"Creatine kinase \["),
    # Glued CamelCase from a LIS export resolved to nothing; `eGFR` and
    # `HbA1c` must not be split by the same rule.
    ("TotalCholesterol",            r"^cholesterol \[",              r"LDL|HDL|VLDL"),
    ("HemoglobinA1c",               r"hemoglobin a1c",               r""),
    ("AlanineAminotransferase-ALT", r"alanine aminotransferase",     r""),
    ("CreatineKinase-MB",           r"creatine kinase\.MB",          r"Creatine kinase \["),
    ("Creatine Kinase MB",          r"creatine kinase\.MB",          r"Creatine kinase \["),
    ("肌酸激酶同工酶",                 r"creatine kinase\.MB",          r"interpretation"),
    ("Lactate Dehydrogenase-LDH1",  r"lactate dehydrogenase 1",      r""),
    ("LDH-5",                       r"lactate dehydrogenase 5",      r""),
    ("Alkaline Phosphatase-BALP",   r"alkaline phosphatase\.bone",   r""),
    ("BALP",                        r"alkaline phosphatase\.bone",   r""),
    ("bone-specific alkaline phosphatase", r"alkaline phosphatase\.bone", r""),
    ("骨碱性磷酸酶",                   r"alkaline phosphatase\.bone",   r""),
    # A letter-hyphen-number tail is a series member the index spells joined.
    # `Vitamin D-3` used to fall through to the bare stem and answer GALAD.
    ("Vitamin D-3",                 r"vitamin D3",                   r"GALAD"),
    ("Vitamin B-12",                r"cobalamin",                    r"GALAD"),
    ("Complement C-3",              r"complement C3",                r"Sc5b-9"),
    ("alkaline phosphatase",        r"alkaline phosphatase",         r""),
    # ── endocrine & vitamins ─────────────────────────────────────────────────
    ("HbA1c",                       r"hemoglobin a1c",               r""),
    # Kept exactly as on main (F011 pin): the bracket is a METHOD
    # qualifier, unrelated to the script-boundary fold.
    ("HbA1c(NGSP)",                 r"hemoglobin a1c",               r""),
    ("TSH",                         r"thyrotropin|thyroid stimulating", r""),
    ("free T4",                     r"thyroxine",                    r""),
    ("vitamin D",                   r"vitamin d|calcidiol|hydroxyvitamin", r""),
    ("vitamin B12",                 r"cobalamin|vitamin b12",        r""),
    ("ferritin",                    r"ferritin",                     r""),
    # `FER` answered 2498-4 Iron: French LOINC names the iron component `Fer`.
    ("FER",                         r"ferritin",                     r"^iron"),
    ("Ferritin (FER)",              r"ferritin",                     r"^iron"),
    # `TT4` answered 3024-7, free thyroxine; the antibodies resolved to nothing.
    ("TT4",                         r"^thyroxine \(T4\)",           r"free"),
    ("总甲状腺素(TT4)",               r"^thyroxine \(T4\)",           r"free"),
    ("Total thyroxine",             r"^thyroxine \(T4\)",           r"free"),
    ("TT3",                         r"^triiodothyronine \(T3\)",    r"free"),
    ("总三碘甲状腺原氨酸(TT3)",        r"^triiodothyronine \(T3\)",    r"free"),
    ("Total triiodothyronine",      r"^triiodothyronine \(T3\)",    r"free"),
    ("超敏促甲状腺激素",              r"thyrotropin",                  r"receptor"),
    ("抗TPO抗体",                    r"thyroperoxidase ab",           ""),
    ("TPO抗体",                      r"thyroperoxidase ab",           ""),
    ("抗甲状腺过氧化物酶抗体",          r"thyroperoxidase ab",           ""),
    ("Thyroid peroxidase antibody", r"thyroperoxidase ab",           ""),
    ("抗TG抗体",                     r"thyroglobulin ab",             ""),
    ("TG-Ab",                       r"thyroglobulin ab",             r"triglyceride"),
    ("抗甲状腺球蛋白抗体",              r"thyroglobulin ab",             ""),
    ("Thyroglobulin antibody",      r"thyroglobulin ab",             ""),
    ("促甲状腺素受体抗体",              r"thyrotropin receptor ab",      r"fetus"),
    # `TRAb` answered 63363-6, the antibody in blood from a fetus.
    ("TRAb",                        r"thyrotropin receptor ab",      r"fetus"),
    # Fasting insulin answered nothing in any spelling (a user's checkup,
    # 2026-11-11): the fasting code is a TIME sibling the name never reached.
    ("空腹胰岛素",                    r"^insulin .*--fasting",         r""),
    ("Insulin, Fasting",            r"^insulin .*--fasting",         r""),
    ("胰岛素(空腹)",                   r"^insulin .*--fasting",         r""),
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
    # Third sweep: names off production rows migrated out of th_series_data.
    # The measure word ("count", "percentage", "计数", "数") and the specimen
    # prefix ("serum") stood between the report and the index.
    ("monocyte count",              r"monocytes",                    r""),
    ("neutrophil percentage",       r"neutrophils",                  r""),
    ("absolute lymphocyte count",   r"lymphocytes",                  r""),
    ("中性粒细胞计数",                r"neutrophils",                  r""),
    ("单核细胞数",                    r"monocytes",                    r""),
    ("白细胞总数",                    r"leukocytes",                   r""),
    ("serum cystatin c",            r"cystatin c",                   r""),
    ("serum uric acid",             r"urate",                        r""),
    ("mean corpuscular volume",     r"MCV|mean corpuscular volume",  r""),
    ("mean platelet volume",        r"platelet.*entitic mean volume", r""),
    ("mean corpuscular hemoglobin", r"^MCH\b",                        r"MCHC"),
    ("mean corpuscular hemoglobin concentration", r"MCHC",           r""),
    ("total calcium",               r"^calcium \[",                  r"ionized"),
    ("inorganic phosphorus",        r"phosphate",                    r""),
    ("estimated glomerular filtration rate", r"glomerular filtration rate", r""),
    ("lipoprotein a",               r"lipoprotein a",                r""),
    ("red blood cell distribution width", r"distwidth|distribution width", r""),
    # `FT3` in the index is T3 resin uptake, a different test with the same
    # letters; the overrides send every spelling of free T3 to the component.
    ("FT3",                         r"triiodothyronine.*free|free.*triiodothyronine|T3.*free", r"uptake|T3RU"),
    ("free T3",                     r"triiodothyronine.*free|free.*triiodothyronine|T3.*free", r"uptake|T3RU"),
    ("游离T3",                       r"triiodothyronine.*free|free.*triiodothyronine|T3.*free", r"uptake|T3RU"),
    ("红细胞计数",                   r"erythrocyte|red blood cell",   r""),
    # Both answered 24360-0, the hemoglobin and hematocrit PANEL, which no
    # unit admits: a hematocrit printed under either name was never coded.
    ("红细胞比容",                   r"hematocrit",                   r"panel"),
    ("红细胞比积",                   r"hematocrit",                   r"panel"),
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
    ("総ビリルビン",                  r"bilirubin",                    r"direct|conjugated"),
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
    # The stages and the skin temperature a watch reports: LOINC 2.75 names
    # them and no alias table reached the everyday spelling.
    ("深睡",                        r"deep sleep duration",          r""),
    ("REM sleep",                   r"REM sleep duration",           r""),
    ("浅睡时长",                     r"light sleep duration",         r""),
    ("skin temperature",            r"body surface temperature",     r""),
    ("皮肤温度",                     r"body surface temperature",     r""),
    ("sleep latency",               r"falling asleep",               r""),
    ("入睡潜伏期",                    r"falling asleep",               r""),
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
    # Bare column headers: `Height` answered left kidney height by ultrasound
    # and `Systolic` a peak pressure during a septal defect.
    ("Height",                      r"^body height",                 r"kidney|testis|percentile"),
    ("Height (cm)",                 r"^body height",                 r"kidney|testis|percentile"),
    ("Systolic",                    r"^systolic blood pressure",     r"peak|septal"),
    ("Diastolic",                   r"^diastolic blood pressure",    r""),
    # ── fifth sweep: the surfaces a report actually prints ───────────────────
    #
    # (a) "名称(缩写)" — the single commonest shape on a Chinese report.
    #     Handled as a CLASS in engine/resolver.py, not row by row: strip the trailing
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
    # (d) 日本語 健康診断 names no alias table reaches — it carries
    #     diseases and organisms, not observations (3% of its targets are keys
    #     the observation index can look up).
    ("LDLコレステロール",               r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("HDLコレステロール",               r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("血清鉄",                       r"^iron \[",                     r"binding|saturation"),
    ("フェリチン",                    r"ferritin",                     r""),
    # A half-width hyphen (or a plain space) between the Latin abbreviation
    # and the katakana resolves the same as the fused spelling above
    # (a tester's report, F011): `lexical.fold_script_boundary` folds the
    # separator away as a surface variant, tried after the fused spelling
    # has already missed.
    ("LDL-コレステロール",              r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    ("HDL-コレステロール",              r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("LDL コレステロール",              r"cholesterol.*LDL|LDL.*cholesterol", r"HDL"),
    # ── Russian: as a Russian lab report prints it (МЕДСИ, INVITRO, Moscow
    #    state clinics). Cyrillic has no variants of its own in the release,
    #    so every term here lands through resolver_overrides.tsv. Collected
    #    from real reports, 2019-2026. Note the shapes: «Билирубин прямой» is
    #    three tokens of pure Cyrillic, and «Нейтрофилы (общ.число)» carries a
    #    parenthetical qualifier the way 空腹血糖(GLU) carries an abbreviation —
    #    but here it MEANS "share of the total", so the row maps it to the
    #    fraction, never the absolute count.
    ("СОЭ",                        r"sedimentation",                r""),
    ("Тромбоциты",                  r"platelet",                     r""),
    ("Нейтрофилы (общ.число)",      r"neutrophils/leukocytes",       r"\[#"),
    ("Нейтрофилы, абс.",            r"neutrophils \[#",              r"/leukocytes"),
    ("Средний объем эритроцитов",    r"mcv|mean corpuscular volume",  r"platelet"),
    ("MCHC (ср. конц. Hb в эр.)",    r"mchc",                         r""),
    ("Билирубин общий",             r"bilirubin",                    r"direct|conjugated|indirect"),
    ("Билирубин прямой",            r"bilirubin\.direct|conjugated", r"indirect"),
    ("Билирубин непрямой",          r"bilirubin\.indirect|indirect", r"\.direct|conjugated"),
    ("АЛТ (Аланиновая трансаминаза)", r"alanine aminotransferase",   r""),
    ("АСТ (Аспарагиновая трансаминаза)", r"aspartate aminotransferase", r""),
    ("Глюкоза",                     r"glucose",                      r"tolerance|urine|a1c"),
    ("Общий белок",                 r"protein",                      r"urine"),
    ("Мочевина",                    r"urea",                         r"urine"),
    ("Мочевая кислота",             r"urate|uric acid",              r"urine"),
    ("Креатинкиназа",               r"creatine kinase",              r""),
    ("Щелочная фосфатаза",          r"alkaline phosphatase",         r""),
    ("Холестерин общий",            r"cholesterol",                  r"LDL|HDL|VLDL|non.?HDL"),
    ("Кальций ионизированный",       r"ionized",                      r""),
    ("Фосфор неорганический",       r"phosphate",                    r""),
    ("С-реактивный белок",          r"c reactive",                   r""),
    ("Ревматоидный фактор",         r"rheumatoid factor",            r""),
    ("Иммуноглобулин Е (IgE)",      r"total ige|immunoglobulin e",   r"specific"),
    ("ТТГ",                         r"thyrotropin",                  r""),
    ("Тиреотропный гормон (ТТГ)",    r"thyrotropin",                  r""),
    ("Антитела к тиреоглобулину (анти-ТГ)", r"thyroglobulin ab",      r"\[mass"),
    ("Антитела к тиреопероксидазе (анти-ТПО)", r"thyroperoxidase ab", r""),
    ("Т4 свободный",                r"thyroxine.*free|free.*thyroxine", r""),
    ("Витамин 25(ОН) D",            r"25.hydroxyvitamin",            r""),
    ("Витамин D (25-OH)",           r"25.hydroxyvitamin",            r""),
    ("Витамин В12",                 r"cobalamin|vitamin b12",        r""),
    ("Фолиевая кислота",            r"folate",                       r""),
    # ── Turkish: as a Turkish lab report prints it. Both cases, because the
    #    all-caps half was the bug (discussion #137): `İ` casefolds to `i` plus a
    #    combining dot above and `I` to `i` where Turkish reads `ı`, so neither
    #    matched a Turkish row until `lexical.turkish_i_variants` was tried.
    ("Total Kolesterol",            r"cholesterol",                  r"LDL|HDL|VLDL|non.?HDL"),
    ("HDL Kolesterol",              r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("Trigliserit",                 r"triglyceride",                 r""),
    ("Glukoz (açlık)",              r"fasting glucose",              r"tolerance|urine|post"),
    ("TOTAL KOLESTEROL",            r"cholesterol",                  r"LDL|HDL|VLDL|non.?HDL"),
    ("HDL KOLESTEROL",              r"cholesterol.*HDL|HDL.*cholesterol", r"LDL"),
    ("TRİGLİSERİT",                 r"triglyceride",                 r""),
    ("GLUKOZ (AÇLIK)",              r"fasting glucose",              r"tolerance|urine|post"),
    # `ürik asit`/`açlık kan şekeri` were missing their own row (Discussion
    # #137); capitalised, they reach it the same way `GLUKOZ (AÇLIK)` does.
    ("ürik asit",                   r"urate|uric acid",              r""),
    ("ÜRİK ASİT",                   r"urate|uric acid",              r""),
    ("Ürik asit",                   r"urate|uric acid",              r""),
    ("açlık kan şekeri",            r"fasting glucose",              r"tolerance|urine|post"),
    ("AÇLIK KAN ŞEKERİ",            r"fasting glucose",              r"tolerance|urine|post"),
    # Estonian (Synlab Eesti): the three vitamin spellings a real report
    # prints that the release variants miss.
    ("Vitamiin D (25-OH)",         r"25.hydroxyvitamin",            r""),
    ("Vitamiin B12",               r"cobalamin|vitamin b12",        r""),
    ("Holotranskobalamiin",        r"transcobalamin",               r""),
    # ── a CJK name run straight into its abbreviation (#305) ────────────────
    # A real checkup report that printed its whole CBC and urinalysis
    # sections this way: the Chinese name followed directly by the Latin
    # abbreviation, no bracket, no space. `OfflineResolver._find_seam`
    # answers these the same way the bracket shape already did, but only
    # when BOTH halves agree (see MUST_NOT_RESOLVE for the abstentions this
    # same split must not turn into guesses).
    ("白细胞WBC",                     r"leukocytes",                   r""),
    ("红细胞RBC",                     r"erythrocytes",                 r""),
    ("血红蛋白HGB",                    r"^hemoglobin \[",               r"a1c|glycated"),
    ("血小板PLT",                     r"platelets",                    r""),
    # Already correct on main; pinned so the unification does not disturb them.
    ("游离T3",                       r"triiodothyronine.*free",       r""),
    ("维生素D",                       r"25.hydroxyvitamin",            r""),
    ("尿微量白蛋白mALB",                 r"microalbumin",                 r""),
    ("糖化血红蛋白HbA1c",                r"hemoglobin a1c",               r""),
    ("促甲状腺激素TSH",                  r"thyrotropin",                  r""),
    ("甲胎蛋白AFP",                    r"alpha-1-fetoprotein",          r""),
    # The common CBC and urinalysis abbreviations (#305), glued to a Chinese
    # stem that agrees: each is the catalogue's own reading, not a guess.
    ("淋巴细胞LYMPH",                  r"lymphocytes/leukocytes",       r""),
    ("单核细胞MONO",                   r"monocytes/leukocytes",         r""),
    ("嗜碱性粒细胞BASO",                 r"basophils/leukocytes",         r""),
    ("红细胞压积HCT",                   r"^hematocrit",                  r""),
    ("平均红细胞体积MCV",                 r"mcv",                         r""),
    ("平均血红蛋白含量MCH",                r"^mch\b",                      r"mchc"),
    ("平均血红蛋白浓度MCHC",               r"mchc",                        r""),
    ("红细胞分布宽度RDW",                 r"distwidth",                    r""),
    ("平均血小板体积MPV",                 r"platelet.*entitic mean volume", r""),
    ("血小板分布宽度PDW",                 r"platelet distribution width",  r""),
    ("尿糖GLU",                      r"glucose.*urine",               r""),
    # ── 肌酐(Cre) must not refuse as two analytes (#306) ─────────────────────
    # `Cre` alone used to resolve to 73834-4, a carbapenem-resistant
    # culture, and veto the creatinine stem. `Cre` is now a curated alias
    # for Creatinine (resolver_overrides.tsv), the same as `CREA`/`Cr`, so
    # this agrees BY NAME now -- R2's measurement arbitration (see the
    # dedicated test) is for names the vocabulary has no alias for at all.
    ("Cre",                          r"^creatinine \[",               r""),
    ("CRE",                          r"^creatinine \[",               r""),
    ("肌酐(Cre)",                     r"^creatinine \[",               r""),
    ("肌酐(CREA)",                    r"^creatinine \[",               r""),
    ("肌酐(Cr)",                     r"^creatinine \[",               r""),
    # ── vital signs as a check-up or an outpatient note prints them ─────────
    # `Pulse` answered 8867-4 and `脉搏` nothing; `身長` (ja, height) answered
    # a transthyretin "half peak height" (1.6.2 release test).
    ("脉搏",                          r"^heart rate$",                 r""),
    ("脈搏",                          r"^heart rate$",                 r""),
    ("脉率",                          r"^heart rate$",                 r""),
    ("Pulse rate",                   r"^heart rate$",                 r""),
    ("脈拍",                          r"^heart rate$",                 r""),
    ("心拍数",                         r"^heart rate$",                 r""),
    ("腋温",                          r"axillary temperature",         r""),
    ("腰围",                          r"waist circumference",          r""),
    ("腹囲",                          r"waist circumference",          r""),
    ("身長",                          r"^body height$",                r"transthyretin"),
    ("身长",                          r"^body height$",                r"transthyretin"),
    ("最高血圧",                        r"^systolic blood pressure$",    r""),
    ("最低血圧",                        r"^diastolic blood pressure$",   r""),
    ("收缩期血压",                       r"^systolic blood pressure$",    r""),
    # ── PSA: total, not its density (15324-7) or free fraction (10886-0) ────
    ("前列腺特异性抗原",                    r"^prostate specific ag \[mass", r"free|prostate volume"),
    ("PSA",                          r"^prostate specific ag \[mass", r"free|prostate volume"),
    ("总前列腺特异性抗原",                   r"^prostate specific ag \[mass", r"free|prostate volume"),
    ("tPSA",                         r"^prostate specific ag \[mass", r"free|prostate volume"),
    ("fPSA",                         r"prostate specific ag free \[", r"total"),
    ("f/tPSA",                       r"free/prostate specific ag\.total", r""),
    # ── one abbreviation printed with its own hyphen (#322) ─────────────────
    # The half that resolved used to answer: `HBs-Ag` the surface ANTIBODY,
    # `Cys-C` an amino-acid screen, `NT-proBNP` BNP itself.
    ("Cys-C",                        r"^cystatin c \[",               r"cysteine|anserine"),
    ("CYS-C",                        r"^cystatin c \[",               r"cysteine|anserine"),
    ("血清胱抑素C",                      r"^cystatin c \[",               r""),
    ("HBs-Ag",                       r"surface Ag",                   r"surface Ab"),
    ("HBe-Ag",                       r"virus e Ag",                   r"virus e Ab"),
    ("Anti-HBs",                     r"surface Ab",                   r"surface Ag"),
    ("HBc-Ab",                       r"core Ab",                      r"core Ag"),
    ("Anti-HBe",                     r"virus e Ab",                   r"virus e Ag"),
    ("NT-proBNP",                    r"prohormone N-Terminal",        r""),
    ("UMA-CR",                       r"^albumin/creatinine",          r""),
    # The refusal is for a role word only: a method or standard tag after a
    # real abbreviation still answers through it (review of #331).
    ("HbA1c-NGSP",                   r"^hemoglobin a1c/",             r""),
    ("ANA-IFA",                      r"^nuclear .*Ab",                r""),
    # `Anti` is a prefix, not a role tail, and a role tail that agrees with
    # the answer keeps it (second review of #331).
    ("Anti-dsDNA",                   r"DNA double strand .*Ab",       r""),
    ("Anti-SSA",                     r"Sjogrens syndrome-A .*Ab",     r""),
    ("HAV-Ab",                       r"Hepatitis A virus .*Ab",       r""),
    ("Thyroglobulin Antibody-Ab",    r"^thyroglobulin Ab",            r""),
    ("Anti-TPO-Ab",                  r"^thyroperoxidase Ab",          r""),
    # ── vitamin D: one series whatever the spelling (#323) ──────────────────
    # 62292-8 (D2+D3) split a person's history from 1989-3 by spelling alone;
    # `25羟维生素D` and `25-OH维生素D` were pinned misses, the first because
    # its curated row targeted a phrase the index does not hold.
    ("25-hydroxyvitamin D",          r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("Total 25-hydroxyvitamin D",    r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("Vitamin D 25-OH",              r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-OH-D",                      r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-OH-D3",                     r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-OH-VitD",                   r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25羟维生素D",                     r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-OH维生素D",                   r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-羟维生素D",                    r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-羟基维生素D3",                  r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25羟基维生素D3",                   r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("血清25-羟基维生素D",                r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("25-羟基维生素D(25-OH-D)",         r"^25-hydroxyvitamin D3 \[",      r"D2"),
    ("维生素D3",                       r"^25-hydroxyvitamin D3 \[",      r"D2|cholecalciferol"),
    ("Витамин D",                    r"^25-hydroxyvitamin D3 \[",      r"D2"),
    # ── lipoprotein(a) and its neighbours (#332) ────────────────────────────
    # `脂蛋白(a)` answered total lipoproteins through its stem, `载脂蛋白(a)`
    # apolipoprotein A through case folding, `总脂蛋白` Lp(a) through zh.tsv.
    ("脂蛋白(a)",                       r"^lipoprotein a \[",            r"^lipoproteins"),
    ("脂蛋白（a）",                      r"^lipoprotein a \[",            r"^lipoproteins"),
    ("脂蛋白(A)",                       r"^lipoprotein a \[",            r"^lipoproteins"),
    ("血清脂蛋白(a)",                     r"^lipoprotein a \[",            r"^lipoproteins"),
    ("脂蛋白(a)(Lp(a))",                r"^lipoprotein a \[",            r"^lipoproteins"),
    ("载脂蛋白(a)",                      r"^apolipoprotein lpa",          r"^apolipoprotein a \["),
    ("载脂蛋白（a）",                     r"^apolipoprotein lpa",          r"^apolipoprotein a \["),
    ("总脂蛋白",                        r"^lipoproteins",                r"^lipoprotein a"),
    ("總脂蛋白",                        r"^lipoproteins",                r"^lipoprotein a"),
    ("总脂蛋白(a)",                     r"",                             r"^lipoprotein a"),
    # The bracket split this does not change: a lone letter still reaches
    # the element through the stem.
    ("钾(K)",                          r"^potassium",                   r""),
    ("磷(P)",                          r"^phosphate",                   r""),
    # ── printed names coded as another analyte (#343) ───────────────────────
    ("中性粒细胞#",                      r"^neutrophils \[#/volume\]",     r"/leukocytes"),
    ("中性粒细胞数",                      r"^neutrophils \[#/volume\]",     r"/leukocytes"),
    ("中性粒细胞计数",                     r"^neutrophils \[#/volume\]",     r"/leukocytes"),
    ("淋巴细胞#",                       r"^lymphocytes \[#/volume\]",     r"/leukocytes"),
    ("淋巴细胞数",                       r"^lymphocytes \[#/volume\]",     r"/leukocytes"),
    ("单核细胞#",                       r"^monocytes \[#/volume\]",       r"/leukocytes"),
    ("嗜酸性粒细胞#",                     r"^eosinophils \[#/volume\]",     r"/leukocytes"),
    ("嗜碱性粒细胞#",                     r"^basophils \[#/volume\]",       r"/leukocytes"),
    # The percentage spellings stay on the ratio.
    ("中性粒细胞",                       r"^neutrophils/leukocytes",      r""),
    ("淋巴细胞百分比",                     r"^lymphocytes/leukocytes",      r""),
    ("baPWV (left)",                 r"^pulse wave velocity",         r"thyrotropin"),
    ("baPWV (right)",                r"^pulse wave velocity",         r""),
    ("SBP",                          r"^systolic blood pressure",     r"binding globulin"),
    ("DBP",                          r"^diastolic blood pressure",    r"binding protein"),
    ("ACR",                          r"^albumin/creatinine",          r"brachial"),
    ("RhD",                          r"^rh \[type\]",                 r"fetal"),
    ("PAB",                          r"^prealbumin",                  r"aminobenzoate"),
    ("LIP",                          r"^lipase",                      r"subparticle"),
    ("总钙",                          r"^calcium \[",                  r"ionized"),
    ("部分凝血活酶时间",                    r"^aPTT",                        r"panel"),
    # The measured spirometry volume, not the predicted one printed beside it.
    ("FVC",                          r"^forced vital capacity \[volume\] respiratory system$", r"predicted"),
    ("用力肺活量",                       r"^forced vital capacity \[volume\] respiratory system$", r"predicted"),
    ("FEV1 % predicted",             r"^fev1 measured/predicted",     r"^fev1$"),
    ("FEV1占预计值%",                  r"^fev1 measured/predicted",     r"^fev1$"),
    ("FVC % predicted",              r"^fvc measured/predicted",      r"pre bronchodilation"),
    ("FVC占预计值%",                   r"^fvc measured/predicted",      r"pre bronchodilation"),
    ("尿胆红素",                        r"^bilirubin\.total \[presence\] in urine", r""),
    # Bare `FEV1` and `FEV1/FVC` are their codes' whole long names; the rank
    # liked FEV1/FVC Predicted for both. A predicted pick with nothing printed
    # saying predicted moves to its measured sibling (`_switch_from_predicted`).
    ("FEV1",                         r"^fev1$",                       r"predicted"),
    ("FEV1/FVC",                     r"^fev1/fvc$",                   r"predicted"),
    ("Forced vital capacity",        r"^forced vital capacity \[volume\] respiratory system$", r"predicted"),
    ("DLCO",                         r"^diffusion capacity\.carbon monoxide$", r"predicted"),
    ("PEF",                          r"^maximum expiratory gas flow", r"predicted"),
    ("凝血酶时间",                       r"^thrombin time$",              r"interpretation"),
    # #357: alone, each answered ANOTHER analyte.
    ("BAS#",                         r"^basophils \[#/volume\]",      r"mycobacterium"),
    ("MON%",                         r"^monocytes/leukocytes",        r"fibrin"),
    ("ALC",                          r"^lymphocytes \[#/volume\]",    r"ethanol"),
    ("DIA",                          r"^diastolic blood pressure",    r"inhibin"),
    ("SYS",                          r"^systolic blood pressure",     r"gradient"),
    ("GLOB",                         r"^globulin",                    r"binding"),
    ("TEMP",                         r"^body temperature",            r"oxygen"),
    ("BIL-T",                        r"^bilirubin\.total",            r"weight"),
    ("BIL-D",                        r"^bilirubin\.direct",           r"weight"),
    ("FAT%",                         r"^percentage of body fat",      r"mass"),
    ("β2M",                          r"^beta-2-microglobulin \[mass/volume\] in serum", r"tumor"),
    ("Non-HDL",                      r"^cholesterol non hdl",         r""),
    ("LBC",                          r"^general categories .* cervical", r"lamellar"),
    ("HPV16",                        r"^human papilloma virus 16 dna", r"\+"),
    ("HPV18",                        r"^human papilloma virus 18 dna", r" ag "),
    ("Other HR HPV",                 r"^human papilloma virus 31\+33", r"16\+18"),
    ("ABO",                          r"^abo group",                   r"reverse"),
    ("Muscle",                       r"^body muscle mass",            r"gene"),
    ("CPK",                          r"^creatine kinase \[",          r"\.mb"),
    ("凝血因子Ⅰ",                       r"^fibrinogen",                  r"calcium"),
    ("hs-cTnI（超敏）",                  r"^troponin i\.cardiac .*high sensitivity", r"reactive"),
    ("前列腺特异抗原",                     r"^prostate specific ag \[",     r"free"),
    ("游离三碘甲腺原氨酸",                   r"^triiodothyronine \(t3\) free", r"resin"),
    ("游离T3*",                        r"^triiodothyronine \(t3\) free", r"resin"),
    ("红细胞总数",                       r"^erythrocytes \[#/volume\]",   r"pitted"),
    ("血清淀粉酶",                       r"^amylase \[enzymatic",          r"fld"),
    # A glued half that names two tests is read by the other half: the stem
    # says plateletcrit, so `PCT` beside it is plateletcrit too.
    ("血小板压积PCT",                    r"^plateletcrit",                r""),
    ("血小板压积(PCT)",                  r"^plateletcrit",                r""),
    ("液基薄层细胞学(TCT)",                r"^general categories",          r"thrombin"),
    ("白细胞总数(TLC)",                   r"^leukocytes \[#/volume\]",     r"taurolithocholate"),
    # #357: standard report wording that answered nothing.
    ("体质量指数",                       r"^body mass index",             r""),
    ("糖链抗原19-9",                     r"^cancer ag 19-9",              r""),
    ("补体第三成分",                      r"^complement c3",               r""),
    ("谷氨酸丙酮酸转氨酶",                   r"^alanine aminotransferase",    r""),
    ("红细胞沉降速度",                     r"^erythrocyte \[sedimentation rate\]", r""),
    ("抗环瓜氨酸多肽抗体",                   r"^cyclic citrullinated peptide", r""),
    ("动脉舒张压",                       r"^diastolic blood pressure",    r""),
    ("左踝肱指数",                       r"^ankle-brachial index",        r""),
    ("细胞角蛋白19可溶性片段",                r"^cytokeratin 19",              r""),
    ("乙型肝炎病毒表面抗原",                  r"^hepatitis b virus surface ag", r""),
    ("N端脑钠肽前体",                     r"^natriuretic peptide\.b prohormone n-terminal", r""),
    ("FBS",                          r"^fasting glucose",             r""),
    ("tHcy",                         r"^homocysteine",                r""),
    ("U-PRO",                        r"^protein \[presence\] in urine", r""),
]

# NOTE: the body-composition, fitness and chronic-condition device names
# from #301's 92-name table -- 脂肪, 肌肉, 血氧, and the rest -- are NOT here.
# They have their own benchmark, `test_engine_coverage_devices.py`, with its
# own printed score, so this module's lab score stays a claim about an
# ordinary LAB REPORT: the thing every new user's first upload actually is.
# See that file's module docstring for why.

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
    ("血脂肪", ("the same lipid panel, 台灣 wording — needs its own refusal row: "
                "the zh-Hant fold reaches nothing here, and without the row the "
                "semantic tier would answer it")),
    ("维生素", ("a heading covering D, B12, folate and E: it used to answer "
               "96450-2 'Hepatocellular carcinoma risk [Score] GALAD', and the "
               "next candidate down is 1823-4 alpha tocopherol, a real serum "
               "concentration and still the wrong answer")),
    ("維生素", "the same heading in 繁體"),
    ("肿瘤标志物", ("answered 53959-3, one specific marker standing in for the "
                  "whole category, on the panel where a wrong answer is least "
                  "forgivable")),
    ("腫瘤標誌物", "the same heading in 繁體"),
    ("绝对不存在的指标名xyzzy", "pure nonsense must never resolve"),
    ("zorbitine", "an invented English-looking name must never resolve either"),
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

    # ── the glued-CJK split (#305) must not guess ────────────────────────────
    # `胆固醇HDL-C` disagrees the same way `胆固醇(HDL-C)` does -- two real
    # analytes, not an abbreviation of each other -- and must stay unresolved.
    ("胆固醇HDL-C", "stem is total cholesterol (2093-3), glued half is HDL (2085-9) — two analytes"),
    # `DNA` carries no code of its own, so the strict split (#305) must not
    # fall back to the stem the way the lenient bracket form would: answering
    # with 乙肝病毒's own Ab-presence code for a reading named `...DNA` would
    # be the same class of wrong as 血红蛋白 -> HbA1c.
    ("乙肝病毒DNA", "乙肝病毒 alone resolves; DNA does not, and the strict split must not borrow its code"),
    # Kept exactly as main resolves it (unresolved): CLASS=CELLMARK excludes
    # bare 癌胚抗原, so the stem side of the split never resolves either.
    ("癌胚抗原CEA", "癌胚抗原 abstains on main (CLASS=CELLMARK); CEA alone answering is not evidence the pair agrees"),
    # Each of these glues a real Chinese CBC/urinalysis name to an
    # abbreviation that means something ELSE entirely, or that splits the
    # same percent/absolute or quantitative/dipstick way `NEUT#`/`NEUT%`
    # already does — #305's own instruction to abstain rather than guess
    # where the stem is ambiguous.
    ("中性粒细胞NEUT", "中性粒细胞 (26511-6, the ratio) vs NEUT (751-8, the absolute count) — percent/absolute ambiguity"),
    ("嗜酸性粒细胞EO", "嗜酸性粒细胞 alone resolves; EO carries no code of its own"),
    # The dipstick abbreviation is refused alone (#357), and a blocked half
    # refuses the whole name, as `HRV (RMSSD)` does.
    ("尿蛋白PRO", "PRO alone is refused: proline, or a urine protein only a heading says"),
    ("尿酮体KET", "KET alone is refused; it answered TP73L gene mutations"),
    ("尿亚硝酸盐NIT", "NIT alone is refused; it answered urea nitrogen"),
    ("白细胞酯酶LEU", "LEU alone is refused; it answered leucine"),
    ("尿胆原URO", "URO alone is refused; it answered free PSA"),
    ("胆红素BIL", "BIL alone is refused; it answered kidney weight"),
    # `BLD` and `SG` carry no alias at all (neither a bracket nor the glued
    # form reaches one), so these stay the honest miss main already gives.
    ("尿潜血BLD", "neither 尿潜血 nor BLD alone is in the index; stays unresolved"),
    ("尿比重SG", "SG carries no alias; 尿比重 alone resolves but the pair does not need to"),
    # #322: one abbreviation cut at its own hyphen borrows nothing from the
    # half that resolves. `HBc-Ag` answered the core ANTIBODY through `HBc`;
    # LOINC's core-antigen code is a tissue stain, so no answer is right.
    ("HBc-Ag", "HBc alone answers the core antibody; the antigen is a different test"),
    ("HBs-Antigen", "HBs alone answers the surface antibody"),
    ("HCV-Ag", "HCV alone answers an RNA viral load, not an antigen"),
    ("CMV-Ab", "CMV alone answers a DNA viral load, not an antibody"),
    ("EBV-Ab", "EBV alone answers a DNA viral load, not an antibody"),
    # #332: the class, not a test; zh.tsv sent it to Lp(a).
    ("脂蛋白类", "the lipoprotein class names no single test"),
    ("脂蛋白類", "Traditional spelling of the same class word"),
    # #343: no specimen, and the English name of the abstaining 基础代谢率.
    ("酸碱度", "pH with no specimen; it answered ionized calcium adjusted to pH 7.4"),
    ("Basal metabolic rate", "answered the MET-and-RMR panel; 基础代谢率 abstains for the same reason"),
    # #357: a dipstick abbreviation with no urine heading; under one,
    # res/loinc/panel_terms.tsv codes it before the resolver is asked.
    ("PRO", "answered proline"),
    ("LEU", "answered leucine"),
    ("KET", "answered TP73L gene mutations"),
    ("NIT", "answered urea nitrogen"),
    ("URO", "answered free PSA"),
    ("BIL", "answered kidney weight"),
    # Two tests by name alone; the unit settles each (READING_CASES).
    ("TLC", "leukocytes on a blood count, total lung capacity on a lung-function report"),
    ("TCT", "a cervical cytology result, or a thrombin clotting time"),
]
# The #301 body-composition device names that must abstain (骨骼肌,
# 基础代谢, 基础代谢率/BMR, 压力) are in `test_engine_coverage_devices.py`
# instead, alongside the rest of that issue's 92-name table — see this
# module's note after CASES.

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


# Where the score above is published, and the phrase each page prints it in.
# The pages said 347/347 through 1.6.2 while this module printed 356/356 after
# #301 (test_engine_coverage_devices.py records it) and 423/423 by 1.6.2: every
# case added here moved the real number and none of the pages followed.
_PUBLISHED_SCORE = {
    "README.md": "{n}/{n}",
    "README.zh-CN.md": "{n}/{n}",
    "README.ja.md": "{n}/{n}",
    "docs/standardization.md": "{n}/{n}",
    "docs/standardization.zh-CN.md": "{n}/{n}",
    "docs/testing.md": "{n} cases",
    "skills/standardize-health-data/SKILL.md": "{n}/{n}",
}


def test_the_published_score_is_the_case_count():
    """Every page that quotes the score quotes this module's case count.

    With `COVERAGE_FLOOR` at 1.0 a passing `test_coverage` prints N/N, N being
    the case count, so the pages are checked without resolving anything. A new
    case changes these pages in the same commit.
    """
    root = pathlib.Path(mirobody.__file__).resolve().parent.parent
    if not (root / "docs").is_dir():
        pytest.skip("docs/ ships in the repository, not in the wheel or the sdist")
    n = len(CASES) + len(MUST_NOT_RESOLVE)
    stale = [
        rel
        for rel, phrase in _PUBLISHED_SCORE.items()
        if phrase.format(n=n) not in (root / rel).read_text(encoding="utf-8")
    ]
    assert not stale, f"these pages do not print the current score {n}/{n}: {stale}"


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
    # ng/mL is a mass concentration for iron and for ferritin alike, so the
    # unit could not catch `FER` answering iron; the name has to be right.
    ("FER",             "42.3", "ng/mL", "20567-4", "ferritin, not 2498-4 iron"),
    # No unit, an unparseable unit, or a unit already in the right family must
    # all leave the answer exactly where `resolve` put it.
    ("HGB",             "140", "g/L",    "718-7",   "g/L is already MCnc"),
    ("hematocrit",      "42",  "%",      "4544-3",  "% has no substance sibling"),
    ("hematocrit",      "0.42", "L/L",   "4544-3",  "L/L is a volume fraction too"),
    ("红细胞比容",          "0.42", "L/L",   "4544-3",  "the SI spelling on a Chinese report"),
    # A checkup prints a flag mark after the name.
    ("γ-谷氨酰基转移酶*",     "28",  "U/L",    "2324-2",  "a flag mark after the name"),
    ("体重指数BMI",          "22.1", "kg/m2", "39156-5", "acronym glued on"),
    ("白球比例",             "1.5", "",       "1759-0",  "ratio said in words"),
    ("中性粒细胞比值",        "60",  "%",      "26511-6", "ratio said in words"),
    ("红细胞分布宽度-变异系数", "13",  "%",      "30385-9", "RDW-CV in words"),
    ("天门冬氨酸/丙氨酸氨基转移酶", "1.1", "",   "1916-6",  "AST/ALT in words"),
    ("白细胞计数",         "6.5", "10*9/L", "26464-8", "count already NCnc"),
    # The differential, both ways: a measure word in the name goes to the
    # fraction component, and the unit then picks the count or the ratio;
    # a bare count component printed as % goes to its `/leukocytes` sibling.
    ("monocyte count",  "0.5", "10*9/L", "26484-6", "ratio component, count unit"),
    ("monocyte percentage", "7.1", "%",  "26485-3", "ratio component, ratio unit"),
    ("中性粒细胞计数",      "4.2", "10*9/L", "26499-4", "ratio component, count unit"),
    ("Monocytes",       "7.1", "%",      "26485-3", "count component, ratio unit"),
    ("neutrophils",     "62",  "%",      "26511-6", "count component, ratio unit"),
    # Units that name two properties: `U/mL` is an enzyme's catalytic
    # concentration and a tumour marker's arbitrary one; `fL` is an entitic
    # volume and the mean one; `mL/min` is a flow and an eGFR without its
    # body-surface term; the eGFR spelling with a multiplication sign parses.
    ("CA19-9",          "15.3", "U/mL",  "24108-3", "arbitrary units, not catalytic"),
    ("ALT",             "25",  "U/L",    "1742-6",  "catalytic, unchanged"),
    ("谷丙转氨酶",          "30",  "IU/L",   "1742-6",  "an enzyme printed in IU/L"),
    ("γ-谷氨酰转肽酶",      "28",  "IU/L",   "2324-2",  "an enzyme printed in IU/L"),
    ("促卵泡激素",          "6.2", "IU/L",   "15067-2", "a hormone in IU/L stays ACnc"),
    ("MCV",             "87.4", "fL",    "30428-7", "entitic mean volume"),
    ("eGFR",            "88",  "mL/(min×1.73 m^2)", "48642-3", "areic volume rate"),
    ("eGFR",            "30",  "mL/min", "48642-3", "eGFR without the area term"),
    ("RDW-CV",          "13.1", "%",     "30385-9", "distribution width printed as %"),
    ("FT3",             "4.5", "pmol/L", "14928-6", "free T3 to moles/volume"),
    # The component survives the unit switch: the unit picks activity or
    # mass WITHIN `creatine kinase.MB`, never the parent's code.
    ("Creatine Kinase-MB", "25", "U/L",  "32673-6", "MB isoenzyme, activity"),
    ("Creatine Kinase-MB", "3.2", "ng/mL", "13969-1", "MB isoenzyme, mass"),
    ("Lactate Dehydrogenase-LDH1", "60", "U/L", "2537-9", "LDH1 activity, not total LDH"),
    ("Alkaline Phosphatase-BALP", "20", "ug/L", "17838-4", "bone ALP mass, in serum"),
    # #306: now a curated alias, `肌酐(Cre)` agrees by name and the unit just
    # picks the variant, same as its CREA/Cr neighbours.
    ("肌酐(Cre)",        "78",  "umol/L", "14682-9", "Cre no longer vetoes the stem"),
    ("肌酐(CREA)",       "78",  "umol/L", "14682-9", "already worked; pinned alongside Cre"),
    ("肌酐(Cr)",         "78",  "umol/L", "14682-9", "already worked; pinned alongside Cre"),
    # Bare `PCT` ties plateletcrit and procalcitonin at the same rank in the
    # raw index (`_KNOWN_TIES`); the unit settles it, the same way R2
    # settles a bracketed stem/label disagreement.
    ("PCT",              "0.25", "%",      "51637-7", "plateletcrit, a volume fraction"),
    ("PCT",              "0.15", "ng/mL",  "33959-8", "procalcitonin, a mass concentration"),
    # Same for a bare `Body fat`: a scale's percentage or its mass.
    ("Body fat",         "27.8", "%",      "41982-0", "percentage of body fat"),
    ("Body fat",         "21.2", "kg",     "73708-0", "body fat mass"),
    # The fasting component survives the switch to moles.
    ("空腹胰岛素",          "58",   "pmol/L", "59179-2", "fasting insulin in moles"),
    # Turkish, through the write path (Discussion #137): capitalised and
    # lowercase both reach the new `ürik asit`/`açlık kan şekeri` rows.
    ("ÜRİK ASİT",        "6.5", "mg/dL",  "3084-1",  "urate, all-caps Turkish"),
    ("ürik asit",        "6.5", "mg/dL",  "3084-1",  "urate, lowercase Turkish"),
    # #322's refusal is for an immunoassay role word only: a name with its
    # element symbol, method or standard still answers (review of #331).
    ("Potassium-K",      "4.2", "mmol/L", "2823-3",  "K is the element, not a fragment"),
    ("Phosphorus-P",     "3.5", "mg/dL",  "2777-1",  "P is the element"),
    ("VLDL-C",           "20",  "mg/dL",  "13458-5", "VLDL is an analyte, not a fragment"),
    # #322: the hyphenated spellings keep their own analyte through the unit.
    ("NT-proBNP",        "85",  "pg/mL",  "33762-6", "NT-proBNP, not BNP"),
    ("Cys-C",            "0.9", "mg/L",   "33863-2", "cystatin C, not an amino-acid screen"),
    # #323: either unit stays in the calcidiol series.
    ("25-羟基维生素D",      "28.5", "ng/mL",  "1989-3",  "25-OH D3, mass"),
    ("25-hydroxyvitamin D", "71",  "nmol/L", "14635-7", "25-OH D3, moles; not the D2+D3 code"),
    ("Витамин D",        "71",  "nmol/L", "14635-7", "Russian, same series as the rest"),
    ("AÇLIK KAN ŞEKERİ", "95",  "mg/dL",  "1558-6",  "fasting glucose, all-caps Turkish"),
    ("açlık kan şekeri", "95",  "mg/dL",  "1558-6",  "fasting glucose, lowercase Turkish"),
    # #332: every printed spelling and unit of Lp(a) is one series.
    ("脂蛋白(a)",          "426",   "mg/L",   "10835-7", "Lp(a), mass, not total lipoproteins"),
    ("脂蛋白（a）",         "0.389", "g/L",    "10835-7", "Lp(a), full-width brackets"),
    ("脂蛋白(a)",          "45",    "nmol/L", "43583-4", "Lp(a), moles, no longer refused"),
    # #343: the count and the molar calcium keep their own codes.
    ("中性粒细胞#",         "4.2",   "10*9/L", "26499-4", "absolute count, not the ratio"),
    ("总钙",              "2.3",   "mmol/L", "2000-8",  "total calcium, moles"),
    ("SBP",              "120",   "mmHg",   "8480-6",  "systolic pressure, not SHBG"),
    ("FVC",              "4.1",   "L",      "19870-5", "measured FVC, not the predicted volume"),
    ("用力肺活量",           "4.1",   "L",      "19870-5", "measured FVC, Chinese"),
    ("FEV1",             "3.2",   "L",      "20150-9", "measured FEV1"),
    ("FEV1/FVC",         "78",    "%",      "19926-5", "measured ratio; `%` admitted for VRto"),
    ("FEV1 % predicted", "85",    "%",      "20152-5", "FEV1 measured/predicted; `%` admitted for RelVol"),
    # #357: the unit settles a name that is two tests.
    ("TLC",              "6.5",   "10*9/L", "26464-8", "a blood count's total leukocytes"),
    ("TLC",              "5.8",   "L",      "19862-2", "a lung-function report's total lung capacity"),
    ("TCT",              "16.5",  "s",      "3243-3",  "a thrombin clotting time"),
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
    ("HBs-Ag",         "阴性", "5196-1",  "serology screen, hyphenated (#322)"),
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


# ---------------------------------------------------------------------------
# What the printed unit is allowed to decide, and what it must refuse
# ---------------------------------------------------------------------------


def test_a_unit_that_confirms_a_code_is_distinguishable_from_one_that_does_not():
    """Three situations used to be byte-identical, and one of them was a bug.

    `resolve_reading("hemoglobin", "11.2", …)` returned `718-7 / lexical /
    candidates=72` for a unit that CONFIRMS the code, a unit that could not be
    parsed at all, and no unit whatsoever. A caller had no way to learn that its
    strongest disambiguator had been silently discarded, which is the whole
    premise of passing the unit in the first place.
    """
    from mirobody.engine import resolve_reading

    confirmed = resolve_reading("hemoglobin", "11.2", "g/dL")
    nonsense = resolve_reading("hemoglobin", "11.2", "furlongs/fortnight")
    absent = resolve_reading("hemoglobin", "11.2", None)

    # All three still answer — withholding a code because we do not know a unit
    # spelling would be the larger error, since unknown spellings are our gap.
    assert confirmed.loinc == nonsense.loinc == absent.loinc == "718-7"

    # But they are no longer the same answer.
    assert confirmed.unit_recognized is True
    assert nonsense.unit_recognized is False
    assert absent.unit_recognized is None
    assert "property" in confirmed.evidence
    assert "property" not in nonsense.evidence
    assert "property" not in absent.evidence


def test_an_unparseable_unit_does_not_claim_the_unit_chose_the_code():
    """`total cholesterol 5.0 bananas` answered 2093-3 — the mg/dL form — with
    a shape identical to a unit-confirmed answer. 5.0 mg/dL cholesterol is
    physiologically absurd, and nothing in the result said the unit had been
    thrown away."""
    from mirobody.engine import resolve_reading

    r = resolve_reading("total cholesterol", "5.0", "bananas")
    assert r.evidence == ("name",)
    assert r.unit_recognized is False


def test_a_unit_that_contradicts_every_reachable_code_is_refused():
    """`hemoglobin 14 %` has no code: hemoglobin is a mass concentration and
    no sibling of it is a fraction. Answering 718-7 would file a percentage
    into a g/dL series, indistinguishable from a correct row, which is why
    this is a refusal carrying its evidence rather than the name's code."""
    from mirobody.engine import resolve_reading

    r = resolve_reading("hemoglobin", "14", "%")
    assert not r.resolved
    assert r.loinc == ""
    assert r.method == "refused"
    assert r.rejected_code == "718-7"
    assert "MCnc" in r.rejected_reason


def test_a_differential_count_printed_as_a_percentage_finds_its_ratio_code():
    """`neutrophils 62 %` used to answer 751-8, an absolute count by a method
    nothing specified, then to refuse. The percentage is a real code under
    `neutrophils/leukocytes`; the five leukocyte types are the one place the
    denominator is determined, and the count code stays on the record as
    what the name alone would have said."""
    from mirobody.engine import resolve_reading

    r = resolve_reading("neutrophils", "62", "%")
    assert r.loinc == "26511-6"
    assert r.method == "lexical"
    assert r.rejected_code == "751-8"
    assert "property" in r.evidence
    # And not for a count that is not a leukocyte type.
    assert resolve_reading("白细胞", "62", "%").method == "refused"


def test_a_component_suffix_is_not_an_abbreviation_of_its_parent():
    """`Creatine Kinase-MB` is not `Creatine Kinase` abbreviated, the way
    `Total Cholesterol-TC` is. `split_trailing_hyphen` finds the SEAM in
    both (#305 made it a pure syntactic split, with no opinion on whether
    the tail is safe to drop) -- `is_component_suffix` is what tells them
    apart, and `resolve()` must still reach two different codes for the
    parent and the suffixed child, or the child is being filed under it."""
    from mirobody.engine import resolve
    from mirobody.lexical import is_component_suffix, split_trailing_hyphen

    assert split_trailing_hyphen("Total Cholesterol-TC") == ("Total Cholesterol", "TC")
    assert not is_component_suffix("TC")
    for parent, child, suffix in (
        ("Creatine Kinase", "Creatine Kinase-MB", "MB"),
        ("Lactate Dehydrogenase", "Lactate Dehydrogenase-LDH1", "LDH1"),
        ("Alkaline Phosphatase", "Alkaline Phosphatase-BALP", "BALP"),
    ):
        assert split_trailing_hyphen(child) == (parent, suffix)
        assert is_component_suffix(suffix)
        p, c = resolve(parent), resolve(child)
        assert p.resolved and c.resolved, (parent, child)
        assert p.loinc != c.loinc, f"{child} filed under {parent}: {c.loinc}"

    # "Vitamin D-3": the hyphen pattern needs 3 `\w` characters immediately
    # before it, and the space in "Vitamin D" leaves only one ("D"), so this
    # one never reaches the seam at all -- still safe, since `resolve()` on
    # the whole glued string ("VitaminD3", via `surface_variants`) is the
    # one that already answers it, unrelated to the hyphen split.
    assert split_trailing_hyphen("Vitamin D-3") == ("", "")
    vit_d, vit_d3 = resolve("Vitamin D"), resolve("Vitamin D-3")
    assert vit_d.resolved and vit_d3.resolved and vit_d.loinc == vit_d3.loinc


def test_a_unit_never_moves_a_reading_to_another_specimen():
    """`albumin 30 mg/24h` names a serum protein with a urine excretion
    unit. The only code with that property is the 24-hour URINE albumin;
    answering it would file a serum row into a urine series. A unit may
    choose between properties of one analyte in one specimen, and when the
    specimen would have to change the reading is refused with both named."""
    from mirobody.engine import resolve_reading

    r = resolve_reading("albumin", "30", "mg/(24.h)")
    assert r.method == "refused"
    assert r.rejected_code == "1751-7"
    assert "Ser/Plas" in r.rejected_reason and "Urine" in r.rejected_reason
    # `Ser` and `Ser/Plas` are the same draw, so a Ql sibling in Ser is reachable.
    assert resolve_reading("rheumatoid factor", "negative", None).loinc == "33910-1"


def test_a_name_that_holds_two_analytes_says_which_two():
    """`血糖(HbA1c)`: the stem is glucose and the parenthetical HbA1c.
    Refused, since neither half is the answer by position, but the refusal
    names both codes so a person can be asked, which a category word's
    refusal (`血脂`) never offers. `Plateletcrit (PCT)` was this test's
    example while the bare `PCT` lookup answered procalcitonin; `PCT` is a
    known tie now, read by the stem beside it, so it answers plateletcrit."""
    from mirobody.engine import resolve

    r = resolve("血糖(HbA1c)")
    assert r.method == "refused"
    assert "2339-0" in r.rejected_reason and "4548-4" in r.rejected_reason
    assert r.rejected_code == ""
    assert resolve("血脂").rejected_reason == ""
    assert resolve("Plateletcrit (PCT)").loinc == "51637-7"


def test_a_bare_abbreviation_can_tie_two_analytes_too():
    """Bare `PCT` is a second shape of the same disagreement (#305's family,
    not a bracket): the raw index ties plateletcrit and procalcitonin at the
    SAME commonness rank, and row order alone used to pick procalcitonin
    every time. `resolve()` now refuses it by name, both codes given, and
    `resolve_reading` settles it from the unit (see READING_CASES)."""
    from mirobody.engine import resolve

    r = resolve("PCT")
    assert r.method == "refused"
    assert r.ambiguous == ("33959-8", "51637-7")
    assert "33959-8" in r.rejected_reason and "51637-7" in r.rejected_reason


def test_a_report_mark_is_answered_as_the_bare_name_would_be():
    """Stripping `测定` or a flag keeps every rule the bare name meets: a
    category stays refused, two analytes in one name stay two, and the word
    alone is no name (#234, pre-existing on main)."""
    from mirobody.engine import resolve

    assert resolve("尿酸(UA)测定").term == "尿酸(UA)测定"
    assert resolve("血脂测定").method == "refused"
    assert "4548-4" in resolve("血糖(HbA1c)测定").rejected_reason
    assert resolve("测定").method == "refused"
    assert resolve("红细胞分布宽度-标准差").method == "refused"


def test_a_qualifier_in_brackets_is_not_read_as_an_abbreviation():
    """A bracket after a name is read through `OfflineResolver._resolve_seam`
    the same way `空腹血糖(GLU)` is, and that split's fallback -- "the bracket
    didn't resolve, so the stem alone is the whole answer" -- is safe only
    when the bracket genuinely abbreviates the stem (#305). `血型(Rh)测定`
    already strips `测定` and reaches this split on main; `Rh`/`定量`/`定性`
    carry no code of their own, so without a guard the stem would answer
    for them too. Each pair must keep its own correct code or abstain --
    never the other's code."""
    from mirobody.engine import resolve

    # `Rh` has no alias, so the old fallback gave it 血型's OWN code, 883-9 --
    # a forward ABO-group answer for a reading that named the Rh factor.
    # `血型(ABO)测定` refused while `ABO` alone answered the reverse typing
    # (1303-7); it answers the ABO group now (#357), so both halves agree.
    abo = resolve("血型(ABO)测定")
    rh = resolve("血型(Rh)测定")
    assert abo.loinc == "883-9"
    assert not rh.resolved and rh.loinc != "883-9"
    assert resolve("血型").loinc == "883-9"  # the bare analyte is untouched

    # 幽门螺杆菌(C14)测定: C14 is its own real test (see
    # test_a_real_abbreviation_is_not_blocked_just_because_a_bracket_uses_it_too)
    # and disagrees on analyte with the stem (17859-0 vs 30565-6), so this
    # pair abstains on that mismatch, needing no guard either.
    c14 = resolve("幽门螺杆菌(C14)测定")
    assert not c14.resolved
    assert c14.loinc != "17859-0"
    assert resolve("幽门螺杆菌").loinc == "17859-0"  # the bare analyte is untouched

    # 乙肝表面抗原(定量)测定 / (定性)测定: neither `定量` nor `定性` has an alias,
    # so the fallback gave the stem's 5196-1 for `定量` too -- a QUALITATIVE
    # (Ord) code for a report that said quantitative. `定性` is guarded
    # alongside it for the mirror case: this bundle's HBsAg code happens to
    # already be the qualitative one, so nothing here is yet observed wrong
    # for `定性`, but nothing ties that agreement to the word either.
    quant = resolve("乙肝表面抗原(定量)测定")
    qual = resolve("乙肝表面抗原(定性)测定")
    assert not quant.resolved and not qual.resolved
    assert resolve("乙肝表面抗原").loinc == "5196-1"  # the bare analyte is untouched


def test_a_real_abbreviation_is_not_blocked_just_because_a_bracket_uses_it_too():
    """`C14`/`C13` are real printed test names (tetradecanoylcarnitine and its
    neighbour, on an acylcarnitine or newborn-screening panel beside `C16`/
    `C18`) and must keep resolving on their own. Unlike `Rh`/`定量`/`定性`,
    which carry no code of their own, `C14` going into
    `resolver_overrides.tsv` as `!unresolved` would abstain those panels to
    guard one bracket shape, 幽门螺杆菌(C14)测定. That pair, and its C13 and
    尿素呼气试验 siblings, still abstain without a block: the bracket
    resolves to the carnitine analyte, a different one than either stem, so
    `OfflineResolver.resolve`'s "two analytes in one name" check refuses
    them -- confirmed here by the refusal's own `rejected_reason` naming
    both codes, not merely `resolved is False`, so a future change that made
    the two analytes agree would make this test catch it too."""
    from mirobody.engine import resolve

    assert resolve("C14").loinc == "30565-6"
    assert resolve("C13").loinc == "102321-7"

    for term, stem_code in (
        ("幽门螺杆菌(C14)测定", "17859-0"),
        ("幽门螺杆菌(C13)测定", "17859-0"),
        ("尿素呼气试验(C14)", "29891-9"),
        ("尿素呼气试验(C13)", "29891-9"),
    ):
        r = resolve(term)
        assert not r.resolved, f"{term!r} must abstain"
        assert r.method == "refused"
        # The two-analyte-mismatch path names both codes in its reason; a
        # `!unresolved` block refusal (see `血型(Rh)测定` above) does not.
        assert stem_code in r.rejected_reason and "two analytes" in r.rejected_reason, (
            f"{term!r} refused for the wrong reason: {r.rejected_reason!r}"
        )


def test_an_override_chain_does_not_dead_end_on_an_organ():
    """`Standing Height` answered 15286-8, a KIDNEY ultrasound, on main: its
    row pointed at `Height`, itself redirected to `Body height`, and
    `resolver_overrides.tsv`'s one-hop chain never took the second step,
    so the bare key `height` reached the raw index instead and `_pick`
    took whichever of seven Height-by-US rows ranked first. Retargeted
    straight at `Body height`. A general abstain-on-unstated-specimen rule
    was tried and reverted (measured against the full eval set: 1,418
    correct answers lost for 427 catches, mostly `Urine`/`Isolate`/`Air`/
    `Ventilator`/`Bld.dot`, none a dedicated-procedure specimen) -- this
    fix is independent of that, a dead override chain found along the way."""
    from mirobody.engine import resolve

    assert resolve("Standing Height").loinc == "8302-2"


def test_abbreviated_override_targets_do_not_cascade_to_the_wrong_specimen():
    """`亚硝酸盐`/`尿亚硝酸盐` -> `Nitrite Ur Ql`: when that abbreviated hop
    key has no posting of its own in the alias index (as it did not, here),
    `_lookup` falls through to the next candidate -- the bare Chinese term
    -- and a plain `亚硝酸盐` posts eleven rows, Serum/Plasma's 55948-4
    ranked above the urine dipstick row the override meant. Several
    `Ur`-shorthand targets (`Prot Ur Ql`, `SP GR Ur`, `Leukocyte esterase`,
    ...) had the same latent risk; all now name their full LOINC form."""
    from mirobody.engine import resolve

    for term, loinc in (
        ("亚硝酸盐", "32710-6"), ("尿亚硝酸盐", "32710-6"),
        ("白细胞酯酶", "5799-2"), ("尿白细胞酯酶", "5799-2"),
        ("specific gravity", "2965-2"), ("creatinine clearance", "2164-2"),
        # Bare 隐血 is genuinely ambiguous (urine vs. stool occult blood);
        # main answers the stool code and that is kept, not abstained.
        ("隐血", "2335-8"), ("大便隐血", "2335-8"), ("粪便隐血", "2335-8"),
    ):
        assert resolve(term).loinc == loinc, term


def test_r3_lite_switches_off_a_dedicated_procedure_specimen():
    """R3-lite (#306): `D-dimer`, as an English report prints it, answers
    42499-4 on main -- Fibrin D-dimer in CEREBROSPINAL FLUID, a lumbar
    puncture nobody asked for -- because that is the one row its own
    hyphenated key posts; a plasma code sits one surface variant away
    (`d dimer`, spaced). Unlike R3 (reverted), this never abstains: it
    switches to the best-ranked SAME-COMPONENT row whose SYSTEM is an
    ordinary venipuncture, among every surface variant's candidates, and
    keeps today's answer when none exists. `Myelin basic protein` (CSF
    vs. Serum, genuinely tied) switches the same way."""
    from mirobody.engine.resolver import OfflineResolver, _PROCEDURE_SYSTEMS
    from mirobody.engine import resolve

    d_dimer = resolve("D-dimer")
    assert d_dimer.loinc not in ("42499-4", "42727-8"), "must not stay on the CSF row"
    assert d_dimer.axes[2] not in _PROCEDURE_SYSTEMS

    myelin = resolve("Myelin basic protein")
    assert myelin.loinc == "56661-2", "switched to Serum, not left on CSF's 2638-5"

    # Stated, so left exactly where it is: a term that names the procedure
    # specimen itself must keep answering it, never be switched away.
    assert resolve("Myelin basic protein (CSF)").loinc == "2638-5"

    # Untouched systems (#306's own boundary): a dedicated-procedure switch
    # must never reach into the vocabulary a general abstain rule cost so
    # much to touch (Urine, Stool, Isolate, Air, Bld.dot, Ventilator).
    r = OfflineResolver()
    for code, system in (
        ("2965-2", "Urine"), ("2335-8", "Stool^Patient.gastrointestinal"),
    ):
        axes = r.axes_of(code)
        assert axes is not None and axes[3] == system


def test_coagulation_panel_abbreviations():
    """`凝血四项`/`血凝常规` print PT, APTT, TT, FIB and sometimes INR, D-D,
    FDP, AT-III as bare letters. PT/APTT/INR were already right; `FIB`
    answered 76615-4 (lysosomal acid lipase, cultured fibroblasts -- the
    only thing "FIB" shares with fibrinogen is the three letters) and is
    now a curated alias to the Clauss (clotting) assay, the routine method
    a panel reports; `D-D` had no alias and now reaches the same plasma
    D-dimer code `D-dimer` itself does. `FDP`/`AT-III` resolve by name and
    correctly ABSTAIN rather than guess once a unit is given (FDP's Qn
    siblings are a different COMPONENT, "fibrin+fibrinogen fragments", not
    "fibrin degradation products"; AT-III's RelCCnc has no recognized `%`
    unit family) -- real, narrow gaps, left alone rather than patched
    blind. `TT`/`Thrombin time`/`凝血酶时间` name alone answered 21032-8,
    an Interpretation code, until `_pick` honoured a key that is a code's
    whole long name (`thrombin time` is 3243-3's); a unit switches to
    3243-3 without crossing to 3173-2 (aPTT) the way it used to -- see
    `test_a_unit_switch_keeps_the_coagulation_test_identity` for the
    mechanism."""
    from mirobody.engine import resolve, resolve_reading

    for term, loinc in (("PT", "5902-2"), ("APTT", "3173-2"), ("INR", "34714-6")):
        assert resolve(term).loinc == loinc, term
    assert resolve_reading("PT", "12", "s").loinc == "5902-2"
    assert resolve_reading("APTT", "30", "s").loinc == "3173-2"

    assert resolve("FIB").loinc == "3255-7"
    assert resolve_reading("FIB", "3.2", "g/L").loinc == "3255-7"
    assert resolve("D-D").loinc == resolve("D-二聚体").loinc == "71427-9"
    assert resolve_reading("D-D", "0.3", "ug/mL").loinc == "71427-9"

    assert resolve("FDP").loinc == "29263-1"
    assert not resolve_reading("FDP", "2.5", "ug/mL").resolved
    assert resolve("AT-III").loinc == "27811-9"
    assert not resolve_reading("AT-III", "95", "%").resolved

    assert resolve("TT").loinc == "3243-3"
    for term in ("TT", "Thrombin time", "凝血酶时间"):
        assert resolve_reading(term, "17", "s").loinc == "3243-3", term


def test_a_unit_switch_keeps_the_coagulation_test_identity():
    """`variant_for_reading`'s sibling search matched by COMPONENT alone,
    and `coagulation` (133 axis rows) is the one case in the shipped
    table where that names no real test: PT, aPTT, TT and Recalcification
    time are told apart ONLY by what induced the clot (`Coag.tissue
    factor induced`/`Coag.surface induced`/`Coag.thrombin induced`/
    `Coag.calcium ion induced`), so `resolve_reading("Thrombin time",
    "17", "s")` crossed to 3173-2, aPTT -- a different analyte entirely.
    `_method_identity` requires a sibling's own first two dotted METHOD
    segments to agree once the chosen row's does, leaving an ordinary
    analyte (a method under two segments, or none) unconstrained, exactly
    as before. `_is_venipuncture_draw` closes the second half: Thrombin
    time's Interpretation row is axis SYSTEM `Bld`, its Qn sibling's is
    `PPP`, the SAME venipuncture recorded at a different specimen
    GRANULARITY -- scoped to `identity` so it never substitutes for the
    specimen check on a term this mechanism does not apply to."""
    from mirobody.engine.resolver import get_resolver

    r = get_resolver()
    tt_by_unit = r.variant_for_reading("21032-8", "17", "s")
    assert tt_by_unit.code == "3243-3" and tt_by_unit.outcome == "switched"

    for code, value, unit, stays in (
        ("5902-2", "12.1", "s", "5902-2"),   # PT keeps its own code
        ("3173-2", "31", "s", "3173-2"),     # aPTT keeps its own code
        ("34714-6", "1.0", "", "34714-6"),   # INR keeps its own code
    ):
        v = r.variant_for_reading(code, value, unit)
        assert v.code == stays, code


def test_stool_fat_globules_keep_resolving_after_bare_fat_targets_body_mass():
    """#301 gives bare `脂肪`/`Fat` the body-composition device's code
    (73708-0) instead of stool fat's 16853-4/2270-7. The trade-off named in
    the issue: a stool panel's bare `脂肪` result (阴性/阳性) would now be
    refused rather than coded to the stool analyte — accepted, because a
    stool panel prints `脂肪球`/`脂肪滴` (fat globules/droplets), a different
    string entirely, which this change must leave exactly as it found it."""
    from mirobody.engine import resolve

    assert resolve("脂肪").loinc != "16853-4" and resolve("脂肪").loinc != "2270-7"
    for term in ("脂肪球", "脂肪滴"):
        r = resolve(term)
        assert r.loinc != "73708-0", f"{term!r} must not inherit bare 脂肪's body-fat-mass code"


def test_a_run_of_flag_marks_does_not_backtrack():
    """The rule's first spelling, `(?:\\s*[*↑↓]+|\\s*测定)+\\s*$`, backtracked
    exponentially (CodeQL py/redos): 22 `*` and one more character took 0.4 s,
    and every two more `*` took four times as long, so 26 take seconds and 40
    take a day. A name is whatever a report or an MCP client sends. The scan
    that replaced it answers both in microseconds; the bound is loose enough
    for a slow runner and still fails in seconds, not never."""
    import time

    from mirobody.engine import resolve
    from mirobody.lexical import strip_report_marks

    hostile = "尿酸" + "*" * 26 + "x"
    resolve("尿酸")  # the first call loads the bundle; that is not what is timed
    started = time.perf_counter()
    assert strip_report_marks(hostile) == ""
    assert resolve(hostile).loinc == ""
    assert time.perf_counter() - started < 1.0
    assert strip_report_marks("尿酸" + " 测定*↑" * 10_000) == "尿酸"


def test_the_unit_still_selects_between_two_real_siblings():
    """The refusal above must not have cost the capability it protects."""
    from mirobody.engine import resolve_reading

    molar = resolve_reading("total cholesterol", "5.0", "mmol/L")
    mass = resolve_reading("total cholesterol", "193", "mg/dL")
    assert molar.loinc == "14647-2"
    assert mass.loinc == "2093-3"
    assert molar.rejected_code == "2093-3"  # the name alone would have said this
    assert "property" in molar.evidence


def test_the_measurement_settles_a_stem_label_disagreement():
    """#306: `resolve()` cannot see the unit, so a name-only "two analytes"
    refusal carries both codes in `Resolution.ambiguous` rather than only in
    `rejected_reason`'s prose. `resolve_reading` checks each candidate
    independently with `variant_for_reading` and answers the one the
    measurement is compatible with -- exactly one, not "whichever came
    first". `血糖(HbA1c)` is the clean demonstration: the SAME name,
    answered oppositely by two different units, because the measurement
    carries the evidence the name alone does not."""
    from mirobody.engine import resolve, resolve_reading

    glucose, hba1c = "2339-0", "4548-4"
    bare = resolve("血糖(HbA1c)")
    assert bare.method == "refused" and bare.ambiguous == (glucose, hba1c)

    as_glucose = resolve_reading("血糖(HbA1c)", "5.4", "mmol/L")
    assert as_glucose.resolved and as_glucose.loinc == "15074-8"  # glucose, moles/volume
    assert as_glucose.rejected_code == hba1c
    assert as_glucose.evidence == ("name", "property")

    as_hba1c = resolve_reading("血糖(HbA1c)", "6.1", "%")
    assert as_hba1c.resolved and as_hba1c.loinc == hba1c
    assert as_hba1c.rejected_code == glucose

    # Both candidates compatible (胆固醇/HDL-C both take a lipid unit) or
    # neither (no unit at all) is not evidence for either side, so it stays
    # refused exactly as `resolve()` alone already has it.
    both_compatible = resolve_reading("胆固醇(HDL-C)", "1.2", "mmol/L")
    assert not both_compatible.resolved and not both_compatible.loinc
    neither_has_a_unit = resolve_reading("血糖(HbA1c)", "", "")
    assert not neither_has_a_unit.resolved and not neither_has_a_unit.loinc


def test_a_qualitative_cre_does_not_borrow_creatinines_scale():
    """`CRE` is now a curated alias for Creatinine (#306, next to `CREA`/
    `Cr`), so a microbiology report's rarer `CRE 阴性` (never a number) must
    still be refused by the SCALE gate: creatinine's only code is `Qn`
    (`Mass/volume` or `Moles/volume`), with no `Ord`/`Nom` sibling for
    `variant_for_reading` to switch to, so a nominal value finds nothing
    compatible and the write path keeps the reading uncoded rather than
    filing a qualitative microbiology flag as a creatinine number."""
    from mirobody.collect import observations as obs
    from mirobody.engine import resolve_reading

    assert resolve_reading("CRE", "阴性", "").loinc == ""

    row = {
        "name_key": "CRE", "unit_ucum": "", "kind": obs.KIND_MEASUREMENT,
        "source_kind": obs.SOURCE_FILE, "name_text": "CRE", "local_key": "CRE|",
        "value_kind": "nominal", "value_text": "阴性", "unit_text": "", "value_num": None,
    }
    coding = obs.coding_for(row, {})
    assert coding.outcome == obs.standardize.OUTCOME_NEEDS_INPUT
    assert coding.code is None


#: Codes the PROPERTY gate refuses one of their own EXAMPLE_UCUM_UNITS. A
#: ratchet: lower it when a fix lands. Most of the rest needs more than a
#: family entry: `%` admitted for `RelTime` would switch `PT 62 %` to an INR
#: code, and `/mL` is declared on log counts. `%` for `VRto`/`RelVol` came
#: in with the measured spirometry codes (#343): 1087 -> 1066.
REFUSED_OWN_UNIT_CEILING = 1066


def test_the_unit_gate_admits_the_units_loinc_declares(resolver):
    """`BMI 24 kg/m2` was refused: LOINC files BMI (39156-5) as a `Ratio`
    and declares kg/m2 for it, while the family table admitted only `MCnc`
    for kg/m2. 1,247 codes were refused a unit LOINC declares for them."""
    from mirobody._bundle import AXIS_PROPERTY, read_member
    from mirobody.engine import resolve_reading
    from mirobody.units import normalize_unit, unit_families

    table = (read_member("loinc_units.tsv") or b"").decode("utf-8")
    assert table, "loinc_units.tsv is missing from the bundle"
    refused = set()
    for line in table.splitlines()[1:]:
        code, _, declared = line.partition("\t")
        row = resolver._row_for_code(code)
        if row < 0:
            continue
        prop = resolver._axis.field(row, AXIS_PROPERTY)
        for unit in declared.split(";"):
            families = unit_families(normalize_unit(unit.strip()))
            if families and prop not in families:
                refused.add(code)
    assert len(refused) <= REFUSED_OWN_UNIT_CEILING, f"{len(refused)} codes refuse a unit they declare"
    assert resolve_reading("BMI", "24", "kg/m2").loinc == "39156-5"


def test_a_spirometry_percent_is_not_coded_to_the_predicted_ratio():
    """`FEV1`, `FEV1%` and `FEV1/FVC` resolved to 19925-7, FEV1/FVC
    PREDICTED, which is why `%` stayed out of `VRto`/`RelVol`. They answer
    the measured codes now (#343), so `FEV1/FVC 78 %` is the measured ratio,
    `FEV1%` is LOINC's own synonym for it, and a bare `FEV1 85 %`, a percent
    of predicted the name does not state, still asks."""
    from mirobody.engine import resolve_reading

    for name, value in (("FEV1", "85"), ("FEV1/FVC", "78"), ("FEV1%", "85")):
        assert resolve_reading(name, value, "%").loinc != "19925-7"
    assert resolve_reading("FEV1/FVC", "78", "%").loinc == "19926-5"
    assert resolve_reading("FEV1%", "85", "%").loinc == "19926-5"
    assert not resolve_reading("FEV1", "85", "%").resolved


def test_a_milliequivalent_excretion_is_not_a_concentration():
    """`mEq/24h` and `mEq/kg` were spellings of `meq/L`, so `sodium 150
    mEq/24h`, a 24-hour urine excretion, answered serum sodium 2951-2 as a
    concentration."""
    from mirobody.engine import resolve_reading
    from mirobody.units import normalize_unit, unit_family

    assert normalize_unit("mEq/24h") == "meq/(24.h)" and unit_family("meq/(24.h)") == "SRat"
    assert normalize_unit("mEq/kg") == "meq/kg" and unit_family("meq/kg") == "SCnt"
    excretion = resolve_reading("sodium", "150", "mEq/24h")
    assert excretion.loinc == "" and excretion.rejected_code == "2951-2"
    assert resolve_reading("sodium", "140", "mEq/L").loinc == "2951-2"


def test_a_chinese_length_word_is_no_metric_or_imperial_unit():
    """`寸` (cun, a thirtieth of a metre) normalized to `cm` and `尺` (chi, a
    third of a metre) to the foot, so a length printed in either was off by
    a factor of three or more. The US survey foot and inch, which `ft` and
    `in` normalize to, had no factor at all and converted to nothing."""
    from mirobody.units import convert_value, normalize_unit

    assert normalize_unit("寸") is None and normalize_unit("尺") is None
    assert normalize_unit("英寸") == "[in_us]" and normalize_unit("英尺") == "[ft_us]"
    assert convert_value(1, "[ft_us]", "m") == 1200 / 3937
    assert abs(convert_value(12, "[in_us]", "[ft_us]") - 1) < 1e-12


def test_every_conversion_factor_is_the_one_ucum_defines():
    """The conversion table is written by hand and UCUM's own table ships
    beside it, so every unit the table reads must convert by UCUM's ratio.
    Energy, pressure and the US volumes had no factor at all: `kcal` to `kJ`
    and `mmHg` to `kPa` came from a second, hand-copied table in
    `standardize.canonical_units`, and the MCP `convert_unit` refused both."""
    import math

    from mirobody.units import UCUM_FAMILY, conversion_factor, convert_value, convertible, scale
    from mirobody.units.essence import magnitude

    reference: dict[tuple, str] = {}
    checked = 0
    for unit in UCUM_FAMILY:
        parsed = scale(unit)
        ucum = magnitude(unit) if parsed else None
        if ucum is None:
            continue
        ref = reference.setdefault(parsed[0], unit)
        assert ucum[1] == magnitude(ref)[1], (unit, ref)
        assert math.isclose(conversion_factor(unit, ref), ucum[0] / magnitude(ref)[0], rel_tol=1e-9), (unit, ref)
        checked += 1
    assert checked > 150
    assert math.isclose(convert_value(1, "kcal", "kJ"), 4.184)
    assert math.isclose(convert_value(120, "mm[Hg]", "kPa"), 15.99864)
    # A temperature scale has an offset, so it converts without a factor.
    assert convertible("[degF]", "Cel") and conversion_factor("[degF]", "Cel") is None
    assert math.isclose(convert_value(98.6, "[degF]", "Cel"), 37.0)
    assert math.isclose(convert_value(37.0, "Cel", "K"), 310.15)


def test_free_prose_in_the_value_column_constrains_nothing():
    """`scales_for_value` answers (Nar, Doc) for anything it cannot read, which
    is the ABSENCE of a measurement rather than a claim about scale. Treating it
    as a constraint made `尿蛋白` + `见报告` conflict with its own correct code."""
    from mirobody.engine import resolve, resolve_reading

    for value in ("见报告", "clear yellow fluid"):
        r = resolve_reading("尿蛋白", value, None)
        assert r.loinc == resolve("尿蛋白").loinc
        assert r.resolved


def test_a_signed_number_is_a_quantity_and_prose_corroborates_no_scale():
    """`-2.5`, an everyday base excess, classified as free prose, so a
    negative result put no SCALE constraint on its reading. And prose in the
    value column, which constrains nothing, was still reported as `scale`
    evidence."""
    from mirobody.engine import resolve_reading
    from mirobody.value_scale import classify_value, scales_for_value

    assert classify_value("-2.5") == classify_value("-2 mmol/L") == classify_value("<-1") == "qn"
    assert scales_for_value("-2.5") == scales_for_value("2.5")
    assert resolve_reading("total cholesterol", "见报告", "mg/dL").evidence == ("name", "property")
    assert resolve_reading("total cholesterol", "193", "mg/dL").evidence == ("name", "property", "scale")


def test_a_reading_of_exactly_one_keeps_its_number():
    """`1` is also the UCUM unity unit, so a value cell holding exactly 1 was
    read as a unit with no number and stored as narrative: a series 1, 2, 3
    counted three readings and averaged two of them to 2.5."""
    from mirobody.standardize.parse import KIND_QUANTITY, parse_value
    from mirobody.units import normalize_unit, parse_value_unit

    assert parse_value_unit("1") == parse_value_unit("1.0")
    assert parse_value_unit("1").value == 1.0 and parse_value_unit("1").unit is None
    for value, unit in (("1", "mmol/L"), ("1", ""), ("<1", "mg/dL"), ("1", "1")):
        parsed = parse_value(value, unit)
        assert parsed.value_kind == KIND_QUANTITY and parsed.value_num == 1.0, (value, unit, parsed)
    assert normalize_unit("1") == "1"


def test_a_signed_dipstick_grade_is_not_a_quantity():
    """`+1` and `+2` are dipstick grades as often as numbers. Read as a
    quantity once signs counted, 尿蛋白 +1 left Protein [Presence] for the
    mass-concentration code and its series averaged 1 with real mg/L values."""
    from mirobody.engine import resolve_reading
    from mirobody.value_scale import classify_value

    assert classify_value("+1") == classify_value("+2") == classify_value("-1") == "nar"
    assert classify_value("1+") == "ord"
    # 大便隐血, not bare 隐血: "occult blood" alone names no specimen (R3) --
    # urine and stool both print a dipstick grade this way, and the bare
    # word answering stool by commonness alone is #301's class of defect,
    # not a safe default. `test_the_specimen_rule...` pins the abstention.
    for name, value, loinc in (("尿蛋白", "+1", "2887-8"), ("Urine protein", "+2", "2887-8"), ("大便隐血", "+1", "2335-8")):
        assert resolve_reading(name, value, None).loinc == loinc


def test_a_category_word_never_lands_on_a_specific_analyte():
    """A report SECTION heading is refused, not answered with whatever code of
    the wrong kind the index reaches: 尿常规 used to answer a specimen
    collection method, 电解质 a 24-hour urine narrative. An axis rule that
    rejected such candidates cost several hundred correct answers on the
    7,354-case benchmark for eight saved, so the headings are override rows."""
    from mirobody.engine import resolve

    assert not resolve("尿常规").resolved            # was 19159-3, a collection method
    assert not resolve("电解质").resolved            # was 19096-7, a 24h urine narrative


def _override_rows() -> list[tuple[int, str, str]]:
    """`(line number, term, target)` for every data row of the overrides file
    the resolver reads at runtime."""
    from mirobody._bundle import OVERRIDES_PATH

    rows = []
    with open(OVERRIDES_PATH, encoding="utf-8") as f:
        for number, line in enumerate(f, 1):
            if line.startswith("#") or not line.strip():
                continue
            term, _, target = line.rstrip("\n").partition("\t")
            rows.append((number, term, target))
    return rows


def test_a_blocked_category_word_stays_blocked_inside_a_longer_name(resolver):
    """A refusal written for a category word holds for every name derived
    from it. `resolve` checked the block on the name as written, and the stems
    it derives then looked the category word up as an ordinary index key:
    `Stool OB` answered a budgerigar-droppings IgE, `电解质计数` an
    electrolytes panel and `流感 FLU` an influenza assay."""
    blocked = [term for _, term, target in _override_rows() if target == "!unresolved"]
    assert blocked
    leaks = [
        name
        for term in blocked
        for name in (f"Serum {term}", f"{term} count")
        if resolver.resolve(name).resolved
    ]
    assert not leaks, leaks[:10]
    for name in ("Stool OB", "流感 FLU", "电解质计数"):
        assert not resolver.resolve(name).resolved, name
    # A blocked word at the END names the category too, so it is no
    # abbreviation to strip: stripped, `Glucose STOOL` answers blood glucose.
    assert not resolver.resolve("Glucose STOOL").resolved
    # A name that is an index key of its own is not a derived form: `呕吐`
    # is blocked and `呕吐计数` is LOINC's emesis count.
    assert resolver.resolve("呕吐计数").loinc == "94070-0"


def test_every_override_row_does_its_job(resolver):
    """The loader passes over what it cannot use without a word: a row that is
    not exactly `term<TAB>target`, a term repeated later (the first row wins),
    a target the index does not hold. `乙肝e抗原 -> Hepatitis B virus e Ag`
    named a LOINC COMPONENT rather than an index key and answered nothing from
    the day it was written. These checks lived in the unshipped suite, reading
    a path the file has since left."""
    from mirobody.lexical import index_fold

    rows = _override_rows()
    assert len(rows) > 500
    first: dict[str, tuple[int, str]] = {}
    problems = []
    for number, term, target in rows:
        if not term or not target or "\t" in target:
            problems.append(f"line {number}: not term<TAB>target")
            continue
        earlier, earlier_target = first.setdefault(index_fold(term), (number, target))
        if earlier_target != target:
            problems.append(f"line {number}: {term!r} repeats line {earlier} with another target")
        elif target == "!unresolved":
            if resolver.resolve(term).method != "refused":
                problems.append(f"line {number}: {term!r} does not refuse")
        elif not resolver.resolve(target).loinc:
            problems.append(f"line {number}: target {target!r} resolves to nothing")
        elif not resolver.resolve(term).loinc:
            problems.append(f"line {number}: {term!r} still resolves to nothing")
    assert not problems, "\n".join(problems)


def test_what_is_not_a_lab_specimen_but_is_a_result_still_resolves():
    """Excluding `^Patient` is the intuitive rule and it is wrong: these are all
    measured on the person rather than on a specimen. `Type` likewise: blood
    group is a `Type` and a real result."""
    from mirobody.engine import resolve

    assert resolve("步数").loinc == "41950-7"
    assert resolve("体脂率").loinc == "41982-0"
    assert resolve("血型").loinc == "883-9"
    assert resolve("Rh血型").loinc == "10331-7"


def test_a_class_from_another_discipline_is_not_a_lab_result():
    """A perfectly ordinary measurement from a discipline a printed lab report
    never contains has nothing wrong with its PROPERTY, SCALE or SYSTEM; only
    LOINC's own CLASS column tells it apart, and the 1.5.0 cut drops those
    CLASS families from the bundle. Two measured defects were exactly that."""
    from mirobody.engine import resolve

    # CLASS=CELLMARK, the flow-cytometry cell-surface marker, was answering for
    # the serum tumour marker. Only the abbreviation reached the right code.
    assert not resolve("癌胚抗原").resolved
    assert not resolve("carcinoembryonic antigen").resolved
    assert resolve("CEA").loinc == "2039-6"

    # CLASS=DENTAL — a bone volume measured in a tooth space, 34019-0 — used to
    # leak in for bare 骨量 before the 1.5.0 cut dropped that CLASS. #301 gave
    # 骨量 a real target (`resolver_overrides.tsv`'s `bodyBoneMass` row, a
    # scale's bone-mass reading in kg): the dental leak must stay gone, but the
    # term itself is no longer expected to abstain.
    assert resolve("骨量").loinc != "34019-0"
    assert resolve("骨量").loinc == "101685-6"


def test_bone_density_abstains_because_every_code_names_a_site():
    """All twenty bone-density T-score codes carry a SITE — calcaneus, spine,
    hip, femur, radius and ulna. There is no site-unspecified code, so a bare
    `骨密度` cannot be answered without choosing a site the report did not
    print. It used to answer *Bone DXA Calcaneus*: the heel, picked
    arbitrarily."""
    from mirobody.engine import resolve

    assert not resolve("骨密度").resolved


def test_the_class_gate_keeps_the_classes_wearables_live_in():
    """The same lesson as `^Patient`: consumer metrics live in classes that look
    'not lab'. A first attempt gated `H&P.*` and `EKG.*` and lost all of these."""
    from mirobody.engine import resolve

    assert resolve("sleep duration").loinc == "93832-4"   # CLASS=H&P.HX
    assert resolve("QT间期").loinc == "8634-8"             # CLASS=EKG.MEAS
    assert resolve("步数").loinc == "41950-7"              # CLASS=CLIN
    assert resolve("体脂率").loinc == "41982-0"            # CLASS=BDYWGT.ATOM
    # HRV held the EKG.MEAS slot until 1.5.1 sent it to 112429-6, which is
    # CLIN. See res/crosswalks/README.md for that ruling.
    assert resolve("HRV").loinc == "112429-6"


# ── the device catalogue and its crosswalk ──────────────────────────────────
# `res/catalog/metrics.tsv` carries a LOINC code per device metric where the 2026-09
# review of thirteen vendors' data types established one; `res/crosswalks/`
# is that review. The two must agree, and both must name real codes.

#: Metric pairs allowed to share one code: the same quantity at reading grain
#: and at day grain, or two spellings of one measurement site.
_SHARED_CODE_PAIRS = {
    frozenset({"restingHeartRates", "dailyRestingHeartRates"}),
    frozenset({"skinTemperature", "wristTemperatures"}),
    frozenset({"sleepAnalysis_Asleep(Deep)", "dailyDeepSleep"}),
    frozenset({"sleepAnalysis_Asleep(REM)", "dailyRemSleep"}),
    frozenset({"sleepAnalysis_Asleep(Core)", "dailyLightSleep"}),
    frozenset({"sleepAnalysis_Asleep(Unspecified)", "dailyTotalSleepTime"}),
    frozenset({"sleepAnalysis_Awake", "dailyAwakeTime"}),
    frozenset({"sleepAnalysis_InBed", "dailySleepDuration"}),
}


def test_every_catalogue_code_is_a_real_code_with_a_confidence(resolver):
    """A code in the catalogue is in the axis table and says how sure it is.
    The three codes the review found wrong are gone: oxygen saturation was a
    laboratory blood-gas code, skin temperature was core body temperature,
    VO2max was oxygen consumption with no maximum."""
    from mirobody.kernel import metrics

    for m in metrics.ROWS:
        if m.loinc:
            assert m.confidence in metrics.CONFIDENCES, m.name
            assert resolver.axes_of(m.loinc), f"{m.name}: {m.loinc} is not in the axis table"
        else:
            assert not m.confidence, m.name
    assert metrics.METRICS["oxygenSaturations"].loinc == "59408-5"
    assert metrics.METRICS["skinTemperature"].loinc != "8310-5"
    assert metrics.METRICS["vo2Maxs"].loinc != "60842-2"
    # An unverified code is evidence, not an identity.
    assert metrics.METRICS["vo2Maxs"].confidence == metrics.UNVERIFIED
    assert metrics.METRICS["vo2Maxs"].canonical == (metrics.SYSTEM_DEVICE, "vo2Maxs")
    assert metrics.METRICS["steps"].canonical == (metrics.SYSTEM_LOINC, "55423-8")


def test_no_two_metrics_share_a_code_unless_registered():
    """Two metrics under one code would merge two series on recode. The
    pairs above are the same quantity at two grains, and nothing else is."""
    from collections import defaultdict

    from mirobody.kernel import metrics

    by_code: dict[str, set[str]] = defaultdict(set)
    for m in metrics.ROWS:
        if m.loinc:
            by_code[m.loinc].add(m.name)
    for code, names in by_code.items():
        for a in names:
            for b in names:
                if a < b:
                    assert frozenset({a, b}) in _SHARED_CODE_PAIRS, f"{code}: {a} and {b} share a code"


def _same_analyte(resolver, a: str, b: str) -> bool:
    """Two codes of one COMPONENT in one SYSTEM: the unit picks between them."""
    ax_a, ax_b = resolver.axes_of(a), resolver.axes_of(b)
    return bool(ax_a and ax_b) and ax_a[0] == ax_b[0] and ax_a[3] == ax_b[3]


def test_the_crosswalk_names_real_codes_and_catalogue_rows(resolver):
    """Every row of the base table and of the thirteen vendor tables names a
    code the axis table has and, when it names a metric, a catalogue row.
    A confident vendor code and that metric's confident catalogue code are
    the same code, or two unit variants of one analyte (glucose in mmol/L)."""
    from mirobody.kernel import metrics
    from mirobody.standardize import devices

    base = devices.base_table()
    assert len(base) >= 60
    for row in base:
        assert resolver.axes_of(row.loinc), row.loinc
        assert row.confidence in devices.CONFIDENCES, row.loinc
        assert row.fields, f"{row.loinc}: no vendor produces it"
        if row.metric:
            m = metrics.METRICS[row.metric]
            if m.confidence == metrics.CONFIDENT and row.confidence == devices.CONFIDENT:
                assert m.loinc == row.loinc or _same_analyte(resolver, m.loinc, row.loinc), (row.loinc, row.metric)
    for vendor in devices.VENDORS:
        rows = devices.vendor_fields(vendor)
        assert rows, vendor
        for r in rows:
            assert (r.confidence in devices.CONFIDENCES) == bool(r.loinc), (vendor, r.key)
            # An unverified row may hedge the code, not contradict the metric.
            # Six vendors' generic-distance fields were filed under
            # walkingRunningDistances (112427-0, walking or running only) with
            # 103208-5 (any activity) beside it; four were corrected and two
            # kept the pairing, because this check only looked at confident
            # rows. A different COMPONENT is a different measurement whatever
            # the confidence says.
            if r.metric and r.loinc:
                m = metrics.METRICS[r.metric]
                assert not m.loinc or m.loinc == r.loinc or _same_analyte(resolver, m.loinc, r.loinc), (
                    f"{vendor}:{r.key}: {r.metric} is {m.loinc} in the catalogue, {r.loinc} here"
                )
            if r.loinc:
                assert resolver.axes_of(r.loinc), (vendor, r.key, r.loinc)
            if r.metric:
                m = metrics.METRICS[r.metric]
                if r.loinc and m.loinc and r.confidence == m.confidence == metrics.CONFIDENT and m.loinc != r.loinc:
                    assert _same_analyte(resolver, m.loinc, r.loinc), (vendor, r.key, m.loinc, r.loinc)
    assert set(devices.summary()) == set(devices.VENDORS)
    assert {u.reason for u in devices.unmappable()} <= set(devices.REASONS)


def test_the_apple_crosswalk_says_what_the_apple_decoder_does():
    """The public table and the shipped decoder must tell the same story
    about an Apple identifier: the same catalogue metric, or none."""
    from mirobody.kernel.decoders import apple
    from mirobody.standardize import devices

    quantity = {k.lower(): v for k, v in apple.QUANTITY.items()}
    stages = {k.lower(): v for k, v in apple.SLEEP_STAGES.items()}
    checked = 0
    for r in devices.vendor_fields("apple"):
        if r.type == "HKQuantityTypeIdentifier":
            decoded = quantity.get((r.type + r.field).lower())
        elif r.type == "HKCategoryTypeIdentifierSleepAnalysis":
            decoded = stages.get(("HKCategoryValueSleepAnalysis" + r.field).lower())
        else:
            continue
        if decoded is None:
            continue
        assert decoded == r.metric, (r.field, decoded, r.metric)
        checked += 1
    assert checked >= 30
    assert apple.QUANTITY["HKQuantityTypeIdentifierHeartRateVariabilitySDNN"] == "hrvSDNN"


def test_open_wearables_declines_the_apple_quantity_apple_declines():
    """open-wearables' `sleeping_breathing_disturbances` is Apple's
    appleSleepingBreathingDisturbances relayed, and its table filed it under
    sleepDisturbances, 103211-9 Number of awakenings: a count of interrupted
    breaths read as a count of times the person woke. The Apple table
    declines that quantity, so the relay must too."""
    from mirobody.kernel.decoders import open_wearables
    from mirobody.standardize import devices

    direct = devices.lookup("apple", "HKQuantityTypeIdentifier", "appleSleepingBreathingDisturbances")
    assert direct is not None and not direct.metric
    assert open_wearables.CROSSWALK["sleeping_breathing_disturbances"].declined
    sample = {"type": "sleeping_breathing_disturbances", "value": 4, "unit": "count",
              "timestamp": "2026-01-02T07:00:00+08:00"}
    assert open_wearables.decode("timeseries", sample, "Asia/Shanghai") == []
    assert "sleepDisturbances" not in open_wearables.METRICS


@pytest.mark.parametrize("vendor", ["oura", "whoop"])
def test_a_decoded_vendor_crosswalk_says_what_its_decoder_does(vendor):
    """The table and the decoder name the same metric wherever both name one.

    When they disagreed, neither side was right by default: Oura's sleep
    stages and WHOOP's cycle heart rate were wrong in the decoder, WHOOP's
    cycle energy and both workout distances in the table. Each was settled
    against the vendor's own field definition; this keeps them settled.
    """
    from mirobody.kernel import decoders
    from mirobody.standardize import devices

    mapping = getattr(decoders, vendor).MAPPING
    checked = 0
    for r in devices.vendor_fields(vendor):
        entry = mapping.get(r.type, {}).get(r.field)
        if entry is None or not r.metric:
            continue
        assert entry[0] == r.metric, (r.key, entry[0], r.metric)
        checked += 1
    assert checked >= 20, checked


def test_a_catalogue_alias_lets_the_unit_pick_the_variant():
    """The catalogue says `bloodGlucoses` is 2339-0, glucose as a mass
    concentration in blood; a Honor watch prints mmol/L. The alias names the
    analyte and the unit picks the sibling. A unit that fits no sibling
    leaves the alias's code: the alias is a decision, not a guess. Missing
    units still need input; an alias cannot create a canonical quantity."""
    from mirobody import standardize
    from mirobody.standardize.parse import KIND_QUANTITY

    alias = standardize.Alias("catalog", standardize.LOINC_SYSTEM, "2339-0")

    def run(value: str, unit: str) -> standardize.Coding:
        return standardize.code(
            "bloodGlucoses", name_key=standardize.name_key("bloodGlucoses"), local_key="device:bloodGlucoses",
            value_kind=KIND_QUANTITY, value_text=value, unit_text=unit, unit_ucum=unit,
            value_num=float(value), alias=alias,
        )

    assert run("5.6", "mmol/L").code == "15074-8"
    assert run("101", "mg/dL").code == "2339-0"
    assert run("5.6", "").outcome == "needs-input"
    assert run("5.6", "").reason == "unit:missing"
    odd = run("5.6", "%")
    assert odd.coded and odd.code == "2339-0"


def test_a_printed_unit_that_does_not_normalize_picks_no_variant():
    """`空腹血糖 6.1 mmol/l(空腹)`: the printed unit is there but does not
    normalize, so it cannot pick between the mass and the moles code. The
    guard asked whether ANY unit text was printed, and the reading was coded
    to the mass code 1558-6, beside the mg/dL readings it is not. It needs a
    person, like a reading with no unit, and its decision is not the
    no-unit one, or one decision row would stand for two outcomes."""
    from mirobody import standardize

    def run(unit: str) -> standardize.Coding:
        parsed = standardize.parse_value("6.1", unit)
        key = standardize.name_key("空腹血糖")
        return standardize.code(
            "空腹血糖", name_key=key, local_key=f"{key}|{parsed.unit_ucum}", value_kind=parsed.value_kind,
            value_text="6.1", unit_text=unit, unit_ucum=parsed.unit_ucum, value_num=parsed.value_num,
        )

    unrecognized, missing, molar = run("mmol/l(空腹)"), run(""), run("mmol/L")
    assert (unrecognized.outcome, unrecognized.reason) == ("needs-input", "unit:unrecognized")
    assert (missing.outcome, missing.reason) == ("needs-input", "unit:missing")
    assert unrecognized.decision_id != missing.decision_id
    assert molar.coded and molar.code != "1558-6"


def test_evidence_reads_the_same_whichever_entry_point_produced_it():
    """`resolve()` and `resolve_reading()` answer the same term with the same
    code, so they must describe that answer the same way.

    `evidence` was left empty by `resolve()` and set to `("name",)` by
    `resolve_reading()`, so a caller asking `"name" in r.evidence` got False
    from one and True from the other for an identical resolution — a field only
    readable by someone who already knew which function had produced it.
    """
    from mirobody.engine import resolve, resolve_reading

    by_name = resolve("血红蛋白")
    by_reading = resolve_reading("血红蛋白", None, None)
    assert by_name.loinc == by_reading.loinc == "718-7"
    assert by_name.evidence == by_reading.evidence == ("name",)

    # Still empty when nothing was resolved — the field says what corroborated
    # an answer, and there is no answer.
    assert resolve("绝对不存在的指标名xyzzy").evidence == ()

    # And it still grows when the reading carries more than a name.
    assert resolve_reading("total cholesterol", "5.0", "mmol/L").evidence == (
        "name", "property", "scale",
    )


def test_a_parenthetical_name_describes_its_answer_like_its_stem_does():
    """`空腹血糖(GLU)` answers through its stem, and came back with the stem's
    code but empty `evidence` and `axes`: by the field's own definition,
    nothing had corroborated a code the alias table chose."""
    from mirobody.engine import resolve

    whole, stem = resolve("空腹血糖(GLU)"), resolve("空腹血糖")
    assert whole.term == "空腹血糖(GLU)"
    assert whole.loinc == stem.loinc
    assert whole.evidence == stem.evidence == ("name",)
    assert whole.axes == stem.axes != ("", "", "")


# ── what the resolver takes on trust from the bundle ────────────────────────


def test_a_posting_row_is_an_axis_row_and_never_a_skipped_code(resolver):
    """`_lookup` answers with the axis row a posting names: no second lookup
    by name and no skip list. That is right only while the cut writes one
    corpus row per axis row, in the same order, and lists no code it keeps
    in `loinc_skip.txt`. A re-cut that broke either would answer another
    row's code, or a code the cut meant to drop."""
    from mirobody._bundle import AXIS_CODE, AXIS_LCN, read_member

    axis, names = resolver._axis, resolver._names
    assert len(names) == len(axis)
    assert int(resolver._alias_rows.max()) < len(axis)
    assert all(names.raw(row) == axis.field_raw(row, AXIS_LCN) for row in range(len(axis)))
    skipped = set((read_member("loinc_skip.txt") or b"").split())
    assert skipped, "loinc_skip.txt is missing or empty"
    assert skipped.isdisjoint(axis.field_raw(row, AXIS_CODE) for row in range(len(axis)))


def test_a_row_that_names_a_code_but_no_metric_says_so():
    """A candidate code with no metric is not an active vendor mapping.

    The catalogue may already carry the code while the vendor's field or
    aggregation shape remains unverified. The note must not imply either
    successful integration or absence of that code from the catalogue.
    """
    from mirobody.standardize import devices

    markers = ("【未映射：尚未接入本仓指标】", "【未映射：厂商字段未核实】")
    unmarked = []
    marked = 0
    for vendor in devices.VENDORS:
        for row in devices.vendor_fields(vendor):
            if row.metric or not row.loinc:
                continue
            if any(marker in row.note for marker in markers):
                marked += 1
            else:
                unmarked.append(f"{vendor}:{row.key} {row.loinc}")
    assert not unmarked, unmarked[:6]
    assert marked >= 10, marked


def test_a_multi_metric_record_names_its_fields():
    """A record that carries several metrics must say which field is which.

    Health Connect's ActivityIntensityRecord reports MODERATE and VIGOROUS. Its
    two crosswalk rows both left the field empty, so (type, field) identified a
    group of two: records_router could not disambiguate, returned the raw
    identifier as the metric, and *neither* row reached a metric — the reading
    was dropped rather than mis-filed.

    Rows with no metric are excluded: a blank there is a decision not to
    publish the field. Records whose payload is one object the caller splits
    (a blood-pressure pair) are allowlisted — records_router documents that
    split, and the vendor's field names are unavailable for some of them
    (Garmin's health-api reference returned 404).
    """
    from mirobody.standardize import devices

    allowlisted = {
        # One payload the caller splits; records_router documents the split.
        "com.huawei.instantaneous.blood_pressure",
        # Garmin's health-api field reference returned 404 (2026-10-28), so its
        # three multi-metric records cannot be given real field names without
        # guessing. Recorded here so the gap is visible rather than silent.
        "Intensity Minutes", "Sleep", "Body Composition", "Blood Pressure",
    }
    problems = []
    for vendor in devices.VENDORS:
        seen: dict[tuple[str, str], str] = {}
        for row in devices.vendor_fields(vendor):
            if not row.metric or row.type in allowlisted:
                continue
            # A field naming several fields ("maxHeartRate / minHeartRate",
            # "nightSegment[] / napSegment[]") is documentation shorthand: the
            # runtime looks up the exact name and finds nothing, which the
            # audit lists among the paper rows. It cannot be a duplicate key
            # problem because it is not a key at all.
            if "/" in row.field or "[" in row.field:
                continue
            key = (row.type, row.field)
            if key in seen:
                problems.append(f"{vendor}:{row.type}.{row.field or '(blank)'} "
                                f"-> {seen[key]} and {row.metric}")
            seen[key] = row.metric
    assert not problems, problems[:6]


def test_health_connect_activity_intensity_matches_its_two_levels():
    """The record reports moderate and vigorous only; the codes must say so."""
    from mirobody.standardize import devices

    rows = {r.field: r for r in devices.vendor_fields("health_connect")
            if r.type == "ActivityIntensityRecord"}
    assert set(rows) == {"moderate", "vigorous"}, rows
    assert rows["moderate"].metric == "dailyActivityIntensityMedium"
    assert rows["moderate"].loinc == "101689-8"
    assert rows["vigorous"].metric == "dailyActivityIntensityHigh"
    assert rows["vigorous"].loinc == "101690-6"


@pytest.mark.parametrize("code", ["10835-7", "1881-2", "2576-7", "1883-8"])
def test_a_lipoprotein_label_resolves_back_to_its_code(code):
    """The Chinese label a reader sees for each lipoprotein code is a name the
    resolver answers with that code (#332). `脂蛋白（a）`, the app's own label
    for Lp(a), resolved to total lipoproteins; `载脂蛋白（a）` to
    apolipoprotein A."""
    from mirobody.engine import resolve
    from mirobody.standardize import label_for

    label = label_for("http://loinc.org", code, "zh")
    assert label
    assert resolve(label).loinc == code, f"{label!r} -> {resolve(label).loinc}, not {code}"

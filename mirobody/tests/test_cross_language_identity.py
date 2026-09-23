"""One measurement, one code, whatever language the report is printed in.

The CBC differential was right in Chinese and inverted in English.
`surface_variants` strips a trailing `#` or `%` to a bare stem, so `NEUT#` and
`NEUT%` both reach `neut`; Chinese pins that stem to the ratio code with a
curated row, English had none, so `neut` matched the index at an ABSOLUTE code
and `NEUT%` answered it. Measured before the fix: `NEUT%` and `Neutrophils %`
answered 751-8 (a count), `LYMPH#` and `MONO#` answered ratio codes, and a
25-item panel resolved 25/25 in Chinese against 17/25 in English.

These assert the PROPERTY, not the code string: `NCnc` is a count per volume
and `NFr` a fraction of leukocytes, so a row that drifts to the other one
fails here rather than silently merging 62 percent with 4.2 x10^9/L.
"""

from __future__ import annotations

import pytest

import mirobody
from mirobody.translate.code import axes_for

#: The five differential cell lines, with the Chinese spelling a report prints
#: and the analyzer abbreviations an English one prints.
CELLS = [
    ("neutrophil", "中性粒细胞", ["NEUT#", "NEU#", "ANC", "Absolute Neutrophil Count"],
                                 ["NEUT%", "NEU%", "Neutrophils %", "Neutrophil Percentage"]),
    ("lymphocyte", "淋巴细胞", ["LYMPH#", "Lymphs Abs", "Absolute Lymphocyte Count"],
                               ["LYMPH%", "Lymphocytes %", "Lymphocyte Percentage"]),
    ("monocyte", "单核细胞", ["MONO#", "Monos Abs", "Absolute Monocyte Count"],
                             ["MONO%", "Monocytes %", "Monocyte Percentage"]),
    ("eosinophil", "嗜酸性粒细胞", ["EOS#", "EO#", "Eos Abs", "Absolute Eosinophil Count"],
                                   ["EOS%", "EO%", "Eosinophils %", "Eosinophil Percentage"]),
    ("basophil", "嗜碱性粒细胞", ["BASO#", "Basos Abs", "Absolute Basophil Count"],
                                 ["BASO%", "Basophils %", "Basophil Percentage"]),
]


def _code(term: str) -> str:
    """The code, or `""` when the resolver abstains."""
    return mirobody.resolve(term).code


class _Axis:
    """`axis[code]` -> the code's axes. `loinc_axis.csv` stopped shipping with
    the 2.83 cut (the bundle carries the blob the resolver reads), so the axes
    come from the public accessor rather than from a parsed CSV."""

    def __getitem__(self, code: str):
        found = axes_for(code)
        assert found is not None, f"{code} is not in the shipped cut"
        return found[0]


@pytest.fixture(scope="module")
def axis() -> _Axis:
    return _Axis()


@pytest.mark.parametrize("cell,zh,absolute,percent", CELLS, ids=[c[0] for c in CELLS])
def test_absolute_and_percent_stay_apart_in_both_languages(cell, zh, absolute, percent, axis):
    zh_abs, zh_pct = _code(f"{zh}绝对值"), _code(f"{zh}百分比")
    assert axis[zh_abs].property == "NCnc", f"{zh}绝对值 -> {zh_abs}"
    assert axis[zh_pct].property == "NFr", f"{zh}百分比 -> {zh_pct}"

    for term in absolute:
        got = _code(term)
        assert got == zh_abs, f"{term} -> {got}, {zh}绝对值 -> {zh_abs}"
    for term in percent:
        got = _code(term)
        assert got == zh_pct, f"{term} -> {got}, {zh}百分比 -> {zh_pct}"


@pytest.mark.parametrize("cell,zh,absolute,percent", CELLS, ids=[c[0] for c in CELLS])
def test_the_differential_names_no_instrument_method(cell, zh, absolute, percent, axis):
    """A report printing 4.2 does not claim the count was automated.

    Both curated blocks target the method-less code for that reason, and a
    method-bearing target would split the series against the other spelling.
    """
    for term in (f"{zh}绝对值", f"{zh}百分比", absolute[0], percent[0]):
        code = _code(term)
        assert not axis[code].method, f"{term} -> {code} by {axis[code].method}"


#: Spelled-out English names that answered nothing while the abbreviation every
#: analyzer prints answered a code. An English report prints either.
SPELLED_OUT = [
    ("MCV", ["Mean Corpuscular Volume", "Mean Cell Volume"]),
    ("MCH", ["Mean Corpuscular Hemoglobin", "Mean Corpuscular Haemoglobin"]),
    ("MCHC", ["Mean Corpuscular Hemoglobin Concentration",
              "Mean Cell Hemoglobin Concentration"]),
    ("RDW", ["Red Cell Distribution Width", "Red Blood Cell Distribution Width"]),
    ("MPV", ["Mean Platelet Volume"]),
    ("PLT", ["Platelets", "Platelet Count"]),
    ("ESR", ["Erythrocyte Sedimentation Rate", "Sed Rate"]),
]


@pytest.mark.parametrize("abbr,spellings", SPELLED_OUT, ids=[s[0] for s in SPELLED_OUT])
def test_a_spelled_out_index_name_answers_what_its_abbreviation_answers(abbr, spellings):
    want = _code(abbr)
    assert want, f"{abbr} itself stopped resolving"
    for name in spellings:
        got = _code(name)
        assert got == want, f"{name} -> {got}, {abbr} -> {want}"


def test_rdw_cv_and_sd_stay_apart(axis):
    """Reports print both, in different units, so they are not one series: CV
    is a percentage, SD a volume in fL. Merging them puts 13.5 fL on the same
    line as 13.5 %.

    2.82 modelled the SD form as an entitic volume, which the resolver could
    reach. 2.83 replaced it with 115742-9, a DistWidth "by Standard deviation"
    that carries no alias, so no spelling of SD reaches a code of its own. The
    answer is to abstain, not to fall back on the generic width: with a unit
    the engine already refuses and names the reason.
    """
    cv = _code("RDW-CV")
    assert axis[cv].property == "DistWidth"
    assert _code("RDW") == _code("红细胞分布宽度") == cv
    for sd in ("RDW-SD", "Red Cell Distribution Width-SD", "红细胞分布宽度SD"):
        assert not _code(sd), f"{sd} -> {_code(sd)}, which is the CV code"
    refused = mirobody.resolve_reading("RDW-SD", "13.5", "fL")
    assert not refused.code and refused.method == "refused"


def test_the_sedimentation_rate_names_no_obsolete_instrument(axis):
    """红细胞沉降率 answered 4539-3, `by Zetafuge`, while 血沉 and ESR did not."""
    codes = {_code(t) for t in ("ESR", "血沉", "红细胞沉降率", "Erythrocyte Sedimentation Rate")}
    assert len(codes) == 1, codes
    code = codes.pop()
    assert not axis[code].method, f"{code} by {axis[code].method}"


@pytest.mark.parametrize("term", ["Leukocytes", "Erythrocytes", "白细胞", "红细胞"])
def test_a_cell_count_off_a_urinalysis_is_never_a_blood_code(term):
    """Both names appear on a urinalysis as well as on a CBC, and the two are
    counted in different units: a blood count is per volume, a urine one is
    per high-power field. The unit is what separates them, and where it does
    not, the answer is no code rather than the blood one.

    1.4.x got this by leaving the bare plural out of the index entirely. The
    LOINC-only 2.83 corpus makes `Leukocytes` an alias key, so the abstention
    now has to come from the unit, which is also what `白细胞` has always
    relied on.
    """
    for unit in ("/[HPF]", "/HPF"):
        got = mirobody.resolve_reading(term, "5", unit)
        assert not got.code, f"{term} {unit} -> {got.code} ({got.canonical})"


#: Every spelling of the cardiac marker, and the total enzyme it is not.
#: `_TRAILING_ACRONYM` strips a trailing ALL-CAPS token as a repeat of the stem,
#: which is right for `Total cholesterol TC` and wrong here: `-MB` names the
#: isoenzyme. `Creatine Kinase-MB` answered 2157-6, total CK, and with ng/mL
#: crossed to 50756-6, total CK in Blood. `CK-MB` was always right, so two
#: spellings of one test disagreed.
CK_MB = ["CK-MB", "CKMB", "Creatine kinase.MB", "Creatine Kinase-MB",
         "Creatine Kinase MB", "Creatine kinase-MB", "肌酸激酶MB", "肌酸激酶同工酶MB"]
CK_TOTAL = ["CK", "Creatine Kinase", "肌酸激酶"]


@pytest.mark.parametrize("term", CK_MB)
def test_every_ck_mb_spelling_answers_the_isoenzyme(term):
    """13969-1 is the eval ground truth for both `CK-MB` and `肌酸激酶MB`."""
    assert _code(term) == "13969-1", f"{term} -> {_code(term)}"


@pytest.mark.parametrize("term", CK_TOTAL)
def test_the_total_enzyme_is_not_the_isoenzyme(term):
    assert _code(term) == "2157-6", f"{term} -> {_code(term)}"


@pytest.mark.parametrize("term", ["CK-MB", "Creatine Kinase-MB", "肌酸激酶MB"])
def test_the_unit_still_picks_the_property_for_ck_mb(term, axis):
    """A report prints CK-MB as either an activity or a mass, so the unit decides.

    Both targets are method-less, and neither is the total enzyme: the curated
    row must not freeze the property it was written for.
    """
    activity = mirobody.resolve_reading(term, "25", "U/L")
    mass = mirobody.resolve_reading(term, "3.2", "ng/mL")
    assert activity.code == "32673-6", f"{term} U/L -> {activity.code}"
    assert mass.code == "13969-1", f"{term} ng/mL -> {mass.code}"
    for code in (activity.code, mass.code):
        row = axis[code]
        assert not row.method, f"{code} by {row.method}"
        assert row.component == "creatine kinase.mb", row.component


#: One analyte, spelled the way reports in each language print it. Every
#: spelling must answer, and all of them the same code: the code is the series
#: key, so a split here puts one person's readings on two trend lines.
#: Adding a language? Put its spelling in the matching row.
SAME_ANALYTE = {
    "hemoglobin": ["HGB", "hemoglobin", "血红蛋白", "ヘモグロビン", "Гемоглобин", "Гемоглобин общий"],
    "platelets": ["PLT", "platelet count", "血小板计数", "Тромбоциты", "Количество тромбоцитов"],
    "leukocytes": ["WBC", "white blood cell count", "白细胞计数", "Количество лейкоцитов"],
    "erythrocytes": ["RBC", "red blood cell count", "红细胞计数", "Количество эритроцитов"],
    "sedimentation rate": ["ESR", "血沉", "СОЭ", "Скорость оседания эритроцитов"],
    "MCV": ["MCV", "Mean Corpuscular Volume", "Средний объем эритроцитов"],
    "MCH": ["MCH", "Среднее содержание Hb в эритроците"],
    "MCHC": ["MCHC", "Средняя концентрация Hb в эритроците", "МСHС (ср. конц. Hb в эр.)"],
    "ALT": ["ALT", "丙氨酸氨基转移酶", "АЛТ", "АлАТ"],
    "AST": ["AST", "天门冬氨酸氨基转移酶", "АСТ", "АСаТ"],
    "total bilirubin": ["total bilirubin", "总胆红素", "Билирубин общий"],
    "indirect bilirubin": ["indirect bilirubin", "间接胆红素", "Билирубин непрямой"],
    "glucose": ["glucose", "血糖", "Глюкоза"],
    "total protein": ["total protein", "总蛋白", "Общий белок"],
    "urate": ["uric acid", "尿酸", "Мочевая кислота"],
    "urea nitrogen": ["BUN", "blood urea nitrogen", "尿素氮"],
    "creatine kinase": ["CK", "肌酸激酶", "Креатинкиназа"],
    "alkaline phosphatase": ["ALP", "alkaline phosphatase", "碱性磷酸酶", "Щелочная фосфатаза"],
    "total cholesterol": ["total cholesterol", "总胆固醇", "Холестерин общий"],
    "phosphate": ["phosphate", "无机磷", "Фосфор неорганический"],
    "C reactive protein": ["CRP", "C反应蛋白", "С-реактивный белок"],
    "rheumatoid factor": ["rheumatoid factor", "类风湿因子", "Ревматоидный фактор"],
    "TSH": ["TSH", "促甲状腺激素", "ТТГ", "Тиреотропный гормон"],
    "free T4": ["FT4", "free T4", "游离甲状腺素", "Т4 свободный", "Тироксин свободный"],
    "free T3": ["FT3", "free T3", "游离三碘甲状腺原氨酸", "Т3 свободный"],
    "TPO antibody": ["TPOAb", "anti-TPO", "甲状腺过氧化物酶抗体", "Анти-ТПО"],
    "vitamin B12": ["vitamin B12", "维生素B12", "Витамин В12", "Витамин B12", "Vitamiin B12"],
    "folate": ["folate", "叶酸", "Фолиевая кислота"],
}

#: Analytes whose spellings do NOT agree yet, each one an open fix. `want` is
#: the code the row should share where that is settled, None where the
#: convention itself is still open. A fix turns its row into a strict XPASS,
#: which fails on purpose: move the row up into SAME_ANALYTE.
KNOWN_SPLITS = [
    ("urea", ["urea", "尿素", "Мочевина", "Harnstoff"], "3091-6",
     "`urea` lands on 3094-0 Urea nitrogen, a different component"),
    ("direct bilirubin", ["Direct Bilirubin", "DBIL", "直接胆红素", "Билирубин прямой"], "1968-7",
     "the `direct bilirubin` row targets 15152-2 Bilirubin.conjugated"),
    ("vitamin D", ["vitamin D", "维生素D", "Витамин D", "Витамин D (25-OH)", "Vitamiin D (25-OH)"], None,
     "English and Chinese answer 1989-3 (D3), Russian and Estonian 62292-8 (D2+D3)"),
    ("thyroglobulin antibody", ["TgAb", "anti-thyroglobulin antibody", "甲状腺球蛋白抗体", "Анти-ТГ"], "8098-6",
     "the spelled-out English name answers nothing"),
    ("antistreptolysin O", ["ASO", "抗链球菌溶血素O", "АСЛО"], "5370-2",
     "the Chinese name answers nothing"),
    ("iron binding capacity", ["TIBC", "总铁结合力", "ОЖСС"], "2500-7",
     "the Chinese name answers nothing"),
    ("ionized calcium", ["ionized calcium", "离子钙", "Кальций ионизированный"], None,
     "the English name answers nothing"),
    ("neutrophil count", ["NEUT#", "中性粒细胞绝对值", "Нейтрофилы, абс.", "Нейтрофилы абс. к-во"], None,
     "the Russian rows target 751-8, `by Automated count`"),
    ("lymphocyte count", ["LYMPH#", "淋巴细胞绝对值", "Лимфоциты, абс.", "Лимфоциты абс. к-во"], None,
     "the Russian rows target 731-0, `by Automated count`"),
    ("monocyte count", ["MONO#", "单核细胞绝对值", "Моноциты, абс.", "Моноциты абс. к-во"], None,
     "the Russian rows target 742-7, `by Automated count`"),
    ("eosinophil count", ["EOS#", "嗜酸性粒细胞绝对值", "Эозинофилы, абс.", "Эозинофилы абс. к-во"], None,
     "the Russian rows target 711-2, `by Automated count`"),
    ("basophil count", ["BASO#", "嗜碱性粒细胞绝对值", "Базофилы, абс.", "Базофилы абс. к-во"], None,
     "the Russian rows target 704-7, `by Automated count`"),
    ("neutrophil fraction", ["NEUT%", "中性粒细胞百分比", "Нейтрофилы (общ.число)"], None,
     "the Russian row targets 770-8, `by Automated count`"),
]

_ANALYTES = [pytest.param(spellings, None, id=name) for name, spellings in SAME_ANALYTE.items()] + [
    pytest.param(spellings, want, id=name,
                 marks=pytest.mark.xfail(strict=True, reason=f"{why}; fixed? move the row to SAME_ANALYTE"))
    for name, spellings, want, why in KNOWN_SPLITS
]


@pytest.mark.parametrize("spellings,want", _ANALYTES)
def test_every_spelling_of_an_analyte_answers_one_code(spellings, want):
    codes = {term: _code(term) for term in spellings}
    missing = [term for term, code in codes.items() if not code]
    assert not missing, f"no code for {missing}: {codes}"
    assert len(set(codes.values())) == 1, codes
    if want:
        assert set(codes.values()) == {want}, codes

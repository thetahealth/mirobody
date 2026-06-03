"""Specificity-match demote — penalize LOINC candidates whose names carry
qualifiers the query never asked for.

LOINC names encode specificity beyond the 6 named axes (PROPERTY,
TIME_ASPCT, SCALE_TYP, METHOD_TYP, SYSTEM, CLASS) via inline modifiers:

  - ``Cobalamin (Vitamin B12) [Moles/volume] in Serum or Plasma``  ← intrinsic
  - ``Vitamin B12 intake 24 hour Estimated``                       ← intake recall
  - ``Glucose [Mass/volume] in Serum or Plasma --2 hours post meal`` ← challenge
  - ``Lutropin [Units/volume] in Serum or Plasma --baseline``      ← baseline
  - ``EPINEPHrine [Moles/volume] in Plasma --supine``              ← posture
  - ``Corticotropin [Mass/volume] in Plasma --10 AM specimen``     ← time of day
  - ``FEV1 Predicted``                                              ← reference value
  - ``CT Chest W contrast IV``                                     ← imaging contrast

A query like ``维生素B12`` carries none of those qualifiers, so the
candidate that *adds* one is strictly less likely to be the intended
target than the intrinsic-name peer. Cosine doesn't see this asymmetry
— the qualifier tokens read as additional context. We compensate at
score-merge time with an explicit asymmetric demote:

  * candidate has family marker AND query has NO matching license token
  → subtract a fixed weight from cosine

  * candidate has family marker AND query has a license token → no change
    (cosine already prefers the qualifier match)

  * candidate has no family marker → no change

The principle is symmetric to UCUM unit family: don't promote
quantitative specificity the input never offered.

Multilingual: query-side license markers list both Latin (English,
Spanish, German, French) and CJK (Chinese, Japanese, Korean) plus
Cyrillic (Russian) variants. Compile-time per-family combiner emits
``\\b...\\b`` for Latin/Cyrillic and bare substring for CJK.
"""

from __future__ import annotations

import logging
import re

import numpy as np

from ..common import _CODE_BITS, SYSTEM_TO_CODE

log = logging.getLogger(__name__)


# Demote magnitude. Matches the axis-bonus scale (0.03-0.04) so a
# single un-licensed specificity flag is enough to flip a borderline
# top-1 pick toward the intrinsic-name peer but not enough to override
# a clearly-better cosine. One row gets demoted at most once even when
# multiple families match — see ``apply_specificity_penalty``.
DEMOTE_WEIGHT: float = 0.05


# ── Per-family table ──────────────────────────────────────────────────


# Each family carries:
#   ``name_re``    — LOINC display-name regex (IGNORECASE). Marks the
#                    corpus-side specificity bit on rows whose name fires.
#   ``markers``    — multilingual query-side license tokens. Any one of
#                    them appearing anywhere in the query text licenses
#                    this family (no demote applied). Latin/Cyrillic
#                    markers are wrapped with ``\\b`` at compile; CJK
#                    markers (Han / Kana / Hangul) match as bare
#                    substrings since the regex word-boundary doesn't
#                    recognize ideographic edges.
#   ``marker_res`` — optional list of raw regex strings, joined into the
#                    same license-pattern alternation as ``markers``.
#                    Use for patterns no literal-string list can encode
#                    (parenthetical time intervals, numeric quantities).
FAMILIES: dict[str, dict[str, object]] = {
    # ── intake recall / dietary survey instruments ─────────────────
    # 209 LOINC rows ("Vitamin X intake N hour [Measured|Estimated]",
    # "Potassium intake 24 hour", etc.). These are nutrition-survey
    # instruments that record self-reported intake over a window,
    # not lab-style blood/urine concentrations. Lab archetype queries
    # ("维生素 B12") consistently false-positive into this pool when
    # the embedding sees "Vitamin B12" verbatim in the candidate.
    "intake_recall": {
        # ``intake N hour Estimated/Measured`` (timed dietary survey) plus
        # the un-timed ``X intake Estimated/Measured`` IO-balance rows
        # (``Protein intake Estimated`` 9079-5, ``Calcium intake
        # Measured`` 9046-4, ``Fluid intake Estimated`` 8984-7 — all
        # ``^Patient`` SYSTEM, IO_IN class). Both are self-reported intake
        # concepts, never specimen measurements, so a body-composition /
        # lab query with no intake marker should never land on them.
        "name_re": re.compile(
            r"\bintake\s+\d+\s*hours?\b"
            r"|\bintake\s+(?:Estimated|Measured)\b",
            re.IGNORECASE,
        ),
        "markers": [
            "intake", "dietary", "diet", "food recall", "FFQ", "food frequency",
            "摄入", "膳食", "饮食", "回顾",
            "攝入", "飲食",
            "摂取", "食事",
            "섭취", "식이",
            "ingesta", "ingestión", "dieta",
            "Aufnahme", "Ernährung", "Verzehr",
            "apport", "alimentaire", "régime",
            "потребление", "приём", "питание",
        ],
    },

    # ── clinician-set treatment targets / goal values ──────────────
    # ~30 LOINC rows ("25-Hydroxyvitamin D goal", "LDL goal",
    # "Systolic blood pressure goal", "INR goal", ...). These are
    # not lab measurements — they are target values the care team
    # set for a patient and want them to reach. Lab archetype queries
    # ("25-羟基维生素D") false-positive into this pool when the
    # embedder weighs the analyte name strongly enough that the
    # ``goal`` suffix gets diluted.
    "treatment_goal": {
        "name_re": re.compile(r"\bgoal(\s+\[|\s+in\s|$)"),
        "markers": [
            "goal", "target", "desired",
            "目标", "目標", "目的", "靶",
            "목표",
            "objetivo", "meta",
            "Ziel",
            "objectif", "cible",
            "цель", "целевой",
        ],
    },

    # ── challenge / load / post-prandial sampling ──────────────────
    # ~2000 LOINC rows: ``--Nth specimen post xxx challenge``,
    # ``--N hour(s)/minutes post xxx challenge``, ``--pre xxx challenge``,
    # ``--N hours post meal``, ``--pre or post Nhr food intake``.
    # The "after X" / "post-prandial" attractor regularly drags
    # plain ``空腹血糖`` / ``黄体生成素`` queries onto post-load codes
    # whose only differentiator is the timing qualifier.
    "challenge_test": {
        "name_re": re.compile(
            r"--.*\bpost\b"
            r"|\bpostprandial\b"
            r"|\b\d+\s*(?:hour|minute|hr|min)s?\s+post\b"
            r"|\bpre\s+(?:or\s+post|xxx)\b"
            r"|\bpost\s+xxx\b"
            r"|--\d+(?:st|nd|rd|th)?\s+specimen\b"
            r"|\bpre\s+\d+\s*hour\b",
            re.IGNORECASE,
        ),
        "markers": [
            "challenge", "OGTT", "GTT", "post-meal", "postprandial",
            "post-prandial", "post meal", "post dose", "post-dose",
            "post-load", "after meal", "after eating", "after dose",
            "glucose tolerance", "oral tolerance", "tolerance test",
            "fasting",  # ``空腹`` queries DO want the pre-meal qualified variant
            "餐后", "餐前", "饭后", "饭前", "进食后", "服药后", "服药前",
            "激发", "负荷", "试验后", "试验前", "糖耐量", "糖耐", "空腹",
            "餐後", "飯後", "進食後", "服藥後", "激發", "負荷", "試驗後",
            "食後", "食前", "糖負荷", "負荷試験", "絶食",
            "식후", "식전", "부하", "공복",
            "después de comer", "después de la comida",
            "tras la comida", "ayuno", "prueba de tolerancia",
            "nach der Mahlzeit", "nach dem Essen",
            "Belastung", "Belastungstest", "Glukosetoleranz", "nüchtern",
            "après le repas", "à jeun", "charge en glucose",
            "épreuve de charge",
            "после еды", "после нагрузки", "нагрузка", "натощак",
            "постпрандиальный",
        ],
        # Parenthetical time-interval annotations in lab indicator
        # queries ("C-肽(一小时)", "胰岛素(2 hours)", "(0.5 h)") nearly
        # always denote a challenge-test sampling point — the bare
        # analyte without a known protocol wouldn't carry an explicit
        # post-event time. Matches CJK numerals + arabic digits + a
        # short list of common time units. Tolerates both () and
        # （） brackets.
        "marker_res": [
            r"[（(]\s*[半零一两二三四五六七八九十0-9.]+\s*"
            r"(?:小时|小時|分钟|分鐘|時間|시간)\s*[)）]",
            r"[（(]\s*\d+(?:\.\d+)?\s*(?:hr|h|hour|hours|min|minutes)\s*[)）]",
        ],
    },

    # ── baseline / basal sampling ──────────────────────────────────
    "baseline": {
        "name_re": re.compile(r"\bbaseline\b", re.IGNORECASE),
        "markers": [
            "baseline", "basal", "basal level",
            "基线", "基础", "基础值",
            "基線", "基礎", "基礎值",
            "ベースライン", "基礎値",
            "기저", "기준선", "기저치",
            "línea de base", "valor basal",
            "Basalwert", "Ausgangswert",
            "valeur de base", "valeur basale",
            "базовый", "исходный", "базальный",
        ],
    },

    # ── peak / trough drug-level sampling ──────────────────────────
    "trough_peak": {
        "name_re": re.compile(r"--\s*(?:trough|peak|valley)\b", re.IGNORECASE),
        "markers": [
            "trough", "peak", "valley", "Cmax", "Cmin", "C-max", "C-min",
            "谷值", "谷浓度", "峰值", "峰浓度", "最低浓度", "最高浓度",
            "谷濃度", "峰濃度", "最低濃度", "最高濃度",
            "トラフ", "ピーク", "谷値", "ピーク値",
            "트로프", "피크", "최저농도", "최고농도",
            "valle", "pico", "concentración máxima", "concentración mínima",
            "Talspiegel", "Spitzenspiegel", "Maximum", "Minimum",
            "creux", "pic", "concentration maximale", "concentration minimale",
            "пик", "минимум", "максимум",
        ],
    },

    # ── patient posture / activity state ───────────────────────────
    "posture": {
        "name_re": re.compile(
            r"--\s*(?:supine|standing|sitting|seated|upright|recumbent|"
            r"lying|prone|resting)\b",
            re.IGNORECASE,
        ),
        "markers": [
            "supine", "standing", "sitting", "seated", "lying", "upright",
            "recumbent", "prone", "resting", "at rest",
            "卧位", "仰卧", "平卧", "立位", "站立位", "站立", "坐位", "俯卧",
            "静息", "安静", "休息",
            "臥位", "仰臥", "俯臥", "靜息",
            "仰臥位", "安静時",
            "앙와위", "입위", "좌위", "안정시", "안정",
            "supino", "decúbito", "sentado", "de pie", "en reposo", "prono",
            "liegend", "stehend", "sitzend", "in Ruhe", "ruhend",
            "couché", "debout", "assis", "au repos", "allongé",
            "лёжа", "стоя", "сидя", "в покое",
        ],
    },

    # ── respiratory phase / ventilation / bronchodilator state ─────
    "respiratory_phase": {
        "name_re": re.compile(
            r"--\s*(?:during|at\s+end)\s+(?:inspiration|expiration|"
            r"inhalation|exhalation)\b"
            r"|\bon\s+ventilator\b"
            r"|\b(?:pre|post)\s+bronchodilation\b"
            r"|\bbronchodilator\b"
            r"|\b(?:pre|post)\s+inhalation\s+therapy\b",
            re.IGNORECASE,
        ),
        "markers": [
            "inspiration", "expiration", "inhalation", "exhalation",
            "inspiratory", "expiratory", "ventilator", "ventilated",
            "mechanical ventilation", "bronchodilation", "bronchodilator",
            "post-bronchodilator", "pre-bronchodilator",
            "吸气", "呼气", "吸气末", "呼气末", "呼吸末", "呼吸机",
            "机械通气", "支扩", "支气管扩张", "舒张试验", "舒张后", "舒张前",
            "吸氣", "呼氣", "呼吸機", "機械通氣", "支氣管擴張", "舒張試驗",
            "吸気", "呼気", "人工呼吸", "気管支拡張", "気管支拡張薬",
            "흡기", "호기", "인공호흡", "기관지확장",
            "inspiración", "espiración", "ventilador", "broncodilatador",
            "Exspiration", "Beatmung", "Bronchodilatator",
            "ventilateur", "bronchodilatateur",
            "вдох", "выдох", "вентилятор", "бронходилатация",
        ],
    },

    # ── dialysis state ─────────────────────────────────────────────
    "dialysis": {
        "name_re": re.compile(
            r"\b(?:pre|post)?\s*dialysis\b",
            re.IGNORECASE,
        ),
        "markers": [
            "dialysis", "hemodialysis", "haemodialysis", "peritoneal dialysis",
            "dialysed", "dialyzed",
            "透析", "血液透析", "血透", "腹膜透析",
            "투석", "혈액투석", "복막투석",
            "diálisis", "hemodiálisis", "diálisis peritoneal",
            "Dialyse", "Hämodialyse", "Peritonealdialyse",
            "dialyse", "hémodialyse", "dialyse péritonéale",
            "диализ", "гемодиализ", "перитонеальный диализ",
        ],
    },

    # ── dexamethasone suppression test ─────────────────────────────
    "dexamethasone": {
        "name_re": re.compile(r"\bdexamethasone\b", re.IGNORECASE),
        "markers": [
            "dexamethasone", "dexa", "DST", "dex suppression",
            "dexamethasone suppression",
            "地塞米松", "地米", "地塞米松抑制", "抑制试验",
            "抑制試驗",
            "デキサメタゾン", "デキサメタゾン抑制",
            "덱사메타손", "덱사메타손억제",
            "dexametasona",
            "Dexamethason",
            "dexaméthasone",
            "дексаметазон",
        ],
    },

    # ── circadian / time-of-day specimen ───────────────────────────
    "time_of_day": {
        "name_re": re.compile(r"--\s*\d+\s*[AP]M\b", re.IGNORECASE),
        "markers": [
            "AM specimen", "PM specimen", "morning", "evening", "afternoon",
            "night", "noon", "midnight", "diurnal", "nocturnal", "circadian",
            "上午", "下午", "早晨", "早上", "傍晚", "夜间", "夜晚", "中午", "凌晨",
            "晨起", "晨", "晚",
            "夜間",
            "午前", "午後", "朝", "夕方", "正午", "深夜",
            "오전", "오후", "아침", "저녁", "밤", "정오",
            "mañana", "tarde", "noche", "mediodía",
            "morgens", "abends", "nachts", "mittags",
            "matin", "soir", "nuit", "midi", "après-midi",
            "утром", "вечером", "ночью", "утра", "вечера", "днём",
        ],
        # Bracketed clock-time (``（16:00）``, ``(8:00)``, ``（24:00）``)
        # — the standard form Chinese indicator panels use to encode
        # sampling time. Without this, the time_of_day demote would
        # cancel out the clock_time bonus (see
        # :mod:`.challenge_time._build_loinc_clock_index`). ASCII /
        # fullwidth brackets and colons both accepted.
        "marker_res": [
            r"[（(]\s*[0-9]{1,2}\s*[:：]\s*[0-9]{2}\s*[)）]",
        ],
    },

    # ── predicted / reference value ────────────────────────────────
    # FEV1 Predicted (~67 rows). Hit observed for "1秒率 FEV1/FVC"
    # which has no predicted/expected marker; the resolver picked
    # ``FEV1 Predicted`` because cosine alone couldn't distinguish.
    # ``\bmean\b`` deliberately left OFF — too many canonical LOINC
    # names use ``mean`` as the analyte (``Mean platelet volume``,
    # ``Mean cell hemoglobin``), and demoting those would regress
    # the routine CBC analytes far harder than it'd help the
    # rare ``Glucose mean`` case.
    "predicted": {
        "name_re": re.compile(
            r"\b(?:predicted|expected)\b",
            re.IGNORECASE,
        ),
        "markers": [
            "predicted", "expected", "prediction", "reference value",
            "预计", "预测", "预期", "预报", "参考值",
            "預計", "預測", "預期", "預報", "參考值",
            "予測", "予想", "推定", "予測値",
            "예측", "예상", "추정", "참고치",
            "predicho", "esperado", "previsto", "valor de referencia",
            "vorhergesagt", "erwartet", "Referenzwert",
            "prédit", "prévu", "valeur de référence",
            "прогноз", "ожидаемый", "ориентировочный", "расчётный",
        ],
    },

    # ── pediatric / neonatal population qualifier ──────────────────
    "population_specific": {
        "name_re": re.compile(
            r"\b(?:pediatric|paediatric|newborn|neonatal|neonate|infant)\b",
            re.IGNORECASE,
        ),
        "markers": [
            "pediatric", "paediatric", "newborn", "infant", "neonatal",
            "neonate", "child", "children",
            "儿童", "小儿", "儿科", "新生儿", "婴儿", "幼儿",
            "兒童", "小兒", "兒科", "新生兒", "嬰兒", "幼兒",
            "小児", "新生児", "乳児", "幼児",
            "소아", "신생아", "영아", "유아",
            "pediátrico", "neonato", "lactante", "niño", "infantil",
            "pädiatrisch", "Neugeborenes", "Säugling", "Kinder",
            "pédiatrique", "nouveau-né", "nourrisson", "enfant",
            "педиатрический", "новорождённый", "младенец", "ребёнок",
            "детский",
        ],
    },

    # ── contrast / enhanced imaging ────────────────────────────────
    # Critical for the upcoming radiology benchmark — query without
    # explicit ``增强`` / ``contrast`` shouldn't land on contrast-IV
    # imaging codes when the plain (W/O contrast) variant exists.
    "contrast_imaging": {
        "name_re": re.compile(
            r"\bw\s+contrast\b"
            r"|\bwith\s+contrast\b"
            r"|\bcontrast\s+i[av]\b"
            r"|\bgadolinium\b",
            re.IGNORECASE,
        ),
        "markers": [
            "contrast", "enhanced", "gadolinium", "iodinated contrast",
            "with contrast", "post contrast", "IV contrast",
            "增强", "造影", "对比剂", "钆", "碘对比剂", "强化扫描", "增强扫描",
            "增強", "對比劑", "釓", "碘對比劑", "增強掃描",
            "増強", "造影剤", "ガドリニウム",
            "조영", "증강", "조영제", "가돌리늄",
            "contraste", "realce", "realzado", "gadolinio",
            "Kontrastmittel", "kontrastverstärkt",
            "rehaussé", "produit de contraste",
            "контраст", "контрастный", "контрастирование", "гадолиний",
        ],
    },

    # ── transfusion-medicine SUBJECT qualifier ─────────────────────
    # 507 LOINC rows whose names carry ``from Donor`` (275) or ``from
    # Blood product unit`` (198) — blood-bank / supply-chain assays
    # for donor screening or unit QC. Patient-side immunology / blood
    # typing queries (``A 抗原``, ``D 抗原``, ``B 抗体``) cosine-collide
    # with these because the Chinese category prefix ``血液专项检查``
    # / ``血型`` aligns with the ``Blood`` token in ``from Blood product
    # unit`` and ``Red Blood Cells from Donor``. Without demote, queries
    # without any donor/transfusion marker landed on the qualified
    # variant when the bare patient-side code (820-1 ``A Ag on RBC``,
    # 14982-3 ``A variant subtype Ab in Serum or Plasma``) was the
    # intended top-1. Demote (not skip) preserves recall for genuine
    # blood-bank queries that license via ``供血者 / 血制品 / donor /
    # transfusion`` markers.
    "transfusion_subject": {
        "name_re": re.compile(
            r"\bfrom\s+(?:donor|blood\s+product\s+unit)\b",
            re.IGNORECASE,
        ),
        "markers": [
            "donor", "donors", "blood donor", "blood product",
            "blood product unit", "blood bank", "transfusion",
            "allogeneic", "allogenic",
            "供血", "供者", "供血者", "献血", "献血者",
            "血制品", "血液制品", "血库", "输血", "异体输血",
            "獻血", "獻血者", "血製品", "血液製品", "血庫",
            "輸血", "異體輸血",
            "血液製剤",
            "헌혈", "헌혈자", "수혈", "혈액제제",
            "donante", "donador", "transfusión", "banco de sangre",
            "Spender", "Blutspender", "Bluttransfusion", "Blutbank",
            "donneur", "banque de sang",
            "донор", "переливание", "трансфузия", "банк крови",
        ],
    },

    # ── post-meal / post-prandial protocol qualifier ───────────────
    # 65 LOINC rows whose names carry ``--N hour(s) post meal``,
    # ``--N min post meal``, ``--post meal``, or ``--postprandial``.
    # These are MEAL-protocol variants of analytes that also have
    # glucose-load / challenge variants (``--N hours post 75 g glucose
    # PO``, ``--N hours post dose glucose``, ``--N hour post XXX
    # challenge``). When a query specifies only a time interval (e.g.
    # ``胰岛素(半小时)`` under a ``糖尿病筛查`` parent), the embedding
    # spreads thinly across all protocol variants and the cosine
    # winner is essentially noise. Conservative drop: kill ``post
    # meal`` rows unless the query explicitly names a meal context
    # (``餐后`` / ``post-prandial`` / ``nach der Mahlzeit`` / etc.).
    # Strict narrower than ``challenge_test``: that family covers
    # every ``--post`` qualifier including OGTT/fasting; this one
    # targets only the meal subset.
    "post_meal": {
        "name_re": re.compile(
            r"--[^-]*\bpost\s+meal\b"
            r"|--[^-]*\bpostprandial\b",
            re.IGNORECASE,
        ),
        "markers": [
            "post meal", "post-meal", "after meal", "after eating",
            "after food", "post food", "postprandial", "post-prandial",
            "post breakfast", "post lunch", "post dinner",
            "餐后", "饭后", "进食后", "进餐", "进食", "饭前", "餐前",
            "餐後", "飯後", "進食後", "進餐", "進食", "飯前", "餐前",
            "食後", "食前", "食事後", "食事前",
            "식후", "식전",
            "tras la comida", "tras comer",
            "después de comer", "después de la comida",
            "nach der Mahlzeit", "nach dem Essen",
            "après le repas", "après les repas",
            "после еды", "постпрандиальный",
        ],
    },

    # ── mass-ratio / ratio PROPERTY rows ───────────────────────────
    # 1043 LOINC rows whose PROPERTY=MRto / Ratio surface in LCN as
    # ``[Mass Ratio]`` / ``[Ratio]`` — pair-of-analyte ratios
    # (Apo B/Apo A-I, Lipoprotein.beta/Lipoprotein.alpha, X/Creatinine
    # excretion, Aldosterone/Renin, ...). Single-analyte queries
    # (``A型脂蛋白`` Lp(a), ``醛固酮``) cosine-match a ratio code
    # whenever the EntLen-/EntMass-specific variants are demoted and
    # the next-best cosine pick is the ratio — the ``/`` glyph adds
    # no embedding penalty and the COMPONENT prefix (``Lipoprotein.
    # alpha``) matches verbatim. Default to non-ratio when the query
    # carries no ratio marker; explicit pair queries (``LDL/HDL``,
    # ``Apo B/A-I 比值``, ``aldosterone to renin ratio``) license the
    # family via marker keywords / the ``/``-between-analyte regex.
    "mass_ratio": {
        # Two LCN patterns flag pair-of-analyte rows:
        #
        # 1. ``[Mass Ratio]`` — PROPERTY=MRto explicit bracket. Catches
        #    legitimate ratios with single-letter analyte suffixes
        #    (``Apolipoprotein B/Apolipoprotein A-I [Mass Ratio]``)
        #    that the pair-COMPONENT regex would miss. Deliberately
        #    NOT the bare ``[Ratio]`` since 38 LCNs use ``[Ratio]``
        #    as a dimensionless-index unit on a single COMPONENT
        #    (Body mass index, Platelet distribution width,
        #    Fractional excretion of X, Lymphocyte proliferation
        #    stimulated by X, Ab avidity ratios) — those aren't
        #    pair-form and demoting them when the user asks for the
        #    indicator itself is a regression.
        #
        # 2. ``<word3+>/<word3+>`` at the COMPONENT position — catches
        #    the ~3700 PROPERTY=Fraction rows (NFr / MFr / AFr / VFr /
        #    CFr / SFr / *.DF) whose LCN is ``Cells.X/Cells.Y in
        #    Specimen`` style without an explicit ratio bracket, plus
        #    PROPERTY=Ratio pair rows with multi-char analyte names
        #    on both sides. Restricted to the pre-``[`` head so unit
        #    denominators (``[Mass/volume]`` / ``[Mass/time]``) don't
        #    trigger; requires ≥3 alphanum chars on each side of ``/``
        #    to skip subtype letters (``Calcium channel P/Q type``)
        #    and short unit symbols. Same semantics as Ratio (pair-
        #    of-analyte expression) so markers license both together.
        "name_re": re.compile(
            r"\[Mass\s+Ratio\]"
            r"|^[^[]*?\b[A-Za-z][\w.+-]{2,}\s*/\s*[A-Za-z][\w.+-]{2,}\b",
            re.IGNORECASE,
        ),
        "markers": [
            # Ratio markers — explicit
            "ratio", "ratios",
            "比值", "比率", "比例", "对比",
            "比值", "比率", "比例", "對比",
            # CN suffix-style ratio markers: in clinical naming, a
            # bare ``比`` / ``率`` / ``指数`` / ``相对`` suffix
            # frequently denotes a ratio (``A/G 比``, ``1秒率``,
            # ``相对指数``, ``利用率``). Over-licensing risk is
            # bounded — worst case the family doesn't demote, falling
            # back to plain cosine which is the pre-mass_ratio
            # behavior.
            "比", "率", "指数", "相对",
            "比", "率", "指數", "相對",
            "比率",                                  # JA
            "비율", "비례",                           # KO
            "Verhältnis", "Quotient", "Index",
            "rapport", "indice",
            "razón", "cociente", "proporción", "índice",
            "отношение", "соотношение", "коэффициент", "индекс",
            # Fraction markers — same pair-of-analyte semantics
            "fraction", "fractional", "percentage", "percent",
            "百分", "百分比", "比重", "分数", "饱和度", "饱和率",
            "百分比", "比重", "飽和度", "飽和率",
            "分画", "分率",                          # JA
            "분율", "분획", "비중",                   # KO
            "Anteil", "Fraktion", "Prozent", "Sättigung",
            "pourcentage", "saturation",
            "fracción", "porcentaje", "saturación",
            "доля", "процент", "насыщение",
        ],
        # ``/``-between-multi-character-analyte license. Constrained
        # to ≥2 CJK chars or ≥3 Latin letters on each side to avoid
        # licensing unit-denominator patterns like ``mg/dL`` /
        # ``mEq/L`` / ``kPa/s``. Catches ``LA/DGLA``, ``Apo B/Apo A-I``,
        # ``亚油酸/二高 γ- 亚麻酸``, ``LDL/HDL`` (LDL=3, HDL=3 — passes).
        "marker_res": [
            r"[一-鿿]{2,}\s*[/／]\s*[一-鿿]{2,}",
            r"\b[A-Za-z][\w-]{2,}\s*/\s*[A-Za-z][\w-]{2,}\b",
        ],
    },

    # ── particle-size (Entitic length) measurement ─────────────────
    # 6 LOINC rows whose PROPERTY=EntLen surface in LCN as
    # ``[Entitic length]`` — particle / cell *size* measurements
    # (LDL subparticle, platelet mean diameter, RBC distribution
    # width, ...). When a query says ``颗粒数 / particle number``
    # the cosine routinely picks 17782-4 ``Lipoprotein.beta.subparticle
    # [Entitic length]`` over the count variant 54434-6 ``[Moles/
    # volume]`` because both rows share the same COMPONENT and the
    # encoder can't reliably bridge ``数`` ↔ ``Moles/volume`` while
    # heavily anchoring on ``Lipoprotein.beta.subparticle``. The
    # length variant is the specific / less-common request; default
    # to count by demoting EntLen rows unless the query explicitly
    # asks for size / length / diameter.
    "particle_length": {
        "name_re": re.compile(r"\[Entitic\s+length\]", re.IGNORECASE),
        "markers": [
            "length", "size", "diameter", "width",
            "长度", "大小", "直径", "宽度", "粒径",
            "長度", "大小", "直徑", "寬度", "粒徑",
            "サイズ", "直径", "粒径",
            "크기", "직경", "지름",
            "tamaño", "diámetro", "longitud",
            "Größe", "Durchmesser", "Länge",
            "taille", "diamètre", "longueur",
            "размер", "диаметр", "длина",
        ],
    },

    # ── coagulation mixing-study methodology ───────────────────────
    # 70 LOINC rows whose METHOD_TYP carries ``factor substitution``
    # (with / without ``1:1`` / ``1:4`` dilution, ``immediately after``
    # / ``NH post incubation``, optional ``normal plasma`` addition
    # phrasing). These are mixing-study variants of standard coagulation
    # times — PT / aPTT / Thrombin time / Reptilase time / Russell
    # viper venom time. In clinical workflow, mixing studies are
    # ordered when a screening PT/aPTT is prolonged and follow-up
    # discrimination between factor deficiency vs inhibitor is
    # needed; the bare screening ``凝血酶原时间`` / ``PT`` query is
    # the everyday plain PT (5902-2). Without this demote, the 1H-
    # post-incubation variant (96261-3) sneaks into top-1 because the
    # ``Prothrombin time (PT)`` LCN prefix is identical to the
    # plain code's and the cosine gap is razor-thin.
    "factor_substitution": {
        "name_re": re.compile(
            r"\bfactor\s+substitution\b"
            r"|\bmixing\s+study\b",
            re.IGNORECASE,
        ),
        "markers": [
            "factor substitution", "mixing study", "mixing test",
            "incubation", "1:1 mix", "1:4 mix", "normal plasma",
            "纠正试验", "纠正实验", "纠正",
            "糾正試驗", "糾正實驗",
            "因子代替", "因子替代",
            "混合试验", "混合实验",
            "混合試驗", "混合實驗",
            "混合研究",
            "クロスミキシング", "混合試験",      # JA
            "교차혼합", "혼합검사",                 # KO
            "estudio de mezcla", "prueba de mezcla",
            "Plasmatauschversuch", "Mischversuch",
            "épreuve de correction", "test de mélange",
            "коррекция", "смешивание",
        ],
    },

    # ── unspecified-challenge placeholder ──────────────────────────
    # LOINC display-name placeholder ``XXX challenge`` (capital-X
    # exact) marks codes whose challenge agent is intentionally
    # unspecified — meant as a parent/fallback when the specific
    # agent isn't known. For real-world health-check / OGTT queries
    # the specific variants (``--post 75 g glucose PO``, ``--post dose
    # glucose``, ``--post meal``) are always more accurate. Cosine
    # can't tell ``XXX challenge`` from the specific ones (the
    # ``XXX`` token has no semantic weight in embeddings), so the
    # generic 27379-7 / 40293-3 / etc. routinely beat 72902-0 / 1567-7
    # by a hair. Empty marker list (only literal ``XXX`` — which
    # almost never appears in real queries) means this family
    # effectively always fires, hard-dropping the placeholder rows.
    "xxx_challenge": {
        "name_re": re.compile(r"\bXXX\s+challenge\b"),
        "markers": ["XXX"],
    },

    # ── Detection-limit / high-sensitivity assay variants ──────────
    # 36 LOINC rows with ``by Detection limit <= N <unit>`` (Microalbumin
    # ≤ 3.0 mg/L, Thyrotropin ≤ 0.05/0.005 mIU/L, PSA ≤ 0.01 ng/mL,
    # Testosterone ≤ 1.0 ng/dL, Troponin I ≤ 0.01 ng/mL) plus viral-load
    # NAA rows (HCV / HBV / HIV RNA) with ``detection limit = N copies/mL``.
    # These are SPECIFIC high-sensitivity / ultra-sensitive assay variants;
    # plain analyte queries (``24小时白蛋白定量``, ``PSA``, ``TSH``) want
    # the canonical-sensitivity row, not the low-detection-limit one.
    # Cosine drifts when the analyte name (``Microalbumin`` for
    # ``白蛋白``) co-occurs with high cosine on its own.
    "detection_limit": {
        "name_re": re.compile(
            r"by\s+(?:.+?\s+)?detection\s+limit\b"
            # All Microalbumin rows — Microalbumin IS the low-DL Albumin
            # assay name. Plain ``白蛋白`` / ``Albumin`` queries clinically
            # mean the standard Albumin assay; Microalbumin is for the
            # microalbuminuria range and clinicians explicitly mark it
            # with 微量 / micro- / hs- / 高敏 / 检测下限. Default-demote
            # the entire Microalbumin family; the marker list below
            # licenses it back when the query carries explicit intent.
            r"|\bMicroalbumin\b",
            re.IGNORECASE,
        ),
        "markers": [
            # English
            "detection limit", "DL <=", "DL <", "DL = ",
            "ultra-sensitive", "ultrasensitive", "ultra sensitive",
            "high-sensitivity", "high sensitivity", "highly sensitive",
            "hsTroponin", "hs-Troponin", "hs-Trop",
            "hs-cTn", "hs-PSA", "hs-TSH", "uPSA",
            # Microalbumin — the only analyte where ``microalbumin`` IS
            # the canonical clinical term (gold-standard low-DL urine
            # albumin). Including the marker so 微量白蛋白 / microalbumin
            # queries pass through the family unscathed.
            "microalbumin", "micro-albumin", "micro albumin",
            # CN — high-sens / 超敏 / 微量 license the DL variant
            "微量白蛋白", "尿微量白蛋白",
            "超敏", "高敏", "超敏感", "高敏感",
            "高敏肌钙蛋白", "超敏肌钙蛋白",
            "超敏C反应蛋白", "高敏C反应蛋白",
            "检测下限", "檢測下限", "检测限", "檢測限",
            # JA / KR
            "高感度", "超高感度", "검출한계", "초고감도",
            # Romance / Germanic
            "alta sensibilidad", "alta sensitividad",
            "hochsensitiv", "Nachweisgrenze",
            "haute sensibilité", "limite de détection",
            "высокочувствительный", "предел обнаружения",
        ],
    },

    # ── Long-tail `--<qualifier>` catchall ─────────────────────────
    # The 12 specific families above cover ~86% of LOINC rows whose
    # display name carries a ``--`` suffix; the residual ~14% (866
    # rows: ``--during anesthesia``, ``--at end expiration``,
    # ``--week 28``, ``--light-adapted``, ``--immediately``, etc.) is
    # too long-tail to enumerate cleanly. This catchall demotes ANY
    # ``--``-bearing LOINC when the query carries no specificity
    # marker from any of the 12 specific families. Markers populated
    # post-hoc just below the FAMILIES literal — the union of every
    # other family's markers means a query that licenses ANY specific
    # specificity dimension also licenses the catchall.
    "any_dash": {
        "name_re": re.compile(r"--", re.IGNORECASE),
        "markers": [],         # populated below
        "marker_res": [],      # populated below
    },
}


# Populate ``any_dash`` markers as the union of all OTHER families'
# markers and marker_res. Semantics: a query is licensed for the
# catchall demote iff it carries ANY specificity marker. When the
# query is specificity-silent, every ``--``-bearing LOINC gets the
# demote regardless of which specific family it belongs to — closes
# the long-tail gap without enumerating every dash token. Specific
# families still operate independently on top, so a query licensing
# ``posture`` (say) still leaves ``respiratory_phase``-flavored rows
# demoted via their own family.
_any_dash_markers: list[str] = []
_any_dash_marker_res: list[str] = []
for _k, _fam in FAMILIES.items():
    if _k == "any_dash":
        continue
    _any_dash_markers.extend(_fam["markers"])  # type: ignore[arg-type]
    _extra = _fam.get("marker_res")
    if _extra:
        _any_dash_marker_res.extend(_extra)  # type: ignore[arg-type]
FAMILIES["any_dash"]["markers"] = _any_dash_markers
FAMILIES["any_dash"]["marker_res"] = _any_dash_marker_res


# ── Multilingual marker regex compile ─────────────────────────────────


# Detects whether a marker string is CJK (Han/Kana/Hangul). CJK
# markers skip the Latin/Cyrillic ``\\b`` word-boundary wrap since
# Python's ``\\b`` doesn't fire on ideographic edges. Everything else
# (Latin, Cyrillic, Greek) keeps word boundaries — necessary so
# ``intake`` doesn't match inside ``intakeable`` etc.
_CJK_CHAR_RE = re.compile(
    r"[぀-ヿ"        # Hiragana + Katakana
    r"㐀-䶿"         # CJK Unified Ext A
    r"一-鿿"         # CJK Unified
    r"가-힯"         # Hangul Syllables
    r"豈-﫿]"        # CJK Compatibility
)


def _compile_marker_pattern(
    markers: list[str],
    marker_res: list[str] | None = None,
) -> re.Pattern:
    """Compile a single regex that matches any of *markers* or *marker_res*.

    Latin/Cyrillic markers wrapped with ``\\b``; CJK markers matched
    as bare substrings. Markers are ``re.escape``-d so ``post-meal``,
    ``c-max``, etc. survive without alternation regressions. Raw regex
    patterns in *marker_res* are appended verbatim — the caller is
    responsible for any boundary / case handling those need.
    """
    parts: list[str] = []
    seen: set[str] = set()
    for m in markers:
        if not m or m in seen:
            continue
        seen.add(m)
        esc = re.escape(m)
        if _CJK_CHAR_RE.search(m):
            parts.append(esc)
        else:
            parts.append(r"\b" + esc + r"\b")
    if marker_res:
        parts.extend(marker_res)
    if not parts:
        # Sentinel that never matches — keeps query_licensed_families'
        # search() call uniform without a None branch.
        return re.compile(r"(?!)")
    return re.compile("|".join(parts), re.IGNORECASE)


# Compiled once per family at module import. Two-pass build keeps the
# FAMILIES table edit-friendly (markers as plain Python lists) while
# the hot path uses precompiled patterns.
_QUERY_RES: dict[str, re.Pattern] = {
    key: _compile_marker_pattern(
        fam["markers"],  # type: ignore[arg-type]
        fam.get("marker_res"),  # type: ignore[arg-type]
    )
    for key, fam in FAMILIES.items()
}


# ── Corpus-side mask building (cached) ────────────────────────────────


def _build_loinc_masks(cache: dict) -> dict[str, np.ndarray]:
    """Scan LOINC display names and tag each family's hit rows.

    Returns ``{family_key: bool[N]}`` over the full corpus; non-LOINC
    rows are always False so the mask AND-s cleanly into the per-system
    selector inside the adapter. Result is cached on *cache* under
    ``_specificity_masks`` — subsequent queries reuse the same arrays.
    """
    canonical = np.asarray(cache["canonical"])
    sys_arr = (canonical >> _CODE_BITS) & 0x7
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    names = cache.get("names")
    if not names:
        return {}
    n = canonical.shape[0]
    loinc_idx = np.where(is_loinc)[0]

    masks: dict[str, np.ndarray] = {}
    for key, fam in FAMILIES.items():
        rx: re.Pattern = fam["name_re"]  # type: ignore[assignment]
        m = np.zeros(n, dtype=bool)
        hits = 0
        for i in loinc_idx:
            nm = names[i]
            if nm and rx.search(nm):
                m[i] = True
                hits += 1
        masks[key] = m
        log.info(
            "specificity mask %s: %d / %d LOINC rows flagged",
            key, hits, int(is_loinc.sum()),
        )
    return masks


def _ensure_loinc_masks(cache: dict) -> dict[str, np.ndarray]:
    """Lazy cached accessor for ``_build_loinc_masks``."""
    masks = cache.get("_specificity_masks")
    if masks is None:
        masks = _build_loinc_masks(cache)
        cache["_specificity_masks"] = masks
    return masks


# ── Query-side API ────────────────────────────────────────────────────


def query_licensed_families(query_text: str) -> set[str]:
    """Return the set of family keys whose markers appear in *query_text*.

    Consumed by the filter pipeline (``mirobody.indicator.fhir.
    resolve.pipeline._LOINC_FILTERS``) to gate hard-drop family masks
    by per-query license markers. Empty string returns the empty set.

    Bridge to ``challenge_time.query_time_intervals``: bare-CJK time
    tokens (``2小时血糖``, ``半小时胰岛素``) are recognized only by the
    challenge-time scanner, not by ``challenge_test.marker_res`` which
    matches PARENTHESIZED forms like ``(2小时)`` only. Without this
    bridge, ``_challenge_time_keep`` strict-keeps the ``--2 hours...``
    rows for the query while ``any_dash`` (catchall, union markers)
    independently drops every ``--``-bearing row, AND-merging to an
    empty keep mask. Bridging here keeps the two filters consistent:
    if the query carries time, it licenses both ``challenge_test`` and
    (via the family-union) ``any_dash``. Specimen-duration suppression
    (``24小时尿``, ``小时粪``, …) is honored inside
    ``query_time_intervals`` so this bridge inherits it for free.
    """
    if not query_text:
        return set()
    from .challenge_time import query_time_intervals
    licensed = {
        key for key, rx in _QUERY_RES.items()
        if rx.search(query_text)
    }
    if query_time_intervals(query_text):
        licensed.add("challenge_test")
        licensed.add("any_dash")
    return licensed


def specificity_penalty(
    cache: dict,
    query_texts: list[str],
    valid_idx: list[int],
    weight: float = DEMOTE_WEIGHT,
) -> tuple[np.ndarray | None, list[set[str]]]:
    """Build the (B, N) penalty matrix and return per-query licensed sets.

    For each query *b*:

    1. Determine which families are licensed (query text carries at
       least one marker).
    2. OR-union the corpus masks of every UN-licensed family.
    3. Subtract *weight* at every position in that union — so a row
       gets demoted exactly once even if it matches multiple
       un-licensed families.

    Returns ``(penalty, licensed)``. ``penalty`` is ``None`` when no
    LOINC names are loaded (caller skips the addition); otherwise it's
    a contiguous fp32 array the caller adds straight into ``scores_all``.
    ``licensed[b]`` is the set of family keys licensed for query *b*
    (for diagnostic surfacing).
    """
    licensed_per_q: list[set[str]] = [set() for _ in valid_idx]
    masks = _ensure_loinc_masks(cache)
    if not masks:
        return None, licensed_per_q

    n_rows = int(np.asarray(cache["canonical"]).shape[0])
    B = len(valid_idx)
    penalty = np.zeros((B, n_rows), dtype=np.float32)
    for b, qi in enumerate(valid_idx):
        qt = query_texts[qi] if qi < len(query_texts) else ""
        licensed = query_licensed_families(qt) if qt else set()
        licensed_per_q[b] = licensed
        unlicensed = [k for k in masks if k not in licensed]
        if not unlicensed:
            continue
        union = masks[unlicensed[0]]
        for k in unlicensed[1:]:
            union = union | masks[k]
        penalty[b, union] -= weight
    return penalty, licensed_per_q


__all__ = [
    "DEMOTE_WEIGHT",
    "FAMILIES",
    "query_licensed_families",
    "specificity_penalty",
]

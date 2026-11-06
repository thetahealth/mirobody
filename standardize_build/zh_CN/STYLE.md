# LOINC 2.83 zh-CN variant: review, completion and polish — style guide

You are improving LOINC's official Simplified Chinese linguistic variant (zhCN5, LOINC 2.83).
That variant translates the six Parts of every term (COMPONENT, PROPERTY, TIME_ASPCT, SYSTEM,
SCALE_TYP, METHOD_TYP) but leaves LONG_COMMON_NAME and ConsumerName empty. The result will be
submitted back to LOINC (Regenstrief) as a translation contribution and shown to Chinese-speaking
patients in a health-record app. Accuracy beats elegance; never invent meaning.

## General rules (all tasks)

- Simplified Chinese, mainland usage. Prefer national standard medical terms (全国科学技术名词审定委员会
  医学名词; 卫生行业标准 WS/T; 《中国药典》 drug names; standard organism names). If no standard Chinese name
  exists (rare organisms, allergen species, gene symbols, many drugs), keep the Latin/English name as is.
- Greek letters as symbols when the Chinese term uses them: α β γ δ κ λ (γ-谷氨酰转移酶, β2-微球蛋白,
  α1-抗胰蛋白酶). Never "Gamma 谷氨酰转移酶" or "Beta-2-微球蛋白".
- Keep established abbreviations that the English uses (LDL, HDL, MCV, INR, aPTT, HbA1c, IgG, IgE, RNA,
  DNA, HIV, PCR, CA 125 ...). Put them after the Chinese name in full-width parentheses when the English
  name IS the abbreviation: "MCV" → "平均红细胞体积（MCV）".
- Spacing: one half-width space between Chinese and Latin letters or digits (血红蛋白 A1c, 载脂蛋白 B,
  C 反应蛋白, 24 小时尿液) — this matches the existing variant. No space next to Greek letters or full-width
  punctuation.
- Punctuation: full-width ，（）；：in Chinese text. Keep LOINC's half-width square brackets [ ] in long names.
- Do not merge or drop information. Every specimen, method, timing, challenge, and qualifier in the English
  must survive in the Chinese.
- If you are unsure, keep the closest literal rendering and say so in `note`. Never guess a clinical meaning.

## Task P — review the official Part translations (检验)

Input: one English Part (with its axis), the official zh rendering(s), and example long names that use it.
Output one verdict per Part:

- `ok`: meaning correct and term acceptable. Most Parts should be ok. Do not restyle LOINC's formal Part
  syntax: "." separates subparts, "^" a suffix or challenge, "/" a ratio, "+" a sum; keep them.
- `fix`: the official rendering is wrong or should change — (a) wrong meaning / mistranslation;
  (b) left in English where a standard Chinese term exists (Gamma 谷氨酰转移酶 → γ-谷氨酰转移酶,
  Alpha-1-胎儿球蛋白 → 甲胎蛋白); (c) a clearly non-standard term where the standard differs; (d) garbled or
  truncated text. Give `zh_proposed` and a one-line `reason`.
- `style`: meaning correct but the wording is awkward or ungrammatical Chinese (e.g. "胆固醇.LDL 之中的");
  give a better `zh_proposed` that keeps the Part syntax characters, and a `reason`.

Some English Parts are generic by LOINC's design (COMPONENT "Observation" for MCV / specific gravity, translated
观察). A faithful translation of a generic Part is `ok`.

Several official renderings are STALE: they were translated from an earlier LOINC release and still carry
subparts the current English Part no longer has (English COMPONENT "Coagulation" with method "Coag.tissue factor
induced", while the old Chinese still says 组织因子诱导型凝血过程 / 凝血功能分析). When the input lists more than
one official rendering, or the rendering holds information the English Part does not, give `fix` with the
rendering of the CURRENT English Part only; the proposal is applied to every code that uses this Part.

## Task N — long common name (完善 LONG_COMMON_NAME)

Translate the English LONG_COMMON_NAME of each code. Mirror its structure, Chinese clauses separated by "，":

    <analyte> [<bracket>]，<specimen>[，<timing/challenge>][，<method>]

- analyte: the natural standard Chinese clinical name of the measured thing, consistent with the reviewed
  COMPONENT Part (use the Part's term, not its formal syntax): "Cholesterol in LDL" → 低密度脂蛋白胆固醇;
  "Hemoglobin A1c/Hemoglobin.total" → 血红蛋白 A1c/总血红蛋白; "Neutrophils/100 leukocytes" →
  中性粒细胞/100 个白细胞.
- bracket: translate with the BRACKET GLOSSARY below, exactly. If the English has no bracket, add none.
- specimen: use the reviewed SYSTEM Part ("in Serum or Plasma" → 血清/血浆; "in Blood" → 全血; "in Urine" →
  尿液; "in 24 hour Urine" → 24 小时尿液). "Patient"/unspecified systems are omitted, as the English omits them.
- method: "by Automated count" → 自动计数法; "by calculation" → 计算法; "by Test strip" → 试纸条法;
  "by Immunoassay" → 免疫测定法; follow the reviewed METHOD Part for others, ending in 法 when it names a method.
- "X of <specimen>" names read specimen-first, as Chinese reports do: "Specific gravity of Urine" → 尿液比重;
  "pH of Arterial blood" → 动脉血 pH; "Color of Urine by Auto" → 尿液颜色，自动法.
- An English name that IS an abbreviation gets the Chinese name first: INR → 国际标准化比值（INR）;
  PT → 凝血酶原时间（PT）; aPTT → 活化部分凝血活酶时间（aPTT）; MCV → 平均红细胞体积（MCV）.
- If an official Part is wrong (see Task P), use the correct term and say so in `note`.
- Examples:
  - Cholesterol in LDL [Moles/volume] in Serum or Plasma → 低密度脂蛋白胆固醇 [物质的量/体积]，血清/血浆
  - Cholesterol in LDL [Mass/volume] in Serum or Plasma by calculation → 低密度脂蛋白胆固醇 [质量/体积]，血清/血浆，计算法
  - Hemoglobin A1c/Hemoglobin.total in Blood → 血红蛋白 A1c/总血红蛋白，全血
  - MCV [Entitic mean volume] in Red Blood Cells by Automated count → 平均红细胞体积（MCV）[实体平均体积]，红细胞，自动计数法
  - Glucose [Presence] in Urine by Test strip → 葡萄糖 [存在]，尿液，试纸条法
  - Ampicillin [Susceptibility] → 氨苄西林 [药敏]
  - Glucose [Mass/volume] in Serum or Plasma --2 hours post 75 g glucose PO → 葡萄糖 [质量/体积]，血清/血浆，口服 75 g 葡萄糖后 2 小时
  - Systolic blood pressure → 收缩压

## Task D — display name (完善 LinguisticVariantDisplayName)

LOINC defines this column as "a translation of the LOINC term as a whole … the style and format is left up
to the translator". It is the name shown to a patient in a list of their results, so it must be the natural
name a Chinese clinical lab report prints for THIS test, and it must not be confusable with a different test.

- Name the measured quantity, not the generic LOINC component: 788-0 → 红细胞分布宽度（RDW）, not 红细胞;
  32623-1 → 平均血小板体积（MPV）; 5902-2 → 凝血酶原时间（PT）; 11556-8 → 氧分压; 4548-4 → 糖化血红蛋白;
  22748-8 and 13457-7 → 低密度脂蛋白胆固醇; 2500-7 → 总铁结合力.
- Specimen: omit it for blood, serum or plasma (the default on a report); name any other specimen first, as
  reports do: 尿葡萄糖, 尿比重, 尿蛋白, 脑脊液蛋白, 粪便隐血, 24 小时尿蛋白, 动脉血氧分压, 脐静脉血血红蛋白.
- Keep a qualifier only when it makes a different test: 超敏 C 反应蛋白 (hs-CRP), 空腹血糖, 餐后 2 小时血糖,
  游离甲状腺素, 直接胆红素. Drop pure method or unit variants (calculated vs direct LDL, mass vs molar) —
  they are the same test to a patient.
- Ratios and percentages as reports print them: 中性粒细胞百分比 (Neutrophils/Leukocytes), 白球比 (Albumin/
  Globulin), 总胆固醇/高密度脂蛋白胆固醇比值.
- Organisms, susceptibilities and allergens: 金黄色葡萄球菌培养, 氨苄西林药敏, 屋尘螨特异性 IgE.
- Panels: the panel name a Chinese order sheet uses: 全血细胞计数, 肝功能组套, 血脂组套, 基础代谢组套.
- Short: usually under 16 Chinese characters; add the common abbreviation in （） when reports use it.

## Task C — consumer name (完善 ConsumerName)

Translate LOINC's English ConsumerName for patients: plain, natural, the name a Chinese patient sees on a
report. Keep the trailing specimen after "，" using the SPECIMEN GLOSSARY.

- LDL cholesterol, Blood → 低密度脂蛋白胆固醇，血液
- Alanine Aminotransferase (ALT; SGPT), Blood → 丙氨酸氨基转移酶（ALT；SGPT），血液
- Cancer Ag 125, Blood → 糖类抗原 125（CA 125），血液   (the Chinese clinical name; the abbreviation may be added)
- HbA1c/Total Hgb, Blood → 糖化血红蛋白（HbA1c）/总血红蛋白，血液
- Mean Corpuscular Volume (MCV), Red blood cells → 平均红细胞体积（MCV），红细胞
- Neutrophils/White Blood Cells, Blood → 中性粒细胞/白细胞，血液
- LOINC's English consumer names are sometimes generic ("Coagulation, Blood" for prothrombin time). Translate
  them faithfully anyway — this column is LOINC's text in Chinese; Task D is where the precise name goes.

## BRACKET GLOSSARY (exact)

Presence 存在 | Mass/volume 质量/体积 | Units/volume 单位/体积 | Moles/volume 物质的量/体积 | Titer 滴度 |
Susceptibility 药敏 | Identifier 鉴定 | Mass Ratio 质量比 | #/volume 数目/体积 | Molar ratio 摩尔比 |
Interpretation 解释 | Mass/time 质量/时间 | Enzymatic activity/volume 酶活性/体积 | Ratio 比值 | #/area 数目/面积 |
Type 类型 | Mass/mass 质量/质量 | Moles/time 物质的量/时间 | # 数目 | Partial pressure 分压 |
Enzymatic activity/mass 酶活性/质量 | Log #/volume 对数数目/体积 | Multiple of the median 中位数倍数 |
Likelihood 可能性 | Volume Fraction 体积分数 | Volume 体积 | Pure mass fraction 纯质量分数 | # Ratio 数目比 |
Time 时间 | Morphology 形态 | Volume Rate/Area 体积速率/面积 | Genotype 基因型 | Pure number fraction 纯数目分数 |
Moles/mass 物质的量/质量 | type 类型 | Percentile 百分位数 | log units/volume 对数单位/体积 |
Entitic mean volume 实体平均体积 | Diameter 直径 | Sedimentation Rate 沉降速率 | Length fraction 长度分数 |
Mass 质量 | Entitic mass 实体质量 | Enzymatic activity ratio 酶活性比 | interpretation 解释 | Entitic length 实体长度 |
Length 长度 | Saturation Fraction 饱和度 | Entitic number 实体数目 | DistWidth 分布宽度 | Score 评分 |
Pure volume fraction 纯体积分数 | presence 存在 | Entitic volume 实体体积 | Molar amount 物质的量 |
Entitic Mass/volume 实体质量/体积 | Entitic substance 实体物质的量 | Entitic Catalytic Activity 实体催化活性 |
Enzymatic activity/time 酶活性/时间 | Mass Fraction 质量分数 | Function 功能 | Entitic Moles/volume 实体物质的量/体积 |
Z-score Z 评分 | Molar fraction 摩尔分数 | Time Ratio 时间比 | Finding 所见 | Acceleration 加速度 | Flow 流量 |
Appearance 外观 | Mass or Moles 质量或物质的量 | Pressure rate 压力速率 | Mass/kg/time 质量/千克/时间 | NFr 数目分数 |
Mass ratio 质量比 | (Moles/volume)*2 (物质的量/体积)*2 | Moles 物质的量 | Area 面积 | Quintile 五分位数 |
Log Number Ratio 对数数目比 | Date and time 日期和时间 | (Mass/volume)*2 (质量/体积)*2 | Pressure 压力 | Units 单位 |
Pure catalytic fraction 纯催化分数 | Velocity 速度

## SPECIMEN GLOSSARY (consumer names; long names use the SYSTEM Part)

Blood 血液 | Urine 尿液 | Specimen 标本 | Spinal fluid 脑脊液 | Blood or tissue specimen 血液或组织标本 |
Body fluid 体液 | Stool 粪便 | Tissue 组织 | Urine sediment 尿沉渣 | Red blood cells 红细胞 | Saliva 唾液 |
Dried blood spot 干血斑 | Synovial fluid 关节液 | Semen 精液 | Nasopharynx 鼻咽 | Vaginal fluid 阴道分泌物 |
Meconium 胎粪 | Peritoneal fluid 腹腔积液 | Stone 结石 | Amniotic fluid 羊水 | Infected blood 感染血液 |
Pleural fluid 胸腔积液 | Cervix 宫颈 | Genital specimen 生殖道标本 | Blood Product Unit 血液制品单位 | Nose 鼻 |
White blood cells 白细胞 | Bone marrow 骨髓 | control Blood 对照血液 | Throat 咽喉 | Cord blood 脐带血 |
Bronchial specimen 支气管标本 | Sputum 痰液 | Donor Blood 献血者血液 | Hair 毛发 | Vitreous fluid 玻璃体液 |
Pericardial fluid 心包积液 | Urine+Blood 尿液+血液 | Skin 皮肤 | Arterial cord blood 脐动脉血 | Anal 肛门 |
Blood or urine 血液或尿液 | Venous cord blood 脐静脉血 | Stomach fluid 胃液 | Bone 骨 | Milk 乳汁 | Bile 胆汁 |
Wound 伤口 | Sweat 汗液 | Ear 耳 | Eye 眼 | Liver 肝 | Brain 脑 | Platelets 血小板 | Reticulocytes 网织红细胞 |
Respiratory system specimen 呼吸道标本 | Cervix Or Vagina Specimen 宫颈或阴道标本 | Shallow wound 浅表伤口 |
Deep wound 深部伤口 | Abscess 脓肿 | Bronchoalveolar lavage specimen 支气管肺泡灌洗液 |
gastrointestinal lower Stool 下消化道粪便 | Upper respiratory 上呼吸道 | Lower respiratory 下呼吸道

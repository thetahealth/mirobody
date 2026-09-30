# Reference: where translating health data goes wrong

Every row below was produced by running the shipped resolver against bundle
`loinc-2.83+2026.09.17-aacb2c715b56`, not from memory. Reproduce any of it
with `mirobody resolve "<term>"`; `mirobody/tests/test_skills.py` in the
repository re-runs all of it on every change.

---

## 1. The same name is two different codes

LOINC encodes the unit and the result type into the identity. **Pass the value
and the unit whenever the report prints them.**

| Name as printed | Value + unit | LOINC | What it actually is |
|---|---|---|---|
| 中性粒细胞 | `62 %` | `26511-6` | Neutrophils/Leukocytes, a **ratio** |
| 中性粒细胞 | `4.2 10*9/L` | `26499-4` | Neutrophils, an **absolute count** |
| 淋巴细胞 | `30 %` | `26478-8` | Lymphocytes/Leukocytes |
| 淋巴细胞 | `1.8 10*9/L` | `26474-7` | Lymphocytes [#/volume] |
| total cholesterol | `5.0 mmol/L` | `14647-2` | Cholesterol [**Moles**/volume] |
| total cholesterol | `193 mg/dL` | `2093-3` | Cholesterol [**Mass**/volume] |
| triglycerides | `1.7 mmol/L` | `14927-8` | Triglyceride [Moles/volume] |
| triglycerides | `150 mg/dL` | `2571-8` | Triglyceride [Mass/volume] |
| creatinine | `88 umol/L` | `14682-9` | Creatinine [Moles/volume] |
| creatinine | `1.0 mg/dL` | `2160-0` | Creatinine [Mass/volume] |

**The percentage / absolute-count pair is the single most common misreading of
a CBC.** A neutrophil percentage of 62 and a neutrophil count of 4.2 are
different measurements with different reference ranges; treating one as the
other produces a confident, wrong conclusion.

---

## 2. Adjacent names that are not the same test

These resolve correctly. The danger is *you* conflating them, not the resolver.

| Name | LOINC | Canonical |
|---|---|---|
| HDL / 高密度脂蛋白 | `2085-9` | Cholesterol in HDL [Mass/volume] |
| LDL / 低密度脂蛋白 | `13457-7` | Cholesterol in LDL, **by calculation** |
| non-HDL cholesterol | `43396-1` | Cholesterol non HDL [Mass/volume] |
| Cholesterol/HDL Ratio | `9830-1` | Cholesterol.total/Cholesterol in HDL [**Mass Ratio**] |
| total bilirubin | `1975-2` | Bilirubin.total |
| direct bilirubin | `15152-2` | Bilirubin.**conjugated** |
| indirect bilirubin | `1971-1` | Bilirubin.indirect |
| free T4 / 游离甲状腺素 | `3024-7` | Thyroxine (T4) **free** |
| total T4 | `3026-2` | Thyroxine (T4) |
| TSH | `3016-3` | Thyrotropin |

Two traps:

- **A ratio is not a concentration.** `Cholesterol/HDL Ratio` is dimensionless;
  reading it against a cholesterol reference range is meaningless.
- **LDL is usually calculated, not measured** (`by calculation` in the canonical
  name). It is derived from total cholesterol, HDL and triglycerides, so a high
  triglyceride value makes the LDL number less reliable: worth a sentence when
  both are flagged.

---

## 3. Category headings are not results

A panel heading groups rows; it is not a measurement. The resolver abstains on
these:

| Term | Result |
|---|---|
| 血脂 (lipids) | **unresolved** |
| 三大常规 | **unresolved** |
| 甲功 | **unresolved** |
| 心肌酶 | **unresolved** |
| 生化全套 | **unresolved** |
| 凝血功能 | **unresolved** |
| 免疫球蛋白 | **unresolved** |
| 微量元素 | **unresolved** |
| 激素六项 | **unresolved** |
| 维生素 (vitamins) | **unresolved** |
| 尿常规 (urinalysis) | **unresolved** |
| 肿瘤标志物 (tumor markers) | **unresolved** |
| 电解质 (electrolytes) | **unresolved** |

The last four used to resolve wrongly (`维生素` answered a liver-cancer risk
score) and were fixed in the 1.5 bundle; they stay here because a rebuild can
bring a wrong answer back, and the test that pins them would say so.

Some headings legitimately **do** have a LOINC panel code. These are correct,
not errors:

| Term | LOINC | Canonical |
|---|---|---|
| 肝功能 | `24325-3` | Hepatic function 2000 panel - Serum or Plasma |
| 肾功能 | `24362-6` | Renal function 2000 panel |
| 血常规 | `57021-8` | CBC W Auto Differential panel |
| 血压 | `85354-9` | Blood pressure panel with all children optional |

A `panel` in the canonical name means the row is a heading. **Do not report a
single value against a panel code.**

---

## 4. Category words deliberately unresolved, verified 2026-09-30

The resolver is lexical, so a category word can land on a plausible specific
code. These six surfaces are now explicit refusals in the shipped bundle:

| Term | Means | Result | Why |
|---|---|---|---|
| `免疫` | "immunity" (category) | **unresolved** | A category word must not become one rare-disease immunoblot |
| `stool` | a specimen | **unresolved** | A specimen word must not become one bird-droppings allergy test |
| `重金属` / `heavy metals` | a category | **unresolved** | The report must supply the actual analytes and specimen |
| `激素` | "hormones" (category) | **unresolved** | A category must not become a hormone treatment |
| `enzymes` | a category | **unresolved** | One enzyme must not stand in for the whole category |

**The detection rule, which generalizes beyond this list:**

> If the canonical name that comes back is dramatically more specific than the
> name on the page (a category word landing on one named analyte, a score, a
> treatment, or a different specimen) treat it as unresolved.

An unresolved category is correct behavior. A new suspiciously specific match is
worth [reporting](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml).
A wrong term is the highest-leverage bug this project takes.

---

## 5. Surface shapes that need a retry

Extractors mangle names in predictable ways. Before reporting a name as unknown,
try the cleaned-up form:

| As extracted | Also try |
|---|---|
| `Total Cholesterol-TC` | `Total Cholesterol` |
| `空腹血糖(GLU)` | `空腹血糖` |
| `ALT(GPT)` | `ALT` |
| `血紅素` (Traditional) | already handled: folds to `血红蛋白` → `718-7` |

The resolver already folds Traditional to Simplified Chinese, already tries
the hyphen-stripped spelling of `Name-ABBREV`, and already resolves both
halves of `名称(缩写)` and refuses when they disagree. The retry is for shapes
it has not learned yet.

---

## 6. A whole report, end to end

The checkup shipped at
[`demo/upload/you_annual_checkup_2026-05.pdf`](https://github.com/thetahealth/mirobody/blob/main/demo/upload/you_annual_checkup_2026-05.pdf)
prints nine analytes. Run every row through `resolve_reading` with its
printed unit and all nine resolve:

| Name as printed | Value + unit | LOINC | Name-only would give |
|---|---|---|---|
| `Glycated Hemoglobin-HbA1c` | `5.2 %` | `4548-4` | same |
| `Fasting Blood Glucose-FBG` | `4.9 mmol/L` | `14771-0` | `1558-6` (mass) |
| `Total Cholesterol-TC` | `4.45 mmol/L` | `14647-2` | `2093-3` (mass) |
| `Low-Density Lipoprotein-LDL` | `2.48 mmol/L` | `22748-8` | `13457-7` (mass) |
| `High-Density Lipoprotein-HDL` | `1.50 mmol/L` | `14646-4` | `2085-9` (mass) |
| `Triglycerides-TG` | `0.95 mmol/L` | `14927-8` | `2571-8` (mass) |
| `Systolic Blood Pressure` | `116 mmHg` | `8480-6` | same |
| `Diastolic Blood Pressure` | `75 mmHg` | `8462-4` | same |
| `Resting Heart Rate` | `57 bpm` | `40443-4` | same |

**Five of nine codes change when the unit is passed.** A reading filed under
the mass code with a molar value compares against nothing, which is the whole
reason section 1 exists.

Refusals still happen on real reports. `Lipid-Low-Density Lipoprotein
Particle Number` (LDL-P, a particle *count*) is unresolved today: the nearest
reachable code, `43727-7 Lipoprotein.beta.subparticle.small`, is a different
measurement, and a model reaching for the closest plausible match would take
it. The resolver returns nothing instead. On a report that prints it, the
answer has a fourth section, and it is not an apology:

> **Could not identify (1)**: `Lipid-Low-Density Lipoprotein Particle Number`.
> This is a specialized lipid subfraction; I could not confirm what it
> measures, so I have not interpreted its value. Your lab printed a reference
> range for it; worth asking whoever ordered the panel why it was included.

---

## 7. Reference ranges do not come from here

This resolver maps **names to codes**. It does not supply reference ranges, and
neither should you from memory; ranges vary by lab, method, age and sex.

**Use the range printed on the page.** If a row has no printed range, report
that the row has no range rather than inventing one.

# Reference: where reading a lab report goes wrong

Every row below was produced by running the shipped resolver, not from memory.
Reproduce any of it with `mirobody resolve "<term>"`.

---

## 1. The same name is two different codes

LOINC encodes the unit and the result type into the identity. **Pass the value
and the unit whenever the report prints them.**

| Name as printed | Value + unit | LOINC | What it actually is |
|---|---|---|---|
| 中性粒细胞 | `62 %` | `26511-6` | Neutrophils/Leukocytes — a **ratio** |
| 中性粒细胞 | `4.2 10*9/L` | `26499-4` | Neutrophils — an **absolute count** |
| 淋巴细胞 | `30 %` | `26478-8` | Lymphocytes/Leukocytes |
| 淋巴细胞 | `1.8 10*9/L` | `26474-7` | Lymphocytes [#/volume] |
| total cholesterol | `5.0 mmol/L` | `14647-2` | Cholesterol [**Moles**/volume] |
| total cholesterol | `193 mg/dL` | `2093-3` | Cholesterol [**Mass**/volume] |
| triglycerides | `1.7 mmol/L` | `14927-8` | Triglyceride [Moles/volume] |
| triglycerides | `150 mg/dL` | `2571-8` | Triglyceride [Mass/volume] |
| creatinine | `88 umol/L` | `14682-9` | Creatinine [Moles/volume] |
| creatinine | `1.0 mg/dL` | `2160-0` | Creatinine [Mass/volume] |

**The percentage / absolute-count pair is the single most common misreading of a
CBC.** A neutrophil percentage of 62 and a neutrophil count of 4.2 are different
measurements with different reference ranges; treating one as the other produces
a confident, wrong conclusion.

---

## 2. Adjacent names that are not the same test

These resolve correctly — the danger is *you* conflating them, not the resolver.

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
  triglyceride value makes the LDL number less reliable — worth a sentence when
  both are flagged.

---

## 3. Category headings are not results

A panel heading groups rows; it is not a measurement. The resolver abstains on
most of them:

| Term | Result |
|---|---|
| 血脂 (lipids) | **unresolved** ✅ |
| 三大常规 | **unresolved** ✅ |
| 甲功 | **unresolved** ✅ |
| 心肌酶 | **unresolved** ✅ |
| 生化全套 | **unresolved** ✅ |
| 凝血功能 | **unresolved** ✅ |
| 免疫球蛋白 | **unresolved** ✅ |
| 微量元素 | **unresolved** ✅ |
| 激素六项 | **unresolved** ✅ |

Some headings legitimately **do** have a LOINC panel code — these are correct,
not errors:

| Term | LOINC | Canonical |
|---|---|---|
| 肝功能 | `24325-3` | Hepatic function 2000 panel |
| 肾功能 | `24362-6` | Renal function 2000 panel |
| 血常规 | `57021-8` | CBC W Auto Differential panel |
| 血压 | `85354-9` | Blood pressure panel with all children optional |

A `panel` in the canonical name means the row is a heading. **Do not report a
single value against a panel code.**

---

## 4. Known bad resolutions — verified 2026-09-12

The resolver is lexical, so a category word can land on a plausible specific
code. These are **real, reproducible defects** in the shipped bundle
(`loinc-2.82+2026.08.28-af2524b7a285`):

| Term | Means | Resolves to | Why it's wrong |
|---|---|---|---|
| `维生素` | "vitamins" (category) | `96450-2` Hepatotocellular carcinoma risk [Score] GALAD | **Worst case.** A vitamin panel heading becomes a **liver-cancer risk score** |
| `尿常规` | "urinalysis" (panel) | `19159-3` Urinalysis specimen collection method | A *collection method*, not a result |
| `肿瘤标志物` | "tumor markers" (category) | `53959-3` Choriogonadotropin.tumor marker | One specific marker stands in for the whole category |
| `电解质` | "electrolytes" (category) | `19096-7` Electrolytes - single valence [Interpretation] in 24h Urine Narrative | Wrong specimen, wrong result type |

**The detection rule, which generalizes beyond this list:**

> If the canonical name that comes back is dramatically more specific than the
> name on the page — a category word landing on one named analyte, a score, a
> collection method, or a different specimen — treat it as unresolved.

Hitting one is worth
[reporting](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml).
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
| `血紅素` (Traditional) | already handled — folds to `血红蛋白` → `718-7` |

The resolver already folds Traditional to Simplified Chinese (a 3,336-character
table), and already tries the hyphen-stripped spelling of `Name-ABBREV`. The
retry is for shapes it has not learned yet.

---

## 6. A whole real report, end to end

The lab report shipped at [`demo/lab_report_2025-10-15.pdf`](https://github.com/thetahealth/mirobody/blob/main/demo/lab_report_2025-10-15.pdf)
prints 12 analytes. Run every name through the resolver and you get **8 codes
and 4 refusals** — verified 2026-09-12:

**Resolved (8):**

| Name as printed | LOINC | Canonical |
|---|---|---|
| `Total Cholesterol-TC` | `2093-3` | Cholesterol [Mass/volume] |
| `Blood Glucose` | `2339-0` | Glucose [Mass/volume] in Blood |
| `Cholesterol/HDL Ratio` | `9830-1` | Cholesterol.total/Cholesterol in HDL [Mass Ratio] |
| `LDL/HDL Ratio` | `11054-4` | Cholesterol in LDL/Cholesterol in HDL [Mass Ratio] |
| `Lipid-Free Fatty Acids` | `15066-4` | Fatty acids.nonesterified [Moles/volume] |
| `Lipid-Phospholipids` | `2568-4` | Phospholipid [Mass/volume] |
| `Non-HDL Cholesterol-Non-HDL` | `43396-1` | Cholesterol non HDL [Mass/volume] |
| `Lipid-Low-Density Lipoprotein Calculated` | `13457-7` | Cholesterol in LDL, by calculation |

**Refused (4)** — the shipped bundle has no key for these analytes at all:

| Name as printed | What it would need |
|---|---|
| `Postprandial Blood Glucose-PBG` | a 2-hour post-meal glucose code |
| `Lipid-Oxidized Low-Density Lipoprotein` | oxidized LDL |
| `Lipid-Small Dense Low-Density Lipoprotein Cholesterol` | sdLDL-C |
| `Lipid-Low-Density Lipoprotein Particle Number` | LDL-**P**, a particle *count* |

**The fourth one is the whole argument for this skill.** The nearest reachable
code is `43727-7` (`Lipoprotein.beta.subparticle.small`) — a *different
measurement*. A model reaching for the closest plausible match would take it.
The resolver returns nothing instead.

So on a real report your output has a fourth section, and it is not an apology:

> **Could not identify (4)** — `Postprandial Blood Glucose-PBG`,
> `Lipid-Oxidized Low-Density Lipoprotein`,
> `Lipid-Small Dense Low-Density Lipoprotein Cholesterol` and
> `Lipid-Low-Density Lipoprotein Particle Number`. These are specialized lipid
> subfractions; I could not confirm what each measures, so I have not
> interpreted their values. Your lab printed reference ranges for them — worth
> asking whoever ordered the panel why these were included.

This report is also why the project says coverage is **211/211 on ordinary
checkup panels** and not "complete". It went from **12 readings / 0 resolved**
to 8/12 by adding thirteen override rows; the remaining four need new bundle
entries, not aliases. The history is in
[`docs/roadmap.md`](https://github.com/thetahealth/mirobody/blob/main/docs/roadmap.md).

---

## 7. Reference ranges do not come from here

This resolver maps **names to codes**. It does not supply reference ranges, and
neither should you from memory — ranges vary by lab, method, age and sex.

**Use the range printed on the page.** If a row has no printed range, report
that the row has no range rather than inventing one.

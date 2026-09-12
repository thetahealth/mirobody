---
name: dont-guess-my-labs
description: Stop guessing when reading someone's lab report. Every indicator name on the page is checked against an offline LOINC resolver before you say what it measures — and a name the resolver does not know is reported as unknown instead of explained from memory. Use whenever a blood test, lab report, or health checkup result is shared, in any language.
license: Apache-2.0
metadata:
  author: thetahealth
  homepage: https://github.com/thetahealth/mirobody
---

# Don't Guess My Labs

You are good at reading lab reports and bad at knowing when you have misread
one. The failure is never "I don't know" — it is a fluent, confident paragraph
about the wrong analyte.

This skill gives you a fact anchor: an offline resolver that maps an indicator
name, in any language, to a LOINC code — and **returns nothing when it does not
know the term**. Check the page against it before you explain anything.

## Setup

```bash
pip install mirobody
```

Two packages, ~52 MB, depends on numpy only. **No API key, no network, no
Docker.** The vocabulary ships inside the wheel; the first call pays a few
seconds to load the bundle.

## Workflow

### 1. Read the original document, not a summary

Layout carries meaning: grouped panels, flags (`H` / `L` / `↑` / `↓`),
footnotes about specimen or method, and the reference range printed next to
each row. Read the PDF or image itself.

### 2. Extract every row verbatim

For each row keep four things exactly as printed — **do not translate, do not
normalize, do not expand abbreviations**:

| name | value | unit | printed reference range |
|---|---|---|---|

The original spelling is what you will check in step 3. `Total Cholesterol-TC`
must stay `Total Cholesterol-TC`.

### 3. Resolve every name before you explain it

```bash
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン" "空腹血糖(GLU)" "血脂"
```

```
  LDL cholesterol  LOINC 13457-7   Cholesterol in LDL [Mass/volume] ...   [63 candidates]
  血红蛋白          LOINC 718-7     Hemoglobin [Mass/volume] in Blood      [72 candidates]
  ヘモグロビン      LOINC 718-7     Hemoglobin [Mass/volume] in Blood      [72 candidates]
  空腹血糖(GLU)     LOINC 1558-6    Fasting glucose [Mass/volume] ...
  血脂             unresolved — not in the lexical index
```

Pass every name in one call. The canonical name that comes back — not your
memory — is what the row measures.

**`血脂` returning nothing is the point.** It is a category ("lipids"), not an
observation, so there is no code to return. See step 5 for what to do with it.

### 4. Pass the value and the unit whenever you have them

LOINC encodes the unit and the result type into the identity, so **the same
name is two different codes depending on what was measured**:

```python
from mirobody.engine import resolve_reading

resolve_reading("中性粒细胞", "62", "%").loinc        # 26511-6  a percentage
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc   # 26499-4  an absolute count

resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # 14647-2  [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL").loinc   # 2093-3   [Mass/volume]
```

Name-only resolution picks the most common form. When the report prints a unit,
use it — this is the single most common way a lab report gets misread.

### 5. An unresolved name is a fact about your knowledge, not a gap to fill

When the resolver returns nothing, **say so**. Do not fall back on what the
name looks like it means.

> **Could not identify (1)** — `血脂` (as printed). This is a category heading
> rather than a single measurement, so I cannot tell you what one value under
> it represents. The individual rows beneath it — total cholesterol,
> triglycerides, HDL, LDL — all resolved and are covered above.

Two things to check before reporting a name as unknown:

- **Is it a panel heading rather than a row?** `血脂`, `三大常规`, `肝功能`
  group rows; they are not results themselves.
- **Did the extractor mangle it?** `Total Cholesterol-TC` is a name glued to
  its abbreviation. Try the plain name as well before calling it unknown.

### 6. Treat a suspicious resolution with the same suspicion as no resolution

The resolver is lexical, and a category word can still land on a plausible
specific code. A known live example:

```
mirobody resolve "肿瘤标志物"
  肿瘤标志物   LOINC 53959-3   Choriogonadotropin.tumor marker [Units/volume] ...
```

`肿瘤标志物` means "tumor markers" — a whole category. It resolved to **one
specific marker**, which is wrong in exactly the way this skill exists to
prevent.

**The check:** if the canonical name that comes back is dramatically more
specific than the name on the page, treat it as unresolved and say so. A code
is evidence, not proof.

If you hit one of these,
[report the term](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml) —
a wrong term is the highest-leverage bug this project takes.

### 7. Flag against the PRINTED reference range

Ranges differ by lab, method, age and sex. **The range on the page beats any
range you remember.** If a row has no printed range, say that the row has no
range rather than supplying one.

Order the walkthrough: out-of-range first, borderline second, normal last —
normals summarized in one line, not fifteen "this is fine" paragraphs.

### 8. Explain, three sentences per flagged item

What the indicator measures (**from the canonical name, not memory**), which
direction it moved and by how much, and what commonly influences it (fasting
state, hydration, recent exercise, common medications). Name influences as
possibilities, never as conclusions.

## Boundaries

- **No diagnosis, no treatment advice.** "Your ALT is 2× the upper limit" is a
  fact; "you have liver disease" is a diagnosis. State facts, then recommend
  discussing flagged results with their clinician — by name: "worth asking your
  doctor about the ALT and AST together."
- **Never invent a value, a unit, or a range.** If a scan row is illegible, list
  what you could not read.
- **Keep the report's original language** for indicator names, adding a
  translation in parentheses when the conversation is in another language.
- **Do not report a LOINC code to the user unless they asked.** The code is your
  working evidence. What they want is what the row measures and whether it is
  out of range.

## Example shape of a good answer

> **Out of range (2)**
> - LDL cholesterol 4.2 mmol/L (ref < 3.4) — 24% above the upper limit, up from
>   3.8 in March. LDL is the cholesterol fraction most linked to cardiovascular
>   risk; diet, weight change and genetics all move it.
> - ALT 68 U/L (ref 7–56) — mildly elevated. ALT is a liver enzyme; intense
>   exercise in the prior 48h, alcohol, and some medications commonly raise it.
>
> **Borderline (1)** — fasting glucose 5.9 mmol/L (ref 3.9–6.1), high-normal and
> drifting up across your last three reports: 5.4 → 5.7 → 5.9.
>
> **Normal (12)** — CBC, kidney panel, thyroid and vitamins all within range.
>
> **Could not identify (1)** — `肿瘤标志物` is a category heading, not a single
> measurement; I did not interpret it. The specific markers listed under it
> resolved and are included above.
>
> Worth raising with your clinician: the LDL trend and the ALT, ideally together
> with the triglycerides from this same report.

## What this skill does not do

It resolves **names**. It does not supply reference ranges, extract text from a
scan, or store anything. Reference ranges come from the page; a whole pipeline —
document extraction, unit normalization, history, charts, an agent that reads
the original file and cites the page — is
[Mirobody](https://github.com/thetahealth/mirobody), which this resolver is a
part of.

Coverage is **211/211** on the panels an ordinary checkup prints, in English,
Chinese (Simplified and Traditional) and Japanese. Beyond those, expect gaps —
and prefer a reported gap over a confident guess.

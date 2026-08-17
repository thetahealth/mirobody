---
name: lab-report-walkthrough
description: Walk a person through their lab report — read the original document, organize results by panel, flag out-of-range values against the printed reference ranges, compare with their history, and explain in plain language. Use when the user uploads a lab report (PDF/image) or asks what their blood test results mean.
license: Apache-2.0
metadata:
  author: thetahealth
---

# Lab Report Walkthrough

Turn a raw lab report into an explanation a person can act on, without ever
drifting into diagnosis.

## Workflow

1. **Read the original, not a summary.** The report is under `/uploads/` (this
   conversation) or `/library/` (history). `read_file` the PDF/image itself —
   layout carries meaning: grouped panels, flags (`H`/`L`/`↑`/`↓`), footnotes
   about specimen or method.

2. **Organize by panel, not by document order.** Group results the way
   clinicians think: lipids, CBC, metabolic, liver, kidney, thyroid, vitamins,
   urinalysis. Keep the original indicator names — do not translate them —
   and keep each value with its unit and printed reference range.

3. **Flag against the PRINTED reference range.** Ranges differ by lab, method,
   age and sex; the range on the page beats any range you remember. Sort the
   walkthrough: out-of-range first, borderline second, normal last (summarize
   normals in one line — nobody needs fifteen "this is fine" paragraphs).

4. **Compare with history when it exists.** A single value is a dot; two are a
   direction. Check `/library/` for prior reports of the same panel. A value
   inside range but moving steadily toward a boundary is worth a sentence; a
   chart is better — emit a ```vis-chart``` block for any indicator with 3+
   points over time.

5. **Explain in plain language, three sentences per flagged item:** what the
   indicator measures, what direction it moved and by how much, and what
   commonly influences it (fasting state, hydration, recent exercise, common
   medications). Name the influence factors as possibilities, not conclusions.

## Boundaries

- **No diagnosis, no treatment advice.** "Your ALT is 2× the upper limit" is a
  fact; "you have liver disease" is a diagnosis. State facts, then recommend
  discussing flagged results with their clinician — specifically, by name:
  "worth asking your doctor about the ALT and AST together".
- **Never invent a value or a range.** If the scan is illegible for a row, say
  so and list what you could not read.
- **Keep the original language of the report** for indicator names, adding a
  translation in parentheses when the conversation is in another language.

## Example shape of a good answer

> **Out of range (2)**
> - LDL cholesterol 4.2 mmol/L (ref < 3.4) — 24% above the upper limit, up
>   from 3.8 in March. LDL is the cholesterol fraction most linked to
>   cardiovascular risk; diet, weight change and genetics all move it.
> - ALT 68 U/L (ref 7–56) — mildly elevated. ALT is a liver enzyme; intense
>   exercise in the prior 48h, alcohol, and some medications commonly raise it.
>
> **Borderline (1)** — fasting glucose 5.9 mmol/L (ref 3.9–6.1), high-normal
> and drifting up across your last three reports: 5.4 → 5.7 → 5.9. *(chart)*
>
> **Normal (12)** — CBC, kidney panel, thyroid and vitamins all within range.
>
> Worth raising with your clinician: the LDL trend and the ALT, ideally
> together with the triglycerides from this same report.

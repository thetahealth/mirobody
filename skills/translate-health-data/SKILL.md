---
name: translate-health-data
description: Turn raw health data into standardized data, and read a lab report without guessing. A lab report PDF, photo, spreadsheet or CSV becomes readings with LOINC codes and UCUM units; an Apple Health export becomes JSON lines; a symptom or diagnosis in any language becomes an ICPC-3 code; any reading becomes a FHIR Observation. Use whenever health data must be standardized for a pipeline, dataset or app, and whenever a blood test, lab report or checkup result is shared and each row must be identified from the vocabulary rather than from memory. Resolution is offline and needs no key; parsing a document needs one model key.
license: Apache-2.0
metadata:
  author: thetahealth
  homepage: https://github.com/thetahealth/mirobody
---

# Translate health data

Health data arrives under whatever name the source used. Last year's checkup
wrote `A1c`, this year's `HbA1c`, the clinic `Glycated Hemoglobin`; a Chinese
report prints `血红蛋白` for what an English one calls `hemoglobin`. One
test, several names, nothing to compare until each lands on one code. And a
model reading a report is fluent about the rows it got wrong: the failure is
never "I don't know", it is a confident paragraph about the wrong analyte.

This skill is the offline translator for both problems: names to LOINC,
units to UCUM, complaints and diagnoses to ICPC-3, a FHIR Observation for
the result. **The codes come from the vocabulary, never from you.** When the
vocabulary does not know a term the answer is a refusal, and a refusal is an
output to report, not a gap to fill from memory.

## Setup

```bash
pip install mirobody              # resolution, units, ICPC-3, Apple export: no key, no network
pip install 'mirobody[parse]'     # adds document parsing, which needs one model key
```

The first is two packages, numpy and this one; the vocabulary ships inside
the wheel and the first call loads it in a few seconds. With `uvx`, no
install: `uvx mirobody resolve 血红蛋白`. For parsing, put ONE key in the
environment or a `.env` in the working directory (`OPENROUTER_API_KEY`,
`OPENAI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `DEEPSEEK_API_KEY`
or `DASHSCOPE_API_KEY`). The model reads the document into rows; the codes
still come from the offline resolver.

## Which command, by what you were given

| You have | Do |
| --- | --- |
| Names, values and units already extracted | `resolve_reading(name, value, unit)` in Python, or `mirobody resolve` for names alone |
| A document: PDF, photo, Excel, Word, PowerPoint, CSV, text | `mirobody parse <file>` |
| An Apple Health `export.zip` or `export.xml` | `mirobody import apple <path> --out facts.jsonl` |
| A symptom or a diagnosis in words | `resolve_symptom(text)` / `resolve_condition(text)` |
| A unit to normalize or convert | `mirobody.units` |
| A FHIR Observation for another system | `standardize_reading(name, value, unit)` |
| A report to explain to the person it belongs to | Sections 1 and 8 |
| Garmin, Oura or WHOOP data | Connect the device to a running Mirobody (the `mirobody` skill); there is no standalone export reader |

## 1. Names to codes, and why the unit travels with the name

```bash
mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

```
  LDL cholesterol  LOINC 13457-7   Cholesterol in LDL [Mass/volume] in Serum or Plasma by calculation
  血红蛋白          LOINC 718-7     Hemoglobin [Mass/volume] in Blood
  ヘモグロビン      LOINC 718-7     Hemoglobin [Mass/volume] in Blood
  空腹血糖(GLU)     LOINC 1558-6    Fasting glucose [Mass/volume] in Serum or Plasma
  血脂             unresolved: not in the lexical index, ...
```

Two languages, one code. `血脂` (lipids) is a category, not one observation,
so it resolves to nothing rather than to a guess. Pass every name in one call;
the canonical name that comes back, not your memory, is what the row measures.

LOINC encodes the unit and the result type into the identity, so **the same
name is two codes depending on what was measured**. Pass the value and unit
whenever the source prints them:

```python
from mirobody.engine import resolve, resolve_reading

resolve("total cholesterol").loinc                          # '2093-3'   [Mass/volume], the common default
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2'  [Moles/volume]: the unit picked the code
resolve_reading("中性粒细胞", "62", "%").loinc                 # '26511-6'  a percentage
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc           # '26499-4'  an absolute count
```

On the checkup shipped at `demo/upload/you_annual_checkup_2026-05.pdf`, nine
printed rows resolve 9/9, and **five of the nine codes are different when
the unit is passed**: every mmol/L row moves from the mass code to the molar
one. Resolving names alone files those five under the wrong code, and the
percentage-versus-count pair is the single most common misreading of a CBC.

The result carries the decision, not just the code:

```python
r = resolve_reading("Glycated Hemoglobin-HbA1c", "5.2", "%")
r.resolved, r.loinc, r.canonical     # True, '4548-4', 'Hemoglobin A1c/Hemoglobin.total in Blood'
r.method                             # 'lexical'
```

## 2. A document to rows

```bash
mirobody parse demo/upload/you_annual_checkup_2026-05.pdf
```

```
  Glycated Hemoglobin-HbA1  5.2 %             LOINC 4548-4     Hemoglobin A1c/Hemoglobin.total in Blood
  Fasting Blood Glucose-FB  4.9 mmol/L        LOINC 14771-0    Fasting glucose [Moles/volume] in Serum or Plasma
  Total Cholesterol-TC      4.45 mmol/L       LOINC 14647-2    Cholesterol [Moles/volume] in Serum or Plasma
  ...
9 readings · 9 resolved to standard codes · offline lexical index
collected 2026-05-06, as the document prints it
```

A PDF with a text layer, a spreadsheet or a Word file is read locally and only
the row extraction goes to the model; a scanned page or a photo goes to the
model as an image, which is why the key's model must be multimodal. In
Python, `parse_file(path)` returns the same readings as objects, each with
`name`, `value`, `unit`, `collected` and a `resolution`. `--no-resolve`
returns the rows as extracted, without codes.

## 3. An Apple Health export to JSON lines

```bash
mirobody import apple export.zip --out facts.jsonl     # export.xml or the unpacked folder also work
```

```
  heartRates        1 readings   62 … 62
  steps             1 readings   1834 … 1834
5 readings · 5 indicators · 2026-09-29 … 2026-09-29
Facts written to facts.jsonl
```

One line per record, keyed by Mirobody's standard device indicator
(`heartRates`, `steps`, `bodyMasss`, `systolicPressures`, `bloodGlucoses`,
316 in all), with the value, the unit as Apple wrote it, and the effective
window in epoch milliseconds:

```json
{"metric_key": "heartRates", "value_num": 62.0, "effective_start_ms": 1790665200000, "effective_end_ms": 1790665205000, "unit": "count/min", "modality": "sensed", ...}
```

No key, no database; the file is streamed, so a multi-year export of a few
hundred megabytes is fine. Apple records carry their own UTC offset; `--tz`
is only the fallback for the few that do not.

## 4. A symptom or a diagnosis to ICPC-3

Complaints and diagnoses are not lab analytes and never go through `resolve`:
a fever is not a fever-virus assay. They have their own axis, with the same
refusal rule.

```python
from mirobody.translate import resolve_symptom, resolve_condition

resolve_symptom("头疼").code            # 'NS01'        Headache; 'headache' and 'головная боль' answer the same
resolve_symptom("疼").outcome          # 'refused'     too broad: r.reason == 'icpc3:too-broad'
resolve_condition("2型糖尿病").code     # 'TD72'        Type 2 diabetes mellitus
resolve_condition("糖尿病").outcome     # 'needs-input' which type? r.reason == 'icpc3:ambiguous'
resolve_condition("hypertension").code # 'KD73'        Hypertension, uncomplicated
```

Three outcomes, each an instruction: `coded` gives `code` and `display`;
`refused` means keep the words and do not code; `needs-input` means the text
is ambiguous and the source, or the user, has to say which.

## 5. Units

```python
from mirobody.units import normalize_unit, unit_family, convert_value, parse_value_unit

normalize_unit("mmol/l")                 # 'mmol/L'    UCUM spelling
normalize_unit("10^9/L")                 # '10*9/L'
normalize_unit("µmol/L")                 # 'umol/L'
unit_family("mg/dL")                     # 'MCnc'      mass concentration
parse_value_unit("<0.5 ng/mL")           # ParsedQuantity(comparator='<', value=0.5, unit='ng/mL')

convert_value(100, "mg/dL", "mmol/L", loinc_code="2339-0")    # 5.55   glucose, by its molar mass
convert_value(1.0, "mg/dL", "umol/L", loinc_code="2160-0")    # 88.4   creatinine
convert_value(62, "%", "10*9/L", loinc_code="26511-6")        # None   a percentage is not a count: refused
```

A mass-to-molar conversion needs the analyte, which is why `loinc_code` is
the argument: the bridge is the molar mass of what that code measures.

## 6. A FHIR Observation

```python
from mirobody.engine import standardize_reading
standardize_reading("血红蛋白", "13.5", "g/dL")
```

```json
{"resourceType": "Observation", "status": "final",
 "code": {"text": "血红蛋白",
          "coding": [{"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"}]},
 "valueQuantity": {"value": 13.5, "unit": "g/dL", "system": "http://unitsofmeasure.org", "code": "g/dL"},
 "extension": [{"url": "https://mirobody.ai/fhir/StructureDefinition/coding-decision", "extension": [
    {"url": "method", "valueString": "lexical"}, {"url": "evidence", "valueString": "name"}, ...]}]}
```

`code.text` keeps the name as the source wrote it; the `coding-decision`
extension records how the code was chosen, so a reviewer can audit the row.

## 7. The same functions over MCP, without the server

```bash
uvx mirobody mcp
```

Serves six tools over stdio, offline and with no key: `resolve_indicator`,
`normalize_unit`, `convert_unit`, `standardize_reading`,
`standardize_complaint` and `standardize_report`. Point Claude Desktop, Cursor
or any MCP client at that command when the agent should call the resolver as
a tool rather than through Python.

## Rules that keep the output honest

- **Keep the name as printed** beside the code. `Total Cholesterol-TC` stays
  `Total Cholesterol-TC`; the resolver already tries the hyphen-stripped and
  parenthesis-stripped spellings and resolves both halves of `名称(缩写)`, so
  do not pre-clean, translate or expand abbreviations.
- **A `panel` in the canonical name is a heading**, not a value. `肝功能`
  resolves to `24325-3 Hepatic function 2000 panel - Serum or Plasma`; the rows
  under it get their own codes, the heading gets none.
- **A refusal is a row with no code, not a dropped row.** Write the name, the
  value and the unit, leave the code empty, and say why: unresolved, refused
  (too broad), or needs-input (ambiguous).
- **Treat a suspiciously specific match as unresolved.** The resolver is
  lexical; a category word can land on one named assay. Today `免疫`
  ("immunity") answers a rare-disease immunoblot and `stool` a bird-droppings
  allergy test. If the canonical name is far more specific than the source's
  word, do not use it, and
  [report the term](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml):
  a wrong term is the highest-leverage bug this project takes.
- **Reference ranges are not in this skill.** Carry the range the source
  printed; never supply one.

`reference.md` holds the confusion tables: the unit pairs, adjacent names
that are not the same test, category headings that abstain, the known wrong
resolutions, the extractor shapes worth a retry, and the demo report row by
row.

## 8. Reading a report to the person it belongs to

The same resolver, one more rule: **explain nothing you have not resolved.**

1. Read the original document, not a summary. Layout carries the panels, the
   `H`/`L` flags and the printed reference range beside each row.
2. Extract every row verbatim: name, value, unit, printed range.
3. Resolve every name in one call before saying what any row measures. Pass
   the unit. The canonical name is what the row is.
4. A name the resolver does not know stays unknown, and you say so. Check
   first whether it is a panel heading or a mangled spelling.
5. Flag against the printed range only. A row with no printed range has no
   range; do not supply one.
6. Three sentences per flagged item: what it measures (from the canonical
   name), how far and which way it moved, what commonly influences it, named
   as possibilities. Out-of-range first, borderline second, normals in one
   line.

Boundaries: no diagnosis and no treatment advice ("ALT is twice the upper
limit" is a fact, "you have liver disease" is a diagnosis; point at what to
raise with a clinician, by name). Never invent a value, a unit or a range.
Keep the report's language for indicator names, with a translation in
parentheses when the conversation is in another one. Do not print codes
unless asked; they are your evidence, not the answer.

> **Out of range (2)**: LDL cholesterol 4.2 mmol/L (ref < 3.4), 24% above the
> limit and up from 3.8 in March; ALT 68 U/L (ref 7–56), mildly elevated.
> **Borderline (1)**: fasting glucose 5.9 mmol/L (ref 3.9–6.1), drifting up
> across three reports. **Normal (12)**: CBC, kidney, thyroid, vitamins.
> **Could not identify (1)**: `肿瘤标志物` is a category heading; the markers
> under it resolved and are covered above. Worth raising with your clinician:
> the LDL trend and the ALT, together with the triglycerides.

## What a clean row looks like

| name (as printed) | value | unit (UCUM) | system | code | display | method | source |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Total Cholesterol-TC | 4.45 | mmol/L | LOINC | 14647-2 | Cholesterol [Moles/volume] in Serum or Plasma | lexical | checkup.pdf p.1 |
| 血脂 | | | | | | unresolved | report.jpg p.2 |
| 头疼 | | | ICPC-3 | NS01 | Headache | coded | journal 2026-09-29 |

Everything in the table came from the source or the vocabulary. Nothing in
it came from memory.

## Scope

Storing, charting, and Garmin, Oura or WHOOP data are the running
application, one `./deploy.sh` away and covered by the `mirobody` skill.
Coverage is 296/296 on the tests an ordinary checkup prints, in English,
Chinese (Simplified and Traditional), Japanese, Russian and Estonian, against
bundle `loinc-2.83+2026.09.17-aacb2c715b56`. Beyond those panels, expect
gaps, and prefer a reported gap to a confident guess.

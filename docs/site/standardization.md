# Standardizing health data

Three reports can print one test three ways: `LDL-C 2.48 mmol/L`,
`低密度脂蛋白胆固醇 96 mg/dL`, `LDL Cholesterol 101 mg/dL`. No chart can put
them on one line until something decides they are the same measurement and
how their numbers compare. Standardization is that decision. It runs offline
on your machine, needs no model API key, and when it cannot decide, it leaves
the reading uncoded and says why instead of guessing.

## The steps one reading goes through

| Step | What happens | Example |
| --- | --- | --- |
| 1. Keep what was printed | The name, value, unit, reference range and flag are stored exactly as printed. Everything below is derived beside them and never overwrites them. | `LDL-C` · `2.48` · `mmol/L` · `<3.4` |
| 2. Read the value | The value is typed as a number, a graded marker, a label or text. A comparator is kept and the unit is normalized to UCUM. | `<0.5 mg/L` is 0.5 with `<`; `阴性` is a label; `120/80` is text, not a number |
| 3. Reject the impossible | A reading with no time, a time in the future, or a percentage above 100 is not stored. It is counted with its reason; nothing is filled in. | A report with no date asks for one instead of being dated today |
| 4. Find the analyte | The folded name is looked up in the bundled vocabulary: a code you confirmed first, then the project's hand-written rows and Chinese translations, then LOINC's own names in 21 languages. | `血红蛋白`, `血紅素`, `ヘモグロビン` and `hemoglobin` all reach `718-7` |
| 5. Let the reading pick the code | LOINC builds the kind of unit into the code. The printed unit picks mass or molar concentration, and a word like `阴性` picks the qualitative code. A unit never moves a reading to another specimen. | `LDL-C 2.48 mmol/L` → `22748-8` (moles); `96 mg/dL` → `13457-7` (mass); `尿糖 阴性` → `2349-9` (presence) |
| 6. Decide or abstain | Each reading ends **coded**, **needs input** or **refused**, with the reason stored beside it. | `血脂` names a panel and is refused; `血糖(HbA1c)` names two tests and asks you |
| 7. File it into a series | Codes that differ only in unit kind or method share one series, and values are compared through a molar-mass bridge. | 2.48 mmol/L, 96 mg/dL and 101 mg/dL land in one LDL series, about 2.5–2.6 mmol/L |

Answers then read the series, not the raw names: a trend across the three
reports is one line, and every number in the answer links back to the row and
the file it came from.

## Sources of readings

| Source | How it reaches step 1 | Where a model is involved |
| --- | --- | --- |
| A lab report (PDF, photo, spreadsheet) | Tables are read by rules first; the model reads only the rows the rules could not. | Reading the page. The model copies the name as printed and never chooses a code. |
| A wearable or Apple Health | Each vendor field is decoded into the catalogue's unit by a fixed table. | None |
| A sentence in the health journal | `头疼，血压150/95` is split into a complaint and two readings. | Splitting and typing the sentence. Codes come from the vocabulary. |
| The HTTP API | You send the name, value and unit as printed. | None |

Complaints and diagnoses go to a separate vocabulary, ICPC-3: `头疼` is
`NS01` and `高血压` is `KD73`. A complaint is never coded as a lab test.

## Readings that are not coded

An uncoded reading is still stored and still answers questions. It stays in its
own series under the name it was printed with, so it is never mixed into
another test.

| Outcome | Typical reasons | What you can do |
| --- | --- | --- |
| Needs input | The name is unknown; the name holds two tests; the unit is missing or unknown; the unit contradicts every code of the analyte | Confirm the code once and every reading printed with that name is recoded: `POST /api/v1/health-indicators/confirm-code` ([file processing](../file-processing.md)) |
| Refused | The name is a category or panel (`血脂`, `lipid panel`), or the row is a note, not a measurement | Nothing; this is deliberate |

If a common name is not recognized, report it: that is how new languages and
hospital spellings get added (see [Improve the vocabulary](#improve-the-vocabulary)).

## Try it

Names only, from a terminal, offline after the first download:

```bash
uvx --python 3.12 mirobody resolve LDL-C 低密度脂蛋白胆固醇 血脂
```

A full reading, in Python, as a FHIR Observation that records how the code was
chosen:

```python
from mirobody.engine import resolve_reading, standardize_reading

resolve_reading("LDL-C", "2.48", "mmol/L").loinc   # '22748-8'  the unit picked moles
resolve_reading("LDL-C", "96", "mg/dL").loinc      # '13457-7'  ...and mass here
resolve_reading("albumin", "30", "mg/24h").loinc   # ''  a serum name with a urine unit

standardize_reading("LDL-C", "2.48", "mmol/L")     # code, valueQuantity, decision
```

## Limits

- **It is lexical.** A name the vocabulary does not hold returns `unresolved`,
  never the nearest match. Recall grows by adding rows, not by guessing.
- **It codes measurements, not vendor scores.** Body Battery or a stress score
  is one company's formula and has no LOINC code.
- **A code says what was measured, not whether it is normal.** Highs and lows
  come from the range and flag the report printed.
- **The vocabulary is versioned.** `mirobody.BUNDLE_VERSION` names the LOINC
  release and cut. After an upgrade, `mirobody recode` recodes stored readings
  under the new vocabulary, and each change is kept in the coding history.

## Improve the vocabulary

Run `mirobody resolve "<name as printed>"`. If the answer is wrong or empty,
[report the term](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)
or add one row to `mirobody/res/loinc/resolver_overrides.tsv` with a test case.
Russian, Estonian and Turkish report spellings arrived this way.

The vocabulary's scope, licence, measurements and the code behind each step are
in the [standardization reference](../standardization.md).

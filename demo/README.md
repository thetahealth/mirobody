# Demo data

Two accounts, and the files that make them a demo of the whole engine rather
than a screenshot. Nothing here is real: every value is generated, and no file
describes a person. Turn it on with `SEED_DEMO_DATA=true` (`compose.yaml`
already does) and sign in with the codes in `config.yaml`.

| | `you@mirobody.ai` | `mom@mirobody.ai` |
| --- | --- | --- |
| Who | healthy, active, labs in range | heavier, less active, sleeping badly, HbA1c above range |
| Seeded | a year of device readings + `seed/you_lab_2025-11.md` | a year of device readings + `seed/mom_lab_2025-05.md` (mg/dL) + `seed/mom_lab_2025-11.md` |
| Upload | `upload/you_annual_checkup_2026-05.pdf`, `upload/you_lipid_panel_2026-08.csv`, and three earlier years that print one test three ways: `upload/you_lab_report_zh_2023-03.pdf`, `upload/you_checkup_photo_zh_2024-04.jpg`, `upload/you_lab_results_en_2025-03.xlsx` | `upload/mom_physical_2026-06.jpg`, `upload/mom_clinic_visit_2026-07.xlsx` |
| Circle | reads mom's record, view-only | shares their record with you |

## `seed/` — what the database gets directly

One lab panel per account, and for mom a second, older one, written by
`mirobody/server/demo.py` alongside a year of generated device readings (weight, resting heart rate, steps, blood
pressure, and sleep for the account that tracks it). That is the baseline, so
the charts are not empty before you have done anything.

## `upload/` — what you upload

Deliberately **not** seeded, so dropping one on the Data page is not a no-op.
The PDF and the photo are each the second panel for their account and repeat
every analyte the seeded panel carries, so uploading one adds a second point to
a series that already exists rather than a lone indicator with nothing to
compare to. The csv and the spreadsheet then add a third.

Four formats, because a health record arrives as whatever the lab, the clinic
and the family actually produce:

| File | Format | What it is | Readings |
| --- | --- | --- | --- |
| `you_annual_checkup_2026-05.pdf` | PDF | this year's panel, printed by the clinic | 9 |
| `mom_physical_2026-06.jpg` | JPG | a phone photo of a printed slip, read by the vision model | 9 |
| `you_lipid_panel_2026-08.csv` | CSV | a different lab's export, with its own wording | 5 |
| `mom_clinic_visit_2026-07.xlsx` | XLSX | what the clinic typed into a spreadsheet | 4 |
| `you_lab_report_zh_2023-03.pdf` | PDF | a hospital lab slip in Chinese, analytes by abbreviation, mmol/L | 16 |
| `you_checkup_photo_zh_2024-04.jpg` | JPG | a photo of a check-up centre's sheet in Chinese, full names, mg/dL | 16 |
| `you_lab_results_en_2025-03.xlsx` | XLSX | an international clinic's export in English, mg/dL | 16 |

## Three years, three names, two units

中文说明见 [`docs/walkthrough.zh-CN.md`](../docs/walkthrough.zh-CN.md)。

The four files above are in English and, for `you`, in one unit. A person's
own decade of reports is neither, so three more files give `you` three
earlier years from three places that share no wording:

| File | Printed by | Prints LDL cholesterol as | Unit | Code | In mmol/L |
| --- | --- | --- | --- | --- | --- |
| `you_lab_report_zh_2023-03.pdf` | a hospital lab, Chinese | `LDL-C` | mmol/L | 22748-8 | 3.62 |
| `you_checkup_photo_zh_2024-04.jpg` | a check-up centre, Chinese | `低密度脂蛋白胆固醇` | mg/dL | 13457-7 | 3.31 |
| `you_lab_results_en_2025-03.xlsx` | an international clinic, English | `LDL Cholesterol` | mg/dL | 13457-7 | 2.90 |

By name alone all three resolve to 13457-7 (`mirobody resolve LDL-C
低密度脂蛋白胆固醇 "LDL Cholesterol"`). The unit then picks the code, the molar
22748-8 for the slip in mmol/L, and both codes file under one series, so the
three are one line. Each report carries sixteen rows (blood count, liver,
kidney, glucose, the four lipids), and every one of the sixteen lands on one
series across the three: `HGB` in g/L with `血红蛋白` and `Hemoglobin` in
g/dL, `CREA` in µmol/L with `肌酐` and `Creatinine` in mg/dL, `FPG` with
`空腹血糖` and `Glucose, Fasting`.

What to expect after uploading the three, signed in as `you` or as anyone:

- **Indicators** lists each of the sixteen once, with three records. On the
  demo account, cholesterol, fasting glucose and HbA1c have four, because
  the seeded 2025-11 panel is on the same series.
- **The LDL drawer** draws one line through 2023, 2024 and 2025. A series
  reads in the unit most of its readings carry, so with these three alone it
  is `140 → 128 → 112 mg/dL`, the first converted from the slip's 3.62
  mmol/L. Upload the two 2026 files as well and mmol/L has the majority:
  `3.62 → 3.31 → 2.90 → 2.48 → 2.41 mmol/L`, five spellings on one line.
  A converted row says what the report printed (`128 mg/dL`), and every row
  opens the file it was read from.
- **Ask** how LDL has changed over the years and the answer cites the same
  numbers.

Measured 2026-11-12 on a 1.6.1 stack running both models on the machine
(MiniCPM5-2B answering, GLM-OCR reading the photo): sixteen readings from
each file, filed under 2023-03-21, 2024-04-16 and 2025-03-25, and the five
values above in the drawer and in the answer. The rows of the PDF and of the
workbook are read by the table rules, off the text layer and the cells, not
by a model; the photo needs a model that reads images. It was not run
against a cloud vision model.

The hospital, the check-up centre, the clinic, the people and every number
on these three are invented, and each says so on its face.

## Two vocabularies, one code

`you_lipid_panel_2026-08.csv` names its analytes the way its lab does:
`Cholesterol, Total` where the PDF prints `Total Cholesterol-TC`. Both resolve
to **14647-2**, so the Indicators table shows one code over two spellings and a
question about cholesterol finds every file that carries it. Measured
2026-09-17 on a running stack: asked how the cholesterol had changed, the agent
read all three files and answered `4.60 → 4.45 → 4.38 mmol/L`, naming the file
each number came off. Six pairs of spellings, six matches.

`mom_clinic_visit_2026-07.xlsx` does the same across SOURCES rather than labs:
its `Body weight` and `Systolic Blood Pressure` sit beside the watch's
`bodyMasss` and `systolicPressures`, carrying **29463-7** and **8480-6** just as
those do. Two sources, two rows, one code.

Your two 2026 files match in units, so they make the three-spellings point
alone; your three earlier years (above) and mom's do not, on purpose:
`seed/mom_lab_2025-05.md` is from a US clinic and
prints glucose and cholesterol in mg/dL, the 2025-11 panel and the uploads in
mmol/L. LOINC puts the unit in the identity (cholesterol is **2093-3** in
mg/dL and 14647-2 in mmol/L), the two fold into one series through the molar
mass, and every view answers that series in one unit: the drawer charts
`198 mg/dL` as 5.12 mmol/L, and its source column says what the report
printed. With every demo file in mmol/L, no demo walk-through ever showed a
series in two units, and a chart that drew 212 mg/dL and 4.90 mmol/L as a fall
from 212 to 4.9 shipped (thetahealth/mirobody#271). The DEVICE path converts
before it stores: `standardize.convert_to_standard` turns `154.5 lb` into
`70.08 kg`, and `examples/02_standardize_a_reading.py` prints that offline.

## `e2e/` — two labs, two units

`units_us_lab_2025-03.csv` (mg/dL) and `units_cn_lab_2026-06.csv` (mmol/L)
are the case from #271: one person's total cholesterol and LDL from two labs.
Uploaded to one record they are one series each, read in mmol/L. The local
e2e harness's `units` stage uploads both and checks the drawer's rows, the
statistics and what the model reads; `scripts/e2e_docker.sh` checks the same
on mom's seeded record without an upload.

Every file carries ONE collection date, because `resolve_report_date` gives a
whole file a single date. A spreadsheet of seven mornings lands as one reading:
measured, 7 rows in and 1 row out.

## Regenerating

    python demo/generate.py

Rebuilds `upload/` from the panels declared in that script, to the same bytes
on every run: the PDFs are written out by hand and the workbooks are saved
with a fixed timestamp. The two photos need the fonts macOS ships (Arial and
STHeiti) and are the same bytes for one version of Pillow and of those fonts.
The `seed/` markdown is hand-written, because its values have to agree with `_PROFILES` in
`mirobody/server/demo.py`; `tests/server/test_member_seed.py` checks that they
do.

Dates are literals, not offsets from today, so a replay upserts the same rows
and the recorded walkthrough keeps matching what a reader sees. The device
window ends 2026-09-14 (`_LAST_DAY`).

## Exporting the walkthrough

    python demo/export.py --output /tmp/mirobody-demo.json

Exports the same two accounts, their 2,022 seeded readings, the view-only
sharing grant and the ten source files. The seven upload files are marked
`stage: "upload"`; they must stay outside a client's initial record. Each file
carries its collection date, readings and SHA-256, and the fixture carries the
source commit and a deterministic data digest. No database, model key or
deployment configuration is read. A hosted walkthrough can translate labels
and explanations while retaining the original names, dates, units and values.

The [public walkthrough](https://chat.mirobody.ai/demo?ref=github-demo&lang=en)
opens in English; the language control at the top switches between English and
Simplified Chinese. It replays synthetic data. To continue with your own
records it sends you to [Theta](https://www.thetahealth.ai/), on the web or as a
mobile app; it does not use the local demo accounts and codes in this
repository.

## Not in the wheel

This directory sits beside `frontend/` and for the same reason: the application
is `git clone && ./deploy.sh`, never a `pip install`, so demo data inside the
package would be dead weight for everyone who installs the library.
`scripts/check_wheel_data.py` fails the build if any of it turns up there.
`DEMO_DATA_DIR` points the seed somewhere else.

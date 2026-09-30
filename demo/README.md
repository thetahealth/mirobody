# Demo data

Two accounts, and the files that make them a demo of the whole engine rather
than a screenshot. Nothing here is real: every value is generated, and no file
describes a person. Turn it on with `SEED_DEMO_DATA=true` (`compose.yaml`
already does) and sign in with the codes in `config.yaml`.

| | `you@mirobody.ai` | `mom@mirobody.ai` |
| --- | --- | --- |
| Who | healthy, active, labs in range | heavier, less active, sleeping badly, HbA1c above range |
| Seeded | a year of device readings + `seed/you_lab_2025-11.md` | a year of device readings + `seed/mom_lab_2025-11.md` |
| Upload | `upload/you_annual_checkup_2026-05.pdf`, `upload/you_lipid_panel_2026-08.csv` | `upload/mom_physical_2026-06.jpg`, `upload/mom_clinic_visit_2026-07.xlsx` |
| Circle | reads mom's record, view-only | shares their record with you |

## `seed/` — what the database gets directly

One lab panel per account, written by `mirobody/server/demo.py` alongside a
year of generated device readings (weight, resting heart rate, steps, blood
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

The units match across the files on purpose. LOINC puts the unit IN the
identity: cholesterol is 14647-2 in mmol/L and **2093-3** in mg/dL, two codes
for one analyte, and a demo that mixed them would show the comparability key
rather than the three-spellings point these files make. The DEVICE path
converts before it stores: `translate.convert_to_standard` turns `154.5 lb`
into `70.08 kg`, and `examples/02_standardize_a_reading.py` prints that offline.

Every file carries ONE collection date, because `resolve_report_date` gives a
whole file a single date. A spreadsheet of seven mornings lands as one reading:
measured, 7 rows in and 1 row out.

## Regenerating

    python demo/generate.py

Rebuilds `upload/` from the panels declared in that script. The `seed/`
markdown is hand-written, because its values have to agree with `_PROFILES` in
`mirobody/server/demo.py`; `tests/server/test_member_seed.py` checks that they
do.

Dates are literals, not offsets from today, so a replay upserts the same rows
and the recorded walkthrough keeps matching what a reader sees. The device
window ends 2026-09-14 (`_LAST_DAY`).

## Not in the wheel

This directory sits beside `frontend/` and for the same reason: the application
is `git clone && ./deploy.sh`, never a `pip install`, so demo data inside the
package would be dead weight for everyone who installs the library.
`scripts/check_wheel_data.py` fails the build if any of it turns up there.
`DEMO_DATA_DIR` points the seed somewhere else.

# Ambiguities these tables carry

The tables beside this file say which LOINC code a vendor's field is. This
file is for the cases where that question has more than one right answer, so
the next person does not rediscover them by finding a number that looks wrong.

Everything here is a decision, with the evidence and the date. A row in a TSV
cannot hold a paragraph, and `metrics.tsv` cannot hold a comment at all
(`csv.DictReader`, no `#` handling), so they live here.

## One word, two measurements: `steps`

| | |
| --- | --- |
| `steps` | **55423-8** *Number of steps in unspecified time Pedometer*, `XXX`, `Pedometer` |
| `dailySteps` | **41950-7** *Number of steps in 24 hour Measured*, `24H`, `Measured` |

Both are right, and the difference is the TIME axis. A wearable streams step
deltas as it counts them, and that stream has no time window, so it is
55423-8. What it shows the person, and what almost every vendor's API returns
as "steps", is the day, which is 41950-7.

So the source decides, and `collect.observations.coding_for` reads it off the
provenance: a device batch takes the catalogue's answer, anything typed or
read off a report takes the vocabulary's. `steps` from a wearable is 55423-8;
`steps` a person writes down is 41950-7, because a person writing "steps:
8,432" means the day.

This is not a hedge. Filing both under one code would put a minute's worth of
deltas on the same line as a day's total, and the sum of a day of 55423-8 is
the same number as one 41950-7, which is exactly why the mistake is invisible
once it is made.

Owner's ruling, 2026-09-20: most wearables report the daily figure, so 41950-7
is what "steps" means without further qualification.

## One measurement, two catalogue rows: `sleepDuration`

`dailyTotalSleepTime` carries **93832-4** *Sleep duration*. `sleepDuration`
describes the same quantity ("Total sleep time" against "Daily total sleep
time") and carries no code.

That is not an omission to fill by copying the code across. No vendor
crosswalk routes anything to `sleepDuration`: the eight that carry a
total-sleep field route it to `dailyTotalSleepTime`. A second name under 93832-4 would let two writers
disagree about which to use and produce two series of one quantity.

Owner's ruling, 2026-09-20: `sleepDuration` maps to `dailyTotalSleepTime`.
The demo seed was the only writer and now writes the latter. The row stays
uncoded until something produces it; if nothing ever does, it should be
deleted rather than coded.

## Three codes, one algorithm: `hrvSDNN`

| | |
| --- | --- |
| `hrvSDNN` | **112429-6** *Heart rate variability SDNN*, `^Patient`, no method |
| | 76643-6 *R-R interval.standard deviation by EKG*, `Heart`, `EKG` |
| | 80404-7 *R-R interval.standard deviation*, `Heart`, no method |

LOINC has no code for heart rate variability with the algorithm unstated: all
three are SDNN. So a bare "HRV" cannot be coded without asserting SDNN, and
the question is only which of the three.

Until 1.5.1 the vocabulary answered 76643-6 and the catalogue answered
112429-6, so the same quantity grouped into two series depending on whether it
arrived from a device or off a report.

Owner's ruling, 2026-09-22: 112429-6 for both. HRV comes mostly from devices,
and 112429-6 is the one code that says only what the word says. The other two
add `SYSTEM=Heart`, and 76643-6 adds `METHOD=EKG`. Measured after the change:
no text reaches either of them, including their own long common names.

`hrvRMSSD` and `hrvDatas` keep no code. RMSSD is a different statistic with no
LOINC code at all, and `hrvDatas` does not say which statistic it is.

## Mass and percentage are two rows: body water and bone

| | |
| --- | --- |
| `bodyWaterMass` / `bodyWater` | **101683-1** `Mass` / **101684-9** `MFr` |
| `bodyBone` / `bonePercentage` | **101685-6** `Mass` / **101686-4** `MFr` |

An impedance scale reports both, and they differ in PROPERTY, so they are four
codes and four rows. 1.5.0 coded the mass pair and left the percentages
uncoded; 1.5.1 codes them and adds `bonePercentage`, which had no row at all.

Do not derive one from the other here. A scale publishes whichever its vendor
chose, and a derived value that looks measured cannot be told apart later.

## The pattern behind these

A catalogue name is a vendor's word, not a clinical term, and several are
ordinary English: `cholesterol` is dietary intake here and serum cholesterol
on a lab report, and `sodium`, `caffeine`, `carbohydrates`, `fibre` and
`alcohol` are the same shape. Reading a typed name through this catalogue
files all of them in the device namespace and loses the LOINC code the
vocabulary would have given.

Nine catalogue names collide with a term the vocabulary answers. The list is
worth re-measuring when the catalogue grows:

```python
from mirobody.kernel import metrics
from mirobody.engine import resolve
[m.name for m in metrics.ROWS if resolve(m.name).resolved]
```

## Why the ranges are not in here, and these are not in the ranges

`mirobody/schema/41_device_rules.sql` seeds `indicator_valid_rules`, one row
per metric, saying what values are possible for it. It is keyed by the same
catalogue name these tables use, so merging the two looks natural. It would be
wrong in both directions.

**These tables answer identity, the SQL answers plausibility.** Which LOINC
code a vendor field is, against whether 350 bpm is a heart rate. A wrong code
files a reading under another measurement; a wrong range marks a real reading
as suspect. Neither answer helps with the other question.

**These ship, the SQL does not.** `pip install mirobody` gets the crosswalks
and reads them through `mirobody.translate.devices` with no database and no
network, which is what lets `mirobody parse` code a device reading offline.
The table exists only where there is a Postgres. Moving identity into SQL
would put it behind a dependency the library contract forbids: import-linter
holds `mirobody` to numpy and the standard library.

**The layering is already two-tier and deliberate.** `kernel.quality.
value_gate` refuses only dimension-level impossibilities (a percentage outside
0 to 100, a non-finite number) and runs in the library; the per-metric ranges
run in a deployment. The narrow gate is not a weaker copy of the wide one.

What would be worth doing is the opposite direction: the ranges are the only
device fact that exists ONLY in SQL, so a library user gets no per-metric
gate at all. Two columns in `metrics.tsv` would fix that and leave the SQL
seed derived from them. That is a change to the catalogue's shape, so it is
the owner's call, not a refactor.

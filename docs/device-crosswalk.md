# Device crosswalk: wearable fields to LOINC

Which LOINC code is Apple Health's step count? Is the HRV a Fitbit reports the
same statistic as the one an Apple Watch reports? Where does Huawei's 深睡
land, and is WHOOP's slow-wave sleep the same thing? No vendor API answers
these: none of the thirteen read here carries a LOINC code of its own, and
every integration maps by hand and keeps the result to itself.

This is that mapping, published, with the half nobody publishes: the 71
device quantities that have **no** LOINC code, and why. A code you cannot
find is a gap in the standard; a code you find wrongly is a wrong number in
someone's chart. The tables were built to make both visible.

Every code was checked by hand against the six LOINC 2.83 axes
(COMPONENT, PROPERTY, TIME, SYSTEM, SCALE, METHOD) on 2026-09-09. Each row
carries a **confidence**:

| | |
| --- | --- |
| `confident` | the six axes agree with the vendor's own definition of the field |
| `unverified` | semantically close; a person must confirm before a reading is filed under it |
| *(empty)* | no code; the note says why |

The catalogue in [`mirobody/res/catalog/metrics.tsv`](../mirobody/res/catalog/metrics.tsv) takes
only the confident codes as a metric's identity. An unverified code stays
visible here and in the tables, and the metric keeps its own namespace until
someone confirms it.

## What is in it

All tables live in [`mirobody/res/crosswalks/`](../mirobody/res/crosswalks/) and
ship with `pip install mirobody`.

| File | One row per | Columns |
| --- | --- | --- |
| `loinc_device_base.tsv` | LOINC code (68) | `loinc name axes metric confidence` then one column per vendor holding that vendor's field name, verbatim, then `note` |
| `<vendor>.tsv` (13 files) | vendor field | `type field metric loinc confidence note`; an empty `loinc` is a decline, and the note says why |
| `unmappable.tsv` | quantity with no code (71) | `reason metric vendors detail disposition` |
| `open_wearables.tsv` | open-wearables `SeriesType` | the seam to that project's device-access layer; see the file header |
| `README.md` | one decision | the cases where a field has more than one right code, with the ruling and its date |

Loading them:

```python
from mirobody.translate import devices

devices.lookup("apple", "HKQuantityTypeIdentifier", "stepCount")
# VendorField(vendor='apple', type='HKQuantityTypeIdentifier', field='stepCount',
#             metric='steps', loinc='55423-8', confidence='confident', note='...')

devices.by_code("112429-6").fields
# {'apple': 'heartRateVariabilitySDNN'}        one vendor; the others publish RMSSD

[u.metric for u in devices.unmappable() if u.reason == "VENDOR_SCORE"]
```

Rendering them, which is how the tables below were made:

```
python scripts/device_crosswalk_report.py vendors | reasons | base
```

## By vendor

Fields read is every field or data type the vendor documents for health
data; a field with a code is one the review could place on a LOINC code;
confident is the subset that needs no further confirmation. The source
column links the document each table was read from, all on 2026-09-09.

| Vendor | Fields read | With a code | Confident | Source |
| --- | ---: | ---: | ---: | --- |
| Apple HealthKit (`apple.tsv`) | 50 | 38 | 31 | [1](https://developer.apple.com/tutorials/data/documentation/healthkit/hkquantitytypeidentifier.json) · [2](https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategorytypeidentifier.json) · [3](https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategoryvaluesleepanalysis.json) |
| Google Health Connect (`health_connect.tsv`) | 44 | 30 | 24 | [1](https://developer.android.com/reference/androidx/health/connect/client/records/package-summary) |
| Huawei Health Kit (`huawei.tsv`) | 78 | 48 | 34 | [1](https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/overview-0000001177423513) · [2](https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/health-sampling-data-0000001131423778) |
| Honor Health Kit (`honor.tsv`) | 58 | 36 | 23 | [1](https://developer.honor.com/) |
| Samsung Health Data SDK (`samsung.tsv`) | 26 | 17 | 15 | [1](https://developer.samsung.com/health/data/api-reference) |
| Fitbit Web API (`fitbit.tsv`) | 31 | 22 | 16 | [1](https://dev.fitbit.com/build/reference/web-api/) |
| WHOOP API v2 (`whoop.tsv`) | 34 | 19 | 11 | [1](https://api.prod.whoop.com/developer/doc/openapi.json) |
| Oura API v2 (`oura.tsv`) | 54 | 25 | 14 | [1](https://cloud.ouraring.com/v2/static/json/openapi-1.41.json) |
| Garmin Health API (`garmin.tsv`) | 23 | 17 | 6 | [1](https://developer.garmin.com/gc-developer-program/health-api/) |
| vivo BlueOS health API (`vivo.tsv`) | 20 | 14 | 11 | [1](https://developers-watch.vivo.com.cn/api/health/health/) · [2](https://developers.vivo.com/doc/d/4ea8ba1ec4cd44bd8bdaca9f3fecf795) |
| Xiaomi Health cloud (`xiaomi.tsv`) | 13 | 8 | 8 | [1](https://dev.mi.com/) |
| Zepp OS Sensor module (`zepp.tsv`) | 11 | 11 | 8 | [1](https://docs.huami.com/) |
| OPPO theme engine health data (`oppo.tsv`) | 5 | 4 | 4 | [1](https://open.oppomobile.com/documentation/page/info?id=13624) · [2](https://open.oppomobile.com/wiki/new-doc/index.json) |
| **13 vendors** | **447** | **289** | **205** | |

Garmin publishes only its metric families; field-level schemas need
developer approval, so most Garmin rows are unverified until the schema is
read. Honor's service mirrors Huawei's and its cloud side is read-only. OPPO
exposes four values to watch-face scripts and no typed health API; Xiaomi's
public namespace is a workout data cloud with no sleep, blood oxygen, HRV,
temperature or stress types.

## What no code fits

The reason is the point: it decides what to do with the quantity.

| Reason | Metrics | Meaning |
| --- | ---: | --- |
| `ALGO_MISMATCH` | 1 | LOINC has the concept but codes another algorithm |
| `VENDOR_SCORE` | 11 | a proprietary score; LOINC does not code these |
| `NO_CONCEPT` | 24 | the concept is absent from the axis table |
| `UNIT_INCOMPARABLE` | 7 | a near code exists, but the vendor's unit or weighting makes the number incomparable |
| `RELATIVE` | 3 | a deviation from a baseline, not an absolute quantity |
| `NOT_MEASUREMENT` | 5 | a goal, a recommendation or a plan |
| `QUALITY_META` | 9 | a data-quality or device-state signal |
| `NOT_PERSON` | 4 | an environmental quantity |
| `OUT_OF_SCOPE` | 7 | raw waveforms, location, nutrition |
| **total** | **71** | |

The one `ALGO_MISMATCH` row is the most consequential line in the whole
crosswalk, and it is the first trap below. The nine `QUALITY_META` rows
(Fitbit `sleep.infoCode` and `tempSkin.logType`, WHOOP `score_state`,
`percent_recorded`, `user_calibrating` and `total_no_data_time_milli`, Oura
`non_wear_time`, Huawei's ventilator `maskOff`) are not health data, but
they say how far to trust the health data beside them, and they are worth
storing next to the reading rather than dropping.

## Read this before normalising across vendors

These are the places where two vendors' numbers look comparable and are not.
Each one silently produces a wrong value if ignored.

1. **HRV: one vendor publishes SDNN, five publish RMSSD, four publish
   nothing.** Apple's `heartRateVariabilitySDNN` is SDNN. Health Connect's
   `HeartRateVariabilityRmssdRecord`, Huawei's `heartRateVariabilityRMSSD`,
   Fitbit's `hrv.value.dailyRmssd`, WHOOP's `recovery.score.hrv_rmssd_milli`
   and Oura's `sleep.average_hrv` are RMSSD. Samsung, vivo, Xiaomi and Zepp
   publish no HRV type. Garmin publishes beat-to-beat RR intervals, from which
   SDNN can be computed. LOINC 2.83 codes only the SDNN family (112429-6,
   76643-6, 80404-7); `RMSSD` has zero hits in the axis table. The two are
   different definitions and cannot share a row: the catalogue keeps
   `hrvSDNN` (coded), `hrvRMSSD` (uncoded) and `hrvDatas` for a series whose
   statistic is not stated. Syncing an Oura ring into Apple Health writes
   RMSSD into the field Apple defines as SDNN; a downstream reader that trusts
   the field name is then wrong.
2. **Skin temperature depends on the site.** 61008-9 *Body surface
   temperature* (Temp/Qn/Surface) is the quantitative code for a wrist
   device, marked unverified because the wrist has no code of its own and the
   code's IEEE origin is a monitor's body temperature. 60830-7 *Finger
   temperature* is the confident code for a ring (Oura). 60833-1 is the toe.
   39106-0 *Temperature of Skin* is a nominal finding and cannot hold a
   number; 60839-8 is the infant-incubator context. Health Connect's
   `SkinTemperatureRecord` carries `measurementLocation`, the one source that
   lets a decoder pick the site automatically.
3. **Deviation is not temperature.** Fitbit's `tempSkin.value.nightlyRelative`
   and Oura's `temperature_deviation` are offsets from a personal baseline;
   WHOOP's `skin_temp_celsius` and Health Connect's `baseline` are absolute.
   An offset goes to `temperatureDelta`, which has no LOINC code and must
   never be filed under 61008-9 or 8310-5.
4. **Weighted minutes are not minutes.** Fitbit active-zone minutes count
   cardio and peak minutes twice, by the documentation's own words; Garmin
   intensity minutes weight vigorous minutes the same way. Undo the weighting
   before any LOINC duration code, or do not code them.
5. **Energy units.** WHOOP's `cycle.score.kilojoule` is kilojoules; every
   other vendor is kilocalories. Divide by 4.184.
6. **Heart-rate zones align nowhere.** Fitbit has four, WHOOP five, the
   catalogue five, LOINC three plus a maximum zone. Not coded; the only
   comparable quantity across vendors is total moderate-plus-vigorous time.
7. **Activity intensity bands half align.** Oura's low, medium and high map
   onto LOINC's light, moderate and vigorous durations (101688-0, 101689-8,
   101690-6) and is the one confident case. Health Connect's
   `ActivityIntensityRecord` has two bands and no light. Huawei's
   `exercise_intensity.v2` is not an intensity band at all; its field is
   `exercise_type`, the kind of exercise.
8. **Four vocabularies for the sleep stages.** Deep sleep is Apple
   `asleepDeep`, Health Connect `DEEP`, Huawei 深睡, Fitbit `deep`, WHOOP
   `slow_wave_sleep` and Oura `deep_sleep_duration`; light sleep is Huawei
   浅睡, Health Connect `LIGHT`, Fitbit and Oura `light`, and Apple
   `asleepCore`. All of them land on 93831-6 and 93830-8, but Apple's Core and
   WHOOP's slow-wave sleep are marked unverified: synonymous, not the same
   name.
9. **The sleep mode is a routing key.** Fitbit has two log modes, `classic`
   without stages and `stages` with them. Huawei's `sleep_type` has three
   values: 1 with every field, 2 with no REM, 3 with only a total. Oura's
   `sleep.type` includes `late_nap`. Route on the mode before coding stages,
   or a stage the mode does not provide is stored as zero. This is the most
   dangerous line in this list.
10. **A field name is not its meaning.** vivo's `DATA_TYPES.WALKING_SPEED` is
    in steps per minute: a cadence, not a speed. Read the unit column of the
    vendor's documentation, never the name.
11. **Statistical capability is a constraint.** vivo's watch API supports
    minimum and maximum for heart rate, blood oxygen and stress, and sums for
    standing, intensity, steps, distance and calories, and a mean for
    nothing. A mean code such as 103205-1 has no vivo source; do not build
    the mapping because it "should" exist.
12. **Glucose depends on the unit.** Honor's `BLOOD_GLUCOSE.measureValue` is
    in mmol/L and codes to 15074-8, not to the catalogue's 2339-0 (mg/dL).
    The catalogue alias names the analyte and the printed unit picks the
    variant; `mirobody.translate.code` does this for every alias.
13. **Total is not active.** Oura's `total_calories` and Health Connect's
    `TotalCaloriesBurnedRecord` include basal metabolism; the catalogue's
    `activeCalories` does not. Only the active figure is coded confidently.

## LOINC's own axis defects

The LOINC 2.75 batch that added consumer-device concepts is uneven. Four
codes carry axes that contradict their names; every one is marked unverified
here, and all four are worth raising with Regenstrief.

| LOINC | Defect | Effect |
| --- | --- | --- |
| 103219-2 Maximum respiratory rate | TIME is `XXX^min`, contradicting the COMPONENT's *Maximum* | a rendering from the axes contradicts itself |
| 103217-6 Mean respiratory rate | SCALE is empty; CLASS is `HRTRATE.ATOM`, the heart-rate class | six vendors' sleep respiratory rate points here |
| 103209-3 Mean oxygen saturation | PROPERTY is `NRat`, a rate; a saturation is a fraction | fights the UCUM check for `%` |
| 112432-0 Walking asymmetry | SCALE is `Ord`; Apple's value is a percentage | a number filed under an ordinal code |

## How the catalogue uses it

- A metric's identity is `("loinc", code)` only when the code is confident
  (`Metric.canonical`); otherwise it is `("mirobody-device", name)`, so an
  unverified code never becomes a series key.
- The device catalogue's answer for a metric is handed to
  `mirobody.translate.code` as a catalogue alias. The alias names the analyte;
  when the printed unit fits a sibling of that code in the same specimen, the
  sibling is used (glucose in mmol/L), and when it fits none, the alias stands.
- Apple's SDNN identifier is decoded to `hrvSDNN`; the Apple gait, perfusion
  and six-minute-walk identifiers, previously quarantined, now have rows.
- Five invariants in
  [`test_engine_coverage.py`](../mirobody/tests/test_engine_coverage.py) hold
  the tables and the catalogue together: every code is in the axis table with
  a confidence, no two metrics share a code unless registered as one quantity
  at two grains, every crosswalk row names a real code and a real catalogue
  row, the Apple table says what the Apple decoder does, and the unit gate on
  an alias.

## The base table

One row per code. The vendor column lists who produces the quantity; the
field names are in the TSV.

| LOINC | Name | Catalogue metric | Confidence | Vendors |
| --- | --- | --- | --- | --- |
| 8867-4 | Heart rate | `heartRates` | confident | apple, health_connect, huawei, honor, samsung, fitbit, oura, garmin, vivo, xiaomi, zepp, oppo |
| 40443-4 | Heart rate^resting | `restingHeartRates` | confident | apple, health_connect, huawei, honor, fitbit, whoop, oura, garmin, vivo |
| 103205-1 | Mean heart rate | `dailyHeartRateAvg` | confident | huawei, honor, whoop, oura |
| 101692-2 | Maximum heart rate | `dailyHeartRateMax` | confident | huawei, honor, whoop, oppo |
| 103222-6 | Heart rate.minimum | `dailyHeartRateMin` | confident | huawei, honor, oura, oppo |
| 112429-6 | Heart rate variability SDNN | `hrvSDNN` | confident | apple, garmin |
| 59408-5 | Oxygen saturation in Arterial blood by Pulse oximetry | `oxygenSaturations` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, oura, garmin, vivo, zepp |
| 103209-3 | Mean oxygen saturation |  | unverified | huawei, honor |
| 9279-1 | Respiratory rate | `respiratoryRates` | confident | apple, health_connect, huawei, garmin |
| 103217-6 | Mean respiratory rate | `dailyRespiratoryRates` | unverified | huawei, fitbit, whoop, oura, garmin |
| 103218-4 | Minimum respiratory rate | `dailyMinRespiratoryRates` | confident | huawei |
| 103219-2 | Maximum respiratory rate | `dailyMaxRespiratoryRates` | unverified | huawei |
| 8310-5 | Body temperature | `bodyTemperatures` | confident | apple, health_connect, huawei, honor, samsung, fitbit, garmin, zepp |
| 61008-9 | Body surface temperature | `skinTemperature` | unverified | apple, health_connect, huawei, samsung, whoop, garmin |
| 60830-7 | Finger temperature |  | confident | health_connect, oura |
| 60833-1 | Toe temperature |  | confident | health_connect |
| 8480-6 | Systolic blood pressure | `systolicPressures` | confident | apple, health_connect, huawei, honor, samsung, garmin |
| 8462-4 | Diastolic blood pressure | `diastolicPressures` | confident | apple, health_connect, huawei, honor, samsung, garmin |
| 2339-0 | Glucose [Mass/volume] in Blood | `bloodGlucoses` | confident | apple, health_connect, huawei, honor, samsung, fitbit, garmin |
| 93832-4 | Sleep duration | `dailyTotalSleepTime` | confident | apple, health_connect, huawei, honor, samsung, fitbit, oura, garmin, vivo, zepp |
| 93831-6 | Deep sleep duration | `dailyDeepSleep` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, oura, garmin, vivo, zepp |
| 93830-8 | Light sleep duration | `dailyLightSleep` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, oura, garmin, vivo, zepp |
| 93829-0 | REM sleep duration | `dailyRemSleep` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, oura, garmin, vivo, zepp |
| 103210-1 | Awakening duration | `dailyAwakeTime` | unverified | apple, health_connect, huawei, honor, samsung, fitbit, whoop, oura, garmin, vivo |
| 103215-0 | Wake time after sleep onset |  | unverified | health_connect |
| 103213-5 | Duration in bed | `sleepAnalysis_InBed` | confident | apple, health_connect, huawei, honor, fitbit, whoop, oura, garmin, vivo |
| 103212-7 | Duration of falling asleep | `dailySleepLatency` | confident | huawei, fitbit, oura, garmin |
| 103211-9 | Number of awakenings | `sleepDisturbances` | confident | huawei, honor, whoop, oura |
| 103214-3 | Duration of out of bed |  | unverified | health_connect, huawei |
| 103216-8 | Number of times getting out of bed |  | unverified | huawei |
| 103220-0 | Duration of snoring |  | unverified | huawei |
| 103221-8 | Number of snoring episodes |  | confident | huawei |
| 69990-0 | Apnea hypopnea index 24 hour | `apneaHypopneaIndex` | confident | huawei, samsung |
| 70002-1 | Obstructive apnea index 24 hour | `obstructiveApneaIndex` | confident | huawei |
| 70003-9 | Central apnea index 24 hour |  | unverified | huawei |
| 55423-8 | Number of steps in unspecified time Pedometer | `steps` | confident | apple, health_connect, huawei, honor, samsung, fitbit, garmin, vivo, xiaomi, zepp, oppo |
| 41950-7 | Number of steps in 24 hour Measured | `dailySteps` | confident | huawei, honor, oura |
| 103208-5 | Distance traveled | `dailyDistance` | confident | health_connect, huawei, honor, fitbit, whoop, oura, garmin, vivo, xiaomi, zepp |
| 112427-0 | Distance walking or running | `walkingRunningDistances` | confident | apple |
| 100304-5 | Flights climbed [#] Reporting Period | `floors` | confident | apple, health_connect, samsung, fitbit |
| 101687-2 | Elevation climbed | `altitudeGain` | confident | health_connect, huawei, honor, garmin, zepp |
| 41981-2 | Calories burned | `activeCalories` | confident | apple, health_connect, huawei, honor, fitbit, whoop, oura, garmin, vivo, xiaomi, zepp |
| 41979-6 | Calories burned in 24 hour Calculated | `dailyCaloriesActive` | confident | health_connect, huawei, samsung, oura |
| 101688-0 | Duration of light activity | `dailyActivityIntensityLow` | confident | oura |
| 101689-8 | Duration of moderate activity | `dailyActivityIntensityMedium` | confident | health_connect, oura, garmin |
| 101690-6 | Duration of vigorous activity | `dailyActivityIntensityHigh` | confident | health_connect, oura, garmin |
| 101691-4 | Duration of physical activity | `exerciseMinutes` | unverified | apple, huawei, honor, samsung, vivo |
| 112425-4 | Standing time 1 day |  | unverified | apple, vivo, zepp |
| 103223-4 | Workout intensity | `trainingLoad` | unverified | oura, zepp |
| 94122-9 | Oxygen consumption (VO2)/Body weight^peak during exercise | `vo2Maxs` | unverified | apple, health_connect, huawei, honor, fitbit, oura, garmin, zepp |
| 77196-4 | Pulse wave velocity | `pulseWaveVelocity` | confident | oura |
| 61006-3 | Perfusion index Tissue by Pulse oximetry | `perfusionIndex` | confident | apple |
| 29463-7 | Body weight | `bodyMasss` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, garmin, xiaomi |
| 8302-2 | Body height | `heights` | confident | apple, health_connect, huawei, honor, samsung, fitbit, whoop, garmin, xiaomi |
| 39156-5 | Body mass index (BMI) | `bmis` | confident | apple, huawei, samsung, fitbit, garmin, xiaomi |
| 41982-0 | Percentage of body fat Measured | `bodyFatPercentages` | confident | apple, health_connect, huawei, samsung, fitbit, garmin, xiaomi |
| 91557-9 | Lean body weight | `bodyFatFreeWeight` | confident | apple, health_connect, huawei, samsung, garmin |
| 101683-1 | Body water mass | `bodyWaterMass` | confident | health_connect, huawei, samsung, garmin |
| 101684-9 | Percentage of body water | `bodyWater` | confident | huawei, samsung, garmin |
| 101685-6 | Body bone mass | `bodyBone` | confident | health_connect, huawei, samsung, garmin |
| 101686-4 | Body bone percentage | `bonePercentage` | confident | huawei, samsung, garmin |
| 8280-0 | Waist Circumference at umbilicus by Tape measure | `waistCircumferences` | unverified | apple |
| 112434-6 | Walking double support [Percentile] | `walkingDoubleSupportPercentage` | confident | apple |
| 112432-0 | Walking asymmetry | `walkingAsymmetryPercentage` | unverified | apple |
| 112431-2 | Stair ascent speed | `stairAscentSpeed` | confident | apple |
| 112430-4 | Stair descent speed | `stairDescentSpeed` | confident | apple |
| 112426-2 | Physical effort |  | confident | apple |
| 64098-7 | Six minute walk test | `sixMinuteWalkDistance` | confident | apple |

## Sources

Every table names the document it was read from in its header comment; the
same list is `mirobody.translate.devices.SOURCES`. All were read on
2026-09-09 against LOINC 2.83.

| Vendor | Documents |
| --- | --- |
| Apple HealthKit | [hkquantitytypeidentifier.json](https://developer.apple.com/tutorials/data/documentation/healthkit/hkquantitytypeidentifier.json), [hkcategorytypeidentifier.json](https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategorytypeidentifier.json), [hkcategoryvaluesleepanalysis.json](https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategoryvaluesleepanalysis.json) |
| Google Health Connect | [androidx.health.connect.client.records](https://developer.android.com/reference/androidx/health/connect/client/records/package-summary) |
| Huawei Health Kit | [data-type overview](https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/overview-0000001177423513), [sampling data types](https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/health-sampling-data-0000001131423778) and their field-level pages |
| Honor Health Kit | [developer.honor.com](https://developer.honor.com/), Fitness > Health service (kitId 11005); SDK `com.hihonor.mcs:fitness-health:1.0.0.302` from `https://developer.hihonor.com/repo` |
| Samsung Health Data SDK | [API reference](https://developer.samsung.com/health/data/api-reference) |
| Fitbit Web API | [reference](https://dev.fitbit.com/build/reference/web-api/), per-endpoint response tables |
| WHOOP API v2 | [openapi.json](https://api.prod.whoop.com/developer/doc/openapi.json) |
| Oura API v2 | [openapi-1.41.json](https://cloud.ouraring.com/v2/static/json/openapi-1.41.json) |
| Garmin Health API | [program page](https://developer.garmin.com/gc-developer-program/health-api/); field-level schemas need approval |
| vivo | [BlueOS health API](https://developers-watch.vivo.com.cn/api/health/health/), [cloud interface](https://developers.vivo.com/doc/d/4ea8ba1ec4cd44bd8bdaca9f3fecf795) |
| Xiaomi | public `com.xiaomi.micloud.fit.*` data types via [dev.mi.com](https://dev.mi.com/) |
| Zepp | [docs.huami.com](https://docs.huami.com/) Sensor module, plus the shared-data list published through Xiaomi's third-party notice |
| OPPO | [theme-engine health data](https://open.oppomobile.com/documentation/page/info?id=13624), [documentation index](https://open.oppomobile.com/wiki/new-doc/index.json) |
| open-wearables | [github.com/the-momentum/open-wearables](https://github.com/the-momentum/open-wearables) |

LOINC is copyright Regenstrief Institute, Inc., used under the
[LOINC license](https://loinc.org/kb/license/); see `LICENSE-3RD-PARTY`
section 3. The codes and the shortened long common names in these tables are
LOINC content; the mapping judgements and the notes are this project's.

## Changing it

Edit the TSV and open a pull request. The gate tests reject a code the axis
table does not have, a metric name the catalogue does not have, a confident
vendor code that contradicts the metric's confident catalogue code (unless the
two are unit variants of one analyte), and an Apple row that disagrees with
the Apple decoder. A new vendor is a new `<vendor>.tsv`, a `Source` in
`mirobody.translate.devices`, and a column in the base table. Rerun
`scripts/device_crosswalk_report.py` and paste the tables here.

Notes inside the tables are in the language they were written in; many are
in Chinese, since the review was done against several vendors' Chinese
documentation. The column names and the confidence vocabulary are English.

# Standardization in depth

**English** · **[中文](standardization.zh-CN.md)**

The long form of the README's **② Standardize** stage: how a printed reading
becomes a coded, comparable one, step by step and module by module; what the
shipped vocabulary is, what it deliberately does not do, and which LOINC
release it is cut from. The reader-facing summary is
[site/standardization.md](site/standardization.md). Figures about the
vocabulary come from `res/loinc/fhir_loinc_bundle.NOTICE`, which the bundle
stamp keeps honest.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/collect-standardize-agent-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="images/collect-standardize-agent.svg">
  <img src="images/collect-standardize-agent.svg" alt="Collect, Standardize, Agent: three stages, left to right" width="920">
</picture>
</p>

## From a printed line to a comparable reading

Every reading, whatever brought it in, passes through one writer,
`collect/observations.py` (`ingest()`), in one transaction. The steps it calls
live in `mirobody/standardize/`, `mirobody/engine/` and `mirobody/units/` and
touch no database, clock or model, so the same input gets the same coding on
every machine. The running example is three reports of one test:
`LDL-C 2.48 mmol/L`, `低密度脂蛋白胆固醇 96 mg/dL` and
`LDL Cholesterol 101 mg/dL`.

### 0. Where a reading comes from

| Source | Module | Where a model takes part |
| --- | --- | --- |
| Lab report, photo, spreadsheet | `collect/files/services/indicator_extractor.py`: the table rules in `table_indicators.py` read every row that looks like a reading (columns mapped by header text, never by position); the model reads only what they leave. `indicator_store.py` hands the rows to `ingest()` | Reading the page. The prompt (`prompts/file_indicator_extract.py`) requires the name copied as printed, untranslated, because it is the key step 5 looks up. The model is never asked for a code |
| Wearable, Apple Health | `kernel/decoders/` converts each vendor field into the unit `kernel/metrics.py` stores it in; the route from a vendor payload to a stored row, with its aggregation and election, is [pipeline.md](pipeline.md) stages 1–8 | None |
| A journal sentence | `collect/sentence.py` splits `头疼，血压150/95` into a complaint and two readings; a part must quote the sentence, and negated, hypothetical or someone else's statements are not written | Splitting and typing. Codes come from step 5 |
| HTTP API | The name, value and unit as printed | None |

### 1. Keep the print (`observations.prepare`)

`name_text`, `value_text`, `unit_text`, `ref_text` and `flag_text` are stored
verbatim. Every derived column sits beside them; none replaces them.
`th_observation` is append-only: a correction is a new row that amends the old
one.

### 2. Fold the name (`standardize/fold.py`, `name_key`)

NFKC, casefold, one space between Latin words, no space beside a CJK character,
no trailing punctuation: `白细胞 计数` → `白细胞计数`, `ＨＤＬ－Ｃ` → `hdl-c`. It
does not translate, strip specimen words or expand abbreviations; those are
coding decisions and are recorded as such. Every writer and reader uses this
one function, because a second fold that differed by one rule would split one
series into two.

### 3. Type the value (`standardize/parse.py`, `parse_value`)

A value is a `quantity` (always with a number), `ordinal` (`++`), `nominal`
(`阴性`), `narrative` or `absent`. A comparator is kept apart (`<0.5` is 0.5 and
`<`), the unit is normalized to UCUM by `mirobody.units`, and a unit printed
inside the value cell still counts as the printed unit. `120/80` stays
narrative: it is two readings the extractor should split, not a 120.

### 4. Reject the impossible (`kernel/quality.py`)

`time_gate` rejects a reading with no start, an end before its start, an
implausibly long interval or a time more than a day in the future;
`value_gate` rejects a non-finite number and a percentage outside 0–100. A
rejected draft is counted by reason in the `Report`. Nothing is defaulted: a
missing time is never today.

### 5. Code it (`standardize/code.py`, `code`)

`observations.coding_for` picks the vocabulary by kind: a complaint or a
diagnosis goes to ICPC-3 (`standardize/icpc3.py`), a note is never coded, and
a measurement goes to `code()`, which asks in this order of authority:

1. **A confirmed alias** (`th_coding_alias`), the person's own before a global
   one. It names the analyte; a printed unit may still pick that analyte's
   mass or moles sibling.
2. **The device catalogue**, for a device batch only. Its vendor names are
   ordinary words (`cholesterol` is dietary intake there), so a typed name
   never consults it.
3. **The lexical resolver**, `engine.resolve_reading`, whose only accepted
   method is `lexical`.

Inside the resolver (`engine/resolver.py`):

- **Categories refuse.** A term whose target is `!unresolved` in
  `resolver_overrides.tsv` (`血脂`, `lipid panel`) is refused, and every
  surface variant is checked, so `lipid_panel` cannot walk around the block.
- **Candidate keys, most specific first.** The alias-table hop comes before
  the index: `resolver_overrides.tsv` and `aliases_src/zh_curated.tsv` and
  `zh.tsv` are statements that one term means one concept, while an index hit
  is every LOINC row sharing a surface string. Then surface variants
  (full-width forms, dash shapes, `snake_case`: `lexical.surface_variants`). A
  trailing abbreviation is stripped only when it repeats the stem
  (`总胆固醇 TC`) and kept when it adds a specimen or a qualifier
  (`Protein CSF`, `Glucose OGTT`, `胆固醇 HDL`), so those stay unresolved
  rather than quietly answering the code of the stem alone. Measure words (`计数`,
  `count`) are dropped last, so they can only turn a miss into a hit.
- **`Name (abbreviation)` must agree.** Both halves are resolved. One analyte:
  the stem wins (`空腹血糖(GLU)` → 1558-6). Two analytes: refused, with both
  codes named so a person can choose (`血糖(HbA1c)` gives 2339-0 and 4548-4).
- **Picking among postings** (`_pick`): LOINC's `COMMON_TEST_RANK`, plain
  serum, plasma or blood over cord or capillary specimens, and an exact
  `COMPONENT` match, unless it would turn a patient measurement into an organ
  or a device (`weight` must not become stone weight).
- **The reading picks the variant** (`variant_for_reading`). LOINC codes the
  PROPERTY and SCALE into the identity, so the unit's property family and the
  value's kind select the sibling with the same full `COMPONENT` and specimen,
  method-less first. The outcomes:

| Outcome | Example | Result |
| --- | --- | --- |
| `agreed` | `LDL Cholesterol 101 mg/dL` | 13457-7, as the name gave |
| `switched` | `LDL-C 2.48 mmol/L` | 22748-8 (moles); the name alone gave 13457-7 |
| `switched` | `尿糖 阴性` | 2349-9 (presence, `Ord`); the name alone gave 2350-7 (`Qn`) |
| `unit-unrecognized` | `glucose 5.6 mmol/xyz` | the name's code, marked unit not recognized |
| `axis-conflict` | `albumin 30 mg/24h` | no code: the unit fits albumin only in urine, and a unit may pick the property, never the specimen |

Back in `code()`, two last gates: a number is not filed under an ordinal code,
and a quantity with no usable unit is not filed under an analyte whose codes
differ by unit (`FPG 4.76` waits for mg/dL or mmol/L).

Every reading ends with one outcome, written to `th_coding_current`,
`th_coding_history` and `th_coding_decision` beside the row:

| Outcome | Reasons |
| --- | --- |
| `coded` | the code, its series, the vocabulary release, the rule, and the evidence (`name`, `property`, `scale`, `alias`) |
| `needs-input` | `engine:no-match`, `name:ambiguous`, `unit:conflict`, `unit:missing`, `unit:unrecognized`, `scale:<kind>-vs-<scale>` |
| `refused` | `engine:refused` (a category), `alias:not-standard`, a note |

The `decision_id` is a hash of the folded name, the unit, the value kind, the
release and the rule, so two readings that fold the same share one decision,
and a recode under a newer release is a replay of `code()` over the frozen
extraction.

The library returns the same decision without a database, as a FHIR R4
Observation: the printed name stays in `code.text`, the coding is present only
when the resolver answered, and an extension records how
(`engine/observation.py`, `standardize_reading("LDL-C", "2.48", "mmol/L")`):

```json
{
  "resourceType": "Observation",
  "status": "final",
  "code": {
    "text": "LDL-C",
    "coding": [{"system": "http://loinc.org", "code": "22748-8",
                "display": "Cholesterol in LDL [Moles/volume] in Serum or Plasma"}]
  },
  "valueQuantity": {"value": 2.48, "unit": "mmol/L",
                    "system": "http://unitsofmeasure.org", "code": "mmol/L"},
  "extension": [{
    "url": "https://mirobody.ai/fhir/StructureDefinition/coding-decision",
    "extension": [
      {"url": "method", "valueString": "lexical"},
      {"url": "evidence", "valueString": "name"},
      {"url": "evidence", "valueString": "property"},
      {"url": "evidence", "valueString": "scale"},
      {"url": "property", "valueString": "SCnc"},
      {"url": "scale", "valueString": "Qn"},
      {"url": "system", "valueString": "Ser/Plas"},
      {"url": "unitRecognized", "valueBoolean": true},
      {"url": "bundle", "valueString": "loinc-2.83+2026.10.27-126ecba69782"},
      {"url": "rejectedCode", "valueString": "13457-7"},
      {"url": "rejectedReason", "valueString": "the name alone gives 13457-7 (MCnc/Qn)"}
    ]
  }]
}
```

### 6. File it into a series (`standardize/series.py`, `units.canonicalize`)

A LOINC code is narrower than what a chart may plot together, so the series
key folds two axes:

    series_id = loinc:COMPONENT|SYSTEM|TIME|SCALE|dim(PROPERTY)

METHOD is rolled up and mass and molar PROPERTY fold to one dimension, the rule
LOINC's own groups (LG55-6, LG100-4) follow. 22748-8 and 13457-7 are both
`loinc:cholesterol.in ldl|Ser/Plas|Pt|Qn|QCnc`. The values agree through a
canonical pair stored beside the printed one, with the molar mass keyed by
LOINC code: 2.48 mmol/L is 0.00248 mol/L, 96 mg/dL is 0.002483 mol/L and
101 mg/dL is 0.002612 mol/L. An uncoded reading has a series too,
`local:<name_key>|<unit>`, which never merges with another name.

### 7. Read it back (`collect/query.py`)

The catalogue lists series, not printed names. A statistic runs over the
printed values when the series has one unit, over the canonical values when
every reading has one canonical unit, and is withheld otherwise (albumin in
g/L and umol/L has no molar mass to bridge them). Raw rows carry both values.
Answers cite the rows they rest on ([answers.md](answers.md),
`kernel/citations.py`).

### 8. Change your mind

`observations.confirm_alias` (the API is `POST /api/v1/health-indicators/confirm-code`,
[file-processing.md](file-processing.md)) records what a printed name means
and recodes every row that carries it. `mirobody recode` replays step 5 over
stored readings after an upgrade. Either way the row itself never changes; a
history row records the old coding and the cause.

## What the layer provides

Standardization here is not a lookup table but a complete terminology-normalization system:

- **63,416 codes**, cut from LOINC 2.83 by one rule in one pass: laboratory
  and clinical observations, narrative and document scales dropped, panels
  kept for the laboratory and vital-sign subclasses. **1,023,273 folded
  designations** point at them, from the component, the long and short names,
  the related names, the display and consumer names and the 21 linguistic
  variants the release ships (`res/loinc/fhir_loinc_bundle.NOTICE`).
- **English first, Chinese beside it.** 22,578 Chinese aliases plus this
  project's own curated rows sit in front of the index, so `hemoglobin`,
  `血红蛋白` and `血紅素` all land on LOINC 718-7. The other languages resolve
  through what LOINC itself publishes, since its 21 LinguisticVariants are
  inputs to the alias index, rather than through files of our own: the
  Japanese one was a UMLS derivation, and the five machine-derived ones
  (de·es·fr·ko·ru) could not be traced to the variants they claimed. Both are
  gone; see `LICENSE-3RD-PARTY`.
- **繁體中文 is two problems, handled as two.** Script folding is mechanical
  (a shipped 3,336-character zh-Hant → zh-Hans table); vocabulary is not — Taiwan
  usage picks different words, and folding `血紅素` yields the HbA1c code. Those
  terms are curated under their Traditional spelling, and a curated row always
  beats a fold.
- **Units** normalized against 330 UCUM units over 59 PROPERTY families,
  with dimensional analysis, a molar-mass bridge keyed by LOINC code, and an
  explicit refusal for `%` vs `10*9/L`. 902 catalogue members cover device and laboratory indicators. The
  UCUM specification ships beside the tables (`res/ucum/ucum-essence.xml`,
  2.2, unmodified), and a gate requires every unit they name to be one UCUM
  defines and every conversion factor to equal UCUM's.
- **Complaints and diagnoses on ICPC-3.** `resolve_symptom("头疼")` gives
  `NS01`, `resolve_condition("高血压")` gives `KD73`: 1,218 ICPC-3 codes (the S
  and D components), shipped verbatim, reached through English, Chinese,
  Japanese and Russian spellings we wrote (繁體中文 through the same fold, with
  Taiwan words such as 氣喘 curated under their own spelling). The two are
  separate indexes so a complaint never lands on a diagnosis, and both abstain
  with a reason rather than guess. Measured on 200 Chinese and English
  complaints from real consultation texts, the gold labels set by three
  independent model annotators (three different models, blind to the
  resolver): no wrong code on 142 scorable symptoms, and a code for one in
  five; the rest abstain. The Japanese and Russian surfaces are newer and have
  not been measured that way yet.
- **Everything here is lexical, and abstaining is the ceiling we keep.** A term
  the vocabulary does not know returns `unresolved`, not a nearest neighbour.
  1.4.x shipped an opt-in cosine-recall tier beside this one; 1.5.0 deleted it.
  It could not abstain — for a term it had never seen it returned its nearest
  neighbour with the confidence of a correct answer, and measured on the LOINC
  matrix, nonsense scored 0.78 while genuine names went as low as 0.56, so no
  threshold separated them. It also never ran: the matrix was 108,248 rows ×
  1024 dims, specific to one (provider, model) pair, and was never published,
  so `get_index()` returned `None` in a wheel install and in a source tree
  alike. An opt-in nobody could opt into, in front of an answer we would not
  have trusted. If you want better recall, the honest lever is a curated row in
  `res/loinc/resolver_overrides.tsv`.
- **We measure the claim instead of asserting it.**
  [`test_engine_coverage.py`](../mirobody/tests/test_engine_coverage.py) scores the offline
  resolver against the panels an ordinary checkup includes, written the way a report
  prints them, in English, 简体中文, 繁體中文, 日本語, Russian, Estonian and Turkish, plus
  the wearable vocabulary the platform API teaches. **345/345 today; it scored 32/94 the day it
  was written.** It grades *clinical* correctness: answering `血红蛋白` with the
  HbA1c code is a failure, and `血脂` is required to resolve to nothing.

```bash
pytest mirobody/tests/test_engine_coverage.py -s   # offline, about a second
```

### Nothing embeds at runtime

`mirobody.bundle` exposes the vocabulary to build-time tools, so you can embed
the LOINC rows with your own model if you want a recall index of your own, but
neither `resolve()` nor the app reads one. A question about a person's own
readings goes through `query_health_indicators`, which ranks that person's
series (their printed names, and the LOINC display names and codes stored
beside each reading) lexically and falls back to the offline resolver's code.
No embedding provider is needed anywhere on that path.

### Which LOINC, and what it does and does not cover

The shipped bundle is cut from **LOINC 2.83**, and the package says so at
runtime rather than in a comment that can drift:

```python
>>> import mirobody; mirobody.BUNDLE_VERSION
'loinc-2.83+2026.10.27-126ecba69782'
```

The release, the cut date, and a digest over the bundle's own members — so a
build-time consumer of the vocabulary and a runtime `pip` pin can be asserted
to be the same corpus, which the package version alone never told you.
[LOINC's licence](https://loinc.org/license/) requires every copy to carry the
version number; `res/loinc/fhir_loinc_bundle.NOTICE` does, and
`scripts/stamp_bundle_version.py --check` keeps the stamp honest.

**What the cut contains.** 63,416 of the 99,737 ACTIVE codes in 2.83, chosen
by rule rather than by hand (`standardize_build/loinc_cut.py`): CLASSTYPE
laboratory or clinical; CLASS families that never hold a reading dropped
(surveys, documents, radiology, administrative); SCALE_TYP `Doc`, `Nar`, `-`,
`Set` and `Multi` dropped; physical-exam classes kept only at `Qn` or `Ord`;
panels kept for the laboratory subclasses plus the vital-sign and
personal-record ones. Rows are dropped, never edited, which is what the
[licence](https://loinc.org/license/) section 3 requires; the 153 rows carrying
someone else's copyright notice are dropped rather than reproduced.

1.4.x stayed on 2.82 for a reason that no longer exists: the axis table and the
corpus were built from different sources and joined through a folded
`LONG_COMMON_NAME`, so a release that renamed 2,842 of those names broke 3,486
links. The 1.5.0 bundle is cut from one release in one pass, so there is no
join to break. Two things follow that could not before — the axis table now
carries `TIME_ASPCT`, without which a spot urine protein and a 24-hour
collection share one series key, and it carries `CLASS`, which retires a
10,045-line gate file that had to be regenerated by hand.

**Measured, on 7,354 real report spellings.** On the 6,992 whose expected code
is inside the cut, coverage is 0.951 and the wrong-rate 0.025. The other 362
expect a code the cut drops (330: narrative, document or exam finding), one
2.83 retired (24), or no code at all (8); the resolver abstains on 273 of them
instead of answering, which is the point of the cut and not a regression. The
DISCOURAGED and DEPRECATED codes 1.4.x could still answer with are gone with
the `STATUS` gate.

**LOINC covers more of the wearable world than people expect.** It is not only
lab panels: `BDYWGT.*` codes body composition (`101685-6` body bone mass,
`73964-9` body muscle mass, `101684-9` percentage of body water), `HRTRATE.*`
distinguishes resting heart rate (`40443-4`) from a spot reading, and there are
codes for step counts (`41950-7`), sleep stages (`93831-6` deep, `93830-8`
light), HRV SDNN (`112429-6`), VO₂ peak and elevation climbed. Where it stops
is vendor composites — Garmin's Body Battery and stress score have no code,
correctly, because they are one company's formula rather than a measurement.

**Coverage of a vocabulary is not the same as recall on it**, and the gap is
ours, not LOINC's: `Body bone mass` resolves to `101685-6` here, while the
Chinese `骨量` resolves to nothing, because no alias routes it.
That is what [`res/loinc/resolver_overrides.tsv`](../mirobody/res/loinc/resolver_overrides.tsv)
is for — a row written by a person beats a surface match in the index, every
time.

→ [loinc.org](https://loinc.org/) · [licence](https://loinc.org/license/) ·
[release notes](https://loinc.org/kb/) · the download is free but requires an
account, which is why the derived bundle ships and the source release does not.

→ [Pipeline](pipeline.md) · [Walkthrough](walkthrough.md) ·
[Demo data](../demo/README.md)

---


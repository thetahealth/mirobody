# Standardization in depth

**English** · **[中文](standardization.zh-CN.md)**

The long form of the README's **② Translate (standardize)** stage: what the shipped
vocabulary is, what it deliberately does not do, and which LOINC release it is
cut from. Every exact figure here is the same
one the README quotes; the README is held to the artifacts those figures come
from, and this page follows it.

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/collect-translate-agent-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="images/collect-translate-agent.svg">
  <img src="images/collect-translate-agent.svg" alt="Collect, Translate, Agent: three stages, left to right" width="920">
</picture>
</p>

## What the layer provides

Standardization here is not a lookup table but a complete terminology-normalization system:

- **63,416 codes**, cut from LOINC 2.83 by one rule in one pass: laboratory
  and clinical observations, narrative and document scales dropped, panels
  kept for the laboratory and vital-sign subclasses. **692,577 folded
  designations** point at them, from the component, the long and short names,
  the related names and the 21 linguistic variants the release ships.
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
- **Units** normalized against 328 UCUM units over 59 PROPERTY families,
  with dimensional analysis, a molar-mass bridge keyed by LOINC code, and an
  explicit refusal for `%` vs `10*9/L`. 316 standard device indicators.
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
  prints them, in English, 简体中文, 繁體中文 and 日本語 — plus the wearable
  vocabulary the platform API teaches. **213/213 today; it scored 32/94 the day it
  was written.** It grades *clinical* correctness: answering `血红蛋白` with the
  HbA1c code is a failure, and `血脂` is required to resolve to nothing.

```bash
pytest mirobody/tests/test_engine_coverage.py -s   # offline, about a second
```

### Two semantic indexes, and which one you get for free

The matrix above is the **downloadable-corpus** tier — LOINC rows embedded once,
built by you against your own embedding model. Inside the app, a question about
a person's own readings does not use it: `query_health_indicators` ranks the
person's own series (their printed names, the LOINC display names and codes the
writer stored beside each reading) lexically, and falls back to the offline
resolver's code. No embedding provider is needed for that path, and neither
index changes what `resolve()` answers.

### Which LOINC, and what it does and does not cover

The shipped bundle is cut from **LOINC 2.83**, and the package says so at
runtime rather than in a comment that can drift:

```python
>>> import mirobody; mirobody.BUNDLE_VERSION
'loinc-2.83+2026.09.17-aacb2c715b56'
```

The release, the cut date, and a digest over the bundle's own members — so a
build-time consumer of the vocabulary and a runtime `pip` pin can be asserted
to be the same corpus, which the package version alone never told you.
[LOINC's licence](https://loinc.org/license/) requires every copy to carry the
version number; `res/loinc/fhir_loinc_bundle.NOTICE` does, and
`scripts/stamp_bundle_version.py --check` keeps the stamp honest.

**What the cut contains.** 63,416 of the 99,737 ACTIVE codes in 2.83, chosen
by rule rather than by hand (`translate_build/loinc_cut.py`): CLASSTYPE
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

**Measured, on 7,354 real report spellings.** Coverage is unchanged at 0.963
and wrong answers fall from 0.035 to 0.025 — on the 6,780 cases whose expected
code is inside the cut. The other 362 expect a narrative, document or
exam-finding code, or one 2.83 retired: the resolver now abstains on those
instead of answering, which is the point of the cut and not a regression. The
650 DISCOURAGED and DEPRECATED codes 1.4.x could still answer with are gone
with the `STATUS` gate.

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
Chinese `骨量` resolves to a dental volume code, because no alias routes it.
That is what [`res/loinc/resolver_overrides.tsv`](../mirobody/res/loinc/resolver_overrides.tsv)
is for — a row written by a person beats a surface match in the index, every
time.

→ [loinc.org](https://loinc.org/) · [licence](https://loinc.org/license/) ·
[release notes](https://loinc.org/kb/) · the download is free but requires an
account, which is why the derived bundle ships and the source release does not.

→ [Standardization](https://docs.mirobody.ai/en/api-reference/standardization/) ·
[Architecture](https://docs.mirobody.ai/en/concepts/architecture/) ·
[Data flow](https://docs.mirobody.ai/en/concepts/data-flow/)

---


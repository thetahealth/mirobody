# Standardization in depth

**English** · **[中文](standardization.zh-CN.md)**

The long form of the README's **② Translate (standardize)** stage: what the shipped
vocabulary is, what it deliberately does not do, which LOINC release it is cut
from and why, and the opt-in semantic tier. Every exact figure here is the same
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

- **Concept graph**: 440,961 nodes · 22,044,110 cross-vocabulary edges ·
  **595,746 source ids** distilled into canonical concepts (LOINC · SNOMED CT ·
  RxNorm bridges).
- **49,253 multilingual aliases** (中文 22,578 · 日本語 16,809 · +5:
  de·es·fr·ko·ru). `hemoglobin`, `血红蛋白`, `血紅素` and `ヘモグロビン` all land
  on LOINC 718-7.
- **繁體中文 is two problems, handled as two.** Script folding is mechanical
  (a shipped 3,336-character zh-Hant → zh-Hans table); vocabulary is not — Taiwan
  usage picks different words, and folding `血紅素` yields the HbA1c code. Those
  terms are curated under their Traditional spelling, and a curated row always
  beats a fold.
- **Units** normalized to 326 UCUM families, with dimensional analysis, a
  molar-mass bridge keyed by LOINC code, and an explicit refusal for `%` vs
  `10*9/L`. 305 standard pulse indicators.
- **A second tier exists, and stays opt-in.** Everything above is lexical, so it
  abstains on terms it does not know — an honest ceiling. Cosine recall
  ([`indicator/semantic.py`](../mirobody/indicator/semantic.py)) reaches past it but
  **cannot abstain**: for a term it has never seen it returns its nearest
  neighbour with the confidence of a correct answer, and no threshold separates
  the two. **No matrix ships and none is published to download**: it is 108,248
  LOINC rows × 1024 dims (~221 MB) and it is specific to one (provider, model)
  pair, so `scripts/build_loinc_embeddings.py` builds yours against the
  embedding model you configure. A matrix from a different model does not
  error — it ranks confidently in the wrong space, which is why the build
  stamps `<matrix>.meta.json` and loading refuses a mismatch. Until you point
  `MIROBODY_SEMANTIC_INDEX` at one, `resolve()` is unchanged; after, use it to
  *suggest* a code a human confirms, never to mint an identity.
  → [Semantic recall](https://docs.mirobody.ai/en/concepts/semantic-recall/) — the
  benchmark, the two axis gates, and why `min_score` is not a correctness threshold.
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

The shipped bundle is cut from **LOINC 2.82**, and the package says so at
runtime rather than in a comment that can drift:

```python
>>> import mirobody; mirobody.BUNDLE_VERSION
'loinc-2.82+2026.08.28-af2524b7a285'
```

The release, the cut date, and a digest over the bundle's own members — so a
build-time consumer of the vocabulary and a runtime `pip` pin can be asserted
to be the same corpus, which the package version alone never told you.
[LOINC's licence](https://loinc.org/license/) requires every copy to carry the
version number; `res/fhir_loinc_bundle.NOTICE` does, and
`scripts/stamp_bundle_version.py --check` keeps the stamp honest.

**Why 2.82 and not 2.83.** The axis table and the 677k-row corpus are coupled
through the folded `LONG_COMMON_NAME`, and 2.83 renamed 2,842 of them
(`Cerebral spinal fluid` → `Cerebrospinal Fluid` and that family). Measured:
upgrading the axis alone loses **3,486** name→code links and gains none, so a
real upgrade means rebuilding the corpus — which spans SNOMED CT, RxNorm, CVX
and DCM, each licensed separately and none redistributable here. The known
cost of staying: 650 codes that 2.83 has marked DISCOURAGED or DEPRECATED are
still answerable, which shows up as 52 of the 6,815 benchmark cases that
resolve. Withholding them was measured too and not taken — LOINC offers a
replacement for only 9 of the 658, so it would mostly turn a dated code into no
code, and a reading with no code cannot be grouped at all.

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
That is what [`res/resolver_overrides.tsv`](../mirobody/res/resolver_overrides.tsv)
is for — a row written by a person beats a surface match in the index, every
time.

→ [loinc.org](https://loinc.org/) · [licence](https://loinc.org/license/) ·
[release notes](https://loinc.org/kb/) · the download is free but requires an
account, which is why the derived bundle ships and the source release does not.

→ [Standardization](https://docs.mirobody.ai/en/api-reference/standardization/) ·
[Architecture](https://docs.mirobody.ai/en/concepts/architecture/) ·
[Data flow](https://docs.mirobody.ai/en/concepts/data-flow/)

---


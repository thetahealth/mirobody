# `mirobody/res/` — the shipped terminology data

Everything the resolvers read at runtime. No code, no schema: the DDL lives in
[`mirobody/schema/`](../schema/) because `mirobody serve` creates its own
tables, and that is code. Under what terms each file may be redistributed is
[`LICENSE-3RD-PARTY`](../../LICENSE-3RD-PARTY), which has one row per artifact
here and nothing else.

## Layout

One directory per vocabulary, because the question a reader arrives with is
"which file do I edit when X resolves wrong", and the answer is always inside
the vocabulary that got it wrong.

```
res/
├── loinc/        ① the LOINC bundle, and everything that steers it
├── icpc3/        ② the ICPC-3 tabular list, and our surfaces onto it
├── catalog/      ③ the indicator catalogue and its display labels
├── crosswalks/   ④ thirteen vendors' device fields -> LOINC
├── ucum/         ⑤ the UCUM specification's own table, verbatim
├── dose_forms.tsv
└── EXTERNAL.tsv
```

`dose_forms.tsv` and `EXTERNAL.tsv` sit at the top because they belong to no
vocabulary: the first is UCUM annotation spellings for the medication model,
the second is a manifest of data that is deliberately NOT in the checkout.

## What each file is, and who opens it

| File | What it is | Read by |
| --- | --- | --- |
| `loinc/fhir_loinc_bundle.tar.gz` | LOINC 2.83, cut to 63,416 codes. The resolver's whole vocabulary | `_bundle.BUNDLE_PATH` |
| `loinc/fhir_loinc_bundle.NOTICE` | LOINC's required notice and the cut rule | prose; LOINC licence §9 |
| `loinc/aliases_src/zh.tsv` | Chinese designations claimed from LOINC's linguistic variants | `_bundle.alias_source_files` |
| `loinc/aliases_src/zh_curated.tsv` | our corrections to the above, and they win | same |
| `loinc/resolver_overrides.tsv` | term → target for what the index gets wrong, ours | `engine`, first in precedence |
| `loinc/recall_synonyms.tsv` | query-side synonyms, ours | `kernel.query` |
| `icpc3/icpc3.tsv` | ICPC-3 S and D components, 1,218 codes, verbatim | `translate.icpc3` |
| `icpc3/icpc3.NOTICE` | WONCA's attribution, and what "verbatim" means here | prose |
| `ucum/ucum-essence.xml` | UCUM 2.2, byte for byte; an edited copy is refused | `units.essence`, and the gates on our unit tables |
| `ucum/ucum-essence.NOTICE`, `ucum/UCUM-LICENSE.md` | Regenstrief's notice and licence, which must travel with the file | prose; UCUM License §3 |
| `icpc3/symptoms_{zh,en}.tsv` | everyday words for a complaint → an S code, ours | `translate.icpc3` |
| `icpc3/conditions_{zh,en}.tsv` | everyday words for a diagnosis → a D code, ours | `translate.icpc3` |
| `catalog/metrics.tsv` | the indicator catalogue: unit, window, aggregation, code | `kernel.metrics` |
| `catalog/labels/zh.tsv` | Chinese display names for catalogue members | `kernel.metrics.register_labels` |
| `crosswalks/*.tsv` | a vendor's data types → LOINC, with confidence and source | `translate.devices` |
| `dose_forms.tsv` | UCUM dose-form annotations | `kernel.meds` |
| `EXTERNAL.tsv` | what is NOT here, and where it was | `scripts/fetch_data.sh` |

`crosswalks/` has its own [README](crosswalks/README.md) for the mapping
judgements behind it.

## Two rules this directory lives by

**A file here has a reader, and a licence row.** Both are enforced, because
both have rotted before: a NOTICE outlived the bundle it documented by a
release, and `LICENSE-3RD-PARTY` kept claiming four artifacts a cut had already
removed. The maintainers' suite fails on either, so adding a file here means
adding its licence row and its reader in the same commit.

**The `.gitattributes` patterns are `**`, not `*`.** The bundle is a Git LFS
object, and a pattern that stops matching its path checks out the 130-byte
pointer text in its place. That presents as "the bundle is corrupt", never as
"the path is wrong", so it is worth knowing that the two are the same bug.

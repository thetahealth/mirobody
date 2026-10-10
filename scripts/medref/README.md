# scripts/medref — build the offline medical-reference index

Build side of the agent's `search_medical_reference` tool
(`mirobody/agent/medref.py`). The runtime is read-only over the built file;
everything network happens here, ahead of time.

```
scripts/medref/raw/                     raw downloads (gitignored)
├── mplus_topics_YYYY-MM-DD.xml         MedlinePlus health-topic dump
└── openfda/<GENERIC>.json + _manifest.json  one label per curated generic
scripts/medref/
├── fetch_openfda.py                    label download (the only network step)
├── zh_aliases.tsv                      zh↔en synonym table, ours (Apache-2.0)
├── build_index.py                      -> mirobody/res/medref/index.sqlite3
└── test_build_index.py                 fixture-corpus test (stdlib unittest)
```

The index is **build output and stays out of git** (`.gitignore` names it):
a deployment rebuilds on site rather than cloning 25 MB, and without it the
tool answers "unavailable" explicitly and its evidence tests skip. The TERMS
under which the corpora redistribute travel in the tracked
`mirobody/res/medref/NOTICE` — keep it equal to what a rebuild ingests.

## Rebuild

From the repository root:

```bash
mkdir -p scripts/medref/raw && cd scripts/medref/raw
curl -O https://medlineplus.gov/xml/mplus_topics_compressed_<date>.zip
unzip mplus_topics_compressed_<date>.zip          # yields mplus_topics_<date>.xml
cd ../..
python3 scripts/medref/fetch_openfda.py           # skips files already fetched; deletes are refilled
python3 scripts/medref/build_index.py             # writes mirobody/res/medref/index.sqlite3; ~4 s
python3 -m pytest scripts/medref/test_build_index.py -q
```

`build_index.py --medlineplus/--openfda/--aliases/--out` override every
default; the defaults are `raw/` here and the runtime's read path above.

## Corpora and licenses (verified 2026-10-08)

| Corpus | What is bundled | License | Evidence |
| --- | --- | --- | --- |
| MedlinePlus health-topic XML dump (en 1017, es 1016 topics) | `full-summary`, title, also-called only | Public domain (US Gov) **with attribution**: "Source: MedlinePlus, National Library of Medicine." | medlineplus.gov/copyright.html |
| openFDA `drug/label.json`, 177 chronic-disease generics | selected sections (indications, dosage, warnings, …) | CC0 1.0 Universal | open.fda.gov/license |

Two boundaries the builder keeps and the shipped NOTICE repeats:

- MedlinePlus' copyright page names what is NOT public domain: the A.D.A.M.
  Medical Encyclopedia articles and the ASHP drug monographs. Neither is in
  the XML dump (summaries only); do not scrape them in.
- MedlinePlus has no Chinese full text (en + es only; Chinese exists only as
  outbound links). Chinese retrieval goes through `zh_aliases.tsv`: passages
  are tagged at build time, queries are segmented/expanded with the same
  table at runtime, so the two sides cannot drift apart.

Skipped: WHO/CDC guidance pages — offline redistribution rights are not
uniformly clear across their pages (mixed partner content), so they stay out
until a corpus with a clean license is identified.

## Schema of the built file

```
meta(key, value)                 corpus version ("medref+<date>-<digest>"), counts
aliases(zh PRIMARY KEY, en)      zh_aliases.tsv verbatim; runtime query expansion reads it
passages_fts: FTS5(rid, source, lang, url UNINDEXED;  title, section, text, zh;
                   tokenize='unicode61 remove_diacritics 2')
rid     "<doc>:<section>:<n>"    tool surfaces it as ref:<source>:<rid>
source  medlineplus | openfda
lang    en | es
```

unicode61 has no CJK segmentation and no stemming — the `zh` alias column and
the runtime's 4+-letter prefix terms are the answers, both deliberate. The
read-side column layout is asserted by the repo's
`mirobody/tests/test_medref_reference.py`; the build side by
`test_build_index.py` here.

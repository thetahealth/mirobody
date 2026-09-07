# Documentation

The rule this directory exists to enforce:

> **A package carries a short `README.md` saying what it is. Long-form guides
> live here.**

`mirobody` is published to PyPI, so anything inside the package tree lands in
someone's `site-packages`. A 2,000-line guide to writing a data provider is
contributor documentation — valuable on GitHub, dead weight in an install.

This repo was written by several people over two years, and it showed: guides
were named `FILE_PROCESSING_GUIDE.md`, `INTEGRATION_GUIDE.md`, `DESIGN.md`,
`TEST_README.md` and `CLAUDE.md`, scattered at whatever depth their author
happened to be working. One of them (`pulse/CLAUDE.md`) was shipping AI working
notes to PyPI under a filename that told open-source readers to ignore it.

## Guides

| | Guide | For |
| --- | --- | --- |
| | [pipeline.md](pipeline.md) | the eleven stages a reading passes through, the invariant each holds, and what is deliberately NOT done |
| ① | [provider-setup.md](provider-setup.md) | turning ON Garmin / Oura / Whoop — credentials, callback URLs, boot-log truth |
| ① | [provider-guide.md](provider-guide.md) | writing a data provider end to end — the long one |
| ① | [file-processing.md](file-processing.md) | a file becomes text by kind (`mirobody/documents/`: PDF text layer, OCR for scanned pages only, Office, text), then LLM extraction |
| ① | [apple-health.md](apple-health.md) | Apple Health export + CDA import |
| ② | [vocabulary-build.md](vocabulary-build.md) | rebuilding `mirobody/res/` from the raw LOINC / SNOMED CT / RxNorm releases — needs a UMLS licence |
| ③ | [answers.md](answers.md) | the one health-data tool: its matrix, its envelope, its governance, and the PHI discipline |
| ③ | [medications.md](medications.md) | the medication model, its state tables and its instruction grammar (provisional) |
| ③ | [frontend.md](frontend.md) | how the bundled web client is served, and how to replace it |
| | [backup-restore.md](backup-restore.md) | what to copy, how to get it back, and what changes on upgrade |
| | [testing.md](testing.md) | test layout, markers, snapshots, release gates |
| | [aggregation-tests.md](aggregation-tests.md) | the daily-rollup test suite in detail |

Start at the README's **Repository layout** section ([README.md](../README.md)) if you want the map rather than a
specific subsystem, and [roadmap.md](roadmap.md) for known gaps and deferred
work — each entry states the measurement that motivated it.

## Package READMEs

Short, and about *that package only*:

- [`mirobody/pulse/`](../mirobody/pulse/README.md) — ① Collect
- [`mirobody/indicator/`](../mirobody/indicator/README.md) — ② Standardize
- [`mirobody/agent/`](../mirobody/agent/README.md) — ③ Answers
- [`mirobody/agent/tools/`](../mirobody/agent/tools/README.md) — the MCP tool surface
- [`mirobody/pulse/apple/`](../mirobody/pulse/apple/README.md) — Apple Health import
- [`mirobody/pulse/aggregate/`](../mirobody/pulse/aggregate/README.md) — daily rollups
- [`mirobody/pulse/standardize/`](../mirobody/pulse/standardize/README.md) — health indicators, units & standardization
- [`mirobody/schema/`](../mirobody/schema/README.md) — database schema, contract and bootstrap
- [`mirobody/pulse/providers/`](../mirobody/pulse/providers/README.md) — provider directory layout
- [`mirobody/utils/config/`](../mirobody/utils/config/README.md) — configuration
- [`mirobody/units/`](../mirobody/units/README.md) — UCUM units, families, conversions
- [`mirobody/kernel/`](../mirobody/kernel/__init__.py) — the kernel; the module docstring is its README (the stage → module map)
- [`mirobody/documents/`](../mirobody/documents/__init__.py) — documents → text; likewise

## Adding documentation

Ask which one you are writing:

- **"What is this package?"** → the package's own `README.md`. Keep it short
  enough that someone reads all of it.
- **"How do I do X?"** → a new file here, named for the task in
  `lower-case-with-hyphens.md`, linked from the table above.

Not `GUIDE`, not `DESIGN`, not `NOTES`, and never `CLAUDE.md` — that name is
reserved for the gitignored working file at the repo root and must never be
committed.

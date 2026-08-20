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
| ① | [provider-setup.md](provider-setup.md) | turning ON Garmin / Oura / Whoop — credentials, callback URLs, boot-log truth |
| ① | [provider-guide.md](provider-guide.md) | writing a data provider end to end — the long one |
| ① | [file-processing.md](file-processing.md) | the file-parsing pipeline (8 formats, LLM extraction) |
| ① | [apple-health.md](apple-health.md) | Apple Health export + CDA import |
| ③ | [frontend-shipping.md](frontend-shipping.md) | how the built web client ships and gets served |
| | [testing.md](testing.md) | test layout, markers, snapshots, release gates |
| | [aggregation-tests.md](aggregation-tests.md) | the daily-rollup test suite in detail |

Start at [the Architecture section of the README](../README.md#-architecture) if you want the map rather than a
specific subsystem, and [roadmap.md](roadmap.md) for known gaps and deferred
work — each entry states the measurement that motivated it.

## Package READMEs

Short, and about *that package only*:

- [`mirobody/pulse/`](../mirobody/pulse/README.md) — ① Collect
- [`mirobody/indicator/`](../mirobody/indicator/README.md) — ② Standardize
- [`mirobody/agent/`](../mirobody/agent/README.md) — ③ Answers
- [`mirobody/agent/tools/`](../mirobody/agent/tools/README.md) — the MCP tool surface
- [`mirobody/agent/resources/`](../mirobody/agent/resources/README.md) — ChatGPT Apps widgets
- [`mirobody/pulse/standardize/`](../mirobody/pulse/standardize/README.md) — health indicators, units & standardization
- [`mirobody/schema/`](../mirobody/schema/README.md) — database schema, contract and bootstrap
- [`mirobody/pulse/providers/`](../mirobody/pulse/providers/README.md) — provider directory layout
- [`mirobody/utils/config/`](../mirobody/utils/config/README.md) — configuration

## Adding documentation

Ask which one you are writing:

- **"What is this package?"** → the package's own `README.md`. Keep it short
  enough that someone reads all of it.
- **"How do I do X?"** → a new file here, named for the task in
  `lower-case-with-hyphens.md`, linked from the table above.

Not `GUIDE`, not `DESIGN`, not `NOTES`, and never `CLAUDE.md` — that name is
reserved for the gitignored working file at the repo root and must never be
committed.

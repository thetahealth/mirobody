# Documentation

These guides are also published, in English and Chinese, at
[docs.mirobody.ai](https://docs.mirobody.ai/en/self-host). The site renders the
files here, so this directory is where they are edited.

## Start here

| You want to | Read |
| --- | --- |
| Run the whole product and see a first answer | [quickstart.md](quickstart.md) ([中文](quickstart.zh-CN.md)) · [walkthrough.md](walkthrough.md) ([中文](walkthrough.zh-CN.md)) |
| Keep every model on your own machine | [local-models.md](local-models.md) ([中文](local-models.zh-CN.md)) |
| Choose a model by privacy, quality and cost | [model-choice.md](model-choice.md) ([中文](model-choice.zh-CN.md)) · [local-models-roadmap.md](local-models-roadmap.md) ([中文](local-models-roadmap.zh-CN.md)) |
| Back up and restore a deployment | [backup-restore.md](backup-restore.md) |

## How it works

| Stage | Guide | For |
| --- | --- | --- |
| | [pipeline.md](pipeline.md) | the stages a reading passes through, the invariant each holds, and what is deliberately not done |
| | [repository-layout.md](repository-layout.md) ([中文](repository-layout.zh-CN.md)) | the directory map, and the two forms the code ships in: library and application |
| ① | [provider-setup.md](provider-setup.md) ([中文](provider-setup.zh-CN.md)) | turning on Garmin, Oura and Whoop: credentials, callback URLs, and how to confirm a provider runs |
| ① | [apple-health.md](apple-health.md) | Apple Health export and CDA import |
| ① | [file-processing.md](file-processing.md) | how a file becomes text by kind, then readings |
| ② | [standardization.md](standardization.md) ([中文](standardization.zh-CN.md)) | ② Translate in depth: alias tiers, the LOINC release, what the bundled cut covers |
| ② | [device-crosswalk.md](device-crosswalk.md) | wearable vendors' fields to LOINC, with a confidence and a source per row |
| ③ | [answers.md](answers.md) | the health-data tools: the matrix, the envelope, the governance, the PHI discipline |
| ③ | [medications.md](medications.md) | the medication model, its state tables and its instruction grammar |
| ①②③ | [genetics.md](genetics.md) ([中文](genetics.zh-CN.md)) | genotype uploads, bounded queries, VCF and FHIR export, CPIC coverage |
| ③ | [frontend.md](frontend.md) | how the bundled web client is served, and how to replace it |

## Contributing

[provider-guide.md](provider-guide.md) writes a device provider end to end;
[testing.md](testing.md) covers test layout and markers;
[`benchmarks/`](../benchmarks/README.md) explains how a resolver change is
scored; [roadmap.md](roadmap.md) lists known gaps, each with the measurement
behind it.

Each package also carries a short `README.md` about that package only, for
example [`mirobody/collect/`](../mirobody/collect/README.md),
[`mirobody/translate/`](../mirobody/translate/README.md),
[`mirobody/agent/`](../mirobody/agent/README.md) and
[`mirobody/agent/tools/`](../mirobody/agent/tools/README.md).

## Editing these files

- **"What is this package?"** goes in the package's own `README.md`, short
  enough to read in full. **"How do I do X?"** goes here, in a file named for
  the task (`lower-case-with-hyphens.md`), linked from the tables above.
- A guide with a Chinese edition is named `<guide>.zh-CN.md`. Edit the pair
  together, as with the two READMEs; a heading that other pages link keeps its
  English id in the Chinese edition (`<a id="…"></a>` above the heading).
- Never commit a `CLAUDE.md`: that name is reserved for a gitignored working
  file at the repository root.

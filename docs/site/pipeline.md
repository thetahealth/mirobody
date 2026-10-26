# Data pipeline

Mirobody collects health records, standardizes names and units, and makes the
results available to the web client, agent and MCP clients.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../images/collect-standardize-agent-dark.svg">
  <img src="../images/collect-standardize-agent.svg" alt="Collect, standardize and query health records" width="920">
</picture>

## From source to answer

| Stage | Result |
| --- | --- |
| ① Collect | Accept files, device data and journal entries; retain their source. |
| ② Standardize | Resolve recognized indicators to standard codes and convert compatible units. |
| ③ Agent | Query the selected record, compare results and cite the supporting data. |

A file can finish uploading before its readings finish processing. Check the
extraction result before assuming that its measurements are available.
Device records may be combined into daily values; queries state which view
and date window they use.

## Accuracy and corrections

- An unrecognized name remains unresolved; the engine does not invent a code.
- Incompatible units stay separate rather than being forced into one series.
- Query results retain source information so you can inspect the original file.
- Corrected or removed records affect later queries and citation lookups.
- Missing data means no recorded result, not evidence that a condition is absent.

## Data processing

Terminology and unit conversion run offline. Document recognition and chat
use the models selected by the deployment; cloud models receive the inputs
needed for their tasks. See [model selection](../model-choice.md) for local,
cloud and hybrid data flows.

For integration details, see the [full pipeline reference](../pipeline.md).

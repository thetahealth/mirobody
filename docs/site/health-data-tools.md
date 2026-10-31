# Health-data tools

The agent and authenticated MCP clients query the selected person's record.
Account identity is supplied by the session, token or personal URL; it is not
a model-provided tool argument.

## Choose a tool

| Tool | Data |
| --- | --- |
| `query_health_indicators` | Measurements, reported symptoms and diagnoses. |
| `query_medications` | Medication plans, recorded doses and course history. |
| `query_genetic_data` | Genotype calls by site, gene or region. |
| `query_pharmacogenomics` | CPIC drug–gene coverage. |
| `resolve_indicator` | Offline indicator-name lookup. |
| `normalize_unit`, `convert_unit` | Offline unit normalization and conversion. |

MCP hides record tools when the account has none of the corresponding data.
Medical-reference tools are optional and query general sources rather than the
person's record. See [MCP setup](https://docs.mirobody.ai/en/tools/mcp-integration).

## Read measurements

First discover names, then query the required view:

```python
query_health_indicators(keywords=["cholesterol"])
query_health_indicators(indicators=["<name from the result>"], view="latest")
```

| View | Result |
| --- | --- |
| `raw` | Individual readings. |
| `minute`, `hour`, `day`, `week`, `month` | Time buckets. |
| `stats` | Count, minimum, maximum, average, sum, first, last and change. |
| `latest` | Most recent value per indicator. |

Without `keywords` or `indicators`, the result is a catalogue, even if a view
was requested. `start` and `end` are inclusive dates in the record owner's
timezone, so the last 7 days start six days before today; omitted
measurement dates mean the whole recorded history.

## Interpret results

Results identify status (`ok`, `partial` or `error`), date window, view,
provenance and assumptions. A truncated result is not the full history:
narrow the query or use a suitable aggregate view. Daily and coarser views
count days, so their averages can differ from statistics over individual readings.

Missing data means no recorded evidence, not that a condition is absent.
Keep returned row IDs when citing or calculating from results. Use the
[local calculator](../python-analysis.md) for additional arithmetic.
Full schemas and result conventions are in the [tool reference](../answers.md).

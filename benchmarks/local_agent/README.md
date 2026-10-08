# Local-model Agent probe

`compare_mimo_bonsai.py` puts six synthetic health questions to any
OpenAI-compatible chat endpoint, such as a local `llama-server`, using the
shipped system prompt and the real `query_health_indicators` schema
(`mirobody.kernel.query.TOOL_SCHEMA`). The tool behind it is a mock that
answers in the shapes the real tool returns: `view="day"` gives
`period`/`avg` rows, `view="stats"` a summary and `view="latest"` the last
value. `query.validate_request` refuses an unknown parameter or view, as it
does in the product. The saved `results/` predate `view` and record the
`resolution`/`aggregate` schema they ran against. An unmatched indicator returns the catalogue, as the real
tool's fallback does, and an identical repeated call is refused. All readings
and the optional report image are synthetic.

| Case | The question (asked in Chinese) |
| --- | --- |
| `mixed_chart` | fasting glucose (mmol/L) and HbA1c (%) for January–March 2026 in one chart, with a summary |
| `same_chart` | the last three LDL and total cholesterol results in one trend chart |
| `single_point` | the latest HbA1c, result only |
| `table_only` | last year's three LDL results as a table, no chart |
| `missing` | this year's HbA1c, when none is recorded |
| `medication_caution` | recent blood pressure and LDL, and whether to stop the blood-pressure drug |

The script records each tool call, its arguments, the reply, the finish reason
and per-call latency; it does not score them. Read a chart answer against the
prompt's rules: every requested series and point present and none invented,
and unlike units never sharing one Y axis.

Start a model server, then run from a source clone with `[app,test]`
installed:

```bash
python benchmarks/local_agent/compare_mimo_bonsai.py \
  --base http://127.0.0.1:8088/v1 --model YOUR_SERVER_ALIAS \
  --case mixed_chart --max-rounds 12 --output /tmp/local-agent-mixed.json
```

Omit `--case` to run all six. `--prompt` replays another prompt file,
`--vision` adds the synthetic report image from `demo/upload/`, and
`--simple-tool` swaps the full schema for a keywords-only one. The script
downloads no weights, starts no server and reads no personal data. Some GGUF
chat templates emit invalid tool-call JSON with the full schema; the MiMo runs
below used a corrected template passed with `--chat-template-file`
(SHA-256 `8a7f5ffceabe3f674a53f71ab85d3c6941ed4003e2aeddee633be5a23dfdca7a`).

`prompts/before.jinja` is the system prompt before the focused-trend change
(SHA-256 `680c953e6022dccfbeadc02b00ec57382b086ea3e1c6c8b69f126da318744885`) and
`prompts/v3.jinja` an intermediate version. `results/` holds recorded runs,
named by model, quantization, prompt version and case set, taken on
2026-09-28 on a 48 GB Apple-silicon machine with `llama-server --reasoning on --jinja
-c 16384 -ngl 99` at temperature 0. The models' reasoning text was removed
from them. They are single runs on synthetic tasks, not success rates.

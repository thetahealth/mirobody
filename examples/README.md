# Examples

Runnable, in order of how much they need. Every one of these was executed
before being committed; if one fails on your machine that is a bug, not a
missing step.

| | Example | Needs | Shows |
|---|---|---|---|
| 01 | [`01_resolve_offline.py`](01_resolve_offline.py) | `pip install mirobody` | ② Translate: name → LOINC, any language, fully offline |
| 02 | [`02_standardize_a_reading.py`](02_standardize_a_reading.py) | `pip install mirobody` | ① Collect: unit conversion + the indicator catalogue |
| 03 | [`03_parse_a_lab_report.py`](03_parse_a_lab_report.py) | + one model key | a document → standardized readings in one call |
| 04 | [`04_mcp_tool_surface.py`](04_mcp_tool_surface.py) | `pip install 'mirobody[parse]'` | ③ Answer: exactly what an external MCP client receives |
| 05 | [`05_agent_server_preflight.py`](05_agent_server_preflight.py) | `pip install 'mirobody[app]'` | whether this machine can run the full server, and what is missing |
| 06 | [`06_care_circle_rules.py`](06_care_circle_rules.py) | `pip install mirobody` | who may read whose record — the rule behind the README's demo |
| 07 | [`07_claude_agent_sdk.py`](07_claude_agent_sdk.py) | a running mirobody + `pip install claude-agent-sdk` + a model key | another agent runtime answering from this data over `/mcp` — no mirobody import at all |

```bash
pip install mirobody
python examples/01_resolve_offline.py
python examples/02_standardize_a_reading.py
python examples/03_parse_a_lab_report.py          # no key needed without a file
python examples/06_care_circle_rules.py
pip install 'mirobody[parse]' && python examples/04_mcp_tool_surface.py

# 07 needs a running stack (./deploy.sh) and a personal MCP URL (POST /personal/mcp):
pip install claude-agent-sdk
MIROBODY_MCP_URL=http://127.0.0.1:18060/mcp/<secret> ANTHROPIC_API_KEY=sk-ant-... \
  python examples/07_claude_agent_sdk.py
```

## The split these are arranged around

01, 02 and 06 need **nothing but the package** — no database, no network, no
API key. That is the point of the engine layer: standardizing health data is
normally the step that forces you to send it somewhere, and here it is not.
06 is offline for a different reason — the authorization decision is a value
object and two properties, so the rule can be shown without a database holding
anybody's data.

04 is also offline and keyless, but it imports the MCP tool directory
(`mirobody.mcp.tool`), and that module reaches `mirobody.utils`, which is the
`[parse]` extra — on a bare install it stops at `ModuleNotFoundError: ruamel`.
It does not need the `mcp` package itself; `[parse]` is enough. This table used
to claim the bare install was.

03 needs one model key, and only for the *extraction* half — reading the page.
The standardization that follows is deterministic and runs offline, which is
why the script still does something useful with no key at all.

05 is the boundary. The chat server, the HTTP MCP endpoint and the agent need
PostgreSQL, Redis and secrets. It reports every prerequisite at once instead of
letting you find them one traceback at a time.

07 is the other side of that boundary: it does not import mirobody. It hands
Anthropic's Claude Agent SDK — Claude Code's harness as a library — a mirobody
MCP URL and a question, and prints what happens. mirobody ships one agent of
its own (on deepagents); this is the proof that the tools reach every other
runtime the same way, described well enough to be used unaided. Two recorded
runs against the demo record, through OpenRouter's Anthropic-compatible
endpoint (`ANTHROPIC_BASE_URL=https://openrouter.ai/api`,
`ANTHROPIC_AUTH_TOKEN=$OPENROUTER_API_KEY`), 2026-09-05:

```
question: How has my HbA1c moved over time? Cite the dates and values you used.

→ tool  mcp__mirobody__resolve_indicator  {'names': ['HbA1c']}
← result 174 chars: {"success":true,"message":"1/1 resolved","results":[{"name":"HbA1c","resolved":true,"loinc":"4548-4"
→ tool  mcp__mirobody__query_health_indicators  {'keywords': ['HbA1c', 'hemoglobin A1c', 'glycated hemoglobin'], 'aggregate': 'stats'}
← result 349 chars: {"result":"(constants: indicator=GlycatedHemoglobin-HbA1c, unit=%)\ntime|value\n2024-11-12 09:15:00|

Your HbA1c has only been recorded twice, both within the normal range:

| Date | HbA1c |
|---|---|
| 2024-11-12 | 5.3% |
| 2025-05-06 | 5.2% |

Over that ~6-month span it moved slightly downward, from 5.3% to 5.2% … There's no
data recorded before Nov 2024 or after May 2025, so I can't say anything about trends
outside that window — absence of records doesn't imply anything about your status.

[success] turns=3 cost=$0.0337
```

Asked "How has my LDL moved?" against the same record, the model resolved the
name, queried, fell back to the catalogue, and answered that LDL has never been
recorded — "this doesn't mean your LDL is normal or abnormal, just that no lab
result for it is in the record". Four turns. That is the tool description doing
its job on a model that had never seen this project.

## Adding one

Keep them runnable and keep them honest: print real output, do not stub, and if
a step needs something the reader may not have, say so and degrade to the part
that does not. An example that cannot run is worse than no example — it is a
claim nobody checked.

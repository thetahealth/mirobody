# Examples

Runnable, in order of how much they need. Every one of these was executed
before being committed; if one fails on your machine that is a bug, not a
missing step.

| | Example | Needs | Shows |
|---|---|---|---|
| 01 | [`01_resolve_offline.py`](01_resolve_offline.py) | `pip install mirobody` | ② Standardize: name → LOINC, any language, fully offline |
| 02 | [`02_standardize_a_reading.py`](02_standardize_a_reading.py) | `pip install mirobody` | ① Collect: unit conversion + the indicator catalogue |
| 03 | [`03_parse_a_lab_report.py`](03_parse_a_lab_report.py) | + one model key | a document → standardized readings in one call |
| 04 | [`04_mcp_tool_surface.py`](04_mcp_tool_surface.py) | `pip install mirobody` | ③ Answers: exactly what an external MCP client receives |
| 05 | [`05_agent_server_preflight.py`](05_agent_server_preflight.py) | `pip install 'mirobody[app]'` | whether this machine can run the full server, and what is missing |
| 06 | [`06_care_circle_rules.py`](06_care_circle_rules.py) | `pip install mirobody` | who may read whose record — the rule behind the README's demo |

```bash
pip install mirobody
python examples/01_resolve_offline.py
python examples/02_standardize_a_reading.py
python examples/03_parse_a_lab_report.py          # no key needed without a file
python examples/04_mcp_tool_surface.py
python examples/06_care_circle_rules.py
```

## The split these are arranged around

01, 02, 04 and 06 need **nothing but the package** — no database, no network, no
API key. That is the point of the engine layer: standardizing health data is
normally the step that forces you to send it somewhere, and here it is not.
06 is offline for a different reason — the authorization decision is a value
object and two properties, so the rule can be shown without a database holding
anybody's data.

03 needs one model key, and only for the *extraction* half — reading the page.
The standardization that follows is deterministic and runs offline, which is
why the script still does something useful with no key at all.

05 is the boundary. The chat server, the HTTP MCP endpoint and the agents need
PostgreSQL, Redis and secrets. It reports every prerequisite at once instead of
letting you find them one traceback at a time.

## Adding one

Keep them runnable and keep them honest: print real output, do not stub, and if
a step needs something the reader may not have, say so and degrade to the part
that does not. An example that cannot run is worse than no example — it is a
claim nobody checked.

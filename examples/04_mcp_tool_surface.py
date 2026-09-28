"""What an external MCP client sees — and calling a tool without a server.

    pip install 'mirobody[agent]'
    python examples/04_mcp_tool_surface.py

Mirobody's tools are ordinary Python functions. The MCP layer turns each into a
tool definition — name, description, JSON Schema — using the official SDK's
`func_metadata`, so what a model receives is generated from the signature rather
than hand-maintained beside it.

This script loads that surface in-process and prints it. No server, no OAuth, no
database: this is the *definition* half. Three of the six tools
(`resolve_indicator`, `normalize_unit`, `convert_unit`) need no user data and run
offline; this script calls `resolve_indicator`.

To expose these over HTTP instead, run `mirobody serve` and point a client at
`/mcp` — same functions, same schemas.
"""

import asyncio
import importlib.util
import json
import sys

# `mcp` is the [agent] extra, not the base install — and without this the first
# import below dies several modules deep with a bare `ModuleNotFoundError: mcp`,
# while 05, 06 and 07 all print what to install. Same shape as `cli.py`'s
# `_require_extra`. (2026-09-14 regression report, F-4)
if importlib.util.find_spec("mcp") is None:
    sys.exit("this example needs the MCP SDK — pip install 'mirobody[agent]'")

from mirobody.mcp.tool import call_tool, get_global_descriptions, get_global_tools, load_tools_from_directories

# The same directory scan the server performs at startup. Point it anywhere to
# add your own tools — MCP_TOOL_DIRS in config does exactly this.
#
# The argument works as either a filesystem path (from a source checkout) or a
# module path (from an installed package): the loader tries the directory first
# and falls back to importlib.find_spec. That is why this script runs the same
# from the repo root and from an arbitrary directory after `pip install`.
load_tools_from_directories(["mirobody/agent/tools"])

print(f"{len(get_global_descriptions())} tools discovered\n")

for d in sorted(get_global_descriptions(), key=lambda x: x["name"]):
    schema = d["inputSchema"]
    params = schema.get("properties", {})
    required = set(schema.get("required", []))
    first_line = d["description"].strip().splitlines()[0]

    print(f"  {d['name']}")
    print(f"      {first_line}")
    print(f"      description: {len(d['description'])} chars  ({len(params)} parameters)")
    for pname, pschema in list(params.items())[:6]:
        mark = "*" if pname in required else " "
        kind = pschema.get("type") or ("enum" if "enum" in pschema else "any")
        print(f"        {mark} {pname:<14} {kind}")
    print()

print("(* = required.) Descriptions are long on purpose: an external client's model")
print("gets no other instruction. A thin description is the single most common reason")
print("a tool works in our own agent and fails in someone else's.\n")


# ── calling one, in-process, with no server ──────────────────────────────────
async def main():
    print("Calling `resolve_indicator` directly — offline, no auth:\n")
    result = await call_tool(get_global_tools(), "resolve_indicator",
                             {"names": ["hemoglobin", "血糖", "blood pressure"]})
    print(json.dumps(result, indent=2, ensure_ascii=False)[:700])

    print("\nAnd the same call with a misspelled argument:\n")
    bad = await call_tool(get_global_tools(), "resolve_indicator", {"name": ["hemoglobin"]})
    print(f"  {bad}")
    print("\n  Rejected rather than silently defaulted — a model can act on that error.")

asyncio.run(main())

print("\nThe other three tools (query_health_indicators, query_medications, query_genetic_data) read a user's")
print("stored records, so they need `mirobody serve` and a database.")

"""
MCP Tools Package - Mirobody Open Source Tools

This package provides MCP (Model Context Protocol) tools for AI agents.
Tools are automatically discovered and loaded by the MCP server via
`load_tools_from_directory()` in mirobody/mcp/tool.py.

=============================================================================
TOOL LOADING RULES (from load_tools_from_directory)
=============================================================================

The loader scans this directory with the following rules:

1. DIRECTORY SCANNING
   - Only scans the ROOT directory (os.scandir)
   - Subdirectories are SKIPPED (entry.is_dir() → skip)
   - Subdirectories are safe for assets/helpers

2. FILE FILTERING
   - Only .py files are loaded (entry.name.lower().endswith(".py"))
   - Files starting with underscore are SKIPPED (entry.name.startswith("_"))
   - Use underscore prefix for internal helpers (e.g., _my_helper.py)

3. CLASS FILTERING (load_tools_from_module)
   - Only classes ending with "Service" are registered as tool providers
   - Example: ChartService ✓, MemoryService ✓, ChartHelper ✗
   - Abstract and builtin classes are skipped

4. METHOD FILTERING (load_tools_from_class)
   - Methods starting with underscore are SKIPPED (private methods)
   - Methods from base classes are SKIPPED (only current class methods)
   - Methods imported from other modules are SKIPPED

5. USER_INFO INJECTION
   - If method has `user_info` parameter, it's auto-injected by MCP server
   - Contains: {"user_id": str, "session_id": str, "success": bool}

6. DECLARED SCHEMAS
   - A Service class (or a method) may set `input_schema` to a JSON Schema
     dict; it is published VERBATIM as the tool's `inputSchema` and reaches
     the chat model unchanged, instead of being generated from the signature.
   - That is for a tool whose parameters are a CONTRACT shared with other
     surfaces — `query_health_indicators` publishes `kernel.query.TOOL_SCHEMA`,
     `query_medications` publishes `kernel.meds.TOOL_SCHEMA` and
     `query_genetic_data` publishes `genetic_service.TOOL_SCHEMA`, so the MCP
     tool and the chat tool cannot drift apart. Such a method takes
     `**kwargs`, and the schema's property names become its accepted
     arguments.

=============================================================================
DIRECTORY STRUCTURE
=============================================================================

tools/
├── __init__.py                      # This documentation
├── terminology_service.py           # ② Standardize over MCP: resolve_indicator, normalize_unit
│                                    #   (offline, no user data — works anonymously)
├── health_indicators_service.py     # query_health_indicators — readings: catalogue,
│                                    #   raw rows, buckets, stats, latest (8 parameters)
├── medications_service.py           # query_medications — plan / log / history (5 parameters)
├── genetic_service.py               # query_genetic_data — genotype calls at named
│                                    #   rsIDs, plus the typed neighbours of each
│                                    #   hit (5 parameters)
└── _authz.py                        # who a read is about (shared, not a tool)

Every file here IS a tool. The implementations they read through are not:
`query.HealthQuery` over `th_series_data` is `mirobody/pulse/query.py`, beside
the writer of that table, and the medication stores are `mirobody/pulse/meds/`.

ONE TOOL PER DATA CLASS, EVERY PARAMETER APPLICABLE TO EVERY CALL. Readings,
medications and genetics each have their own grammar, so each has its own
tool; a `kind` switch inside one tool would make most parameters invalid most
of the time. Within a class, search and read are ONE call: a search tool
paired with a read tool cost a model call to discover names before any numbers
could be asked for, and the two halves could disagree about what a window
meant.

Note: filesystem tools (ls, read_file, write_file, edit_file, glob, grep) are
NOT MCP tools here — the agent gets them natively from the deepagents
FilesystemMiddleware, backed by the PgFilesystemBackend in
mirobody/agent/filesystem/backend.py. There is no write_todos and no task/
subagent tool: deepagents 0.7 dropped TodoListMiddleware from its default stack
and the agent does not add it back, and the general-purpose subagent is disabled
outright (see agent.MirobodyAgent._apply_harness_profile).

=============================================================================
CHARTING: ONE PATH — ```vis-chart``` blocks
=============================================================================

Charting belongs to the chat agent alone: the model writes a fenced vis-chart code
block of pure-data JSON (no styling) directly in its reply, and the frontend
renders it as an interactive chart. No tool call, no server round-trip, no PNG.

An external MCP client (Claude Desktop, Cursor, a custom host) brings its own
visualization: the tools return data, never chart markup.

The former ChartService MCP tools (generate_*_chart), which rendered PNGs
through a Node @antv/gpt-vis-ssr toolchain, were removed: the repository's
ONLY Node.js dependency, five chart tools polluting the MCP surface, and 18
"Method not found" warnings on every boot. The `/charts` static mount and its
Docker volume went with them — nothing writes chart files any more, so there
is nothing to serve.

=============================================================================
EXAMPLE: Adding a New Tool
=============================================================================

```python
# new_service.py (in tools/ root, no underscore prefix)

from typing import Any, Dict, Optional

class NewService:  # Must end with "Service"

    def __init__(self):
        self.name = "New Service"

    async def my_tool(
        self,
        param1: str,
        user_info: Optional[Dict[str, Any]] = None,  # Auto-injected
    ) -> Dict[str, Any]:
        \"\"\"
        Tool description (parsed from docstring).

        Args:
            param1: Parameter description.

        Returns:
            Result description.
        \"\"\"
        user_id = (user_info or {}).get("user_id")
        if not user_id or not isinstance(user_id, str):
            return {"success": False, "error": "Authorization required."}

        return {"success": True, "result": f"Result for {param1}"}

    def _helper_method(self):  # Underscore prefix → not exposed as tool
        pass
```
"""

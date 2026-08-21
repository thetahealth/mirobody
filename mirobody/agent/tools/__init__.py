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

=============================================================================
DIRECTORY STRUCTURE
=============================================================================

tools/
├── __init__.py                      # This documentation
├── terminology_service.py           # ② Standardize over MCP: resolve_indicator, normalize_unit
│                                    #   (offline, no user data — works anonymously)
├── health_indicator_service.py      # query_health_indicators — ONE call: search +
│                                    #   read + server-side aggregate, LOINC-coded
└── genetic_service.py               # get_genetic_data

Note: filesystem tools (ls, read_file, write_file, edit_file, glob, grep) are
NOT MCP tools here — the DeepAgent gets them natively from the deepagents
FilesystemMiddleware, backed by the PgFilesystemBackend in
mirobody/agent/deep/backend.py. There is no write_todos and no task/
subagent tool: deepagents 0.7 dropped TodoListMiddleware from its default stack
and DeepAgent does not add it back, and the general-purpose subagent is disabled
outright (see deep_agent._apply_harness_profile).

=============================================================================
CHARTING: ONE PATH — ```vis-chart``` blocks
=============================================================================

Charting belongs to DeepAgent alone: the model writes a fenced vis-chart code
block of pure-data JSON (no styling) directly in its reply, and the frontend
renders it as an interactive chart. No tool call, no server round-trip, no PNG.

BaseAgent does NOT chart — it is the thinnest derivation over the MCP tool
surface, and the MCP client consuming it (Claude Desktop, ChatGPT, a custom
host) brings its own visualization; its prompt tells the model to stick to
text and tables.

The former ChartService MCP tools (generate_*_chart), which rendered PNGs
through a Node @antv/gpt-vis-ssr toolchain, were removed: the repository's
ONLY Node.js dependency, five chart tools polluting the MCP surface, and 18
"Method not found" warnings on every boot. Historic chart PNGs remain served
from the /charts static volume; only the generation path is gone.

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

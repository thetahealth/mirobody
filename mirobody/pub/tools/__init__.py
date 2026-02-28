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
   - Subdirectories are safe for utilities (e.g., files_utils/)

2. FILE FILTERING
   - Only .py files are loaded (entry.name.lower().endswith(".py"))
   - Files starting with underscore are SKIPPED (entry.name.startswith("_"))
   - Use underscore prefix for internal helpers (e.g., _chart_service_schema_loader.py)

3. CLASS FILTERING (load_tools_from_module)
   - Only classes ending with "Service" are registered as tool providers
   - Example: FileReadService ✓, FileHelper ✗
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
├── file_read_service.py             # FileReadService (ls, read_file, glob, grep, ...)
├── file_write_service.py            # FileWriteService (write_file, edit_file)
├── todo_service.py                  # TodoService (write_todos)
├── chart_service.py                 # ChartService
├── _chart_service_schema_loader.py  # Helper (underscore prefix → not loaded)
└── files_utils/                     # Shared utilities (subdirectory → not scanned)
    ├── __init__.py
    ├── backend.py
    ├── compat.py
    ├── eviction.py
    └── global_files.py

=============================================================================
EXAMPLE: Adding a New Tool
=============================================================================

```python
# new_service.py (in tools/ root, no underscore prefix)

from typing import Any, Dict, Optional
from .files_utils import validate_user_info, func_description

class NewService:  # Must end with "Service"

    def __init__(self):
        self.name = "New Service"

    async def my_tool(
        self,
        param1: str,
        user_info: Optional[Dict[str, Any]] = None,  # Auto-injected
    ) -> str:
        \"\"\"
        Tool description (parsed from docstring).

        Args:
            param1: Parameter description.

        Returns:
            Result description.
        \"\"\"
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        return f"Result for {param1}"

    def _helper_method(self):  # Underscore prefix → not exposed as tool
        pass
```
"""

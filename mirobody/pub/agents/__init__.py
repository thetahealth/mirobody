"""
Mirobody Agents Module

Design Principles:
------------------
1. Tools are injected from upstream MCP tools (mirobody/pub/tools/)
2. Agents do not load tools directly; middleware only injects system prompts
3. File operations are provided by file_read_service.py and file_write_service.py

Agent Types:
------------
- DeepAgent: General-purpose deep conversation agent for complex task handling
- MixAgent: Hybrid tool agent with dynamic tool loading support

Tool Loading Flow:
------------------
1. MCP Server scans tools/ directory on startup
2. Tool service classes (e.g., FileReadService) are auto-discovered and registered
3. When agent is created, tools are already injected via MCP framework
4. Middleware only injects system prompts for corresponding tools

References:
-----------
- Tool directory: mirobody/pub/tools/
- Tool loading: mirobody/server/mcp_tools.py
"""

from .deep_agent import DeepAgent
from .mix_agent import MixAgent

__all__ = [
    "DeepAgent",
    "MixAgent",
]

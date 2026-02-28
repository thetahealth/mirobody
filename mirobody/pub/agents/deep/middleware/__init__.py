"""
Deep Agent Middleware

Design Principles:
------------------
Tool-related middleware has been migrated to MCP tools.
Only non-tool middleware remains here.

- File tools: mirobody/pub/tools/file_read_service.py, file_write_service.py
- Global files utils: mirobody/pub/tools/_global_files_utils.py
"""

from .prompt_caching import UniversalPromptCachingMiddleware

__all__ = ["UniversalPromptCachingMiddleware"]

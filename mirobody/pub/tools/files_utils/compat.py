"""
deepagents Compatibility Layer

Imports and re-exports deepagents constants to ensure mirobody MCP tools
stay aligned with deepagents framework capabilities.

Also provides the func_description decorator for setting tool descriptions.
"""

# =============================================================================
# Import from deepagents
# =============================================================================

from deepagents.middleware.filesystem import (
    # System prompts current not used (MCP)
    FILESYSTEM_SYSTEM_PROMPT,
    EXECUTION_SYSTEM_PROMPT,
    # Truncation
    READ_FILE_TRUNCATION_MSG,
    NUM_CHARS_PER_TOKEN,
    EMPTY_CONTENT_WARNING,
    # Defaults
    DEFAULT_READ_OFFSET,
    DEFAULT_READ_LIMIT,

    # tools description from langchain
    LIST_FILES_TOOL_DESCRIPTION,
    READ_FILE_TOOL_DESCRIPTION,
    EDIT_FILE_TOOL_DESCRIPTION,
    WRITE_FILE_TOOL_DESCRIPTION,
    GLOB_TOOL_DESCRIPTION,
    GREP_TOOL_DESCRIPTION,

    # execute tool description (base)
    EXECUTE_TOOL_DESCRIPTION as _BASE_EXECUTE_TOOL_DESCRIPTION,
)

from langchain.agents.middleware.todo import (
    WRITE_TODOS_TOOL_DESCRIPTION,
)

# =============================================================================
# Image Support Constants (defined locally)
# =============================================================================

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
IMAGE_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".bmp": "image/bmp",
}

# =============================================================================
# Global Files Tool Descriptions
# =============================================================================

LIST_GLOBAL_FILES_DESCRIPTION = """Lists all files in the user's personal file library.

This is useful for discovering uploaded files (PDFs, images, Excel, documents, etc.) before fetching them.
You should use this tool to find file_keys, then use fetch_remote_files to load them into current workspace to read.

Usage:
- Results are sorted by upload time (newest first) and deduplicated by file_key
- Use date filters for large libraries: list_global_files(start_date="2024-01-01")
- Use pagination for browsing: list_global_files(offset=50, limit=50)

Examples:
- list_global_files()  # List recent files
- list_global_files(start_date="2024-01-01")  # Files from 2024
- list_global_files(limit=100)  # Get more results

Args:
    start_date: Filter by start date (format: 'YYYY-MM-DD'). Optional.
    end_date: Filter by end date (format: 'YYYY-MM-DD'). Optional.
    offset: Pagination offset. Default: 0
    limit: Number of results per page (max: 200). Default: 50
"""

FETCH_REMOTE_FILES_DESCRIPTION = """Fetches files into current workspace from file_keys or URLs.

**IMPORTANT**: You should ALWAYS use list_global_files first to discover available file_keys.
After fetching, use read_file to access the content.

Usage:
- file_keys from list_global_files: fetch_remote_files(["uploads/report.pdf"])
- URLs (http/https): fetch_remote_files(["https://example.com/data.xlsx"])
- Files are saved to /global_files/ directory

Examples:
- fetch_remote_files(["uploads/report.pdf"])
- fetch_remote_files(["https://example.com/data.xlsx"])

Args:
    sources: List of file_keys or URLs to fetch.
"""

# =============================================================================
# Tool Descriptions with Args (mirobody enhanced versions)
# =============================================================================
# Args format must match mirobody's parse_function() expected format:
#   param_name: description (no leading dash, no type annotation)

LIST_FILES_TOOL_DESCRIPTION += """
Args:
    path: Absolute path to the directory to list. Must be absolute, not relative. Default: "/"
"""

READ_FILE_TOOL_DESCRIPTION += """
Args:
    file_path: Absolute path to the file to read. Must be absolute, not relative.
    offset: Line number to start reading from (0-indexed). Use for pagination of large files. Default: 0
    limit: Maximum number of lines to read. Use for pagination of large files. Default: 100
"""

WRITE_FILE_TOOL_DESCRIPTION += """
Args:
    file_path: Absolute path where the file should be created. Must be absolute, not relative.
    content: The text content to write to the file. This parameter is required.
"""

EDIT_FILE_TOOL_DESCRIPTION +=  """
Args:
    file_path: Absolute path to the file to edit. Must be absolute, not relative.
    old_string: The exact text to find and replace. Must be unique in the file unless replace_all is True.
    new_string: The text to replace old_string with. Must be different from old_string.
    replace_all: If True, replace all occurrences of old_string. If False (default), old_string must be unique. Default: False
"""

GLOB_TOOL_DESCRIPTION += """
Args:
    pattern: Glob pattern to match files (e.g., '**/*.py', '*.txt', '/subdir/**/*.md').
    path: Base directory to search from. Default: "/"
"""

GREP_TOOL_DESCRIPTION += """
Args:
    pattern: Text pattern to search for (literal string, not regex).
    path: Directory to search in. Default: current working directory.
    glob: Glob pattern to filter which files to search (e.g., '*.py'). Optional.
    output_mode: Output format - 'files_with_matches' (file paths only, default), 'content' (matching lines with context), 'count' (match counts per file). Default: 'files_with_matches'
"""

WRITE_TODOS_TOOL_DESCRIPTION += """
Args:
    todos: List of todo items to create/update. Each item should have 'content' (str) and 'status' (one of 'pending', 'in_progress', 'completed').
"""

# =============================================================================
# Code Execution Tool Description (adapted from deepagents EXECUTE_TOOL_DESCRIPTION)
# =============================================================================

# Start from the deepagents base description, then append mirobody-specific
# Args matching the MCP tool signature (command, timeout).
EXECUTE_TOOL_DESCRIPTION = _BASE_EXECUTE_TOOL_DESCRIPTION + """
Important - How file execution works in this environment:
  - Files created via write_file are locally, NOT directly on disk.
  - Before each execute call, all workspace files are automatically synced to the sandbox filesystem.
  - This means: write_file first, then execute — the file will be available at the same path in the sandbox.
  - Sync is one-way (PostgreSQL → local). Files created by execute inside the sandbox are NOT synced back.
  - To retrieve output files created by execute, either print the content in the command itself (e.g., cat output.txt), or include the output in stdout.

Recommended workflow:
  1. Use write_file to create scripts/data files (e.g., write_file("/analysis.py", code))
  2. Use execute to run them (e.g., execute(command="python3 /analysis.py"))
  3. Read results from stdout, or use execute(command="cat /output.csv") for generated files

Additional notes:
  - Common data analysis libraries (pandas, numpy, matplotlib, scipy, etc.) are pre-installed.
  - You can install additional packages: execute(command="pip install <package> && python3 /script.py")
  - For inline code, use: execute(command="python3 -c 'print(2+2)'")

Args:
    command: Shell command to execute in the sandbox environment (e.g., "python3 -c 'print(1+1)'" or "python3 /script.py").
    timeout: Maximum execution time in seconds (1-120). Default: 60.
"""

# =============================================================================
# Feature Flags
# =============================================================================

# Enable multimodal image support (returns base64 image content blocks)
ENABLE_IMAGE_MULTIMODAL = False

# Default token limit before evicting large results to filesystem
DEFAULT_EVICTION_TOKEN_LIMIT = 20000

# =============================================================================
# Decorators
# =============================================================================

def func_description(description: str):
    """
    Decorator to set function's description with auto-generated Args documentation.
    """
    def decorator(func):
        func.__doc__ = description
        return func
    return decorator


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Tool descriptions (from deepagents/langchain)
    "LIST_FILES_TOOL_DESCRIPTION",
    "READ_FILE_TOOL_DESCRIPTION",
    "EDIT_FILE_TOOL_DESCRIPTION",
    "WRITE_FILE_TOOL_DESCRIPTION",
    "GLOB_TOOL_DESCRIPTION",
    "GREP_TOOL_DESCRIPTION",
    "WRITE_TODOS_TOOL_DESCRIPTION",
    # Global files descriptions (mirobody)
    "LIST_GLOBAL_FILES_DESCRIPTION",
    "FETCH_REMOTE_FILES_DESCRIPTION",
    # Code execution description
    "EXECUTE_TOOL_DESCRIPTION",
    # System prompts
    "FILESYSTEM_SYSTEM_PROMPT",
    "EXECUTION_SYSTEM_PROMPT",
    # Image support
    "IMAGE_EXTENSIONS",
    "IMAGE_MEDIA_TYPES",
    # Truncation
    "READ_FILE_TRUNCATION_MSG",
    "NUM_CHARS_PER_TOKEN",
    "EMPTY_CONTENT_WARNING",
    # Defaults
    "DEFAULT_READ_OFFSET",
    "DEFAULT_READ_LIMIT",
    # Feature flags
    "ENABLE_IMAGE_MULTIMODAL",
    "DEFAULT_EVICTION_TOKEN_LIMIT",
    # Decorators
    "func_description",
]

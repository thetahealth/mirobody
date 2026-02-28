"""
Large Result Eviction

Internal write mechanism for oversized tool results.
The agent cannot call write operations directly, but the system
uses this to prevent context overflow.

Usage:
    from .files_utils import maybe_evict_large_result

    result = await some_tool_function()
    result = await maybe_evict_large_result(backend, result, "grep")
"""

import hashlib
import logging
import time
from typing import Any

from .compat import DEFAULT_EVICTION_TOKEN_LIMIT, NUM_CHARS_PER_TOKEN

logger = logging.getLogger(__name__)


def create_content_preview(
    content: str,
    head_lines: int = 5,
    tail_lines: int = 5,
    max_line_length: int = 1000,
) -> str:
    """
    Create a preview showing head and tail of content with truncation marker.

    Args:
        content: The full content string to preview.
        head_lines: Number of lines to show from the start.
        tail_lines: Number of lines to show from the end.
        max_line_length: Maximum characters per line (truncate longer lines).

    Returns:
        Formatted preview string showing head, truncation marker, and tail.
    """
    lines = content.splitlines()

    if len(lines) <= head_lines + tail_lines:
        # If file is small enough, show truncated version
        preview_lines = [line[:max_line_length] for line in lines]
        return "\n".join(preview_lines)

    # Show head and tail with truncation marker
    head = [line[:max_line_length] for line in lines[:head_lines]]
    tail = [line[:max_line_length] for line in lines[-tail_lines:]]
    truncated_count = len(lines) - head_lines - tail_lines

    return (
        "\n".join(head)
        + f"\n... [{truncated_count} lines truncated] ...\n"
        + "\n".join(tail)
    )


async def maybe_evict_large_result(
    backend: Any,
    result: str,
    tool_name: str,
    token_limit: int = DEFAULT_EVICTION_TOKEN_LIMIT,
) -> str:
    """
    Check if result exceeds token limit and evict to filesystem if needed.

    This is an INTERNAL function - the agent cannot call write operations,
    but the system can use this to evict large results.

    Args:
        backend: PostgresBackend instance (must have awrite method).
        result: The tool result string to check.
        tool_name: Name of the tool (for generating unique file path).
        token_limit: Token threshold for eviction (default: 20000).

    Returns:
        Original result if small enough, or truncated message with file reference.
    """
    # Check size threshold (using approximate chars-per-token ratio)
    char_threshold = NUM_CHARS_PER_TOKEN * token_limit
    if len(result) <= char_threshold:
        return result

    # Generate unique file path using timestamp and content hash
    timestamp = int(time.time() * 1000)
    content_hash = hashlib.md5(result[:1000].encode()).hexdigest()[:8]
    file_path = f"/large_tool_results/{tool_name}_{timestamp}_{content_hash}"

    # Internal write - agent cannot access this directly
    try:
        await backend.awrite(file_path, result)
        logger.info(f"Evicted large result ({len(result)} chars) to {file_path}")
    except Exception as e:
        # If write fails, return truncated result with warning
        logger.warning(f"Failed to evict large result to filesystem: {e}")
        max_len = char_threshold
        return result[:max_len] + f"\n\n[Result truncated. Write failed: {e}]"

    # Create preview and return reference
    preview = create_content_preview(result)

    # Return message with file reference and preview
    return f"""Tool result too large ({len(result):,} chars), saved to filesystem at: {file_path}

You can read the result using read_file tool, but read it in chunks:
- read_file("{file_path}", offset=0, limit=100) for first 100 lines
- read_file("{file_path}", offset=100, limit=100) for next 100 lines

Preview (head and tail):
{preview}
"""


__all__ = [
    "create_content_preview",
    "maybe_evict_large_result",
]

"""Shared utilities for `mirobody.agent` (Base / Deep / Mix).

Modules:
- `errors`        — `DeepAgentError`, `ConfigError` raised by the agent loop.
- `stream`        — `StreamConverter` + `TokenUsageCallback` for converting
                    LangGraph stream events into mirobody chunk dicts and
                    tallying token usage / cost.
- `prompt`        — `build_system_prompt` Jinja renderer for an agent's
                    base prompt + tool descriptions + time + user context.
- `file`          — `handle_file_upload` to land incoming files in the
                    backend / workspace.
- `cache_config`  — TTL / size constants used by the FileParser pipeline.
- `coercion`      — `coerce_to_list/int/bool` to normalize tool arguments that
                    LLMs (Qwen/DeepSeek) return with the wrong JSON type.

File-extension classification helpers (`FILE_TYPE_MAP`, `get_file_type`,
`IMAGE_EXTENSIONS`, `IMAGE_MEDIA_TYPES`, `sanitize_filename`) live in the
cross-cutting `mirobody.utils.file_types` module — import them from there
directly, not from this sub-package.
"""

from ...utils.file_types import FILE_TYPE_MAP, get_file_type
from .cache_config import *
from .coercion import coerce_to_list, coerce_to_int, coerce_to_bool
from .errors import DeepAgentError, ConfigError
from .file import handle_file_upload
from .prompt import build_system_prompt
from .stream import StreamConverter, TokenUsageCallback

__all__ = [
    # errors
    "DeepAgentError",
    "ConfigError",
    # stream
    "StreamConverter",
    "TokenUsageCallback",
    # files
    "handle_file_upload",
    "FILE_TYPE_MAP",
    "get_file_type",
    # argument coercion (LLMs send wrong JSON types)
    "coerce_to_list",
    "coerce_to_int",
    "coerce_to_bool",
    # prompts
    "build_system_prompt",
    # cache (re-exported via cache_config import *)
    "CACHE_TTL_REDIS",
    "CACHE_TTL_GLOBAL",
    "CACHE_TTL_LOCAL",
    "CACHE_MAX_FILES",
    "CACHE_MAX_WORKERS",
]
"""Direct per-provider LLM access (`utils.py`, `file_processors/`, `clients.py`)
the utility surfaces: vision, structured extraction, text.

Which model each surface uses is `config.llm.yaml`'s decision
(`UTILS_VISION_MODEL`, `UTILS_TEXT_MODEL`; see `mirobody.utils.config.llm`).
The agent's own model access goes through LangChain (`agent/agent.py`); this
package is the direct-SDK path for extraction and utility calls.
"""

from .clients import client_manager
from .file_processors import (
    FileProcessor,
    unified_file_extract,
    vision_route,
)
from .utils import (
    async_get_structured_output,
    async_get_text_completion,
)

__all__ = [
    "client_manager",
    "FileProcessor",
    "unified_file_extract",
    "vision_route",
    "async_get_structured_output",
    "async_get_text_completion",
]

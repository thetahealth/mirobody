"""Direct per-provider LLM access (`utils.py`, `file_processors/`, `clients.py`)
— the utility surfaces: vision, structured extraction, text.

Which model each surface uses is `config.llm.yaml`'s decision
(`UTILS_VISION_MODEL`, `UTILS_TEXT_MODEL`; see `mirobody.utils.config.llm`).
The agent's own model access goes through LangChain (`agent/agent.py`); this
package is the direct-SDK path for extraction and utility calls.

The other half that used to live here — `batch_ai_response`/`interface.py`
and the `adapters/` stack — was deleted along with its single caller,
`pulse/file_parser/services/file_llm_analyzer.py` (the orphaned
/ws/upload-with-llm-analysis flow).
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
from .hipaa_policy import export_to_env

__version__ = "2.0.0"
__author__ = "AI Team"

__all__ = [
    "client_manager",
    "FileProcessor",
    "unified_file_extract",
    "vision_route",
    "async_get_structured_output",
    "async_get_text_completion",
    "export_to_env",
]

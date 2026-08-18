"""Vision-model file extraction, split by concern.

Was one 842-line module with four providers and no tests. The seams it is cut
along are the ones that already existed inside it as comment banners:

    results.py          prompt shaping + result merging (pure; no I/O)
    media.py            file -> model-ready bytes (image optimisation, PDF pages)
    backends_openai.py  the OpenAI-compatible path: OpenRouter / Qwen / Doubao
    gemini.py           the Gemini path (own SDK, own page loop)
    dispatch.py         provider-selection policy + the unified entry point

The import surface is unchanged: `from .file_processors import X` and
`from mirobody.utils.llm import X` resolve exactly as before. The names below
that start with an underscore are re-exported deliberately — they are internal
to the package but `test_file_processors.py` pins their behaviour, and the
merge rules they implement are the ones a multi-page extraction bug lands in.
"""

from .dispatch import (
    PROVIDER_HANDLERS,
    VisionProviderConfig,
    _handle_doubao,
    _handle_gemini,
    _handle_openrouter,
    _handle_qwen,
    unified_file_extract,
)
from .backends_openai import (
    PROVIDER_EXTRA_PARAMS,
    doubao_file_extract,
    qwen_file_extract,
    vision_file_extract,
)
from .gemini import gemini_file_extract
from .media import FileProcessor
from .results import (
    _build_prompt_with_schema,
    _merge_json_results,
    _merge_page_results,
    clean_json_response,
)

__all__ = [
    "FileProcessor",
    "VisionProviderConfig",
    "PROVIDER_EXTRA_PARAMS",
    "PROVIDER_HANDLERS",
    "clean_json_response",
    "gemini_file_extract",
    "doubao_file_extract",
    "qwen_file_extract",
    "vision_file_extract",
    "unified_file_extract",
]

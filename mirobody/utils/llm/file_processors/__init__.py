"""Vision-model file extraction, split by concern.

Was one 842-line module with four providers and no tests. The seams it is cut
along are the ones that already existed inside it as comment banners:

    results.py          prompt shaping + result merging (pure; no I/O)
    media.py            file -> model-ready bytes (image optimisation, PDF pages)
    backends_openai.py  the one request shape: chat/completions with an image part
    dispatch.py         the route (`UTILS_VISION_MODEL`) + the unified entry point

The names below that start with an underscore are re-exported deliberately —
they are internal to the package but `test_file_processors.py` pins their
behaviour, and the merge rules they implement are the ones a multi-page
extraction bug lands in.
"""

from .dispatch import unified_file_extract, vision_route
from .backends_openai import openai_compatible_file_extract
from .media import FileProcessor
from .results import (
    _build_prompt_with_schema,
    _merge_json_results,
    _merge_page_results,
    clean_json_response,
)

__all__ = [
    "FileProcessor",
    "clean_json_response",
    "openai_compatible_file_extract",
    "unified_file_extract",
    "vision_route",
]

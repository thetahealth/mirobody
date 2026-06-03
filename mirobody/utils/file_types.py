"""File-type / extension constants + helpers shared across the codebase.

Single source of truth for "what kind of file is this extension" lookups.
Consumed by:

- `mirobody/pub/agents/deep/backend.py`             — file_type field in tool results
- `mirobody/utils/llm/file_processors.py`           — image branch in LLM processor

`mirobody/pulse/file_parser/services/compressed_file_processor.py` keeps
its own `SUPPORTED_FILE_TYPES` (a `dict[mime, list[ext]]` for archive
extraction, not extension classification) — different shape, different
purpose, intentionally separate.
"""

import os
import re


# Extension → short type tag for file metadata / display.
FILE_TYPE_MAP: dict[str, str] = {
    ".pdf":  "PDF",
    ".docx": "DOCX",
    ".doc":  "DOC",
    ".png":  "IMAGE",
    ".jpg":  "IMAGE",
    ".jpeg": "IMAGE",
    ".gif":  "IMAGE",
    ".webp": "IMAGE",
    ".bmp":  "IMAGE",
    ".txt":  "TEXT",
    ".md":   "TEXT",
    ".csv":  "CSV",
    ".xlsx": "EXCEL",
    ".xls":  "EXCEL",
    ".html": "HTML",
    ".htm":  "HTML",
    ".json": "JSON",
    ".xml":  "XML",
}


# Image-extension set for vision / multimodal branches.
IMAGE_EXTENSIONS: set[str] = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}


# Extension → MIME type. Used when constructing multimodal LLM
# content blocks for image inputs.
IMAGE_MEDIA_TYPES: dict[str, str] = {
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".gif":  "image/gif",
    ".webp": "image/webp",
    ".bmp":  "image/bmp",
}


def get_file_type(extension: str) -> str:
    """Look up `FILE_TYPE_MAP` for an extension; returns ``"UNKNOWN"`` on miss.

    The argument may be passed with or without a leading dot.
    """
    ext = extension.lower()
    if not ext.startswith("."):
        ext = "." + ext
    return FILE_TYPE_MAP.get(ext, "UNKNOWN")


def sanitize_filename(filename: str, max_length: int = 200) -> str:
    """Strip filesystem-illegal characters and clamp stem length."""
    if not filename:
        return filename

    # Replace illegal Windows/POSIX characters.
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    # Strip control characters.
    filename = re.sub(r"[\x00-\x1f\x7f]", "", filename)
    # Clamp the name part, preserve extension.
    name, ext = os.path.splitext(filename)
    if len(name) > max_length:
        name = name[:max_length]
    return name + ext

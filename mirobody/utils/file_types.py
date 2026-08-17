"""File-type / extension constants + helpers shared across the codebase.

Single source of truth for "what kind of file is this extension" lookups.
Consumed by:

- `mirobody/agent/deep/backend.py`             — file_type field in tool results
- `mirobody/utils/llm/file_processors.py`           — image branch in LLM processor
- `mirobody/pulse/file_parser/`                — handler routing + extraction routing
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


# Extension + MIME sets for the "which extractor handles this?" question.
# The Excel pair existed verbatim in two places — `ExcelHandler.is_excel_file`
# (deciding which handler runs) and `FileAbstractExtractor._is_excel_file`
# (deciding which extraction routine runs). Two copies of the routing table
# meant a new spreadsheet type could reach a handler that then refused to
# extract it.
EXCEL_EXTENSIONS: set[str] = {".xlsx", ".xls", ".xlsm", ".xlsb"}
EXCEL_MIME_TYPES: set[str] = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/vnd.ms-excel.sheet.macroEnabled.12",
    "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
}

TEXT_EXTENSIONS: set[str] = {".txt", ".md", ".csv", ".json", ".xml", ".html", ".htm", ".log"}
TEXT_MIME_TYPES: set[str] = {
    "text/plain",
    "text/markdown",
    "text/csv",
    "application/json",
    "text/xml",
    "application/xml",
    "text/html",
}


def _matches(filename: str, content_type: str | None, exts: set[str], mimes: set[str]) -> bool:
    if not filename:
        return False
    ext = os.path.splitext(filename)[1].lower()
    return ext in exts or (content_type in mimes if content_type else False)


def is_excel_file(filename: str, content_type: str | None = None) -> bool:
    """True for spreadsheet uploads (by extension or MIME type)."""
    return _matches(filename, content_type, EXCEL_EXTENSIONS, EXCEL_MIME_TYPES)


def is_text_file(filename: str, content_type: str | None = None) -> bool:
    """True for plain-text-ish uploads we can decode without a parser."""
    return _matches(filename, content_type, TEXT_EXTENSIONS, TEXT_MIME_TYPES)


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

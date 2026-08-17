"""File-type classification shared across the DeepAgent VFS layer.

``MULTIMODAL_EXTS`` used to exist as two verbatim copies — in ``parser.py``
(upload-time classification) and ``backend.py`` (read-time serving) — held
together only by a "kept in sync" comment. An edit to one copy would silently
make upload-time storage disagree with read-time serving. This module is the
single source for both.
"""

import mimetypes
from pathlib import PurePosixPath

# Extensions deepagents surfaces as multimodal content blocks (stored as raw
# bytes / object-storage offload, served as base64 — never as inline text).
# Mirrors the harness virtual-filesystem docs:
# https://docs.langchain.com/oss/python/deepagents/harness#virtual-filesystem-access
MULTIMODAL_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif",          # image
    ".mp4", ".mpeg", ".mov", ".avi", ".flv", ".mpg", ".webm", ".wmv", ".3gpp",  # video
    ".wav", ".mp3", ".aiff", ".aac", ".ogg", ".flac",                    # audio
    ".pdf", ".ppt", ".pptx",                                             # documents
}

# Curated overrides checked before mimetypes.guess_type, whose answers are
# platform-dependent for some of these (e.g. ``.md`` has no registered type on
# a bare macOS/Linux install) — stored rows need stable values.
_MIME_BY_EXT = {
    ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp",
    ".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
    ".json": "application/json", ".xml": "application/xml", ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
}


def guess_mime(filename: str) -> str:
    ext = PurePosixPath(filename or "").suffix.lower()
    if ext in _MIME_BY_EXT:
        return _MIME_BY_EXT[ext]
    return mimetypes.guess_type(filename or "")[0] or "application/octet-stream"


def is_multimodal(filename: str) -> bool:
    return PurePosixPath(filename or "").suffix.lower() in MULTIMODAL_EXTS

"""File-type classification shared across the DeepAgent VFS layer.

``MULTIMODAL_EXTS`` used to exist as two verbatim copies — in ``parser.py``
(upload-time classification) and ``backend.py`` (read-time serving) — held
together only by a "kept in sync" comment. An edit to one copy would silently
make upload-time storage disagree with read-time serving. This module is the
single source for both.
"""

from pathlib import PurePosixPath

# One implementation, in the engine layer, because object storage and the
# presigned-URL helper need the same answers and cannot import the agent layer.
from ...utils.file_types import guess_mime as guess_mime

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

def is_multimodal(filename: str) -> bool:
    return PurePosixPath(filename or "").suffix.lower() in MULTIMODAL_EXTS

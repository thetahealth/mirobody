"""File-type / extension constants + helpers shared across the codebase.

Single source of truth for "what kind of file is this extension" lookups —
including :func:`guess_mime`, which every layer now calls: object storage when it
sets `Content-Type` on a PUT and the agent's VFS when it decides whether to
serve a row as text or base64. Those were three implementations of the same
question (the third was the presigned-URL helper, since deleted), and they
disagreed.

A FIFTH implementation turned up after the first four were merged, and it was
the one that mattered most: `file_parser/services/db_utils.get_mime_type`, read
at five sites to decide the `content_type` a stored file carries and the type the
model is handed. It disagreed with this table on `.flac`, `.m4a`, `.rar` and
`.wav` — modern IANA names against the legacy `x-` forms — so within a single
deployment the storage path and the agent path described the same bytes
differently. It now delegates here, and this table wins because its values are
already written into stored objects.

They can only agree if the answer does not come from `mimetypes` alone.
`mimetypes` merges the interpreter's built-in table with the host's
`/etc/mime.types`, so its answers are a property of the MACHINE: measured on
this repo's 41 accepted-or-served extensions, 13 change between a developer
laptop and a bare container, and 9 of those become `application/octet-stream` —
`.docx`, `.pptx`, `.flac`, `.m4a`, `.ogg`, `.flv`, `.wmv`, `.rar`, `.aac`.
`Content-Type` is written into the stored object at PUT time, so that turns the
build host into part of the data: the same upload is served as a spreadsheet
from one deployment and as a download from another.

`MIME_BY_EXT` therefore pins every extension this project accepts (uploads:
`file_parser/services/file_uploader.SUPPORTED_EXTENSIONS`; agent serving:
`agent/filesystem/naming.MULTIMODAL_EXTS`) and `mimetypes` is only the fallback for
everything else. The values are the ones this project already stores — the
legacy `x-` forms (`audio/x-aac`, `video/x-flv`) are kept rather than modernized
to their newer IANA names, because changing one would leave a deployment serving
two different content-types for the same extension depending on upload date.
"""

import mimetypes
import os
from pathlib import PurePosixPath


# Extension → MIME, pinned so the answer does not depend on the host's
# /etc/mime.types. Covers every extension in SUPPORTED_EXTENSIONS and
# MULTIMODAL_EXTS; `test_content_type.py` fails if one is added there without
# landing here.
MIME_BY_EXT: dict[str, str] = {
    # images
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
    ".tiff": "image/tiff", ".svg": "image/svg+xml",
    ".heic": "image/heic", ".heif": "image/heif",
    # documents
    ".pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
    ".xlsb": "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    # text
    ".txt": "text/plain", ".md": "text/markdown", ".markdown": "text/markdown",
    ".log": "text/plain", ".csv": "text/csv", ".html": "text/html",
    ".htm": "text/html", ".json": "application/json", ".xml": "application/xml",
    # audio
    ".wav": "audio/x-wav", ".mp3": "audio/mpeg", ".aiff": "audio/x-aiff",
    ".aac": "audio/x-aac", ".ogg": "audio/ogg", ".flac": "audio/x-flac",
    ".m4a": "audio/mp4a-latm",
    # video
    ".mp4": "video/mp4", ".mpeg": "video/mpeg", ".mpg": "video/mpeg",
    ".mov": "video/quicktime", ".avi": "video/x-msvideo", ".webm": "video/webm",
    ".flv": "video/x-flv", ".wmv": "video/x-ms-wmv", ".3gpp": "video/3gpp",
    # archives
    ".zip": "application/zip", ".rar": "application/x-rar-compressed",
    ".7z": "application/x-7z-compressed", ".gz": "application/gzip",
    ".tar": "application/x-tar",
    # types that only the file_parser's own table used to carry
    ".ico": "image/x-icon", ".tif": "image/tiff", ".mkv": "video/x-matroska",
}


def guess_mime(filename_or_ext: str | None) -> str:
    """MIME type for a filename, a suffix, or a bare extension.

    All three shapes reach this in the codebase — a full path from the VFS, a
    `PurePosixPath.suffix`, and a bare extension from callers that only hold
    the suffix text — so all three are accepted. Unknown types get
    `application/octet-stream`, which is the right answer when there is no
    answer; it was only wrong as a catch-all for extensions we do know.
    """
    raw = str(filename_or_ext or "").strip()
    if not raw:
        return "application/octet-stream"
    ext = PurePosixPath(raw).suffix.lower()
    if not ext:                       # a bare extension, with or without the dot
        ext = "." + raw.lstrip(".").lower()
    if ext in MIME_BY_EXT:
        return MIME_BY_EXT[ext]
    return mimetypes.guess_type("x" + ext)[0] or "application/octet-stream"


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

# Word and PowerPoint, modern zip formats only. python-docx and python-pptx
# cannot read legacy binary `.doc`/`.ppt`, so those are deliberately absent —
# they were accepted by the upload gate for a long time with no handler at all,
# which meant the picker took the file and the upload failed at the end.
DOCUMENT_EXTENSIONS: set[str] = {".docx", ".pptx"}
DOCUMENT_MIME_TYPES: set[str] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
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


def is_document_file(filename: str, content_type: str | None = None) -> bool:
    """True for Word/PowerPoint uploads we can extract text from."""
    return _matches(filename, content_type, DOCUMENT_EXTENSIONS, DOCUMENT_MIME_TYPES)


def is_text_file(filename: str, content_type: str | None = None) -> bool:
    """True for plain-text-ish uploads we can decode without a parser."""
    return _matches(filename, content_type, TEXT_EXTENSIONS, TEXT_MIME_TYPES)

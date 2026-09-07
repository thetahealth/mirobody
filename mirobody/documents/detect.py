"""What kind of document is this — by name and type, then by bytes.

Two callers hand in different evidence: an upload carries a filename and a
content type; a multipart part on an API often carries neither reliably (a
generic ``application/octet-stream`` and a synthetic name). So classification
is extension/content-type first and magic bytes as the fallback, and the
answer is one of a closed set of kinds the extractor knows how to read.
"""

from __future__ import annotations

import os

from ..utils.file_types import DOCUMENT_MIME_TYPES, TEXT_EXTENSIONS, TEXT_MIME_TYPES

KIND_PDF = "pdf"
KIND_IMAGE = "image"
KIND_XLSX = "xlsx"
KIND_DOCX = "docx"
KIND_PPTX = "pptx"
KIND_TEXT = "text"

PDF_SUFFIXES: tuple[str, ...] = (".pdf",)
#: `.heic`/`.heif` are what an iPhone camera roll produces; the vision models
#: this project has run accept them natively, so no local transcoding.
IMAGE_SUFFIXES: tuple[str, ...] = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff", ".heic", ".heif")
#: openpyxl reads the zip-based formats only; the pre-2007 binary `.xls` is
#: not a supported upload and never was.
XLSX_SUFFIXES: tuple[str, ...] = (".xlsx", ".xlsm")
DOCX_SUFFIXES: tuple[str, ...] = (".docx",)
PPTX_SUFFIXES: tuple[str, ...] = (".pptx",)

_XLSX_MIME = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel.sheet.macroenabled.12",
}
_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"

#: Every suffix `extract.extract_text` turns into text through a parser or OCR —
#: i.e. the files that SHOULD end up with extracted text. The one source of
#: truth: a hand-copied list once missed `.heic`/`.heif` and `.xlsm`, so a
#: same-turn read did not wait for OCR and handed the model container bytes
#: decoded as prose.
EXTRACTABLE_SUFFIXES: tuple[str, ...] = tuple(
    sorted({*PDF_SUFFIXES, *IMAGE_SUFFIXES, *XLSX_SUFFIXES, *DOCX_SUFFIXES, *PPTX_SUFFIXES})
)


def _ext(filename: str | None) -> str:
    return os.path.splitext(str(filename or ""))[1].lower()


def _ct(content_type: str | None) -> str:
    return (content_type or "").split(";")[0].strip().lower()


def is_pdf(filename: str | None, content_type: str | None = None) -> bool:
    return _ct(content_type).startswith("application/pdf") or _ext(filename) in PDF_SUFFIXES


def is_image(filename: str | None, content_type: str | None = None) -> bool:
    return _ct(content_type).startswith("image/") or _ext(filename) in IMAGE_SUFFIXES


def is_xlsx(filename: str | None, content_type: str | None = None) -> bool:
    return _ct(content_type) in _XLSX_MIME or _ext(filename) in XLSX_SUFFIXES


def is_docx(filename: str | None, content_type: str | None = None) -> bool:
    return _ct(content_type) == _DOCX_MIME or _ext(filename) in DOCX_SUFFIXES


def is_pptx(filename: str | None, content_type: str | None = None) -> bool:
    return _ct(content_type) == _PPTX_MIME or _ext(filename) in PPTX_SUFFIXES


def is_office(filename: str | None, content_type: str | None = None) -> bool:
    """Word or PowerPoint (the zip formats — `DOCUMENT_MIME_TYPES`)."""
    return is_docx(filename, content_type) or is_pptx(filename, content_type) or _ct(content_type) in DOCUMENT_MIME_TYPES


def is_text(filename: str | None, content_type: str | None = None) -> bool:
    """Plain-text-ish: decodable without a parser."""
    return _ct(content_type) in TEXT_MIME_TYPES or _ct(content_type).startswith("text/") or _ext(filename) in TEXT_EXTENSIONS


# --- magic bytes --------------------------------------------------------------------

def looks_pdf(data: bytes) -> bool:
    return data[:5] == b"%PDF-"


def looks_image(data: bytes) -> bool:
    return (
        data[:3] == b"\xff\xd8\xff"  # JPEG
        or data[:4] == b"\x89PNG"
        or data[:4] == b"GIF8"
        or (data[:4] == b"RIFF" and data[8:12] == b"WEBP")
        or data[:2] == b"BM"
    )


def zip_kind(data: bytes) -> str | None:
    """Which Office-family document a zip is, by the part names near its start:
    ``xl/`` (workbook), ``word/`` (document), ``ppt/`` (presentation). ``None``
    for anything else — including a zip that is none of them."""
    if data[:2] != b"PK":
        return None
    head = data[:65536]
    if b"xl/" in head:
        return KIND_XLSX
    if b"word/" in head:
        return KIND_DOCX
    if b"ppt/" in head:
        return KIND_PPTX
    return None


def image_mime(filename: str | None, content_type: str | None, data: bytes) -> str:
    """The image's MIME type, from its bytes when they say, else the declared
    type, else PNG."""
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:4] == b"GIF8":
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    declared = _ct(content_type)
    return declared if declared.startswith("image/") else "image/png"


def kind(filename: str | None, content_type: str | None = None, data: bytes | None = None) -> str | None:
    """The document kind, or ``None`` when nothing here can read it.

    Name and type first; bytes when given and the name/type said nothing
    (or lied — a `.pdf` whose bytes are a JPEG is an image).
    """
    if data:
        if looks_pdf(data):
            return KIND_PDF
        if looks_image(data):
            return KIND_IMAGE
        zipped = zip_kind(data)
        if zipped:
            return zipped
    if is_pdf(filename, content_type):
        return KIND_PDF
    if is_image(filename, content_type):
        return KIND_IMAGE
    if is_xlsx(filename, content_type):
        return KIND_XLSX
    if is_docx(filename, content_type):
        return KIND_DOCX
    if is_pptx(filename, content_type):
        return KIND_PPTX
    if is_text(filename, content_type):
        return KIND_TEXT
    return None


def is_extractable(filename: str | None, content_type: str | None = None) -> bool:
    """Whether this file is turned into text by a parser or OCR (so it should
    end up with extracted text) — everything but plain text and the unknown."""
    return kind(filename, content_type) not in (None, KIND_TEXT)


def is_convertible(filename: str | None, content_type: str | None = None, data: bytes | None = None) -> bool:
    """Same question as `is_extractable`, with the bytes allowed to answer for a
    mislabelled part."""
    return kind(filename, content_type, data) not in (None, KIND_TEXT)

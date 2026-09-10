"""Document bytes → text.

The dispatch is `extract_text`; the readers are the module's public functions
so a caller with a known kind can call one directly. Everything CPU-bound
(rendering a page, decoding a photo, parsing a workbook) runs in a thread —
on the event loop it starves everything else, including a server's WebSocket
keepalive, which is how a multi-page scan once took a connection down.

Two things are the caller's:

* ``ocr(image_bytes, mime) -> text``: the vision model that reads a scanned
  page or a photo, with the caller's prompt and its own "no text" convention
  (return ``""``). Only the pages the text layer cannot read reach it.
* ``cache``: content-addressed (SHA-256 of the bytes) so the same file is
  never OCR'd twice. `MemoryTextCache` is the default; a deployment that
  already stores extracted text keys it by hash and passes its own.

Extractor errors PROPAGATE. Whether a failed extraction is "no text, carry on"
or a 422 is the caller's policy, not this module's; only "nothing here reads
this kind" returns ``""``.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol

from . import detect

logger = logging.getLogger(__name__)

Ocr = Callable[[bytes, str], Awaitable[str]]

#: A page whose text layer has fewer characters than this is treated as a scan.
MIN_PAGE_TEXT = 40
#: Rendering resolution for scanned pages. DPI buys input-token cost, not speed.
RENDER_DPI = 150
#: Scanned pages OCR'd concurrently — the main speed lever on a multi-page scan
#: (a 9-page scan: ~15 s at 4 in flight, ~5 s at 9).
OCR_CONCURRENCY = 8
#: Vision endpoints reject very large payloads (~10 MB base64 is a common cap,
#: and a phone photo of a report is 8–13 MB); text stays legible far below it.
MAX_OCR_IMAGE_BYTES = 6 * 1024 * 1024
MAX_OCR_IMAGE_EDGE_PX = 2200
#: Rows across all sheets of a workbook that make it into the text.
XLSX_ROW_BUDGET = 5000
#: Characters of a text file kept.
TEXT_CHAR_CAP = 100_000
#: Encodings a health document is actually saved in, in the order to try.
TEXT_ENCODINGS = ("utf-8-sig", "gbk", "gb2312", "latin-1")  # utf-8-sig reads plain UTF-8 too, and drops a BOM


class TextCache(Protocol):
    async def get(self, digest: str) -> str | None: ...
    async def put(self, digest: str, text: str) -> None: ...


class MemoryTextCache:
    """A bounded in-process cache: insertion-ordered dict, oldest evicted."""

    def __init__(self, cap: int = 64):
        self._cap = cap
        self._data: dict[str, str] = {}

    async def get(self, digest: str) -> str | None:
        return self._data.get(digest)

    async def put(self, digest: str, text: str) -> None:
        if digest in self._data:
            return
        while len(self._data) >= self._cap:
            del self._data[next(iter(self._data))]
        self._data[digest] = text


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- PDF ---------------------------------------------------------------------------

def _pdf_pages(data: bytes, *, min_page_text: int, dpi: int) -> tuple[list[str], list[tuple[int, bytes]]]:
    """Sync, CPU-bound: each page's text layer, and the pages too thin to trust
    rendered to PNG for OCR. Run through `asyncio.to_thread`."""
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(data)
    try:
        n = len(doc)
        texts: list[str] = [""] * n
        to_ocr: list[tuple[int, bytes]] = []
        for i in range(n):
            page = doc[i]
            try:
                text = (page.get_textpage().get_text_range() or "").strip()
                if len(text) >= min_page_text:
                    texts[i] = text
                else:
                    buf = io.BytesIO()
                    page.render(scale=dpi / 72).to_pil().save(buf, format="PNG")
                    to_ocr.append((i, buf.getvalue()))
            finally:
                page.close()
        return texts, to_ocr
    finally:
        doc.close()


def pdf_text_layer(data: bytes) -> str:
    """Sync: the embedded text layer only, every page, ``--- page N ---`` joined.
    Free and instant for born-digital PDFs; ``""`` for a scan."""
    texts, _ = _pdf_pages(data, min_page_text=1, dpi=RENDER_DPI)
    return "\n\n".join(f"--- page {i + 1} ---\n{t}" for i, t in enumerate(texts) if t)


async def pdf_text(
    data: bytes,
    *,
    ocr: Ocr | None = None,
    min_page_text: int = MIN_PAGE_TEXT,
    dpi: int = RENDER_DPI,
    concurrency: int = OCR_CONCURRENCY,
) -> str:
    """A PDF's full text: each page's text layer, or — for a page whose layer is
    empty or too thin (a scan) — the OCR of the rendered page, concurrently.
    Without an ``ocr`` the scanned pages are left out."""
    texts, to_ocr = await asyncio.to_thread(_pdf_pages, data, min_page_text=min_page_text, dpi=dpi)
    if to_ocr and ocr is not None:
        gate = asyncio.Semaphore(max(1, concurrency))
        failures: list[Exception] = []

        async def _one(index: int, png: bytes) -> None:
            async with gate:
                try:
                    texts[index] = (await ocr(png, "image/png")).strip()
                except Exception as exc:
                    failures.append(exc)
                    logger.warning("pdf ocr: page failed: page_index=%d error_type=%s", index, type(exc).__name__)

        await asyncio.gather(*(_one(i, png) for i, png in to_ocr))
        # One bad page must not lose the other twenty, so a page failure is a
        # warning. But a scan whose EVERY page failed, with no text layer to
        # fall back on, would come back as "" — the same answer as a blank
        # document — and the cause (no vision provider, a model that cannot
        # read images) would be visible only in this log line (#68). That
        # case is the OCR's error, raised.
        if len(failures) == len(to_ocr) and not any(texts):
            raise failures[-1]
    logger.info("pdf: page_count=%d ocr_page_count=%d", len(texts), len(to_ocr))
    return "\n\n".join(f"--- page {i + 1} ---\n{t}" for i, t in enumerate(texts) if t)


# --- images ------------------------------------------------------------------------

def downscale_image(data: bytes, mime: str, *, max_bytes: int = MAX_OCR_IMAGE_BYTES, max_edge: int = MAX_OCR_IMAGE_EDGE_PX) -> tuple[bytes, str]:
    """Sync: a photo too large for a vision endpoint, re-encoded as a JPEG no
    longer than ``max_edge`` on its long side. Returns the input unchanged when
    it is small enough or cannot be decoded (the endpoint then says so)."""
    if len(data) <= max_bytes:
        return data, mime
    try:
        from PIL import Image

        with Image.open(io.BytesIO(data)) as image:
            image = image.convert("RGB")
            edge = max(image.size)
            if edge > max_edge:
                scale = max_edge / edge
                image = image.resize((max(1, int(image.width * scale)), max(1, int(image.height * scale))))
            out = io.BytesIO()
            image.save(out, format="JPEG", quality=85)
        logger.info("image: downscaled for ocr: bytes_before=%d bytes_after=%d", len(data), out.tell())
        return out.getvalue(), "image/jpeg"
    except Exception as exc:
        logger.warning("image: downscale failed, sending the original: error_type=%s", type(exc).__name__)
        return data, mime


async def image_text(data: bytes, mime: str, *, ocr: Ocr) -> str:
    """OCR one image. The caller's ``ocr`` decides what "no text" returns."""
    data, mime = await asyncio.to_thread(downscale_image, data, mime or "image/png")
    return (await ocr(data, mime)).strip()


# --- spreadsheets --------------------------------------------------------------------

def xlsx_sheets(data: bytes) -> list[tuple[str, list[list[str]]]]:
    """Sync: every sheet as ``(name, rows)``, cells as strings, empty cells
    ``""``, all-empty rows dropped. Read-only, computed values."""
    from openpyxl import load_workbook

    workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        sheets: list[tuple[str, list[list[str]]]] = []
        for sheet in workbook.worksheets:
            rows = [["" if v is None else str(v) for v in row] for row in sheet.iter_rows(values_only=True)]
            sheets.append((sheet.title, [r for r in rows if any(c != "" for c in r)]))
        return sheets
    finally:
        workbook.close()


def _markdown_table(header: list[str], body: list[list[str]]) -> list[str]:
    lines = [f"| {' | '.join(header)} |", "|" + "|".join(["---"] * len(header)) + "|"]
    lines.extend(f"| {' | '.join(row)} |" for row in body)
    return lines


def xlsx_text_sync(data: bytes, *, row_budget: int = XLSX_ROW_BUDGET) -> str:
    """Sync: every non-empty sheet as a markdown table (first row = header),
    under one shared row budget so a huge workbook cannot blow up the text."""
    sheets = [(name, rows) for name, rows in xlsx_sheets(data) if rows]
    if not sheets:
        return ""
    parts: list[str] = []
    for name, rows in sheets:
        header, body = rows[0], rows[1:]
        take = min(len(body), max(row_budget, 0))
        parts.append(f"--- sheet: {name} ---")
        parts.extend(_markdown_table(header, body[:take]))
        row_budget -= take
        if len(body) > take:
            parts.append(f"... and {len(body) - take} more rows")
        parts.append("")
        if row_budget <= 0:
            parts.append("... (remaining sheets truncated)")
            break
    return "\n".join(parts).strip()


async def xlsx_text(data: bytes, *, row_budget: int = XLSX_ROW_BUDGET) -> str:
    return await asyncio.to_thread(xlsx_text_sync, data, row_budget=row_budget)


# --- Word / PowerPoint --------------------------------------------------------------

def _table_lines(rows) -> list[str]:
    lines: list[str] = []
    for r, row in enumerate(rows):
        cells = [(c.text or "").strip().replace("|", "/") for c in row.cells]
        lines.append("| " + " | ".join(cells) + " |")
        if r == 0:
            lines.append("|" + "---|" * len(cells))
    return lines


def docx_text_sync(data: bytes) -> str:
    """Sync: paragraphs (headings as markdown headings) and tables, in order of
    appearance. A lab report saved as .docx is text and a table, not a layout
    problem."""
    import docx

    document = docx.Document(io.BytesIO(data))
    parts: list[str] = []
    for para in document.paragraphs:
        text = (para.text or "").strip()
        if not text:
            continue
        style = (para.style.name or "") if para.style else ""
        if style.startswith("Heading"):
            level = style.removeprefix("Heading ").strip()
            parts.append("#" * (int(level) + 1 if level.isdigit() else 2) + f" {text}")
        else:
            parts.append(text)
    for i, table in enumerate(document.tables, 1):
        parts.append(f"## Table {i}")
        parts.extend(_table_lines(table.rows))
    return "\n".join(parts).strip()


def pptx_text_sync(data: bytes) -> str:
    """Sync: one ``## Slide N`` per slide with its text frames and tables."""
    from pptx import Presentation

    deck = Presentation(io.BytesIO(data))
    parts: list[str] = []
    for n, slide in enumerate(deck.slides, 1):
        parts.append(f"## Slide {n}")
        for shape in slide.shapes:
            if shape.has_text_frame:
                for para in shape.text_frame.paragraphs:
                    text = "".join(run.text or "" for run in para.runs).strip()
                    if text:
                        parts.append(text)
            if getattr(shape, "has_table", False):
                parts.extend(_table_lines(shape.table.rows))
    text = "\n".join(parts).strip()
    return text if any(not p.startswith("## Slide") for p in parts) else ""


async def office_text(data: bytes, which: str) -> str:
    return await asyncio.to_thread(docx_text_sync if which == detect.KIND_DOCX else pptx_text_sync, data)


# --- text ----------------------------------------------------------------------------

def decode_text(data: bytes, *, cap: int = TEXT_CHAR_CAP) -> str:
    """Text through the encodings a report is actually saved in; a stubborn
    file is decoded with replacement rather than refused; long files are cut
    with a note saying so."""
    text = None
    for encoding in TEXT_ENCODINGS:
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = data.decode("utf-8", errors="replace")
    if len(text) > cap:
        text = text[:cap] + f"\n\n... (truncated, total {len(data)} bytes)"
    return text.strip()


# --- the dispatch -------------------------------------------------------------------

async def extract_text(
    filename: str | None,
    content_type: str | None,
    data: bytes,
    *,
    ocr: Ocr | None = None,
    cache: TextCache | None = None,
    kinds: tuple[str, ...] | None = None,
    min_page_text: int = MIN_PAGE_TEXT,
    dpi: int = RENDER_DPI,
    ocr_concurrency: int = OCR_CONCURRENCY,
) -> str:
    """The text of one document, by `detect.kind`; ``""`` when nothing here reads
    that kind (or ``kinds`` excludes it). Cached by content digest for the kinds
    that cost a parser or a model call — never for plain text. The PDF knobs
    (scan threshold, render DPI, OCR concurrency) pass through to `pdf_text`.
    """
    which = detect.kind(filename, content_type, data)
    if which is None or (kinds is not None and which not in kinds):
        return ""
    if which == detect.KIND_TEXT:
        return decode_text(data)
    key = digest(data) if cache is not None else None
    if key is not None:
        cached = await cache.get(key)
        if cached is not None:
            return cached
    if which == detect.KIND_PDF:
        text = await pdf_text(data, ocr=ocr, min_page_text=min_page_text, dpi=dpi, concurrency=ocr_concurrency)
    elif which == detect.KIND_IMAGE:
        if ocr is None:
            return ""
        text = await image_text(data, detect.image_mime(filename, content_type, data), ocr=ocr)
    elif which == detect.KIND_XLSX:
        text = await xlsx_text(data)
    else:
        text = await office_text(data, which)
    if key is not None and text:
        await cache.put(key, text)
    return text

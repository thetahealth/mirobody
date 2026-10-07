"""Document bytes → text.

The dispatch is `extract_text`; the readers are the module's public functions
so a caller with a known kind can call one directly. Everything CPU-bound
(rendering a page, decoding a photo, parsing a workbook) runs in a thread:
on the event loop it starves everything else, including a server's WebSocket
keepalive, which is how a multi-page scan once took a connection down.

Two things are the caller's:

* ``ocr(image_bytes, mime) -> text``: the vision model that reads a scanned
  page or a photo, with the caller's prompt and its own "no text" convention
  (return ``""``). Only the pages the text layer cannot read reach it.
* ``cache``: content-addressed (SHA-256 of the bytes) so the same file is
  never OCR'd twice. Without one nothing is cached; `MemoryTextCache` is a
  bounded in-process one, and a deployment that already stores extracted text
  keys it by hash and passes its own.

Extractor errors PROPAGATE. Whether a failed extraction is "no text, carry on"
or a 422 is the caller's policy, not this module's; only "nothing here reads
this kind" returns ``""``. A PDF some of whose pages failed comes back as a
`PartialText`, which is never cached, so the next upload of it reads them again.
"""

from __future__ import annotations

import asyncio
import codecs
import ctypes
import hashlib
import html
import io
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from . import detect, render

logger = logging.getLogger(__name__)

Ocr = Callable[[bytes, str], Awaitable[str]]

#: A page whose text layer has fewer characters than this is treated as a scan.
MIN_PAGE_TEXT = 40
#: Rendering resolution for scanned pages. DPI buys input-token cost, not speed.
RENDER_DPI = 150
#: Scanned pages OCR'd concurrently: the main speed lever on a multi-page scan
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
#: Encodings a health document is actually saved in, in the order to try:
#: utf-8-sig reads plain UTF-8 too and drops a BOM; GBK holds every GB2312 file.
TEXT_ENCODINGS = ("utf-8-sig", "gbk")


class PartialText(str):
    """The text of a document some of whose pages could not be read, as a
    `str` every caller reads as before. `extract_text` never caches one, and a
    caller that stores text by content hash must not either: cached, the pages
    that failed were never read again for the same bytes."""

    missing_pages: tuple[int, ...]

    def __new__(cls, text: str, missing_pages: tuple[int, ...]) -> PartialText:
        partial = super().__new__(cls, text)
        partial.missing_pages = missing_pages
        return partial


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


# --- a text layer's tables ----------------------------------------------------------
#
# A text layer keeps every character but writes a table row as one line of words
# (`Hemoglobin(HGB) 138 g/L 115--150 02`), so `table_indicators` could not tell
# its columns apart and a born-digital report went whole to the model. The
# characters' positions still say where each cell is: a wide gap starts the next
# cell, lines are laid in columns by where their cells overlap, and the result is
# the HTML table the rules read from an OCR tables pass, every character the layer's.

#: Two runs of one line further apart than this many font sizes are two cells.
#: A word space is about a third of the font size; the closest columns in the
#: corpus's check-up books are 0.9 of it apart (`Status` | `Unit`, 8.5 pt at 9.5 pt).
_CELL_GAP = 0.6
#: A line whose baseline is closer than this many font sizes below a table
#: line, with fewer cells, each under one of that line's, is a cell wrapped
#: onto a second line (`10^12/` over `L`, `Normal` over `Range`): 1.15 to 1.4
#: font sizes in the corpus's books, where the next row is 1.6 below.
_WRAP_PITCH = 1.5
#: Characters on one line: baselines this close, in font sizes (a superscript
#: is raised about a third of one).
_SAME_LINE = 0.45


#: A line that opens with a label and its colon (`Exam: | annual physical
#: (clinic)`) names a field of the report, never a row of its table. Kept
#: over a check-up's header, every row's long name lay across its two cells,
#: and `_gridded` dropped the nine rows instead of the label
#: (demo/upload/you_annual_checkup_2026-05.pdf).
_LABEL = re.compile(r"[:：]$")

#: A page's running footer or header, never a table row: `Page 2 of 11 |
#: Printed 2026-02-14 11:37:08` went on under the table above it as a row, and
#: the print time was read as a value of 2026 (corpus p004_2026-02-14_e10a).
_PAGE_MARK = re.compile(r"(?i)^(?:page\s*\d+(?:\s*(?:of|/)\s*\d+)?|第\s*\d+\s*页.*)$")


@dataclass
class _Run:
    """Characters of one line with no wide gap between them: a cell, or part of one."""

    text: str
    left: float
    right: float
    y: float  # the baseline
    size: float


def _runs(textpage) -> list[_Run]:
    """The page's characters as runs, in the text layer's own order and with
    its own spaces (printed or inferred by pdfium), so a cell reads exactly as
    the layer does (`参考值(范围)`, not `参考值 ( 范围 )`)."""
    import pypdfium2.raw as raw

    left, right, bottom, top = (ctypes.c_double() for _ in range(4))
    x, y = ctypes.c_double(), ctypes.c_double()
    runs: list[_Run] = []
    current: _Run | None = None
    for i in range(raw.FPDFText_CountChars(textpage)):
        code = raw.FPDFText_GetUnicode(textpage, i)
        ch = chr(code) if 0 < code < 0x110000 else ""
        if not ch or ch in "\r\n":
            current = None
            continue
        if ch.isspace():
            if current is not None and not current.text.endswith(" "):
                current.text += " "
            continue
        raw.FPDFText_GetCharBox(textpage, i, left, right, bottom, top)
        raw.FPDFText_GetCharOrigin(textpage, i, x, y)
        size = raw.FPDFText_GetFontSize(textpage, i)
        if size < 2:  # a size set in the text matrix, not the font: the glyph is the measure
            size = max(top.value - bottom.value, 1.0) * 1.4
        if (current is None or abs(y.value - current.y) > _SAME_LINE * size
                or left.value < current.right - 0.5 * size
                or left.value - current.right > _CELL_GAP * size):
            current = _Run(ch, left.value, right.value, y.value, size)
            runs.append(current)
        else:
            current.text += ch
            current.right = max(current.right, right.value)
    for run in runs:
        run.text = run.text.strip()
    return runs


def _lines(runs: list[_Run]) -> list[list[_Run]]:
    """Runs grouped by baseline, top of the page first, each line left to
    right with the runs closer than a cell gap joined (a font change inside a
    cell, `平均红细胞血红蛋白量（` then `MCH`, is two runs)."""
    lines: list[list[_Run]] = []
    for run in sorted(runs, key=lambda r: (-r.y, r.left)):
        if lines and abs(lines[-1][0].y - run.y) <= _SAME_LINE * run.size:
            lines[-1].append(run)
        else:
            lines.append([run])
    out = []
    for line in lines:
        cells: list[_Run] = []
        for run in sorted(line, key=lambda r: r.left):
            last = cells[-1] if cells else None
            if last is not None and run.left - last.right <= _CELL_GAP * max(last.size, run.size):
                last.text += (" " if run.left - last.right > 0.25 * run.size else "") + run.text
                last.right = max(last.right, run.right)
            else:
                cells.append(_Run(run.text, run.left, run.right, run.y, run.size))
        out.append(cells)
    return out


def _wrapped_onto(upper: str, lower: str) -> str:
    """A cell's two lines as one: a word wraps at a space, a unit or a CJK
    name at no space (`10^12/` + `L`)."""
    tight = upper[-1:] in "/^-(（" or lower[:1] in ")）" or (upper[-1:] >= "⺀" and lower[:1] >= "⺀")
    return upper + ("" if tight else " ") + lower


def _unwrapped(lines: list[list[_Run]]) -> list[list[_Run]]:
    """`lines` with every wrapped cell's second line joined to the cell above it."""
    out: list[list[_Run]] = []
    last_y: list[float] = []
    for line in lines:
        above = out[-1] if out else None
        if above is not None and len(above) >= 2 and len(line) < len(above) \
                and last_y[-1] - line[0].y < _WRAP_PITCH * line[0].size:
            slack = 0.5 * line[0].size
            targets = [next((a for a in above if abs(a.left - r.left) <= slack
                             or (a.left <= r.left and r.right <= a.right + slack)), None) for r in line]
            if all(t is not None for t in targets) and len({id(t) for t in targets}) == len(targets):
                for a, r in zip(targets, line, strict=True):
                    a.text = _wrapped_onto(a.text, r.text)
                    a.right = max(a.right, r.right)
                last_y[-1] = line[0].y
                continue
        out.append(line)
        last_y.append(line[0].y)
    return out


def _overlaps(a: _Run, b: _Run) -> bool:
    return a.left < b.right and b.left < a.right


def _gridded(block: list[list[_Run]]) -> list[list[_Run]]:
    """`block` less the lines whose cells lie across two cells of other lines:
    a title or a date line over the table, not a row of it. The line across
    the most is dropped first, so a long name under a date line's two cells
    stays (the date line goes, and nothing is across anything). Kept, the
    date line over a slip's header (`报告日期：… | 报告审核时间：…`) joined
    the row-number and name columns under it into one, and none of its rows
    was read (corpus p005_2024-11-25_e02a)."""
    lines = list(block)
    while lines:
        across = [sum(1 for c in line for other in lines
                      if other is not line and sum(_overlaps(c, d) for d in other) >= 2) for line in lines]
        if max(across) == 0:
            break
        lines.pop(across.index(max(across)))
    return lines


def _columns(block: list[list[_Run]]) -> list[tuple[float, float]]:
    """The columns of consecutive table lines: the spans their cells overlap on."""
    spans = sorted((r.left, r.right) for line in block for r in line)
    cols: list[list[float]] = []
    for left, right in spans:
        if cols and left < cols[-1][1]:
            cols[-1][1] = max(cols[-1][1], right)
        else:
            cols.append([left, right])
    return [(a, b) for a, b in cols]


def _fits(block: list[list[_Run]], cols: list[tuple[float, float]]) -> bool:
    """Whether every cell of `block` sits under exactly one of `cols`, no two
    cells of a line under the same one: a table going on under the columns of
    one before it (a page that opens mid-panel, its header on the page before)."""
    for line in block:
        taken: set[int] = set()
        for r in line:
            hit = [k for k, (a, b) in enumerate(cols) if r.left < b and r.right > a]
            if len(hit) != 1 or hit[0] in taken:
                return False
            taken.add(hit[0])
    return True


def _table_html(block: list[list[_Run]], cols: list[tuple[float, float]]) -> str:
    rows = []
    for line in block:
        cells = [""] * len(cols)
        for r in line:
            k = max(range(len(cols)), key=lambda k: min(r.right, cols[k][1]) - max(r.left, cols[k][0]))
            cells[k] = f"{cells[k]} {r.text}".strip()
        rows.append("<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>")
    return "<table>" + "".join(rows) + "</table>"


def _heads(line: list[_Run], block: list[list[_Run]]) -> bool:
    """Whether a line of its own, a gap above `block`, is that block's header:
    one cell over each of its columns. A section title or a blank line
    between a header and its rows (`Test Item | Result | Unit` / `Liver
    function` / the rows) used to leave the header a line alone, not a table."""
    cols = _columns(block)
    return len(line) == len(cols) and _fits([line], cols)


def _layer_tables(textpage, carried: list[tuple[float, float]] | None
                  ) -> tuple[str, list[tuple[float, float]] | None]:
    """(the page's tables as HTML, or "", the columns the next page may go on
    under). A table is two or more consecutive lines of two or more cells; one
    such line alone is a table only when it goes on under `carried`, or heads
    the lines below a gap (`_heads`)."""
    lines = _unwrapped(_lines(_runs(textpage)))
    blocks: list[list[list[_Run]]] = [[]]
    for line in lines:
        if len(line) >= 2 and not _LABEL.search(line[0].text) and not any(_PAGE_MARK.match(r.text) for r in line):
            blocks[-1].append(line)
        elif blocks[-1]:
            blocks.append([])
    tables = []
    lone: list[_Run] | None = None
    for block in (g for b in blocks if (g := _gridded(b))):
        if lone is not None and _heads(lone, block):
            block = [lone, *block]
        lone = None
        if carried is not None and _fits(block, carried):
            cols = carried
        elif len(block) >= 2:
            cols = _columns(block)
        else:
            lone = block[0]
            continue
        tables.append(_table_html(block, cols))
        carried = cols
    return "\n".join(tables), carried


# --- PDF ---------------------------------------------------------------------------

def _pdf_pages(data: bytes, *, min_page_text: int, dpi: int, render_all: bool = False, layer_tables: bool = False
               ) -> tuple[list[str], list[tuple[int, bytes]], list[tuple[int, bytes]]]:
    """Sync, CPU-bound: each page's text layer, the pages too thin to trust
    rendered to PNG for OCR, and (`render_all`) the others rendered too, for a
    tables pass. With `layer_tables`, a text page whose layer lays out a
    table gets that table appended as HTML (`_layer_tables`) and is not
    rendered: its tables are read off the layer, exactly. Run through
    `asyncio.to_thread`."""
    import pypdfium2 as pdfium

    def png(page) -> bytes:
        buf = io.BytesIO()
        page.render(scale=dpi / 72).to_pil().save(buf, format="PNG")
        return buf.getvalue()

    doc = pdfium.PdfDocument(data)
    try:
        n = len(doc)
        texts: list[str] = [""] * n
        to_ocr: list[tuple[int, bytes]] = []
        layered: list[tuple[int, bytes]] = []
        carried = None
        for i in range(n):
            page = doc[i]
            try:
                textpage = page.get_textpage()
                text = (textpage.get_text_range() or "").strip()
                if len(text) >= min_page_text:
                    found = ""
                    if layer_tables:
                        found, carried = _layer_tables(textpage, carried)
                    texts[i] = f"{text}\n\n{found}" if found else text
                    if render_all and not found:
                        layered.append((i, png(page)))
                else:
                    carried = None
                    to_ocr.append((i, png(page)))
            finally:
                page.close()
        return texts, to_ocr, layered
    finally:
        doc.close()


async def pdf_text(
    data: bytes,
    *,
    ocr: Ocr | None = None,
    tables: Ocr | None = None,
    min_page_text: int = MIN_PAGE_TEXT,
    dpi: int = RENDER_DPI,
    concurrency: int = OCR_CONCURRENCY,
) -> str:
    """A PDF's full text: each page's text layer, or (for a page whose layer is
    empty or too thin (a scan)) the OCR of the rendered page, concurrently.
    Without an ``ocr`` the scanned pages are left out, and a page whose OCR
    failed is too: the text is then a `PartialText` naming them. A page that
    has a text layer also gets its tables as HTML: the layer's own
    (`_layer_tables`) where its characters lie in columns, else, with
    ``tables`` (an OCR model's tables pass), that pass's reading of the
    rendered page.

    Measured on the 16 text-layer PDFs of the seed-7 corpus (979 printed rows,
    scored with benchmarks/local_ocr's own checks): the table rules read none
    of them off the layer alone and 920 off its tables, 919 with the printed
    unit and 918 with the printed range, and no row the documents do not
    print; what they leave is blood-pressure pairs and `label: value` prose.
    On the six such pages the OCR benchmark ran GLM-OCR's tables pass on, the
    layer's tables gave the rules as many rows or more (34 against 7 on one)
    with no model call."""
    texts, to_ocr, layered = await asyncio.to_thread(
        _pdf_pages, data, min_page_text=min_page_text, dpi=dpi, render_all=tables is not None,
        layer_tables=True)
    layer_table_page_count = sum(1 for t in texts if "<table>" in t)
    gate = asyncio.Semaphore(max(1, concurrency))
    if layered and tables is not None:
        async def _tables(index: int, png: bytes) -> None:
            async with gate:
                try:
                    found = (await tables(png, "image/png")).strip()
                except Exception as exc:
                    logger.warning("pdf tables: page failed: page_index=%d error_type=%s", index, type(exc).__name__)
                    return
                if found:
                    texts[index] = f"{texts[index]}\n\n{found}"

        await asyncio.gather(*(_tables(i, png) for i, png in layered))
    failed: list[tuple[int, Exception]] = []
    if to_ocr and ocr is not None:

        async def _one(index: int, png: bytes) -> None:
            async with gate:
                try:
                    texts[index] = (await ocr(png, "image/png")).strip()
                except Exception as exc:
                    failed.append((index, exc))
                    logger.warning("pdf ocr: page failed: page_index=%d error_type=%s", index, type(exc).__name__)

        await asyncio.gather(*(_one(i, png) for i, png in to_ocr))
        # One bad page must not lose the other twenty, so a page failure is a
        # warning. But a scan whose EVERY page failed, with no text layer to
        # fall back on, would come back as "": the same answer as a blank
        # document, and the cause (no vision provider, a model that cannot
        # read images) would be visible only in this log line (#68). That
        # case is the OCR's error, raised.
        if len(failed) == len(to_ocr) and not any(texts):
            raise failed[-1][1]
    missing = sorted(i for i, _ in failed) if ocr is not None else [i for i, _ in to_ocr]
    logger.info("pdf: page_count=%d ocr_page_count=%d missing_page_count=%d table_page_count=%d "
                "layer_table_page_count=%d", len(texts), len(to_ocr), len(missing), len(layered),
                layer_table_page_count)
    text = "\n\n".join(f"--- page {i + 1} ---\n{t}" for i, t in enumerate(texts) if t)
    return PartialText(text, tuple(i + 1 for i in missing)) if missing else text


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
            image = render.flatten(image)
            edge = max(image.size)
            if edge > max_edge:
                scale = max_edge / edge
                image = image.resize((max(1, int(image.width * scale)), max(1, int(image.height * scale))))
            out = io.BytesIO()
            image.save(out, format="JPEG", quality=85)
        downscaled_bytes = out.tell()
        logger.info("image: downscaled for ocr: bytes_before=%d bytes_after=%d", len(data), downscaled_bytes)
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


def _md_row(cells: list[str]) -> str:
    """One markdown table row. A cell's line breaks become spaces and its `|`
    a `/`: kept, a header cell written `Reference\\nrange` split the row over two
    lines and a `115|150` cell added a column, and the table rules then read
    every cell after it under the wrong header."""
    return "| " + " | ".join(" ".join(c.split()).replace("|", "/") for c in cells) + " |"


def _markdown_table(header: list[str], body: list[list[str]]) -> list[str]:
    return [_md_row(header), "|" + "---|" * len(header), *(_md_row(row) for row in body)]


def xlsx_text_sync(data: bytes, *, row_budget: int = XLSX_ROW_BUDGET) -> str:
    """Sync: every non-empty sheet as a markdown table (first row = header),
    under one shared row budget so a huge workbook cannot blow up the text."""
    sheets = [(name, rows) for name, rows in xlsx_sheets(data) if rows]
    parts: list[str] = []
    for k, (name, rows) in enumerate(sheets):
        header, body = rows[0], rows[1:]
        take = min(len(body), max(row_budget, 0))
        parts.append(f"--- sheet: {name} ---")
        parts.extend(_markdown_table(header, body[:take]))
        row_budget -= take
        if len(body) > take:
            parts.append(f"... and {len(body) - take} more rows")
        parts.append("")
        if row_budget <= 0 and k + 1 < len(sheets):
            parts.append("... (remaining sheets truncated)")
            break
    return "\n".join(parts).strip()


async def xlsx_text(data: bytes, *, row_budget: int = XLSX_ROW_BUDGET) -> str:
    return await asyncio.to_thread(xlsx_text_sync, data, row_budget=row_budget)


# --- Word / PowerPoint --------------------------------------------------------------

def _table_lines(rows) -> list[str]:
    cells = [[c.text or "" for c in row.cells] for row in rows]
    return _markdown_table(cells[0], cells[1:]) if cells else []


def docx_text_sync(data: bytes) -> str:
    """Sync: paragraphs (headings as markdown headings) and tables, in order of
    appearance. A lab report saved as .docx is text and a table, not a layout
    problem. Read all paragraphs first and all tables after, every panel's
    table landed under the document's last heading, away from the one naming it."""
    import docx
    from docx.table import Table

    document = docx.Document(io.BytesIO(data))
    parts: list[str] = []
    tables = 0
    for block in document.iter_inner_content():
        if isinstance(block, Table):
            tables += 1
            parts.append(f"## Table {tables}")
            parts.extend(_table_lines(block.rows))
            continue
        text = (block.text or "").strip()
        if not text:
            continue
        style = (block.style.name or "") if block.style else ""
        if style.startswith("Heading"):
            level = style.removeprefix("Heading ").strip()
            parts.append("#" * (int(level) + 1 if level.isdigit() else 2) + f" {text}")
        else:
            parts.append(text)
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

def _decoded(data: bytes) -> str:
    """`data` as text: UTF-16 when its byte-order mark says so (Excel's
    "Unicode text" export), else the first of `TEXT_ENCODINGS` that reads
    it, else latin-1, which reads any bytes. latin-1 used to be tried before
    the mark was looked at, so a UTF-16 export came back as `ÿþH\\x00e\\x00`."""
    if data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return data.decode("utf-16", errors="replace")
    for encoding in TEXT_ENCODINGS:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1")


def decode_text(data: bytes, *, cap: int = TEXT_CHAR_CAP) -> str:
    """Text through the encodings a report is actually saved in; a stubborn
    file is decoded rather than refused; long files are cut with a note
    saying so."""
    text = _decoded(data)
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
    tables: Ocr | None = None,
    cache: TextCache | None = None,
    kinds: tuple[str, ...] | None = None,
    min_page_text: int = MIN_PAGE_TEXT,
    dpi: int = RENDER_DPI,
    ocr_concurrency: int = OCR_CONCURRENCY,
) -> str:
    """The text of one document, by `detect.kind`; ``""`` when nothing here reads
    that kind (or ``kinds`` excludes it). Cached by content digest for the kinds
    that cost a parser or a model call, never for plain text and never when
    pages are missing (`PartialText`). The PDF knobs (scan threshold, render
    DPI, OCR concurrency) pass through to `pdf_text`.
    """
    # The WebSocket upload (the only path the web client uses) accumulates
    # chunks into a `bytearray` (`file_upload_manager`), and pypdfium2 answers
    # `TypeError: Invalid input type 'bytearray'`, so EVERY PDF uploaded through
    # the product failed to extract, on every key. Normalised here because this
    # is the one entry every `kind` goes through; the storage backends
    # (`utils/config/storage/local.py`, `aliyun.py`) each already do the same
    # conversion for their own consumer, which is how the type was known and
    # this door still missed.
    if isinstance(data, (bytearray, memoryview)):
        data = bytes(data)

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
        text = await pdf_text(data, ocr=ocr, tables=tables, min_page_text=min_page_text, dpi=dpi, concurrency=ocr_concurrency)
    elif which == detect.KIND_IMAGE:
        if ocr is None:
            return ""
        text = await image_text(data, detect.image_mime(filename, content_type, data), ocr=ocr)
    elif which == detect.KIND_XLSX:
        text = await xlsx_text(data)
    else:
        text = await office_text(data, which)
    if key is not None and text and not isinstance(text, PartialText):
        await cache.put(key, text)
    return text

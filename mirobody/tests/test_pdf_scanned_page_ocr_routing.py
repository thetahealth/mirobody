"""Synthetic reproduction and regression coverage for #304: a scanned lab
page stamped with a printed header and footer has a text layer that clears
``MIN_PAGE_TEXT`` on those two lines alone, so the character-count rule in
``mirobody.documents.extract._pdf_pages`` read it as a born-digital page and
never rendered the table image behind it for OCR. Also covers the review's
two follow-ups: a page whose large image has real text drawn ON it (a
background graphic under a born-digital report, or a searchable scan's own
invisible OCR layer) must stay on its text layer, not be re-OCR'd; and a
page the new rule routes to OCR must still give its own layer text back
when there is no ``ocr`` to call, exactly as main read it before this rule
existed. The fixtures below are built in-process with pypdfium2 (already a
``[parse]`` dependency), not loaded from a file: an A4 page can carry a
raster "lab table" image, a two-line header/footer text layer, and/or a
born-digital paragraph placed in the margin or over the image, in any
combination, which is enough to exercise every branch of the routing rule
without a committed binary. Skipped outright on an install without
``[parse]`` (pypdfium2, Pillow), the same way `test_extraction_regressions.py`
skips without `[app]`.
"""

import ctypes
import io

import pytest

pytest.importorskip("pypdfium2")
pytest.importorskip("PIL")

import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_raw
from PIL import Image, ImageDraw

from mirobody.documents import extract

PAGE_W, PAGE_H = 595.0, 842.0  # A4, in PDF points (1/72 in)

LAB_ROWS = (
    "Test            Result   Unit    Range",
    "Hemoglobin      138      g/L     115-150",
    "WBC             6.2      10^9/L  4.0-10.0",
    "Platelets       230      10^9/L  150-400",
)
HEADER = "Sunrise Checkup Centre - Lab Report"
FOOTER = "Page 3 of 14 Printed 2026-07-02 09:15"
BODY_TEXT = (
    "Physical Examination\n"
    "Blood pressure 118/76 mmHg, pulse 72 bpm, temperature 36.6 C.\n"
    "General appearance normal, no acute distress noted.\n"
    "Conclusion: overall health within expected range for age."
)


def _lab_table_jpeg() -> bytes:
    image = Image.new("RGB", (1200, 60 * len(LAB_ROWS) + 40), "white")
    draw = ImageDraw.Draw(image)
    y = 20
    for row in LAB_ROWS:
        draw.text((20, y), row, fill="black")
        y += 60
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def _add_image(pdf: pdfium.PdfDocument, page: pdfium.PdfPage, jpeg: bytes,
                box: tuple[float, float, float, float]) -> None:
    left, bottom, right, top = box
    obj = pdfium.PdfImage.new(pdf)
    obj.load_jpeg(io.BytesIO(jpeg), pages=[page])
    obj.set_matrix(pdfium.PdfMatrix(right - left, 0, 0, top - bottom, left, bottom))
    page.insert_obj(obj)


def _add_text(pdf: pdfium.PdfDocument, page: pdfium.PdfPage, text: str, x: float, y: float,
              size: float = 10.0) -> None:
    raw_obj = pdfium_raw.FPDFPageObj_NewTextObj(pdf, b"Helvetica", ctypes.c_float(size))
    obj = pdfium.PdfTextObj(raw_obj, pdf=pdf)
    wide = (text + "\x00").encode("utf-16-le")
    buf = ctypes.create_string_buffer(wide, len(wide))
    pdfium_raw.FPDFText_SetText(obj, ctypes.cast(buf, ctypes.POINTER(ctypes.c_ushort)))
    obj.set_matrix(pdfium.PdfMatrix(1, 0, 0, 1, x, y))
    page.insert_obj(obj)


def _build_pdf(*, image_box: tuple[float, float, float, float] | None = None,
               header_footer: bool = False, body_text: str | None = None,
               overlay_text: str | None = None) -> bytes:
    """One A4 page combining, independently: a raster image at `image_box`
    (left, bottom, right, top, in PDF points); a two-line header/footer text
    layer placed OUTSIDE it, in the margin (the #304 scenario); a
    born-digital paragraph placed the same way, above any image; and
    `overlay_text`, placed INSIDE `image_box` instead, line by line, which is
    what a background graphic under real body text or a searchable scan's own
    invisible OCR layer looks like geometrically. A scan covering most of the
    page (`image_box` close to the full sheet) plus `header_footer=True` and
    nothing else is the issue's reproduction; the same `image_box` with
    `overlay_text` is the review's "text drawn ON the image" case; a small
    `image_box` with `body_text` is a born-digital page with a logo."""
    return _build_pages({"image_box": image_box, "header_footer": header_footer,
                             "body_text": body_text, "overlay_text": overlay_text})


def _build_pages(*pages: dict) -> bytes:
    """A PDF of one page per dict, each drawn as `_build_pdf` draws its one."""
    pdf = pdfium.PdfDocument.new()
    for spec in pages:
        _draw_page(pdf, pdf.new_page(PAGE_W, PAGE_H), **spec)
    buf = io.BytesIO()
    pdf.save(buf)
    pdf.close()
    return buf.getvalue()


def _draw_page(pdf: pdfium.PdfDocument, page: pdfium.PdfPage, *,
               image_box: tuple[float, float, float, float] | None = None,
               header_footer: bool = False, body_text: str | None = None,
               overlay_text: str | None = None) -> None:
    if image_box is not None:
        _add_image(pdf, page, _lab_table_jpeg(), image_box)
    if header_footer:
        _add_text(pdf, page, HEADER, 40, 800)
        _add_text(pdf, page, FOOTER, 40, 60, size=9)
    if body_text is not None:
        y = 760.0
        for line in body_text.splitlines():
            _add_text(pdf, page, line, 40, y, size=11)
            y -= 20
    if overlay_text is not None:
        assert image_box is not None, "overlay_text is only meaningful drawn inside an image_box"
        left, bottom, right, top = image_box
        assert right - left > 300 and top - bottom > 200, "fixture must give the overlay room to stay inside the box"
        y = top - 40.0
        for line in overlay_text.splitlines():
            _add_text(pdf, page, line, left + 20.0, y, size=11)
            y -= 20
            assert y > bottom, "overlay_text ran past the bottom of image_box; shorten it or enlarge the box"
    page.gen_content()


#: A scan with margins, well past SCAN_IMAGE_AREA_FRACTION (0.5) of the A4
#: page (515 x 680 of 595 x 842, about 0.74).
SCAN_BOX = (20.0, 80.0, 575.0, 760.0)
#: A letterhead logo, well under it (50 x 35 of the same page, about 0.003).
LOGO_BOX = (40.0, 780.0, 90.0, 815.0)
#: A paragraph long enough to clear MAX_OVERLAID_SCAN_CHARS (20) comfortably
#: once drawn inside SCAN_BOX, simulating a background graphic under real
#: report text or a searchable scan's own invisible OCR layer.
OVERLAY_TEXT = (
    "Checkup Centre Letterhead Background\n"
    "Hemoglobin 138 g/L\n"
    "WBC 6.2 10^9/L\n"
    "Platelets 230 10^9/L"
)


class FakeOcr:
    """Records every page it was asked to read and returns a fixed result,
    so a test can assert both the call count and what the caller saw."""

    def __init__(self, result: str):
        self.result = result
        self.calls = 0

    async def __call__(self, image_bytes: bytes, mime: str) -> str:
        self.calls += 1
        assert mime == "image/png"
        assert image_bytes
        return self.result


# --- _pdf_pages: the routing decision itself, no OCR call ------------------

def test_scan_with_no_text_layer_is_routed_to_ocr():
    data = _build_pdf(image_box=SCAN_BOX)
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert [i for i, _, _ in to_ocr] == [0]
    assert texts[0] == ""


def test_scan_with_header_footer_text_layer_is_still_routed_to_ocr():
    """The bug: a printed header and footer pass MIN_PAGE_TEXT (40 chars) on
    their own, so the scanned table behind them must not be read as a
    born-digital page on that count alone."""
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)
    layer_chars = len(f"{HEADER}\n{FOOTER}")
    assert layer_chars >= extract.MIN_PAGE_TEXT, "fixture must reproduce the issue: layer text alone clears the gate"
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert [i for i, _, _ in to_ocr] == [0]
    # The layer text is carried along for pdf_text to fold in, not discarded.
    assert HEADER in to_ocr[0][2]
    assert FOOTER in to_ocr[0][2]


def test_born_digital_page_with_a_logo_stays_on_its_text_layer():
    data = _build_pdf(image_box=LOGO_BOX, body_text=BODY_TEXT)
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert to_ocr == []
    assert "Physical Examination" in texts[0]


def test_background_image_with_real_text_over_it_stays_on_its_text_layer():
    """Review point 2: image area alone is the wrong signal. A letterhead
    template or watermark can cover most of a born-digital page, with the
    report's real text drawn on top of it; that text is NOT a margin stamp,
    so the page must stay on its layer, not pay for an OCR call that could
    misread a digit the layer already has exactly right."""
    data = _build_pdf(image_box=SCAN_BOX, overlay_text=OVERLAY_TEXT)
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert to_ocr == []
    assert "Hemoglobin" in texts[0]


def test_searchable_scan_with_an_invisible_text_layer_stays_on_its_text_layer():
    """A scan a prior OCR pass already made searchable: the same full-page
    image, with a text layer reproducing the scanned rows positioned over
    it (invisible text, same geometry as `overlay_text`). Re-OCRing it would
    be pure waste and risks a worse reading than the layer it already has."""
    data = _build_pdf(image_box=SCAN_BOX, overlay_text="\n".join(LAB_ROWS))
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert to_ocr == []
    assert "Hemoglobin" in texts[0]


#: #317: a watermark laid over a scan as a text layer, long enough to clear
#: MAX_OVERLAID_SCAN_CHARS, with no value in it.
WATERMARK = "CONFIDENTIAL PATIENT RECORD DO NOT DISTRIBUTE OUTSIDE THIS CLINIC"


def test_scan_under_a_watermark_text_layer_is_routed_to_ocr():
    """#317: the watermark's 65 characters sit on the image, so the page read
    as a searchable scan and the lab table in the image was lost with no
    warning. Text on the image with no digit is not the page's content."""
    data = _build_pdf(image_box=SCAN_BOX, overlay_text=WATERMARK)
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert [page for page, _, _ in to_ocr] == [0]
    assert to_ocr[0][2] == WATERMARK  # kept for `pdf_text` to fold in or fall back to


#: The last page of a searchable scan or a letterhead report: signatures, no
#: value, so no digit. The shape mirobody-gen's two-page reports print.
SIGNATURES = "Performed By: Karen Warren\nVerified By: Karen Barker"


@pytest.mark.parametrize("order", ["table first", "signatures first"])
def test_a_searchable_scans_signature_page_stays_on_its_layer(order):
    """#317's digit rule sent this page to OCR for nothing: another image page
    of the same PDF already shows the layer is a transcription."""
    table = {"image_box": SCAN_BOX, "overlay_text": "\n".join(LAB_ROWS)}
    signed = {"image_box": SCAN_BOX, "overlay_text": SIGNATURES}
    data = _build_pages(*([table, signed] if order == "table first" else [signed, table]))
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert to_ocr == []
    assert any("Karen Barker" in t for t in texts)


def test_a_watermark_on_every_page_of_a_scan_still_goes_to_ocr():
    """No page's layer holds a value, so none vouches for another (#317)."""
    data = _build_pages(*[{"image_box": SCAN_BOX, "overlay_text": WATERMARK}] * 2)
    _, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert [page for page, _, _ in to_ocr] == [0, 1]


def test_a_born_digital_page_does_not_vouch_for_a_watermarked_scan():
    """Only an IMAGE page whose layer held content says the document's layer
    over its images is a transcription; a text page says nothing about it."""
    data = _build_pages({"body_text": BODY_TEXT}, {"image_box": SCAN_BOX, "overlay_text": WATERMARK})
    _, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert [page for page, _, _ in to_ocr] == [1]


async def test_pdf_text_reads_the_table_under_a_watermark():
    data = _build_pdf(image_box=SCAN_BOX, overlay_text=WATERMARK)
    ocr = FakeOcr("Hemoglobin 138 g/L 115-150")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 1
    assert "Hemoglobin 138" in text


def test_born_digital_text_only_page_stays_on_its_text_layer():
    data = _build_pdf(body_text=BODY_TEXT)
    texts, to_ocr, _ = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI)
    assert to_ocr == []
    assert "Physical Examination" in texts[0]


# --- pdf_text: the OCR call count and the merged text ----------------------

async def test_pdf_text_ocrs_a_scan_with_no_text_layer():
    data = _build_pdf(image_box=SCAN_BOX)
    ocr = FakeOcr("Hemoglobin 138 g/L 115-150")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 1
    assert "Hemoglobin" in text


async def test_pdf_text_ocrs_a_scan_behind_a_header_and_footer():
    """End-to-end #304 reproduction: on unfixed main, ocr.calls is 0 and the
    lab rows never reach the output; the fix must make this an OCR call."""
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)
    ocr = FakeOcr("Hemoglobin 138 g/L 115-150\nWBC 6.2 10^9/L 4.0-10.0\nPlatelets 230 10^9/L 150-400")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 1
    assert "Hemoglobin" in text
    assert "Platelets" in text


async def test_pdf_text_does_not_duplicate_a_header_ocr_already_read():
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)
    ocr = FakeOcr(f"{HEADER}\nHemoglobin 138 g/L 115-150\n{FOOTER}")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 1
    assert text.count(HEADER) == 1
    assert text.count(FOOTER) == 1


async def test_pdf_text_keeps_a_header_ocr_left_out():
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)
    ocr = FakeOcr("Hemoglobin 138 g/L 115-150")  # omits the header and footer
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 1
    assert HEADER in text
    assert FOOTER in text
    assert "Hemoglobin" in text


async def test_pdf_text_does_not_ocr_a_born_digital_page_with_a_logo():
    data = _build_pdf(image_box=LOGO_BOX, body_text=BODY_TEXT)
    ocr = FakeOcr("should never be read")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 0
    assert "Physical Examination" in text


async def test_pdf_text_does_not_ocr_a_background_image_with_real_text_over_it():
    data = _build_pdf(image_box=SCAN_BOX, overlay_text=OVERLAY_TEXT)
    ocr = FakeOcr("should never be read")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 0
    assert "Hemoglobin" in text


async def test_pdf_text_does_not_ocr_a_searchable_scan():
    data = _build_pdf(image_box=SCAN_BOX, overlay_text="\n".join(LAB_ROWS))
    ocr = FakeOcr("should never be read")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 0
    assert "Hemoglobin" in text


async def test_pdf_text_does_not_ocr_a_born_digital_text_only_page():
    data = _build_pdf(body_text=BODY_TEXT)
    ocr = FakeOcr("should never be read")
    text = await extract.pdf_text(data, ocr=ocr)
    assert ocr.calls == 0
    assert "Physical Examination" in text


# --- pdf_text: falling back to the layer when OCR cannot run ---------------

async def test_pdf_text_without_ocr_falls_back_to_the_header_footer_layer():
    """Review point 1: a page the new image-area rule routes to OCR must not
    come back emptier than main read it when there is no `ocr` to call (no
    vision model configured, or a caller that never passes one) — only the
    scanned table is lost, same as on main, not the header and footer too."""
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)
    text = await extract.pdf_text(data)  # no ocr=...
    assert HEADER in text
    assert FOOTER in text
    assert "Hemoglobin" not in text
    assert not isinstance(text, extract.PartialText)


async def test_pdf_text_falls_back_to_the_layer_when_ocr_fails_for_that_page():
    """Same fallback, reached through a per-page OCR failure instead of a
    missing `ocr` callable: the page is still named in `missing_pages` (it
    should be retried for the table OCR would have added), but its own
    header and footer are not lost in the meantime."""
    data = _build_pdf(image_box=SCAN_BOX, header_footer=True)

    async def failing_ocr(image_bytes: bytes, mime: str) -> str:
        raise RuntimeError("vision model unavailable")

    text = await extract.pdf_text(data, ocr=failing_ocr)
    assert isinstance(text, extract.PartialText)
    assert text.missing_pages == (1,)
    assert HEADER in text
    assert FOOTER in text
    assert "Hemoglobin" not in text


# --- _merge_ocr_text in isolation -------------------------------------------

def test_merge_ocr_text_appends_lines_ocr_did_not_transcribe():
    merged = extract._merge_ocr_text("table row 1", f"{HEADER}\n{FOOTER}")
    assert merged.startswith("table row 1")
    assert HEADER in merged
    assert FOOTER in merged


def test_merge_ocr_text_skips_lines_already_in_the_ocr_result():
    ocr_text = f"{HEADER}\ntable row 1\n{FOOTER}"
    merged = extract._merge_ocr_text(ocr_text, f"{HEADER}\n{FOOTER}")
    assert merged == ocr_text

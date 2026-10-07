"""Pixels: the only place that opens an image library or a PDF renderer.

`extract` turns a file into text; this turns it into pictures, for the two
callers that need them. A scanned page has to be rasterised before OCR can see
it, and a vision model is handed JPEGs rather than a PDF. Both were doing it
themselves, with their own thresholds and their own idea of what to do with an
alpha channel, so the same page came out at two resolutions depending on which
path reached it.

Nothing here knows what a health document is.
"""

from __future__ import annotations

import io
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

#: A vision endpoint's useful ceiling. Above it the extra pixels cost tokens
#: and buy no accuracy; below it small print in a scanned lab report is lost.
MAX_VISION_EDGE_PX = 1536
JPEG_QUALITY = 85
#: Rendering scale for a PDF page, as a multiple of its 72 dpi natural size.
PDF_RENDER_SCALE = 1.5


def image_info(data: bytes) -> tuple[int, int, str]:
    """``(width, height, format)`` for image bytes; ``(0, 0, "")`` if unreadable."""
    try:
        from PIL import Image

        with Image.open(io.BytesIO(data)) as img:
            return img.width, img.height, (img.format or "")
    except Exception as exc:
        logger.warning("image: could not be read: error_type=%s", type(exc).__name__)
        return 0, 0, ""


def fit_image(
    data: bytes,
    *,
    max_edge: int = MAX_VISION_EDGE_PX,
    quality: int = JPEG_QUALITY,
    fmt: str = "JPEG",
) -> tuple[bytes, dict[str, Any]]:
    """Re-encode an image to fit `max_edge`, returning the bytes and what it cost.

    Transparency is flattened onto white rather than dropped: a PNG lab report
    saved with an alpha channel came through as black-on-black otherwise.
    Returns the input unchanged if it cannot be decoded, so a caller that
    cannot use it hears that from the endpoint rather than from a traceback.
    """
    started = time.time()
    before = len(data)
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(data))
        origin = img.size
        if img.mode in ("RGBA", "LA", "P"):
            flat = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            flat.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img = flat
        elif img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        width, height = img.size
        if width > max_edge or height > max_edge:
            scale = min(max_edge / width, max_edge / height)
            img = img.resize((int(width * scale), int(height * scale)), Image.Resampling.LANCZOS)

        out = io.BytesIO()
        opts: dict[str, Any] = {"format": fmt, "quality": quality, "optimize": True}
        if fmt == "JPEG":
            opts["progressive"] = True
        img.save(out, **opts)
        blob = out.getvalue()
        logger.info("image: fitted: bytes_before=%d bytes_after=%d", before, len(blob))
        return blob, {
            "original_size": before,
            "optimized_size": len(blob),
            "compression_ratio": (1 - len(blob) / before) * 100 if before else 0.0,
            "original_dimensions": origin,
            "optimized_dimensions": img.size,
            "processing_time": time.time() - started,
        }
    except Exception as exc:
        logger.warning("image: fit failed, sending the original: error_type=%s", type(exc).__name__)
        return data, {"error": type(exc).__name__, "original_size": before}


def pdf_pages_as_images(
    data: bytes, *, scale: float = PDF_RENDER_SCALE, max_edge: int = MAX_VISION_EDGE_PX
) -> list[tuple[bytes, dict[str, Any]]]:
    """Every page of a PDF as a JPEG, one entry per page in order."""
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument(data)
    try:
        out: list[tuple[bytes, dict[str, Any]]] = []
        for i in range(len(doc)):
            page = doc[i]
            try:
                buf = io.BytesIO()
                page.render(scale=scale).to_pil().save(buf, format="JPEG", quality=90)
            finally:
                page.close()
            out.append(fit_image(buf.getvalue(), max_edge=max_edge))
        return out
    finally:
        doc.close()


def text_image(text: str, *, size: int = 48) -> bytes:
    """A PNG of one line of black text on white: a known image for probing a vision model."""
    from PIL import Image, ImageDraw, ImageFont

    font = ImageFont.load_default(size=size)
    width = int(ImageDraw.Draw(Image.new("RGB", (1, 1))).textlength(text, font=font)) + 40
    image = Image.new("RGB", (width, size * 2 + 20), "white")
    ImageDraw.Draw(image).text((20, size // 2), text, fill="black", font=font)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()

"""Pixels: an image as a vision model is handed it.

`extract` turns a file into text, rendering a scanned page to PNG on the way;
this fits that page, or a photo, to the size and format a vision model reads
(`fit_image`), with one rule for transparency (`flatten`) shared with the
downscale `extract` applies first. Two paths with their own thresholds and
their own idea of an alpha channel sent the same page at two resolutions.

Nothing here knows what a health document is.
"""

from __future__ import annotations

import io
import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from PIL import Image

logger = logging.getLogger(__name__)

#: A vision endpoint's useful ceiling. Above it the extra pixels cost tokens
#: and buy no accuracy; below it small print in a scanned lab report is lost.
MAX_VISION_EDGE_PX = 1536
JPEG_QUALITY = 85


def image_info(data: bytes) -> tuple[int, int, str]:
    """``(width, height, format)`` for image bytes; ``(0, 0, "")`` if unreadable."""
    try:
        from PIL import Image

        with Image.open(io.BytesIO(data)) as img:
            return img.width, img.height, (img.format or "")
    except Exception as exc:
        logger.warning("image: could not be read: error_type=%s", type(exc).__name__)
        return 0, 0, ""


def flatten(img: Image.Image) -> Image.Image:
    """`img` as RGB, or L when it is grey, with any transparency composited
    onto white. Dropped instead, a transparent pixel keeps the colour stored
    under it, black in a PNG exported from a report viewer, and the black text
    on it is gone: `convert("RGB")` did that to RGBA, and pasting without a
    mask did it to LA."""
    from PIL import Image

    if img.mode in ("P", "PA") or (img.mode in ("L", "RGB") and "transparency" in img.info):
        img = img.convert("RGBA")
    if img.mode in ("RGBA", "LA"):
        flat = Image.new("RGB", img.size, (255, 255, 255))
        flat.paste(img.convert("RGBA"), mask=img.getchannel("A"))
        return flat
    return img if img.mode in ("RGB", "L") else img.convert("RGB")


def fit_image(
    data: bytes,
    *,
    max_edge: int = MAX_VISION_EDGE_PX,
    quality: int = JPEG_QUALITY,
    fmt: str = "JPEG",
) -> tuple[bytes, dict[str, Any]]:
    """Re-encode an image to fit `max_edge`, returning the bytes and what it cost.

    Transparency is flattened onto white (`flatten`). Returns the input
    unchanged, with an `error` in what it cost, if it cannot be decoded, so a
    caller that cannot use it hears that from the endpoint rather than from a
    traceback.
    """
    started = time.time()
    before = len(data)
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(data))
        origin = img.size
        img = flatten(img)

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
        logger.info("image: fitted: bytes_before=%d bytes_after=%d", len(data), len(blob))
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

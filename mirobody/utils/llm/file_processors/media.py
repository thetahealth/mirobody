"""One image as a vision model is handed it: fitted, re-encoded and base64'd.

The pixels are `mirobody.documents.render`'s job, so a page rendered for OCR
and a photo sent here come out the same way; this is the envelope around them.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass

from mirobody.documents import render


@dataclass(frozen=True)
class VisionImage:
    """An image ready for a request: base64 `data` of the `mime` it is in."""

    data: str
    mime: str


def model_ready(image: bytes, mime: str) -> VisionImage:
    """Sync, CPU-bound (52 ms for a rendered A4 page): `image` fitted to
    `render.MAX_VISION_EDGE_PX` as a JPEG, or as it came, under its own
    `mime`, when Pillow cannot decode it. That is a HEIC photo, for which no
    plugin is installed: it used to be sent labelled `image/jpeg`."""
    fitted, cost = render.fit_image(image)
    if "error" in cost:
        return VisionImage(base64.b64encode(image).decode("ascii"), mime)
    return VisionImage(base64.b64encode(fitted).decode("ascii"), "image/jpeg")

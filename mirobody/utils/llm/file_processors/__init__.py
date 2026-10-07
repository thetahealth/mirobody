"""The vision surface: one image to its text, on the routed model.

    dispatch.py         the route (`UTILS_VISION_MODEL`) and the entry point
    media.py            an image fitted and base64'd for a request
    backends_openai.py  the one request shape: chat/completions with an image part
    results.py          asking for JSON and reading it back, shared with the text surface
"""

from .dispatch import ImageNotRead, vision_extract, vision_route

__all__ = [
    "ImageNotRead",
    "vision_extract",
    "vision_route",
]

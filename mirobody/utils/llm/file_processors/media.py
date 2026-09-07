"""Turning a file on disk into something a vision model will accept.

Image downscaling/recompression, PDF page rasterisation, and the OpenAI-shaped
message envelope. The cost of getting this wrong is paid twice — once in
tokens, once in extraction quality — so the optimisation thresholds live here
together rather than at each call site.
"""

from __future__ import annotations

import base64
import io
import logging
import time
from typing import Any

import pypdfium2 as pdfium
from PIL import Image

logger = logging.getLogger(__name__)

# =============================================================================

class FileProcessor:
    """Base file processor with image optimization."""

    @staticmethod
    def optimize_image_for_llm(
        image_data: bytes,
        max_dimension: int = 2048,
        quality: int = 85,
        format: str = "JPEG"
    ) -> tuple[bytes, dict]:
        """Optimize image by reducing resolution and applying compression."""
        try:
            start_time = time.time()
            original_size = len(image_data)

            img = Image.open(io.BytesIO(image_data))
            original_width, original_height = img.size

            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')

            # Resize if too large
            width, height = img.size
            if width > max_dimension or height > max_dimension:
                scale = min(max_dimension / width, max_dimension / height)
                new_width, new_height = int(width * scale), int(height * scale)
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                logger.info(f"Image resized: {original_width}x{original_height} → {new_width}x{new_height}")

            # Save optimized image
            output = io.BytesIO()
            save_kwargs = {"format": format, "quality": quality, "optimize": True}
            if format == "JPEG":
                save_kwargs["progressive"] = True
            img.save(output, **save_kwargs)

            optimized_data = output.getvalue()
            compression_ratio = (1 - len(optimized_data) / original_size) * 100

            stats = {
                "original_size": original_size,
                "optimized_size": len(optimized_data),
                "compression_ratio": compression_ratio,
                "original_dimensions": (original_width, original_height),
                "optimized_dimensions": img.size,
                "processing_time": time.time() - start_time
            }
            logger.info(f"Image optimized: {original_size/1024:.1f}KB → {len(optimized_data)/1024:.1f}KB "
                        f"({compression_ratio:.1f}% reduction)")
            return optimized_data, stats

        except Exception as e:
            logger.warning(f"Image optimization failed: {e}")
            return image_data, {"error": str(e), "original_size": len(image_data)}


# =============================================================================
# Common Processing Utilities
# =============================================================================

def _convert_pdf_to_base64_images(pdf_path: str, scale: float = 1.5) -> list[dict[str, Any]]:
    """Convert PDF pages to optimized base64 images."""
    pdf = pdfium.PdfDocument(pdf_path)
    page_images = []

    for page_num in range(len(pdf)):
        page_start = time.time()
        page = pdf[page_num]
        bitmap = page.render(scale=scale)
        pil_image = bitmap.to_pil()

        img_buffer = io.BytesIO()
        pil_image.save(img_buffer, format="JPEG", quality=90)
        img_data = img_buffer.getvalue()

        optimized_data, stats = FileProcessor.optimize_image_for_llm(
            img_data, max_dimension=1536, quality=85
        )
        base64_image = base64.b64encode(optimized_data).decode('utf-8')

        conversion_time = time.time() - page_start
        page_images.append({
            'page_num': page_num + 1,
            'base64_image': base64_image,
            'conversion_time': conversion_time,
            'stats': stats
        })
        logger.info(f"Page {page_num + 1} converted in {conversion_time:.2f}s")

    pdf.close()
    return page_images


def _build_vision_message(base64_image: str, prompt: str, json_mode: bool) -> list[dict]:
    """Build OpenAI-compatible vision message."""
    text_content = f"{prompt}. Please return the result in JSON format." if json_mode else prompt
    return [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}},
            {"type": "text", "text": text_content}
        ]
    }]


def _read_and_optimize_image(image_path: str) -> tuple[str, dict]:
    """Read image file and return optimized base64 string."""
    with open(image_path, "rb") as f:
        img_data = f.read()
    optimized_data, stats = FileProcessor.optimize_image_for_llm(
        img_data, max_dimension=1536, quality=85
    )
    return base64.b64encode(optimized_data).decode('utf-8'), stats



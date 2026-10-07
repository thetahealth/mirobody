"""Text extraction for a stored document the model opens before anything has
extracted it: the one path of the agent's filesystem that can call a vision
model.

`PgFilesystemBackend._lazy_extract_doc_text` calls it on the first
`read_file` of a document with no text yet, and caches the result in
`th_files.original_text`. Registration used to prepare files here too; the
mounts read `th_files` directly now, so nothing registers.
"""

import logging
from pathlib import PurePosixPath

from mirobody.collect import FileAbstractExtractor
from mirobody.kernel.ops import is_driver_exception

from .naming import guess_mime

logger = logging.getLogger(__name__)


async def extract_text(file_bytes: bytes, filename: str) -> str:
    """The document's text, or "" when nothing could be extracted. Never
    raises. Deduplication applies inside `extract_file_original_text`: two
    models opening the same bytes pay for one OCR."""
    if not file_bytes:
        return ""
    ext = PurePosixPath(filename or "").suffix.lower()
    try:
        text = await FileAbstractExtractor().extract_file_original_text(
            file_content=file_bytes,
            file_type=ext.lstrip("."),
            filename=filename,
            content_type=guess_mime(filename),
        )
    except Exception as e:
        logger.error("document text extraction failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return ""
    return text if text and len(text.strip()) > 10 else ""

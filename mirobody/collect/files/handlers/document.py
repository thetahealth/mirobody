"""Word and PowerPoint uploads: `documents.extract` turns them into markdown,
and from there a lab report saved as a Word document is treated exactly like
one saved as a PDF.

Legacy binary `.doc`/`.ppt` are NOT handled: python-docx and python-pptx read
the zip-based formats only, so they stay out of the accepted set rather than
failing after an upload.
"""

from mirobody.collect.files.handlers.base import BaseFileHandler


class DocumentHandler(BaseFileHandler):
    """Word/PowerPoint handler, built-in extraction only."""

    def get_type_name(self) -> str:
        return "document"

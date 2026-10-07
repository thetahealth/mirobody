"""Word and PowerPoint uploads: `documents.extract` turns them into markdown,
and from there a lab report saved as a Word document is treated exactly like
one saved as a PDF.

Legacy binary `.doc`/`.ppt` are NOT handled: python-docx and python-pptx read
the zip-based formats only, so they stay out of the accepted set rather than
failing after an upload.
"""

from mirobody.collect.files.handlers.base import BaseFileHandler
from mirobody.utils.file_types import is_document_file


class DocumentHandler(BaseFileHandler):
    """Word/PowerPoint handler, built-in extraction only."""

    def get_type_name(self) -> str:
        return "document"

    # Routing table lives in utils.file_types so the handler that accepts a
    # document and the extractor that parses it can never disagree.
    is_document_file = staticmethod(is_document_file)

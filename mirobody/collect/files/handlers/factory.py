from __future__ import annotations

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.collect.files.handlers.base import BaseFileHandler
from mirobody.collect.files.handlers.document import DocumentHandler
from mirobody.collect.files.handlers.excel import ExcelHandler
from mirobody.collect.files.handlers.genetic import GeneticHandler
from mirobody.collect.files.handlers.image import ImageHandler
from mirobody.collect.files.handlers.pdf import PDFHandler
from mirobody.collect.files.handlers.text import TextHandler
from mirobody.documents import detect

#: The handler for each kind `detect.kind` names: the kinds `documents.extract`
#: reads, so a file that reaches a handler is one its text can be read from.
_HANDLERS: dict[str, type[BaseFileHandler]] = {
    detect.KIND_PDF: PDFHandler,
    detect.KIND_IMAGE: ImageHandler,
    detect.KIND_XLSX: ExcelHandler,
    detect.KIND_DOCX: DocumentHandler,
    detect.KIND_PPTX: DocumentHandler,
    detect.KIND_TEXT: TextHandler,
}

#: Bytes `detect.kind` reads: an Office file names its parts within them (`zip_kind`).
_HEAD_BYTES = 64 * 1024


class FileHandlerFactory:
    def __init__(
        self,
        uploader,
        temp_manager,
        indicator_extractor,
        abstract_extractor,
    ):
        self.uploader = uploader
        self.temp_manager = temp_manager
        self.indicator_extractor = indicator_extractor
        self.abstract_extractor = abstract_extractor

    async def get_handler(self, file: UploadFile) -> BaseFileHandler | None:
        """The handler for `file`, or None when nothing here reads it.

        A genotype export first, by its content. Every other kind is
        `detect.kind`'s, from the name, the declared type and the first bytes:
        routing on the declared type alone refused a photo or a PDF a client
        sent as `application/octet-stream`, and took a legacy `.xls` that no
        parser reads, which then completed with no text and no error.
        """
        if await GeneticHandler.is_genetic_file(file):
            handler: type[BaseFileHandler] | None = GeneticHandler
        else:
            await file.seek(0)
            head = await file.read(_HEAD_BYTES)
            await file.seek(0)
            handler = _HANDLERS.get(detect.kind(file.filename, file.content_type, bytes(head)) or "")
        if handler is None:
            return None
        return handler(self.uploader, self.temp_manager, self.indicator_extractor, self.abstract_extractor)

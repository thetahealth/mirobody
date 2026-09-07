from __future__ import annotations



# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality — a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.utils.file_types import is_text_file
from mirobody.pulse.file_parser.handlers.document import DocumentHandler
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler
from mirobody.pulse.file_parser.handlers.image import ImageHandler
from mirobody.pulse.file_parser.handlers.pdf import PDFHandler
from mirobody.pulse.file_parser.handlers.audio import AudioHandler
from mirobody.pulse.file_parser.handlers.text import TextHandler
from mirobody.pulse.file_parser.handlers.genetic import GeneticHandler
from mirobody.pulse.file_parser.handlers.excel import ExcelHandler

class FileHandlerFactory:
    def __init__(
        self,
        uploader,
        temp_manager,
        content_extractor,
        indicator_extractor,
        abstract_extractor,
    ):
        self.uploader = uploader
        self.temp_manager = temp_manager
        self.content_extractor = content_extractor
        self.indicator_extractor = indicator_extractor
        self.abstract_extractor = abstract_extractor

    async def get_handler(self, file: UploadFile) -> BaseFileHandler | None:
        """
        Determine and return the appropriate handler for the file.
        """
        content_type = file.content_type or ""
        filename = file.filename or ""

        # 1. Check for Genetic File (Async check required)
        if await GeneticHandler.is_genetic_file(file):
            return GeneticHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 2. Check for Image
        if content_type.startswith("image/"):
            return ImageHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 3. Check for PDF
        if content_type == "application/pdf":
            return PDFHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 4. Check for Audio
        if content_type.startswith("audio/"):
            return AudioHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 5. Check for Text. Markdown included: browsers send .md as
        # text/markdown, which used to fall through every branch and fail as
        # "unsupported" even though TextHandler parses it identically to .txt.
        # `text/csv` lands here too, and that is the fix for a real bug: it used
        # to be routed to a `CSVHandler` that only delegated to an injected
        # `csv_processor`, which nothing in this project ever injected. The
        # factory therefore returned None for every .csv — no handler at all —
        # while `SUPPORTED_EXTENSIONS` accepted `.csv` and
        # `file_types.TEXT_MIME_TYPES` already called it text. A lab CSV is text:
        # extract it, then run the same indicator extraction as everything else.
        if (content_type.startswith("text/plain")
                or content_type.startswith("text/markdown")
                or is_text_file(filename, content_type)):
             return TextHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 6. Word / PowerPoint, before Excel because both are OOXML zips and
        # only the extension separates them.
        if DocumentHandler.is_document_file(filename, content_type):
            return DocumentHandler(
                uploader=self.uploader,
                temp_manager=self.temp_manager,
                content_extractor=self.content_extractor,
                indicator_extractor=self.indicator_extractor,
                abstract_extractor=self.abstract_extractor,
            )

        # 7. Check for Excel — built-in openpyxl extraction. The
        # `excel_processor` override parameter is gone with the same seam: it
        # was documented as "injected from mcp_server", and no such injector
        # exists here, so the branch was unreachable.
        if ExcelHandler.is_excel_file(filename, content_type):
            return ExcelHandler(
                uploader=self.uploader,
                temp_manager=self.temp_manager,
                content_extractor=self.content_extractor,
                indicator_extractor=self.indicator_extractor,
                abstract_extractor=self.abstract_extractor
            )

        return None


from __future__ import annotations

import logging

from typing import Optional

# `fastapi` lives in the [server] extra, but file parsing is advertised engine
# functionality — a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler
from mirobody.pulse.file_parser.handlers.image import ImageHandler
from mirobody.pulse.file_parser.handlers.pdf import PDFHandler
from mirobody.pulse.file_parser.handlers.audio import AudioHandler
from mirobody.pulse.file_parser.handlers.text import TextHandler
from mirobody.pulse.file_parser.handlers.genetic import GeneticHandler
from mirobody.pulse.file_parser.handlers.excel import ExcelHandler
from mirobody.pulse.file_parser.handlers.csv import CSVHandler
from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

class FileHandlerFactory:
    def __init__(
        self,
        uploader,
        temp_manager,
        content_extractor,
        indicator_extractor,
        abstract_extractor,
        excel_processor=None,  # Optional: injected from mcp_server when Excel support is needed
        csv_processor=None,    # Optional: injected from mcp_server when CSV support is needed
    ):
        self.uploader = uploader
        self.temp_manager = temp_manager
        self.content_extractor = content_extractor
        self.indicator_extractor = indicator_extractor
        self.abstract_extractor = abstract_extractor
        self.excel_processor = excel_processor
        self.csv_processor = csv_processor

    async def get_handler(self, file: UploadFile) -> Optional[BaseFileHandler]:
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

        # 5. Check for Text
        if content_type.startswith("text/plain"):
             return TextHandler(
                self.uploader, 
                self.temp_manager, 
                self.content_extractor, 
                self.indicator_extractor,
                self.abstract_extractor
            )

        # 6. Check for Excel. The handler works with the built-in pandas/openpyxl
        # extraction; an external excel_processor (may be None) only overrides it.
        if ExcelHandler.is_excel_file(filename, content_type):
            return ExcelHandler(
                self.excel_processor,  # optional override; None => built-in extraction
                uploader=self.uploader,
                temp_manager=self.temp_manager,
                content_extractor=self.content_extractor,
                indicator_extractor=self.indicator_extractor,
                abstract_extractor=self.abstract_extractor
            )

        # 7. Check for CSV (only if csv_processor is available)
        if CSVHandler.is_csv_file(filename, content_type):
            if self.csv_processor is not None:
                return CSVHandler(
                    self.csv_processor,  # Pass specific processor
                    uploader=self.uploader,
                    temp_manager=self.temp_manager,
                    content_extractor=self.content_extractor,
                        indicator_extractor=self.indicator_extractor,
                    abstract_extractor=self.abstract_extractor
                )
            else:
                # CSV processor not available, return None to indicate unsupported
                logging.warning(f"CSV file detected but csv_processor not available: {filename}")
                return None

        return None


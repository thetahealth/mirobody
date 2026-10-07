from mirobody.collect.files.handlers.base import BaseFileHandler
from mirobody.utils.file_types import is_excel_file


class ExcelHandler(BaseFileHandler):
    """Excel handler: built-in openpyxl extraction, nothing pluggable.

    The workbook is converted to text (``original_text``), an abstract is
    generated, and indicator extraction is auto-triggered by
    ``BaseFileHandler.process``: identical treatment to a report PDF, on
    both the drive and chat upload paths.
    """

    def get_type_name(self) -> str:
        return "excel"

    # Routing table lives in utils.file_types so the handler that accepts a
    # spreadsheet and the extractor that parses it can never disagree.
    is_excel_file = staticmethod(is_excel_file)

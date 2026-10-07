from mirobody.collect.files.handlers.base import BaseFileHandler


class ExcelHandler(BaseFileHandler):
    """A workbook (`.xlsx`, `.xlsm`), read as markdown tables: from there it is
    treated exactly like a report PDF."""

    def get_type_name(self) -> str:
        return "excel"

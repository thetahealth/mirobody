import logging
from typing import Any, Dict
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext

class ExcelHandler(BaseFileHandler):
    def __init__(self, excel_processor=None, **kwargs):
        super().__init__(**kwargs)
        self.excel_processor = excel_processor

    def get_type_name(self) -> str:
        return "excel"
        
    @staticmethod
    def is_excel_file(filename: str, content_type: str) -> bool:
        if not filename:
            return False
        excel_extensions = [".xlsx", ".xls", ".xlsm", ".xlsb"]
        filename_lower = filename.lower()
        has_excel_extension = any(filename_lower.endswith(ext) for ext in excel_extensions)
        
        excel_mime_types = [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
            "application/vnd.ms-excel.sheet.macroEnabled.12",
            "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
        ]
        has_excel_mime = content_type in excel_mime_types
        return has_excel_extension or has_excel_mime

    async def process(self, ctx: FileProcessingContext) -> Dict[str, Any]:
        # ExcelProcessor takes control of the whole flow usually
        try:
            return await self.excel_processor.process_excel_file(
                file_content=await ctx.file.read(),
                filename=ctx.original_filename or ctx.file.filename,
                content_type=ctx.content_type,
                user_id=ctx.user_id,
                query_user_id=ctx.target_user_id,
                message_id=ctx.message_id,
                progress_callback=ctx.progress_callback,
            )
        except Exception as e:
            logging.error(f"Excel file processing failed: {ctx.filename}, error: {e}", stack_info=True)
            return {
                "success": False,
                "message": f"Excel file processing failed: {str(e)}",
                "type": "excel",
            }

    async def _process_content(self, *args, **kwargs):
        pass


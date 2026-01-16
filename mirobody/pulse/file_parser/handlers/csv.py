"""
CSV file handler for mirobody

This handler processes CSV files, specifically medication orders (医嘱信息).
It delegates the actual processing to CSVProcessor from mcp_server.
"""

import logging
from typing import Any, Dict
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext


class CSVHandler(BaseFileHandler):
    """CSV file handler that delegates to CSVProcessor"""
    
    def __init__(self, csv_processor=None, **kwargs):
        super().__init__(**kwargs)
        self.csv_processor = csv_processor

    def get_type_name(self) -> str:
        return "csv"
        
    @staticmethod
    def is_csv_file(filename: str, content_type: str) -> bool:
        """Check if the file is a CSV file"""
        if not filename:
            return False
        
        csv_extensions = [".csv"]
        filename_lower = filename.lower()
        has_csv_extension = any(filename_lower.endswith(ext) for ext in csv_extensions)
        
        csv_mime_types = [
            "text/csv",
            "application/csv",
            "text/comma-separated-values",
        ]
        has_csv_mime = content_type in csv_mime_types
        
        return has_csv_extension or has_csv_mime

    @staticmethod
    def is_medication_csv(filename: str) -> bool:
        """Check if the CSV file contains medication orders based on filename"""
        if not filename:
            return False
        return "医嘱信息" in filename

    async def process(self, ctx: FileProcessingContext) -> Dict[str, Any]:
        """Process CSV file using CSVProcessor"""
        try:
            return await self.csv_processor.process_csv_file(
                file_content=await ctx.file.read(),
                filename=ctx.original_filename or ctx.file.filename,
                content_type=ctx.content_type,
                user_id=ctx.user_id,
                query_user_id=ctx.target_user_id,
                message_id=ctx.message_id,
                progress_callback=ctx.progress_callback,
            )
        except Exception as e:
            logging.error(f"CSV file processing failed: {ctx.filename}, error: {e}", stack_info=True)
            return {
                "success": False,
                "message": f"CSV file processing failed: {str(e)}",
                "type": "csv",
            }

    async def _process_content(self, *args, **kwargs):
        """Not used - process() handles everything"""
        pass


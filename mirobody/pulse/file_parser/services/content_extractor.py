"""
Content extraction service

Responsible for extracting text content from different types of files
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Extract the content from the file, return only the file content in markdown format, do not return any other information"

class ContentExtractor:
    """Content extraction service class"""

    @staticmethod
    async def extract_from_text_file(file_path: Path) -> str:
        """
        Extract content from text file

        Args:
            file_path: Text file path

        Returns:
            str: File content
        """
        try:
            with open(file_path, encoding="utf-8") as f:
                content = f.read()
            return content
        except Exception as e:
            logger.error(f"Text file reading error: {str(e)}", stack_info=True)
            return ""

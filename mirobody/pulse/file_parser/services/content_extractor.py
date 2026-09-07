"""
Content extraction service

Responsible for extracting text content from different types of files
"""

import logging
from pathlib import Path

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

logger = logging.getLogger(__name__)

DEFAULT_PROMPT = "Extract the content from the file, return only the file content in markdown format, do not return any other information"

class ContentExtractor:
    """Content extraction service class"""

    @staticmethod
    def extract_from_audio_urls(urls: list[str]) -> dict[str, str]:
        """
        Extract text content from audio URLs

        Args:
            urls: List of audio URLs

        Returns:
            Dict[str, str]: Dictionary of URL-corresponding text content
        """
        try:
            logger.info(f"Starting to process audio files: {urls}")
            texts = "" # asr_paraformer_with_urls(urls)
            return texts if texts else {}
        except Exception as e:
            logger.error(f"Audio file content extraction error: {str(e)}", stack_info=True)
            return {}

    @staticmethod
    def extract_from_audio(audio_url: str) -> str:
        """
        Extract text content from single audio URL

        Args:
            audio_url: Audio URL

        Returns:
            str: Extracted text content
        """
        try:
            language = get_req_ctx("language", "en")
            texts = ContentExtractor.extract_from_audio_urls([audio_url])
            return texts.get(audio_url, t("audio_recognition_failed", language))
        except Exception as e:
            language = get_req_ctx("language", "en")
            logger.error(f"Audio content extraction error: {str(e)}", stack_info=True)
            return t("audio_processing_error", language)

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

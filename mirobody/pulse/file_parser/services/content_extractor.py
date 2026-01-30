"""
Content extraction service

Responsible for extracting text content from different types of files
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List

from mirobody.utils.i18n import t
from mirobody.utils.llm import unified_file_extract
from mirobody.utils.req_ctx import get_req_ctx
from mirobody.utils.utils_asr import asr_paraformer_with_urls

DEFAULT_PROMPT = "Extract the content from the file, return only the file content in markdown format, do not return any other information"

class ContentExtractor:
    """Content extraction service class"""

    @staticmethod
    async def extract_from_file(file_path: Path, content_type: str = "text/plain", prompt: str = DEFAULT_PROMPT) -> str:
        """
        Extract text content from file

        Args:
            file_path: File path
            content_type: MIME content type
            prompt: Custom prompt for LLM extraction (default: general extraction)

        Returns:
            str: Extracted text content
        """
        try:
            t1 = time.time()
            logging.info(f"Starting to process file: {file_path}")

            if not file_path or not os.path.exists(file_path):
                logging.error("File does not exist")
                raw_text = "File does not exist, processing failed"
                return raw_text

            file_size = os.path.getsize(file_path)
            logging.info(f"File size: {file_size} bytes")

            if file_size == 0:
                logging.error("File is empty")
                raw_text = "File is empty, cannot process"
                return raw_text

            logging.info("Using unified_file_extract to extract content")
            raw_text = await unified_file_extract(
                file_path=str(file_path),
                prompt=prompt,
                content_type=content_type
            )

            t2 = time.time()
            logging.info(f"File content extraction completed, time taken: {t2 - t1} seconds")
            return raw_text

        except Exception as e:
            logging.error(f"File content extraction error: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    def extract_from_audio_urls(urls: List[str]) -> Dict[str, str]:
        """
        Extract text content from audio URLs

        Args:
            urls: List of audio URLs

        Returns:
            Dict[str, str]: Dictionary of URL-corresponding text content
        """
        try:
            logging.info(f"Starting to process audio files: {urls}")
            texts = asr_paraformer_with_urls(urls)
            return texts if texts else {}
        except Exception as e:
            logging.error(f"Audio file content extraction error: {str(e)}", stack_info=True)
            return {}

    @staticmethod
    async def extract_from_image(file_path: Path, content_type: str = "image/jpeg", prompt: str = DEFAULT_PROMPT) -> str:
        """
        Extract text content from image file

        Args:
            file_path: Image file path
            content_type: MIME content type
            prompt: Custom prompt for LLM extraction

        Returns:
            str: Extracted text content
        """
        try:
            logging.info(f"Starting to process image file: {file_path}")
            raw_text = await ContentExtractor.extract_from_file(file_path, content_type, prompt)
            return raw_text
        except Exception as e:
            logging.error(f"Image content extraction error: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    async def extract_from_pdf(file_path: Path, content_type: str = "application/pdf", prompt: str = DEFAULT_PROMPT) -> str:
        """
        Extract text content from PDF file

        Args:
            file_path: PDF file path
            content_type: MIME content type
            prompt: Custom prompt for LLM extraction

        Returns:
            str: Extracted text content
        """
        try:
            logging.info(f"Starting to process PDF file: {file_path}")
            raw_text = await ContentExtractor.extract_from_file(file_path, content_type, prompt)
            return raw_text
        except Exception as e:
            logging.error(f"PDF content extraction error: {str(e)}", stack_info=True)
            return ""

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
            logging.error(f"Audio content extraction error: {str(e)}", stack_info=True)
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
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            return content
        except Exception as e:
            logging.error(f"Text file reading error: {str(e)}", stack_info=True)
            return ""

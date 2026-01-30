"""
Unified File Parser for DeepAgent

Lightweight adapter that delegates to framework layer (mirobody/pulse/file_parser)
while implementing Agent-specific strategies (PyPDF fallback, full-text extraction).
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Union, BinaryIO

from langchain_community.document_loaders import PyPDFLoader

# unified file parser
from .....pulse.file_parser.services.content_extractor import ContentExtractor

logger = logging.getLogger(__name__)

# Supported file type mapping (inherited from framework)
SUPPORTED_FILE_TYPES = {
    # Document types
    "pdf": "document",
    "docx": "document",
    "doc": "document",
    "csv": "document",
    "xlsx": "excel",
    "xls": "excel",
    "pptx": "presentation",
    "ppt": "presentation",
    # Image types
    "jpg": "image",
    "jpeg": "image",
    "png": "image",
    "gif": "image",
    "webp": "image",
    "bmp": "image",
    # Text/Code types
    "py": "text",
    "js": "text",
    "ts": "text",
    "jsx": "text",
    "tsx": "text",
    "java": "text",
    "c": "text",
    "cpp": "text",
    "md": "text",
    "txt": "text",
    "json": "text",
    "xml": "text",
    "yaml": "text",
    "yml": "text",
    # Add more as needed
}

# Derived sets
IMAGE_EXTENSIONS: set[str] = {ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "image"}
TEXT_CODE_EXTENSIONS: set[str] = {ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "text"}

# Agent-specific prompts for full-text extraction
FULL_TEXT_PROMPT = """Please extract and return ALL the original text content from this file.
Return the complete text exactly as it appears in the document, preserving formatting where possible.
Do not summarize or modify the content - return the full original text."""

FULL_IMAGE_PROMPT = """Please extract and return ALL text content visible in this image.
Return the complete text exactly as it appears, preserving the order and structure where possible.
If there is no text or minimal text in the image, provide a detailed description of the visual content including main subjects, scene, colors, actions, and notable details."""


def get_file_type_from_extension(ext: str) -> str:
    """
    Get standardized file type from extension.
    
    Args:
        ext: File extension (with or without leading dot)
        
    Returns:
        Standardized file type (PDF, IMAGE, TEXT, DOCX, EXCEL, etc.)
    """
    ext = ext.lower().lstrip(".")
    category = SUPPORTED_FILE_TYPES.get(ext, "unknown")
    
    if category == "image":
        return "IMAGE"
    elif category == "document":
        return ext.upper()
    elif category == "text":
        return "TEXT"
    elif category == "excel":
        return "EXCEL"
    elif category == "presentation":
        return "PPTX"
    else:
        return "UNKNOWN"


class FileParser:
    """
    Lightweight file parser for DeepAgent's read_file tool.
    
    Delegates extraction to framework layer (ContentExtractor) while implementing
    Agent-specific strategies like PyPDF fallback for optimal performance.
    
    Workflow:
    1. Create temp file from bytes
    2. Try local extraction (PyPDF for PDFs) - fast, free
    3. Fallback to LLM via ContentExtractor - accurate but costly
    4. Clean up temp file
    """

    def __init__(self):
        """Initialize file parser with framework layer extractor."""
        self.content_extractor = ContentExtractor()
        logger.info("FileParser initialized with framework layer ContentExtractor")
    
    async def parse_file(
        self, 
        file_input: Union[bytes, BinaryIO],
        filename: str,
        file_type: str
    ) -> tuple[str, str, str]:
        """
        Parse file and return full text content.
        
        Args:
            file_input: File content as bytes or BinaryIO
            filename: Original filename
            file_type: File type (PDF, IMAGE, TEXT, etc.)
            
        Returns:
            Tuple of (parsed_text, parse_method, parse_model)
        """
        try:
            # Normalize file type
            file_type = file_type.lower().lstrip(".")
            
            # Read bytes if BinaryIO
            if hasattr(file_input, 'read'):
                if hasattr(file_input, 'seek'):
                    file_input.seek(0)
                file_bytes = file_input.read()
            else:
                file_bytes = file_input
            
            # Create temp file
            suffix = f".{file_type}" if file_type else Path(filename).suffix
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
                temp_file.write(file_bytes)
                temp_file_path = temp_file.name
            
            try:
                # Route to appropriate parser
                if file_type == "pdf":
                    return await self._parse_pdf(temp_file_path)
                
                elif file_type in IMAGE_EXTENSIONS:
                    return await self._parse_image(temp_file_path, file_type)
                
                elif file_type in TEXT_CODE_EXTENSIONS:
                    return await self._parse_text(temp_file_path)
                
                else:
                    # Generic file handling
                    return await self._parse_generic(temp_file_path, filename)
            
            finally:
                # Always cleanup temp file
                try:
                    os.unlink(temp_file_path)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temp file {temp_file_path}: {cleanup_error}")
        
        except Exception as e:
            logger.error(f"File parsing failed for {filename}: {e}", exc_info=True)
            return (f"File parsing failed: {str(e)}", "error", "")
    
    async def _parse_pdf(self, file_path: str) -> tuple[str, str, str]:
        """
        Parse PDF with PyPDF fallback strategy.
        
        Strategy:
        1. Try PyPDF first (fast, local, complete, free)
        2. Fallback to LLM if PyPDF fails or content insufficient
        
        Returns:
            Tuple of (content, method, model)
        """
        # Step 1: Try PyPDF (local extraction)
        try:
            logger.info("Attempting PDF parsing with PyPDF (fast, local)")
            loader = PyPDFLoader(file_path)
            docs = loader.load()
            content = "\n\n".join([doc.page_content for doc in docs])
            
            # Check if content is meaningful
            if content and len(content.strip()) > 50:
                logger.info(f"✅ PyPDF successful: {len(content)} chars")
                return (content, "pypdf_local", "")
            
            logger.warning(f"⚠️ PyPDF content insufficient ({len(content.strip())} chars), falling back to LLM")
        
        except Exception as pypdf_error:
            logger.warning(f"⚠️ PyPDF failed: {pypdf_error}, falling back to LLM")
        
        # Step 2: Fallback to LLM via framework layer
        logger.info("Using LLM extraction for PDF via ContentExtractor")
        content = await self.content_extractor.extract_from_pdf(
            Path(file_path),
            content_type="application/pdf",
            prompt=FULL_TEXT_PROMPT  # Agent's full-text prompt
        )
        
        return (content, "llm_fallback", "gemini/doubao")
    
    async def _parse_image(self, file_path: str, file_type: str) -> tuple[str, str, str]:
        """
        Parse image file via framework layer ContentExtractor.
        
        Returns:
            Tuple of (content, method, model)
        """
        content = await self.content_extractor.extract_from_image(
            Path(file_path),
            content_type=f"image/{file_type}",
            prompt=FULL_IMAGE_PROMPT
        )
        
        return (content, "unified_vision", "gemini/doubao")
    
    async def _parse_text(self, file_path: str) -> tuple[str, str, str]:
        """
        Parse text file via framework layer ContentExtractor.
        
        Returns:
            Tuple of (content, method, model)
        """
        content = await self.content_extractor.extract_from_text_file(
            Path(file_path)
        )
        
        return (content, "direct_read", "")
    
    async def _parse_generic(self, file_path: str, filename: str) -> tuple[str, str, str]:
        """
        Parse generic file type via framework layer ContentExtractor.
        
        Returns:
            Tuple of (content, method, model)
        """
        # Infer content type from filename
        content_type = self._infer_content_type(filename)
        
        content = await self.content_extractor.extract_from_file(
            Path(file_path),
            content_type=content_type,
            prompt=FULL_TEXT_PROMPT
        )
        
        return (content, "unified_extract", "gemini/doubao")
    
    @staticmethod
    def _infer_content_type(filename: str) -> str:
        """Infer MIME content type from filename."""
        import mimetypes
        content_type, _ = mimetypes.guess_type(filename)
        return content_type or "application/octet-stream"

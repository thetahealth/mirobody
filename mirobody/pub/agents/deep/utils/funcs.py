"""
Common utility functions for DeepAgent module.

Centralizes shared utility functions used across multiple files.
"""

import hashlib
import logging
import os
import re
from pathlib import Path

from .constants import FILE_TYPE_MAP, SUPPORTED_FILE_TYPES

logger = logging.getLogger(__name__)


def calculate_content_hash(file_bytes: bytes) -> str:
    """
    Calculate SHA256 hash of file content for cache lookup.
    
    Args:
        file_bytes: File binary content
        
    Returns:
        64-character hexadecimal hash string
        
    Example:
        >>> calculate_content_hash(b"Hello World")
        'a591a6d40bf420404a011733cfb7b19...'
    """
    return hashlib.sha256(file_bytes).hexdigest()


def get_file_type(extension: str) -> str:
    """
    Get file type from extension using FILE_TYPE_MAP.

    Args:
        extension: File extension (with or without leading dot)

    Returns:
        File type string (PDF, IMAGE, TEXT, etc.) or 'UNKNOWN'

    Example:
        >>> get_file_type('.pdf')
        'PDF'
        >>> get_file_type('jpg')
        'IMAGE'
    """
    ext_lower = extension.lower()
    if not ext_lower.startswith('.'):
        ext_lower = '.' + ext_lower
    return FILE_TYPE_MAP.get(ext_lower, 'UNKNOWN')


def get_file_type_from_extension(ext: str) -> str:
    """
    Get standardized file type from extension using SUPPORTED_FILE_TYPES.
    
    This is the parser-specific version that maps to parsing categories.
    
    Args:
        ext: File extension (with or without leading dot)
        
    Returns:
        Standardized file type (PDF, IMAGE, TEXT, DOCX, EXCEL, etc.)
        
    Example:
        >>> get_file_type_from_extension('.pdf')
        'PDF'
        >>> get_file_type_from_extension('png')
        'IMAGE'
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


def sanitize_filename(filename: str, max_length: int = 200) -> str:
    """
    Clean filename by removing illegal characters.

    Args:
        filename: Original filename
        max_length: Maximum length for the name part (excluding extension)

    Returns:
        Sanitized filename safe for filesystem use

    Example:
        >>> sanitize_filename('my<file>:test.pdf')
        'my_file__test.pdf'
    """
    if not filename:
        return filename

    # Remove or replace illegal characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    # Limit filename length (preserve extension)
    name, ext = os.path.splitext(filename)
    if len(name) > max_length:
        name = name[:max_length]
    return name + ext


def sanitize_text(text: str) -> str:
    """
    Sanitize text for PostgreSQL by replacing unsafe characters with spaces.

    Removes/replaces:
    - NULL bytes (0x00) - PostgreSQL incompatible
    - C0/C1 control chars (except tab/newline/CR) - potential issues
    - Zero-width chars (ZWSP, ZWNJ, etc) - security risk
    - Bidirectional marks (LTR/RTL) - Trojan Source attack vector
    - Invalid UTF-8 sequences

    Args:
        text: Raw text from file parsing

    Returns:
        Database-safe text with dangerous chars replaced by spaces

    Example:
        >>> sanitize_text("hello\\x00world\\u200Btest")
        'hello world test'
    """
    if not text:
        return text

    # Pattern: NULL + dangerous C0/C1 controls + zero-width + bidi marks
    # Keep: \t(09) \n(0A) \r(0D)
    pattern = (
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F'  # NULL + C0/C1 controls
        r'\u200B-\u200D\uFEFF'  # Zero-width spaces
        r'\u202A-\u202E'  # Bidirectional marks
        r'\uFFFE\uFFFF]'  # Non-characters
    )

    cleaned = re.sub(pattern, ' ', text)

    # Handle invalid UTF-8 by encoding/decoding with error replacement
    try:
        cleaned = cleaned.encode('utf-8', errors='replace').decode('utf-8')
    except Exception:
        pass

    if cleaned != text:
        diff = len(text) - len(cleaned.replace(' ', ''))
        if diff > 0:
            logger.warning(f"Sanitized {diff} unsafe char(s) from text")

    return cleaned

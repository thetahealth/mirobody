"""
Database utility functions for file parser services

Provides common utilities for:
- JSON serialization/deserialization
- Date parsing
- Error handling decorators
- Logging helpers
"""

import json
from datetime import datetime
from typing import Any, TypeVar
from zoneinfo import ZoneInfo
from mirobody.utils.file_types import guess_mime

# Type variable for generic function return types
T = TypeVar('T')


def safe_json_dumps(data: Any, default: str = "{}") -> str:
    """
    Safely convert data to JSON string
    
    Args:
        data: Data to serialize (dict, list, or other)
        default: Default value if serialization fails
        
    Returns:
        JSON string
    """
    if data is None:
        return default
    if isinstance(data, str):
        return data
    try:
        return json.dumps(data, ensure_ascii=False)
    except (TypeError, ValueError):
        return default


def safe_json_loads(data: Any, default: Any = None) -> Any:
    """
    Safely parse JSON string to Python object
    
    Args:
        data: JSON string or already parsed object
        default: Default value if parsing fails
        
    Returns:
        Parsed Python object or default
    """
    if data is None:
        return default if default is not None else {}
    if isinstance(data, (dict, list)):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return default if default is not None else {}
    return default if default is not None else {}


# Common date formats for parsing
DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
]


def parse_date(date_str: str, default: datetime | None = None) -> datetime | None:
    """
    Parse date string with multiple format support
    
    Args:
        date_str: Date string to parse
        default: Default value if parsing fails
        
    Returns:
        Parsed datetime or default
    """
    if not date_str or not isinstance(date_str, str):
        return default
    
    date_str = date_str.strip()
    if not date_str:
        return default
    
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return default


def get_utc_now() -> datetime:
    """Get current UTC time as naive datetime"""
    return datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)


def extract_first_record(result: list | None) -> dict | None:
    """
    Extract first record from query result
    
    Args:
        result: Query result list
        
    Returns:
        First record dict or None
    """
    if result and len(result) > 0:
        return result[0]
    return None


# MIME type mapping for common file extensions


def get_mime_type(filename: str) -> str:
    """MIME type for a filename, from the one shared table.

    Was a 40-entry `MIME_TYPE_MAP` local to this module — the fifth
    extension-to-MIME implementation in the project, and the one with the most
    reach: five call sites use it to set the `content_type` stored on a file row
    and the type handed to the model. It disagreed with what object storage had
    already written into the object itself on `.flac`, `.m4a`, `.rar` and `.wav`,
    so the same bytes were described two ways inside one deployment.
    """
    return guess_mime(filename)


def get_simple_file_type(file_type: str) -> str:
    """
    Get simplified file type for compatibility
    
    Args:
        file_type: Original file type
        
    Returns:
        Simplified type - "image" for images, "pdf" for PDF, original for others
    """
    if not file_type:
        return ""
    
    file_type_lower = file_type.lower()
    
    if "image" in file_type_lower or file_type_lower in ["png", "jpg", "jpeg", "gif"]:
        return "image"
    
    if "pdf" in file_type_lower:
        return "pdf"

    # Office spreadsheets — map the long MIME (or extension) to a stable "excel"
    # so the drive list `type` matches the file's scene and the frontend can pick
    # the right icon/filter instead of seeing the raw MIME.
    if ("spreadsheet" in file_type_lower or "excel" in file_type_lower
            or file_type_lower in ["xlsx", "xls", "xlsm", "xlsb"]):
        return "excel"

    if "csv" in file_type_lower:
        return "csv"

    return file_type


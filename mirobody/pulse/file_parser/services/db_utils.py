"""
Database utility functions for file parser services

Provides common utilities for:
- JSON serialization/deserialization
- Date parsing
- Error handling decorators
- Logging helpers
"""

import functools
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TypeVar
from zoneinfo import ZoneInfo

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


def parse_date(date_str: str, default: Optional[datetime] = None) -> Optional[datetime]:
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


def parse_iso_datetime(dt_str: str) -> Optional[datetime]:
    """
    Parse ISO format datetime string
    
    Args:
        dt_str: ISO format datetime string
        
    Returns:
        Parsed naive datetime or None
    """
    if not dt_str:
        return None
    
    try:
        # Handle Z suffix
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        parsed = datetime.fromisoformat(dt_str)
        return parsed.replace(tzinfo=None)
    except ValueError:
        pass
    
    # Fallback: try common ISO formats
    try:
        clean_time = dt_str.replace("Z", "").replace("+00:00", "")
        if "T" in clean_time:
            return datetime.strptime(clean_time.split(".")[0], "%Y-%m-%dT%H:%M:%S")
        else:
            return datetime.strptime(clean_time, "%Y-%m-%d")
    except ValueError:
        return None


def get_utc_now() -> datetime:
    """Get current UTC time as naive datetime"""
    return datetime.now(ZoneInfo("UTC")).replace(tzinfo=None)


def db_error_handler(
    default_return: Any = None,
    log_function: str = "",
    error_message: str = "Database operation failed"
) -> Callable:
    """
    Decorator for handling database errors with consistent logging
    
    Args:
        default_return: Value to return on error
        log_function: Function name for logging
        error_message: Error message prefix
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            func_name = log_function or func.__name__
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logging.error(f"{error_message}: {str(e)}", stack_info=True)
                return default_return
        return wrapper
    return decorator


def extract_first_record(result: Optional[List]) -> Optional[Dict]:
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


def format_datetime_iso(dt: Optional[datetime]) -> Optional[str]:
    """
    Format datetime to ISO string
    
    Args:
        dt: Datetime object
        
    Returns:
        ISO format string or None
    """
    if dt and isinstance(dt, datetime):
        return dt.isoformat()
    return None


# MIME type mapping for common file extensions
MIME_TYPE_MAP = {
    # Images
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
    ".bmp": "image/bmp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    # Documents
    ".pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    # Text
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".json": "application/json",
    ".xml": "application/xml",
    ".html": "text/html",
    ".htm": "text/html",
    ".md": "text/markdown",
    # Audio
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".ogg": "audio/ogg",
    ".m4a": "audio/mp4",
    ".flac": "audio/flac",
    # Video
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
    ".mkv": "video/x-matroska",
    # Archives
    ".zip": "application/zip",
    ".rar": "application/vnd.rar",
    ".7z": "application/x-7z-compressed",
    ".tar": "application/x-tar",
    ".gz": "application/gzip",
}


def get_mime_type(filename: str) -> str:
    """
    Get MIME type from filename extension
    
    Args:
        filename: Filename with extension
        
    Returns:
        MIME type string
    """
    if not filename or "." not in filename:
        return "application/octet-stream"
    
    ext = "." + filename.rsplit(".", 1)[-1].lower()
    return MIME_TYPE_MAP.get(ext, "application/octet-stream")


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
    
    return file_type


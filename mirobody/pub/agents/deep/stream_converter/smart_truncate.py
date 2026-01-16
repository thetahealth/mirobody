"""
Smart truncation module for stream converter.

Provides intelligent content truncation with two-stage strategy:
- Stage 1: Field-level truncation (preserve URLs, remove IDs, truncate content)
- Stage 2: Total length enforcement (remove whole elements, never break fields)
"""

import ast
import json
import logging
import os
import re
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Cache for truncation config
_truncation_config_cache: Optional[Dict[str, Any]] = None

# Global cutoff length for query detail content
DETAIL_CUTOFF = int(os.getenv("DETAIL_CUTOFF", 300))

# Maximum total length for tool result after truncation
MAX_TOTAL_LENGTH = int(os.getenv("MAX_TOTAL_LENGTH", 1500))


def get_truncation_config() -> Dict[str, Any]:
    """
    Load truncation strategy configuration.
    
    Three-layer processing strategy (priority high to low):
    1. remove_fields: Hide completely (technical fields useless to users)
    2. preserve_fields: Keep complete (critical info users need, NEVER truncate)
    3. truncate_fields: Truncate but keep (valuable but may be too long)
    
    Returns:
        Truncation config dict
    """
    global _truncation_config_cache
    
    if _truncation_config_cache is not None:
        return _truncation_config_cache
    
    default_config = {
        "preserve_fields": {
            # URLs (most important, never truncate)
            'url', 'urls', 'link', 'links', 'href',
            # File info
            'filename', 'file_name', 'file_path', 'path',
            # Titles
            'title', 'name', 'chart_title',
            # Status
            'success', 'status', 'type', 'message_type', 'is_chart',
            # User-friendly messages
            'message',
        },
        "remove_fields": {
            # Internal IDs
            'id', 'uuid', 'file_key', 'tag_name', '_id', 'internal_id', 'node_id',
            # Technical errors (shown elsewhere in friendly way)
            'error',
        },
        "truncate_fields": {
            # Long text content
            'content', 'description', 'detail', 'text', 'body', 'summary', 'notes',
            # Abstracts and raw data
            'abstract', 'data', 'raw', 'system_reminder',
            # Timestamps
            'date', 'timestamp', 'created_at', 'updated_at',
        },
        "field_max_length": 500,
        "max_array_length": 10,
        "array_fields": {'results', 'files', 'items', 'data', 'list'}
    }
    
    try:
        from mirobody.utils.config import global_config
        config = global_config()
        if config and (config_dict := config.get_dict("STREAM_TRUNCATION_CONFIG", {})):
            logger.info(f"Loaded truncation config: {config_dict}")
            for key in ['preserve_fields', 'truncate_fields', 'remove_fields', 'array_fields']:
                if key in config_dict:
                    default_config[key] = set(config_dict[key]) if isinstance(config_dict[key], list) else default_config[key]
            for key in ['field_max_length', 'max_array_length']:
                if key in config_dict:
                    default_config[key] = config_dict[key]
    except Exception as e:
        logger.warning(f"Failed to load truncation config, using defaults: {e}")
    
    _truncation_config_cache = default_config
    return default_config


def smart_truncate(content: Any, max_length: int = None) -> str:
    """
    Smart truncate content with two-stage strategy:
    Stage 1: Field-level truncation (preserve URLs, remove IDs, truncate content)
    Stage 2: Total length enforcement (remove whole elements, never break fields)
    
    Args:
        content: Content to truncate
        max_length: Max length (defaults to DETAIL_CUTOFF)
        
    Returns:
        Truncated string
    """
    max_length = max_length or DETAIL_CUTOFF
    
    # Stage 1: Parse and apply field-level truncation
    if isinstance(content, dict):
        truncated = truncate_dict(content, max_length)
        # Stage 2: Enforce total length
        truncated = enforce_total_length(truncated, MAX_TOTAL_LENGTH)
        return json.dumps(truncated, ensure_ascii=False)
    
    if isinstance(content, str):
        # Try parsing as JSON or Python dict
        for parse_fn, dump_fn in [(json.loads, json.dumps), (ast.literal_eval, str)]:
            try:
                parsed = parse_fn(content)
                if isinstance(parsed, dict):
                    truncated = truncate_dict(parsed, max_length)
                    truncated = enforce_total_length(truncated, MAX_TOTAL_LENGTH)
                    return dump_fn(truncated, ensure_ascii=False) if dump_fn == json.dumps else str(truncated)
                elif isinstance(parsed, list):
                    truncated = truncate_list(parsed, max_length)
                    truncated = enforce_total_length(truncated, MAX_TOTAL_LENGTH)
                    return dump_fn(truncated, ensure_ascii=False) if dump_fn == json.dumps else str(truncated)
            except (json.JSONDecodeError, TypeError, ValueError, SyntaxError):
                continue
    
    # Fallback: plain text truncation (protecting URLs)
    return truncate_text(str(content), max_length)


def truncate_dict(data: dict, max_length: int) -> dict:
    """
    Smart dict truncation with three-layer strategy.
    
    Priority: 1.Remove → 2.Preserve → 3.Truncate → 4.Auto-handle
    CRITICAL: preserve_fields are NEVER truncated.
    
    Args:
        data: Dictionary to process
        max_length: Max length reference
        
    Returns:
        Processed dict
    """
    config = get_truncation_config()
    preserve = config["preserve_fields"]
    truncate = config["truncate_fields"]
    remove = config.get("remove_fields", set())
    field_max = config["field_max_length"]
    array_fields = config["array_fields"]
    
    result = {}
    
    for key, value in data.items():
        key_lower = key.lower()
        
        # Priority 1: Remove (skip completely)
        if key_lower in remove:
            continue
        
        # Priority 2: Preserve (keep as-is, NEVER truncate)
        if key_lower in preserve:
            result[key] = value
            continue
        
        # Priority 3: Truncate long text
        if key_lower in truncate:
            if isinstance(value, str) and len(value) > field_max:
                result[key] = truncate_text(value, field_max)
            else:
                result[key] = value
            continue
        
        # Special array fields - limit element count
        if key_lower in array_fields and isinstance(value, list):
            result[key] = truncate_list(value, max_length, key_lower)
            continue
        
        # Nested dict - recurse
        if isinstance(value, dict):
            result[key] = truncate_dict(value, field_max)
        # Regular list
        elif isinstance(value, list):
            result[key] = truncate_list(value, max_length)
        # String - truncate if too long (non-preserve fields)
        elif isinstance(value, str) and len(value) > field_max:
            result[key] = value[:field_max] + "..."
        # Basic types - keep as-is
        elif isinstance(value, (int, float, bool, type(None))):
            result[key] = value
        # Unknown type - convert to string and truncate
        else:
            value_str = str(value)
            result[key] = value_str[:field_max] + "..." if len(value_str) > field_max else value_str
    
    return result


def truncate_list(data: list, max_length: int, field_name: str = None) -> list:
    """
    Truncate list by removing whole elements (not truncating individual items).
    
    Args:
        data: List to process
        max_length: Max length reference
        field_name: Optional field name for hint message
        
    Returns:
        Truncated list with omitted hint if needed
    """
    config = get_truncation_config()
    max_array = config["max_array_length"]
    field_max = config["field_max_length"]
    
    # Process each item
    def process(item):
        if isinstance(item, dict):
            return truncate_dict(item, max_length)
        elif isinstance(item, str):
            return truncate_text(item, field_max)
        return item
    
    # Within limit - process all
    if len(data) <= max_array:
        return [process(item) for item in data]
    
    # Over limit - keep first N elements, add omitted hint
    result = [process(item) for item in data[:max_array]]
    omitted = len(data) - max_array
    if field_name in ['results', 'files', 'items']:
        hint = f"...(omitted {omitted} {field_name})"
    else:
        hint = f"...(omitted {omitted} elements)"
    result.append(hint)
    return result


def truncate_text(text: str, max_length: int) -> str:
    """
    Smart text truncation, protecting URL integrity.
    
    Args:
        text: Text to truncate
        max_length: Max length
        
    Returns:
        Truncated text with URLs intact
    """
    if not isinstance(text, str):
        text = str(text)
    
    if len(text) <= max_length:
        return text
    
    # Find all URLs
    urls = list(re.finditer(r'https?://[^\s<>"{}|\\^`\[\]]+', text))
    
    # Adjust truncate position if it would break a URL
    truncate_pos = max_length
    for url_match in urls:
        url_start, url_end = url_match.span()
        if url_start < truncate_pos < url_end:
            # Would cut URL - move truncate point before URL
            truncate_pos = url_start
            break
    
    return truncate_at_boundary(text, truncate_pos)


def truncate_at_boundary(text: str, max_length: int) -> str:
    """
    Truncate at natural sentence/paragraph boundary.
    
    Args:
        text: Text to truncate
        max_length: Max length
        
    Returns:
        Truncated text at boundary with ellipsis
    """
    if len(text) <= max_length:
        return text
    
    # Find best boundary near max_length
    boundaries = ['\n\n', '\n', '。', '！', '？', '. ', '! ', '? ', '；', '; ', '，', ', ', ' ']
    search_start = max(0, max_length - 100)
    search_end = min(len(text), max_length)
    search_text = text[search_start:search_end]
    
    for boundary in boundaries:
        pos = search_text.rfind(boundary)
        if pos != -1:
            return text[:search_start + pos + len(boundary)].rstrip() + "..."
    
    # No boundary found - hard truncate
    return text[:max_length].rstrip() + "..."


def enforce_total_length(data: Any, max_total: int) -> Any:
    """
    Enforce total serialized length by removing WHOLE elements.
    CRITICAL: Never truncate preserve_fields values.
    
    Strategy:
    - For list: Remove elements from end until length satisfied
    - For dict with list fields: Reduce list elements
    - Always keep structure intact, never break individual fields
    
    Args:
        data: Already field-truncated dict or list
        max_total: Maximum total serialized length
        
    Returns:
        Data with total length <= max_total
    """
    # Check current length
    try:
        serialized = json.dumps(data, ensure_ascii=False)
    except (TypeError, ValueError):
        serialized = str(data)
    
    if len(serialized) <= max_total:
        return data
    
    # Need to reduce - work on a copy
    if isinstance(data, list):
        return reduce_list_to_length(data.copy(), max_total)
    elif isinstance(data, dict):
        return reduce_dict_to_length(data.copy(), max_total)
    
    # Plain string - just truncate (shouldn't have preserve fields at this point)
    return serialized[:max_total].rstrip() + "..."


def reduce_list_to_length(data: list, max_total: int) -> list:
    """
    Reduce list by removing whole elements from end.
    
    Args:
        data: List to reduce
        max_total: Target max length
        
    Returns:
        Reduced list with omitted hint
    """
    original_len = len(data)
    
    # Check if last element is already an omitted hint
    has_hint = data and isinstance(data[-1], str) and data[-1].startswith("...")
    if has_hint:
        data = data[:-1]
    
    # Remove elements from end until size fits
    removed = 0
    while len(data) > 1:
        try:
            test_serialized = json.dumps(data, ensure_ascii=False)
        except (TypeError, ValueError):
            test_serialized = str(data)
        
        if len(test_serialized) <= max_total - 50:  # Reserve space for hint
            break
        
        data.pop()
        removed += 1
    
    # Add omitted hint
    if removed > 0 or has_hint:
        total_omitted = removed + (original_len - len(data) - removed if has_hint else 0)
        data.append(f"...(omitted {total_omitted} elements)")
    
    return data


def reduce_dict_to_length(data: dict, max_total: int) -> dict:
    """
    Reduce dict by shrinking list fields (removing whole elements).
    
    Strategy:
    1. Find all list fields that can be reduced
    2. Reduce the largest list first
    3. Repeat until size fits
    4. NEVER touch preserve_fields
    
    Args:
        data: Dict to reduce
        max_total: Target max length
        
    Returns:
        Reduced dict
    """
    config = get_truncation_config()
    preserve = config["preserve_fields"]
    
    while True:
        try:
            serialized = json.dumps(data, ensure_ascii=False)
        except (TypeError, ValueError):
            serialized = str(data)
        
        if len(serialized) <= max_total:
            return data
        
        # Find largest reducible list field
        largest_list_key = None
        largest_list_len = 0
        
        for key, value in data.items():
            # Skip preserve fields
            if key.lower() in preserve:
                continue
            
            if isinstance(value, list) and len(value) > 1:
                if len(value) > largest_list_len:
                    largest_list_key = key
                    largest_list_len = len(value)
        
        if largest_list_key is None:
            # No more lists to reduce - we've done our best
            break
        
        # Remove one element from the largest list
        lst = data[largest_list_key]
        
        # Check if last element is hint
        has_hint = lst and isinstance(lst[-1], str) and lst[-1].startswith("...")
        if has_hint:
            # Remove second-to-last element
            if len(lst) > 2:
                lst.pop(-2)
            else:
                break
        else:
            # Remove last element, add hint
            lst.pop()
            lst.append("...(omitted)")
        
        data[largest_list_key] = lst
    
    return data

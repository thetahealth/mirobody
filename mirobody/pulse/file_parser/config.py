"""
File parser configuration module

This module manages optional dependencies like ExcelProcessor and CSVProcessor.
When running mirobody standalone, these dependencies are not available.
When running mcp_server, it can inject these dependencies for extended functionality.
"""

from typing import Any, Optional

# Global storage for optional processors
_excel_processor_instance: Optional[Any] = None
_csv_processor_instance: Optional[Any] = None


def set_excel_processor(processor) -> None:
    """
    Set the global ExcelProcessor instance.
    
    This should be called by mcp_server during initialization to enable
    Excel file processing capabilities.
    
    Args:
        processor: An ExcelProcessor instance from mcp_server.file_parser
    """
    global _excel_processor_instance
    _excel_processor_instance = processor


def get_excel_processor() -> Optional[Any]:
    """
    Get the global ExcelProcessor instance.
    
    Returns:
        ExcelProcessor instance if set, None otherwise
    """
    return _excel_processor_instance


def clear_excel_processor() -> None:
    """
    Clear the global ExcelProcessor instance.
    
    Useful for testing or when Excel processing should be disabled.
    """
    global _excel_processor_instance
    _excel_processor_instance = None


def set_csv_processor(processor) -> None:
    """
    Set the global CSVProcessor instance.
    
    This should be called by mcp_server during initialization to enable
    CSV file processing capabilities (e.g., medication orders).
    
    Args:
        processor: A CSVProcessor instance from mcp_server.file_parser
    """
    global _csv_processor_instance
    _csv_processor_instance = processor


def get_csv_processor() -> Optional[Any]:
    """
    Get the global CSVProcessor instance.
    
    Returns:
        CSVProcessor instance if set, None otherwise
    """
    return _csv_processor_instance


def clear_csv_processor() -> None:
    """
    Clear the global CSVProcessor instance.
    
    Useful for testing or when CSV processing should be disabled.
    """
    global _csv_processor_instance
    _csv_processor_instance = None


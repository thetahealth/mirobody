"""
File Parser module for processing various file types and extracting health data

Handles file uploads via WebSocket, routes files to type-specific handlers
(PDF, CSV, Excel, audio, image, genetic), extracts health indicators,
and feeds results into the StandardPulseData pipeline.

Architecture:
    WebSocketFileUploadManager — orchestrates upload sessions and progress tracking
    handlers/ — type-specific parsers (factory pattern via handlers/factory.py)
    services/ — shared processing logic (async processing, content extraction, DB)
"""

from mirobody.pulse.file_parser.file_upload_manager import (
    WebSocketFileUploadManager,
    get_websocket_file_upload_manager,
)

__all__ = [
    # File upload manager
    "WebSocketFileUploadManager",
    "get_websocket_file_upload_manager",
]

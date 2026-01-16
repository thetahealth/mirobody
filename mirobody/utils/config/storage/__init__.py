"""
Unified storage layer for file operations

This module provides a unified interface for different storage backends:
- AWS S3
- Aliyun OSS
- MinIO (default fallback)

Usage:
    from mirobody.utils.config.storage import get_storage_client
    
    # Get storage client (lazy initialization)
    storage = get_storage_client()
    
    # Upload file
    url, error = await storage.put(
        key="uploads/file.pdf",
        content=file_bytes,
        content_type="application/pdf"
    )
    
    # Download file
    content, url = await storage.get(key="uploads/file.pdf")
    
    # Delete file
    success, error = await storage.delete(key="uploads/file.pdf")
"""

# Import all storage classes
from .abstract import AbstractStorage
from .aws import AwsStorage
from .aliyun import AliyunStorage
from .minio import MinioStorage

# Import configuration and factory
from .config_manager import StorageConfigManager
from .factory import StorageFactory, get_storage_client

#-----------------------------------------------------------------------------
# Export all
#-----------------------------------------------------------------------------

__all__ = [
    # Storage classes
    "AbstractStorage",
    "AwsStorage",
    "AliyunStorage",
    "MinioStorage",
    
    # Configuration and factory
    "StorageConfigManager",
    "StorageFactory",
    "get_storage_client",
]

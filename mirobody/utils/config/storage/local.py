import asyncio
import logging

from pathlib import Path
from typing import IO, Optional, Dict, Any
from datetime import datetime

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class LocalStorage(AbstractStorage):
    """
    Local filesystem storage implementation
    
    Stores files in a local directory without requiring external services.
    Suitable for single-instance deployments and development environments.
    """
    
    def __init__(
        self,
        base_path   : str = "",
        prefix      : str = "",
        proxy_url   : str = "",
        **kwargs  # Accept and ignore other AbstractStorage parameters
    ):
        """
        Initialize local filesystem storage
        
        Args:
            base_path: Base directory for file storage (default: ./.theta/mcp/upload/)
            prefix: Key prefix for all objects
            proxy_url: Backend proxy URL for file access (e.g., http://localhost:18080/files)
        """

        if not base_path or not proxy_url:
            from ..config import global_config
            config = global_config()
            if config:
                if not base_path:
                    base_path = config.get_str("LOCAL_STORAGE_BASE_PATH")
                if not proxy_url:
                    data_public_url = config.get_str("MCP_PUBLIC_URL")
                    proxy_url = f"{data_public_url.rstrip('/')}/files" if data_public_url else ""

        if not base_path:
            from .constants import DEFAULT_LOCAL_UPLOAD_PATH
            base_path = DEFAULT_LOCAL_UPLOAD_PATH

        # Initialize parent with minimal parameters
        super().__init__(prefix=prefix)
        
        self.base_path = Path(base_path)
        self.proxy_url = proxy_url.strip() if proxy_url else ""
        
        # Ensure base directory exists
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Local storage initialized: base_path={self.base_path}, proxy_url={self.proxy_url}")
        except Exception as e:
            logger.error(f"Failed to create base directory {self.base_path}: {str(e)}")
            raise
    
    #-----------------------------------------------------
    
    def _get_file_path(self, key: str) -> Path:
        """
        Get full file path from key
        
        Args:
            key: File key/path
            
        Returns:
            Full filesystem path
        """
        object_key = self._build_object_key(key)
        return self.base_path / object_key
    
    #-----------------------------------------------------
    
    def _build_url(self, key: str) -> str:
        """
        Build access URL for file
        
        Args:
            key: File key/path
            
        Returns:
            File access URL
        """
        object_key = self._build_object_key(key)
        
        if self.proxy_url:
            # Use proxy URL: http://localhost:18080/files/uploads/file.pdf
            return f"{self.proxy_url.rstrip('/')}/{object_key}"
        else:
            # Fallback: return relative path
            return f"http://localhost:18080/files/{object_key}"
    
    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | IO,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """
        Upload file to local storage
        
        Args:
            key: File key/path
            content: File content (bytes or file-like object)
            content_type: MIME type (optional, for metadata only)
            metadata: Additional metadata (stored as extended attributes if supported)
            expires: Not used for local storage
            
        Returns:
            Tuple of (url, error_message)
        """
        try:
            file_path = self._get_file_path(key)
            
            # Create parent directories
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write content to file
            loop = asyncio.get_running_loop()
            
            if isinstance(content, bytes):
                # Write bytes directly
                await loop.run_in_executor(
                    None,
                    lambda: file_path.write_bytes(content)
                )
            elif isinstance(content, bytearray):
                # Convert bytearray to bytes
                await loop.run_in_executor(
                    None,
                    lambda: file_path.write_bytes(bytes(content))
                )
            elif hasattr(content, "read"):
                # File-like object
                def write_from_file():
                    with open(file_path, "wb") as f:
                        if hasattr(content, "seek"):
                            content.seek(0)
                        chunk_size = 8192
                        while True:
                            chunk = content.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                
                await loop.run_in_executor(None, write_from_file)
            else:
                return None, "Unsupported content type"
            
            # Generate URL
            url = self._build_url(key)
            
            logger.info(f"File saved to local storage: {file_path} -> {url}")
            
            return url, None
            
        except Exception as e:
            error_msg = f"Failed to save file to local storage: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """
        Get file from local storage

        Args:
            key: File key/path

        Returns:
            (content, error). Returns (None, error) on failure.
        """
        try:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return None, f"File not found: {key}"

            # Read file content
            loop = asyncio.get_running_loop()
            content = await loop.run_in_executor(
                None,
                lambda: file_path.read_bytes()
            )

            return content, None

        except Exception as e:
            error_msg = f"Failed to read file from local storage: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> str | None:
        """
        Delete file from local storage

        Args:
            key: File key/path

        Returns:
            Error message string, or None on success.
        """
        try:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                logger.warning(f"File not found for deletion: {file_path}")
                return None  # Consider non-existent file as successfully deleted

            # Delete file
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: file_path.unlink()
            )

            logger.info(f"File deleted from local storage: {file_path}")

            return None

        except Exception as e:
            error_msg = f"Failed to delete file from local storage: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self,
        key: str,
        expires: int = 7200,
        content_type: str | None = None
    ) -> tuple[str | None, str | None]:
        """
        Generate URL for local file access

        Note: Local storage doesn't use signed URLs. Returns proxy URL directly.

        Args:
            key: File key/path
            expires: Not used for local storage
            content_type: Not used for local storage

        Returns:
            (signed_url, error). If successful, error is None.
        """
        try:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return None, f"File not found: {key}"

            return self._build_url(key), None

        except Exception as e:
            error_msg = f"Failed to generate URL for local file: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> tuple[Dict[str, Any] | None, str | None]:
        """
        Get file metadata from local storage

        Args:
            key: File key/path

        Returns:
            (file_info, error). If successful, error is None.
        """
        try:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                return None, f"File not found: {key}"

            loop = asyncio.get_running_loop()

            def get_stat():
                stat = file_path.stat()
                content_type = self.get_content_type_from_filename(file_path.name)

                return {
                    "success": True,
                    "size": stat.st_size,
                    "content_type": content_type,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime),
                    "created": datetime.fromtimestamp(stat.st_ctime),
                    "path": str(file_path)
                }

            return await loop.run_in_executor(None, get_stat), None

        except Exception as e:
            error_msg = f"Failed to get file info from local storage: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

#-----------------------------------------------------------------------------

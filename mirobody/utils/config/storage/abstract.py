from typing import IO, Optional, Dict, Any

#-----------------------------------------------------------------------------

class AbstractStorage:
    """Abstract base class for all storage backends (S3, OSS, MinIO)"""
    
    def __init__(
        self,
        access_key_id       : str = "",
        secret_access_key   : str = "",
        region              : str = "",
        bucket              : str = "",
        prefix              : str = "",
        cdn                 : str = "",
        endpoint            : str = ""
    ):
        self.region = region.strip() if region else ""
        self.access_key_id = access_key_id.strip() if access_key_id else ""
        self.secret_access_key = secret_access_key.strip() if secret_access_key else ""
        self.bucket = bucket.strip() if bucket else ""
        self.prefix = prefix.strip() if prefix else ""
        self.cdn = cdn.strip() if cdn else ""
        self.endpoint = endpoint.strip() if endpoint else ""

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
        Upload file to storage
        
        Args:
            key: File key/path
            content: File content (bytes or file-like object)
            content_type: MIME type of the file
            metadata: Additional metadata
            expires: URL expiration time in seconds
            
        Returns:
            Tuple of (url, error_message). If successful, error_message is None.
        """
        ...

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """
        Get file content and generate access URL
        
        Args:
            key: File key/path
            
        Returns:
            Tuple of (file_content, url). Returns (None, None) on error.
        """
        ...

    async def delete(self, key: str) -> tuple[bool, str | None]:
        """
        Delete file from storage
        
        Args:
            key: File key/path
            
        Returns:
            Tuple of (success, error_message)
        """
        ...

    async def generate_signed_url(
        self, 
        key: str, 
        expires: int = 7200,
        content_type: str | None = None
    ) -> str | None:
        """
        Generate signed URL for file access
        
        Args:
            key: File key/path
            expires: URL expiration time in seconds
            content_type: MIME type for ResponseContentType header (optional)
            
        Returns:
            Signed URL or None on error
        """
        ...

    async def get_file_info(self, key: str) -> Dict[str, Any] | None:
        """
        Get file metadata information
        
        Args:
            key: File key/path
            
        Returns:
            Dictionary containing file info (size, content_type, last_modified, etc.)
            Returns None on error
        """
        ...

    def get_storage_type(self) -> str:
        """
        Get storage type identifier
        """
        return self.__class__.__name__.lower().removesuffix("storage")

    #-----------------------------------------------------

    def _build_object_key(self, key: str) -> str:
        """
        Build full object key with prefix.
        Handles empty prefix correctly without adding extra slashes.
        
        Args:
            key: File key/path
            
        Returns:
            Full object key with prefix (if configured)
        """
        # Clean prefix: remove quotes and whitespace, treat "", '', "null", "none" as empty
        prefix = self.prefix.strip().strip('"').strip("'") if self.prefix else ""
        if prefix.lower() in ("", "null", "none"):
            prefix = ""
        
        if prefix:
            return f"{prefix.strip('/')}/{key.lstrip('/')}"
        return key.lstrip("/")

    #-----------------------------------------------------

    def get_content_type(self, content: bytes) -> str:
        """
        Detect content type from file content using magic numbers
        
        Args:
            content: File content bytes
            
        Returns:
            MIME type string
        """
        try:
            import magic
            content_type = magic.from_buffer(content, mime=True)
            return content_type
        except:
            return "application/octet-stream"

    def get_content_type_from_filename(self, filename: str) -> str:
        """
        Get content type based on file extension
        
        Args:
            filename: File name with extension
            
        Returns:
            MIME type string
        """
        ext = ""
        if filename and "." in filename:
            ext = filename.split(".")[-1].lower()

        content_types = {
            # Images
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
            "gif": "image/gif",
            "webp": "image/webp",
            "svg": "image/svg+xml",
            "ico": "image/x-icon",
            # Documents
            "pdf": "application/pdf",
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xls": "application/vnd.ms-excel",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "ppt": "application/vnd.ms-powerpoint",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "txt": "text/plain",
            # Audio/Video
            "mp3": "audio/mpeg",
            "mp4": "video/mp4",
            "mov": "video/quicktime",
            "avi": "video/x-msvideo",
            "wmv": "video/x-ms-wmv",
            "flv": "video/x-flv",
            "webm": "video/webm",
            # Web
            "html": "text/html",
            "htm": "text/html",
            "css": "text/css",
            "js": "application/javascript",
            "json": "application/json",
            "xml": "application/xml",
            # Archives
            "zip": "application/zip",
            "rar": "application/x-rar-compressed",
            "7z": "application/x-7z-compressed",
            "tar": "application/x-tar",
            "gz": "application/gzip",
        }

        return content_types.get(ext, "application/octet-stream")

#-----------------------------------------------------------------------------

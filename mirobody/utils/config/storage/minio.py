import asyncio
import logging
from functools import partial
from typing import IO, Optional, Dict, Any

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class MinioStorage(AbstractStorage):
    """
    MinIO storage implementation
    
    Supports both public and private bucket access:
    - Public bucket: Returns direct URL (no signature required)
    - Private bucket: Returns signed URL with expiration
    
    Distinguishes between:
    - endpoint: Internal URL for API calls (e.g., http://minio:9000 in Docker)
    - public_url: External URL for browser access (e.g., http://localhost:9000)
    - proxy_url: Backend proxy URL (e.g., http://localhost:18080/files) - recommended for Docker
    """
    
    def __init__(
        self,
        access_key_id       : str = "",
        secret_access_key   : str = "",
        region              : str = "us-east-1",
        bucket              : str = "",
        prefix              : str = "",
        cdn                 : str = "",
        endpoint            : str = "",
        public_url          : str = "",
        proxy_url           : str = "",
        public              : bool = True
    ):
        """
        Initialize MinIO storage client
        
        Args:
            access_key_id: MinIO access key
            secret_access_key: MinIO secret key
            region: Region name (default: us-east-1)
            bucket: Bucket name
            prefix: Key prefix for all objects
            cdn: CDN URL (optional)
            endpoint: MinIO internal endpoint URL (for API calls)
            public_url: MinIO external URL (for browser access)
            proxy_url: Backend proxy URL (recommended for Docker environments)
            public: Whether bucket is public (default: True)
        """
        super().__init__(access_key_id, secret_access_key, region, bucket, prefix, cdn, endpoint)
        
        self.public_url = public_url.strip() if public_url else endpoint
        self.proxy_url = proxy_url.strip() if proxy_url else ""
        self.public = public
        
        # Lazy initialization
        self._client = None
        self._initialized = False
        
        logger.info(
            f"MinIO storage initialized: endpoint={endpoint}, "
            f"public_url={self.public_url}, proxy_url={self.proxy_url}, bucket={bucket}, public={public}"
        )
    
    #-----------------------------------------------------
    
    def _ensure_initialized(self):
        """Ensure MinIO client is initialized"""
        if self._initialized:
            return
        
        try:
            from minio import Minio
            from urllib.parse import urlparse
            
            # Parse endpoint URL to extract host and determine secure mode
            parsed = urlparse(self.endpoint)
            host = parsed.netloc or parsed.path
            secure = parsed.scheme == "https"
            
            # Build client kwargs
            client_kwargs = {
                "endpoint": host,
                "access_key": self.access_key_id,
                "secret_key": self.secret_access_key,
                "secure": secure,
            }
            
            # Only add region if specified
            if self.region:
                client_kwargs["region"] = self.region
            
            self._client = Minio(**client_kwargs)
            
            self._initialized = True
            logger.info(f"MinIO client initialized: host={host}, secure={secure}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {str(e)}", exc_info=True)
            raise
    
    #-----------------------------------------------------
    
    def _build_public_url(self, object_key: str) -> str:
        """
        Build public URL for file access
        
        Priority:
        1. proxy_url (recommended for Docker) - e.g., http://localhost:18080/files/uploads/file.pdf
        2. cdn (if configured)
        3. public_url (direct MinIO access) - e.g., http://localhost:9000/bucket/uploads/file.pdf
        
        Args:
            object_key: Full object key including prefix
            
        Returns:
            Public URL for file access
        """
        # Priority 1: Use proxy URL (recommended for Docker environments)
        if self.proxy_url:
            # Proxy URL format: {proxy_url}/{object_key}
            # e.g., http://localhost:18080/files/uploads/file.pdf
            return f"{self.proxy_url.rstrip('/')}/{object_key}"
        
        # Priority 2: Use CDN if configured
        if self.cdn:
            return f"{self.cdn.rstrip('/')}/{self.bucket}/{object_key}"
        
        # Priority 3: Direct MinIO URL
        return f"{self.public_url.rstrip('/')}/{self.bucket}/{object_key}"
    
    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | IO,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """Upload file to MinIO"""
        try:
            self._ensure_initialized()
            
            # Build full object key with prefix
            object_key = self._build_object_key(key)
            
            # Determine content type
            if not content_type:
                content_type = self.get_content_type_from_filename(key)
            
            # Prepare content
            if isinstance(content, bytes):
                from io import BytesIO
                data = BytesIO(content)
                length = len(content)
            elif isinstance(content, bytearray):
                from io import BytesIO
                data = BytesIO(bytes(content))
                length = len(content)
            elif hasattr(content, "read"):
                # File-like object - need to get length
                if hasattr(content, "seek") and hasattr(content, "tell"):
                    content.seek(0, 2)  # Seek to end
                    length = content.tell()
                    content.seek(0)  # Seek back to start
                else:
                    # Read all content into bytes
                    file_content = content.read()
                    from io import BytesIO
                    data = BytesIO(file_content)
                    length = len(file_content)
                    content = data
                data = content
            else:
                return None, "Unsupported content type"
            
            # Execute upload in thread pool (minio SDK is synchronous)
            loop = asyncio.get_event_loop()
            
            # Build put_object kwargs
            put_kwargs = {
                "bucket_name": self.bucket,
                "object_name": object_key,
                "data": data,
                "length": length,
                "content_type": content_type,
            }
            
            await loop.run_in_executor(
                None,
                partial(self._client.put_object, **put_kwargs)
            )
            
            # Generate URL based on bucket privacy
            if self.public:
                # Public bucket - return direct URL
                url = self._build_public_url(object_key)
            else:
                # Private bucket - return signed URL
                url = await self.generate_signed_url(key, expires)
            
            logger.info(f"File uploaded to MinIO successfully: {object_key} -> {url}")
            
            return url, None
            
        except Exception as e:
            error_msg = f"Failed to upload to MinIO: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """Get file from MinIO and generate access URL"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            
            # Get file content using keyword arguments
            response = await loop.run_in_executor(
                None,
                partial(
                    self._client.get_object,
                    bucket_name=self.bucket,
                    object_name=object_key
                )
            )
            
            content = response.read()
            response.close()
            response.release_conn()
            
            # Generate URL
            if self.public:
                url = self._build_public_url(object_key)
            else:
                url = await self.generate_signed_url(key, 7200)
            
            return content, url
            
        except Exception as e:
            logger.error(f"Failed to get file from MinIO: {str(e)}", exc_info=True)
            return None, None
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> tuple[bool, str | None]:
        """Delete file from MinIO"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                partial(
                    self._client.remove_object,
                    bucket_name=self.bucket,
                    object_name=object_key
                )
            )
            
            logger.info(f"File deleted from MinIO successfully: {object_key}")
            
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to delete from MinIO: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self, 
        key: str, 
        expires: int = 7200,
        content_type: str | None = None
    ) -> str | None:
        """
        Generate URL for MinIO file access
        
        If proxy_url is configured (recommended for Docker), returns proxy URL.
        Otherwise, generates a signed URL for direct MinIO access.
        
        Note: content_type parameter is accepted for interface consistency but
        may not be used for public/proxy URLs. It's used for signed URLs
        in private bucket mode.
        """
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            # Priority 1: Use proxy URL if configured (recommended for Docker)
            if self.proxy_url:
                return self._build_public_url(object_key)
            
            # Priority 2: For public buckets, use direct public URL
            if self.public:
                return self._build_public_url(object_key)
            
            # Priority 3: Generate signed URL for private buckets
            from datetime import timedelta
            
            loop = asyncio.get_event_loop()
            
            # Build presigned URL kwargs
            presigned_kwargs = {
                "bucket_name": self.bucket,
                "object_name": object_key,
                "expires": timedelta(seconds=expires)
            }
            
            # Add response headers if content_type is provided
            if content_type:
                presigned_kwargs["response_headers"] = {
                    "response-content-type": content_type,
                    "response-content-disposition": "inline"
                }
            
            url = await loop.run_in_executor(
                None,
                partial(self._client.presigned_get_object, **presigned_kwargs)
            )
            
            # If public_url differs from endpoint, replace in signed URL
            if self.public_url and self.public_url != self.endpoint:
                url = url.replace(self.endpoint, self.public_url)
            
            return url
            
        except Exception as e:
            logger.error(f"Failed to generate MinIO signed URL: {str(e)}", exc_info=True)
            return None

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> Dict[str, Any] | None:
        """Get file metadata from MinIO"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            stat = await loop.run_in_executor(
                None,
                partial(
                    self._client.stat_object,
                    bucket_name=self.bucket,
                    object_name=object_key
                )
            )
            
            return {
                "success": True,
                "size": stat.size,
                "content_type": stat.content_type,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
                "metadata": stat.metadata or {}
            }
            
        except Exception as e:
            logger.error(f"Failed to get file info from MinIO: {str(e)}", exc_info=True)
            return None

    #-----------------------------------------------------

    async def health_check(self) -> tuple[bool, str]:
        """
        Check if MinIO service is available
        
        Returns:
            Tuple of (is_healthy, message)
        """
        try:
            self._ensure_initialized()
            
            loop = asyncio.get_event_loop()
            
            # Check if bucket exists
            exists = await loop.run_in_executor(
                None,
                partial(self._client.bucket_exists, bucket_name=self.bucket)
            )
            
            if exists:
                logger.info(f"MinIO health check passed: {self.endpoint}, bucket={self.bucket}")
                return True, "MinIO service is healthy"
            else:
                return False, f"Bucket '{self.bucket}' does not exist"
            
        except Exception as e:
            error_msg = f"MinIO health check failed: {str(e)}"
            logger.warning(error_msg, exc_info=True)
            return False, error_msg

#-----------------------------------------------------------------------------

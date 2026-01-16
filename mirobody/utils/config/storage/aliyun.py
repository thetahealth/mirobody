import asyncio
import logging
import traceback
from typing import IO, Optional, Dict, Any
from functools import partial

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class AliyunStorage(AbstractStorage):
    """Aliyun OSS storage implementation"""
    
    def __init__(
        self,
        access_key_id       : str,
        secret_access_key   : str,
        region              : str,
        bucket              : str,
        prefix              : str = "",
        cdn                 : str = "",
        endpoint            : str = ""
    ):
        super().__init__(access_key_id, secret_access_key, region, bucket, prefix, cdn, endpoint)
        
        # Lazy import and initialization
        self._oss2 = None
        self._auth = None
        self._bucket = None
        self._endpoint_url = None
        self._initialized = False
    
    #-----------------------------------------------------
    
    def _ensure_initialized(self):
        """Ensure OSS client is initialized"""
        if self._initialized:
            return
        
        try:
            # Import oss2 with warning suppression
            import warnings
            warnings.filterwarnings("ignore", category=SyntaxWarning, module="oss2")
            import oss2
            self._oss2 = oss2
            
            # Build endpoint URL
            protocol = "https"
            self._endpoint_url = f"{protocol}://{self.endpoint}"
            
            # Create Auth instance
            self._auth = oss2.Auth(self.access_key_id, self.secret_access_key)
            
            # Create Bucket instance
            self._bucket = oss2.Bucket(self._auth, self._endpoint_url, self.bucket)
            
            self._initialized = True
            logger.info(f"Aliyun OSS client initialized: bucket={self.bucket}, endpoint={self.endpoint}")
        except Exception as e:
            logger.error(f"Failed to initialize Aliyun OSS client: {str(e)}", exc_info=True)
            raise
    
    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | IO,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """Upload file to Aliyun OSS"""
        try:
            self._ensure_initialized()
            
            # Build full object key with prefix
            object_key = self._build_object_key(key)
            
            # Prepare headers
            headers = {}
            if content_type:
                headers["Content-Type"] = content_type
            else:
                # Auto-detect content type from filename
                headers["Content-Type"] = self.get_content_type_from_filename(key)
            
            # Add metadata
            if metadata:
                for k, v in metadata.items():
                    headers[f"x-oss-meta-{k}"] = str(v)
            
            # Execute upload in thread pool (oss2 is synchronous)
            loop = asyncio.get_event_loop()
            
            if isinstance(content, bytes):
                result = await loop.run_in_executor(
                    None,
                    partial(self._bucket.put_object, object_key, content, headers=headers)
                )
            elif isinstance(content, bytearray):
                result = await loop.run_in_executor(
                    None,
                    partial(self._bucket.put_object, object_key, bytes(content), headers=headers)
                )
            elif hasattr(content, "read"):
                result = await loop.run_in_executor(
                    None,
                    partial(self._bucket.put_object, object_key, content, headers=headers)
                )
            else:
                return None, "Unsupported content type"
            
            # Generate signed URL
            response_headers = {"response-content-disposition": "inline"}
            url = await loop.run_in_executor(
                None,
                partial(self._bucket.sign_url, "GET", object_key, expires, params=response_headers)
            )
            
            logger.info(f"File uploaded to OSS successfully: {object_key}")
            
            return url, None
            
        except Exception as e:
            error_msg = f"Failed to upload to OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """Get file from Aliyun OSS and generate signed URL"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            
            # Get file content
            result = await loop.run_in_executor(
                None,
                partial(self._bucket.get_object, object_key)
            )
            
            content = result.read()
            
            # Generate signed URL
            response_headers = {"response-content-disposition": "inline"}
            url = await loop.run_in_executor(
                None,
                partial(self._bucket.sign_url, "GET", object_key, 7200, params=response_headers)
            )
            
            return content, url
            
        except Exception as e:
            logger.error(f"Failed to get file from OSS: {str(e)}", exc_info=True)
            return None, None
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> tuple[bool, str | None]:
        """Delete file from Aliyun OSS"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                partial(self._bucket.delete_object, object_key)
            )
            
            logger.info(f"File deleted from OSS successfully: {object_key}")
            
            return True, None
            
        except Exception as e:
            error_msg = f"Failed to delete from OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self, 
        key: str, 
        expires: int = 7200,
        content_type: str | None = None
    ) -> str | None:
        """Generate signed URL for OSS file"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            response_headers = {"response-content-disposition": "inline"}
            
            # Add content type if provided for proper browser rendering
            if content_type:
                response_headers["response-content-type"] = content_type
            
            url = await loop.run_in_executor(
                None,
                partial(self._bucket.sign_url, "GET", object_key, expires, params=response_headers)
            )
            
            return url
            
        except Exception as e:
            logger.error(f"Failed to generate signed URL: {str(e)}", exc_info=True)
            return None

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> Dict[str, Any] | None:
        """Get file metadata from Aliyun OSS"""
        try:
            self._ensure_initialized()
            
            object_key = self._build_object_key(key)
            
            loop = asyncio.get_event_loop()
            file_info = await loop.run_in_executor(
                None,
                partial(self._bucket.get_object_meta, object_key)
            )
            
            return {
                "success": True,
                "size": file_info.content_length,
                "content_type": file_info.content_type,
                "last_modified": file_info.last_modified,
                "etag": file_info.etag,
                "request_id": file_info.request_id,
                "metadata": {
                    k.replace("x-oss-meta-", ""): v 
                    for k, v in file_info.headers.items() 
                    if k.startswith("x-oss-meta-")
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to get file info from OSS: {str(e)}", exc_info=True)
            return None

#-----------------------------------------------------------------------------

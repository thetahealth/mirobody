import asyncio
import logging

from typing import IO, Dict, Any
from functools import partial

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class AliyunStorage(AbstractStorage):
    """Aliyun OSS storage implementation"""
    
    def __init__(
        self,
        access_key_id       : str = "",
        secret_access_key   : str = "",
        region              : str = "",
        bucket              : str = "",
        prefix              : str = "",
        cdn                 : str = "",
        endpoint            : str = "",
        storage_name        : str = ""
    ):
        if not access_key_id or \
            not secret_access_key or \
            not endpoint or \
            not bucket:

            from ..config import global_config
            config = global_config()
            if config:
                storage_name = storage_name.strip()
                if storage_name:
                    storage_name = "_" + storage_name

                if not access_key_id:
                    access_key_id = config.get_str(f"ALI_OSS_ACCESS_KEY{storage_name}")
                if not secret_access_key:
                    secret_access_key = config.get_str(f"ALI_OSS_SECRET_KEY{storage_name}")
                if not endpoint:
                    endpoint = config.get_str(f"ALI_OSS_ENDPOINT{storage_name}")
                if not bucket:
                    bucket = config.get_str(f"ALI_OSS_BUCKET_NAME{storage_name}")
                if not prefix:
                    prefix = config.get_str(f"ALI_OSS_PREFIX{storage_name}")
                if not cdn:
                    cdn = config.get_str(f"ALI_OSS_DOMAIN{storage_name}")

        super().__init__(access_key_id, secret_access_key, region, bucket, prefix, cdn, endpoint)

        # Validate required fields
        missing = [k for k, v in {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
            "endpoint": self.endpoint,
            "bucket": self.bucket,
        }.items() if not v]
        if missing:
            raise ValueError(f"AliyunStorage missing required config: {', '.join(missing)}")

        # Lazy import and initialization
        self._oss2 = None
        self._auth = None
        self._bucket = None
        self._cdn_bucket = None  # Bucket instance using custom domain for signed URLs
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
            
            # Use V2 signature (AuthV2)
            # Content-Disposition is set to inline during upload (in put() method),
            # so we don't rely on response-content-disposition override in signed URLs.
            self._auth = oss2.AuthV2(self.access_key_id, self.secret_access_key)
            
            # Create Bucket instance for API operations (upload, delete, etc.)
            self._bucket = oss2.Bucket(self._auth, self._endpoint_url, self.bucket)

            # If custom domain (cdn) is configured, create a CNAME bucket for signed URLs.
            # Custom domain bypasses OSS's x-oss-force-download restriction.
            if self.cdn:
                cdn_url = self.cdn if self.cdn.startswith("http") else f"https://{self.cdn}"
                self._cdn_bucket = oss2.Bucket(self._auth, cdn_url, self.bucket, is_cname=True)
                logger.info(f"CDN bucket initialized with custom domain: {self.cdn}")
            else:
                self._cdn_bucket = self._bucket

            self._initialized = True
            logger.info(f"Aliyun OSS client initialized: bucket={self.bucket}, endpoint={self.endpoint}, cdn={self.cdn or 'none'}")
        except Exception as e:
            logger.error(f"Failed to initialize Aliyun OSS client: {str(e)}", exc_info=True)
            raise
    
    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | IO,
        content_type: str | None = None,
        metadata: Dict[str, str] | None = None,
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

            # Set Content-Disposition to inline so browsers preview instead of download
            headers["Content-Disposition"] = "inline"
            
            # Add metadata
            if metadata:
                for k, v in metadata.items():
                    headers[f"x-oss-meta-{k}"] = str(v)
            
            # Execute upload in thread pool (oss2 is synchronous)
            loop = asyncio.get_running_loop()
            
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

            # Force update object metadata to ensure Content-Disposition is stored.
            # put_object headers may not reliably persist Content-Disposition,
            # so we use update_object_meta with REPLACE directive as a guarantee.
            meta_headers = {
                "Content-Type": headers.get("Content-Type", "application/octet-stream"),
                "Content-Disposition": "inline",
            }
            await loop.run_in_executor(
                None,
                partial(self._bucket.update_object_meta, object_key, meta_headers)
            )

            # Generate signed URL via custom domain (bypasses x-oss-force-download)
            url = await loop.run_in_executor(
                None,
                partial(self._cdn_bucket.sign_url, "GET", object_key, expires)
            )

            logger.info(f"File uploaded to OSS successfully: {object_key}")
            
            return url, None
            
        except Exception as e:
            error_msg = f"Failed to upload to OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """Get file from Aliyun OSS"""
        try:
            self._ensure_initialized()

            object_key = self._build_object_key(key)

            loop = asyncio.get_running_loop()

            # Get file content
            result = await loop.run_in_executor(
                None,
                partial(self._bucket.get_object, object_key)
            )

            content = result.read()

            return content, None

        except Exception as e:
            error_msg = f"Failed to get file from OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> str | None:
        """Delete file from Aliyun OSS"""
        try:
            self._ensure_initialized()

            object_key = self._build_object_key(key)

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                partial(self._bucket.delete_object, object_key)
            )

            logger.info(f"File deleted from OSS successfully: {object_key}")

            return None

        except Exception as e:
            error_msg = f"Failed to delete from OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self,
        key: str,
        expires: int = 7200,
        content_type: str | None = None
    ) -> tuple[str | None, str | None]:
        """Generate signed URL for OSS file"""
        try:
            self._ensure_initialized()

            object_key = self._build_object_key(key)

            loop = asyncio.get_running_loop()
            # Generate signed URL via custom domain
            url = await loop.run_in_executor(
                None,
                partial(self._cdn_bucket.sign_url, "GET", object_key, expires)
            )

            return url, None

        except Exception as e:
            error_msg = f"Failed to generate signed URL: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> tuple[Dict[str, Any] | None, str | None]:
        """Get file metadata from Aliyun OSS"""
        try:
            self._ensure_initialized()

            object_key = self._build_object_key(key)

            loop = asyncio.get_running_loop()
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
            }, None

        except Exception as e:
            error_msg = f"Failed to get file info from OSS: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

#-----------------------------------------------------------------------------

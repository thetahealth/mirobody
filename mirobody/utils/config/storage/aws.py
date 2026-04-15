import logging

from typing import Any, BinaryIO  # noqa: F401 – BinaryIO used in type hints

from .abstract import AbstractStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class AwsStorage(AbstractStorage):
    """AWS S3 storage implementation"""
    
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
            not region or \
            not bucket:

            from ..config import global_config
            config = global_config()
            if config:
                storage_name = storage_name.strip()
                if storage_name:
                    storage_name = "_" + storage_name

                if not access_key_id:
                    access_key_id = config.get_str(f"S3_KEY{storage_name}")
                if not secret_access_key:
                    secret_access_key = config.get_str(f"S3_TOKEN{storage_name}")
                if not region:
                    region = config.get_str(f"S3_REGION{storage_name}")
                if not bucket:
                    bucket = config.get_str(f"S3_BUCKET{storage_name}")
                if not prefix:
                    prefix = config.get_str(f"S3_PREFIX{storage_name}")
                if not cdn:
                    cdn = config.get_str(f"S3_CDN{storage_name}")

        super().__init__(access_key_id, secret_access_key, region, bucket, prefix, cdn, endpoint)

        # Validate required fields
        missing = [k for k, v in {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
            "region": self.region,
            "bucket": self.bucket,
        }.items() if not v]
        if missing:
            raise ValueError(f"AwsStorage missing required config: {', '.join(missing)}")

        # Lazy initialization
        self._client = None
        self._client_ctx = None
        self._initialized = False

    #-----------------------------------------------------

    async def _ensure_initialized(self):
        """Ensure aioboto3 client is initialized and reused across operations"""
        if self._initialized:
            return

        import aioboto3
        session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region
        )

        client_params = {}
        if self.endpoint:
            client_params["endpoint_url"] = self.endpoint

        self._client_ctx = session.client("s3", **client_params)
        self._client = await self._client_ctx.__aenter__()
        self._initialized = True
        logger.info(f"AWS S3 client initialized: bucket={self.bucket}, region={self.region}")

    async def close(self):
        """Close the persistent S3 client"""
        if self._client_ctx:
            await self._client_ctx.__aexit__(None, None, None)
            self._client = None
            self._client_ctx = None
            self._initialized = False

    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """Upload file to AWS S3"""
        try:
            await self._ensure_initialized()

            # Build full object key with prefix
            object_key = self._build_object_key(key)

            # Prepare put_object parameters
            put_params = {
                "Body": content,
                "Bucket": self.bucket,
                "Key": object_key
            }

            # Add content type
            if content_type:
                put_params["ContentType"] = content_type
            else:
                put_params["ContentType"] = self.get_content_type_from_filename(key)

            # Add metadata
            if metadata:
                put_params["Metadata"] = metadata

            # Upload file
            await self._client.put_object(**put_params)

            # Generate signed URL
            url = await self._client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self.bucket,
                    "Key": object_key,
                    "ResponseContentDisposition": "inline",
                    "ResponseContentType": put_params["ContentType"]
                },
                ExpiresIn=expires
            )
            
            logger.info(f"File uploaded to S3 successfully: {object_key}")
            
            return url, None
        
        except Exception as e:
            error_msg = f"Failed to upload to S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg
    
    #-----------------------------------------------------

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """Get file from AWS S3"""
        try:
            await self._ensure_initialized()

            object_key = self._build_object_key(key)

            response = await self._client.get_object(Bucket=self.bucket, Key=object_key)

            async with response["Body"] as stream:
                file_content = await stream.read()

            return file_content, None

        except Exception as e:
            error_msg = f"Failed to get file from S3: {str(e)}, object_key: {self._build_object_key(key)}, bucket: {self.bucket}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> str | None:
        """Delete file from AWS S3"""
        try:
            await self._ensure_initialized()

            object_key = self._build_object_key(key)

            await self._client.delete_object(Bucket=self.bucket, Key=object_key)

            logger.info(f"File deleted from S3 successfully: {object_key}")

            return None

        except Exception as e:
            error_msg = f"Failed to delete from S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self,
        key: str,
        expires: int = 7200,
        content_type: str | None = None
    ) -> tuple[str | None, str | None]:
        """Generate signed URL for S3 file"""
        try:
            await self._ensure_initialized()

            object_key = self._build_object_key(key)

            params = {
                "Bucket": self.bucket,
                "Key": object_key,
                "ResponseContentDisposition": "inline"
            }

            if content_type:
                params["ResponseContentType"] = content_type

            url = await self._client.generate_presigned_url(
                "get_object",
                Params=params,
                ExpiresIn=expires
            )

            return url, None

        except Exception as e:
            error_msg = f"Failed to generate signed URL: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        """Get file metadata from AWS S3"""
        try:
            await self._ensure_initialized()

            object_key = self._build_object_key(key)

            response = await self._client.head_object(Bucket=self.bucket, Key=object_key)

            return {
                "success": True,
                "size": response.get("ContentLength"),
                "content_type": response.get("ContentType"),
                "last_modified": response.get("LastModified"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {})
            }, None

        except Exception as e:
            error_msg = f"Failed to get file info from S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

#-----------------------------------------------------------------------------

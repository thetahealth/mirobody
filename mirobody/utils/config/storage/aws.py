import logging
import traceback
from typing import IO, Optional, Dict, Any

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
        config              : any = None,
        storage_name        : str = ""
    ):
        super().__init__(access_key_id, secret_access_key, region, bucket, prefix, cdn, endpoint)
        
        # Support legacy config parameter
        if config and hasattr(config, "get_str") and callable(config.get_str):
            if not self.access_key_id:
                self.access_key_id = config.get_str(f"S3_KEY{storage_name}")
            if not self.secret_access_key:
                self.secret_access_key = config.get_str(f"S3_TOKEN{storage_name}")
            if not self.region:
                self.region = config.get_str(f"S3_REGION{storage_name}")
            if not self.bucket:
                self.bucket = config.get_str(f"S3_BUCKET{storage_name}")
            if not self.prefix:
                self.prefix = config.get_str(f"S3_PREFIX{storage_name}")
            if not self.cdn:
                self.cdn = config.get_str(f"S3_CDN{storage_name}")
        
        # Initialize aioboto3 session
        import aioboto3
        
        session_params = {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "region_name": self.region
        }
        
        self.session = aioboto3.Session(**session_params)
        self._client_params = {}
        
        # Add endpoint URL if provided (for MinIO compatibility)
        if self.endpoint:
            self._client_params["endpoint_url"] = self.endpoint

    #-----------------------------------------------------

    async def put(
        self, 
        key: str, 
        content: bytes | IO,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """Upload file to AWS S3"""
        try:
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
        
            
            async with self.session.client("s3", **self._client_params) as client:
                # Upload file
                await client.put_object(**put_params)
                
                # Generate signed URL
                url = await client.generate_presigned_url(
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
        """Get file from AWS S3 and generate signed URL"""
        try:
            object_key = self._build_object_key(key)
            
            async with self.session.client("s3", **self._client_params) as client:
                # Get file content
                response = await client.get_object(Bucket=self.bucket, Key=object_key)
                
                async with response["Body"] as stream:
                    file_content = await stream.read()
                
                # Generate signed URL
                url = await client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": self.bucket,
                        "Key": object_key,
                        "ResponseContentDisposition": "inline"
                    },
                    ExpiresIn=7200
                )
                
                return file_content, url

        except Exception as e:
            logger.error(f"Failed to get file from S3: {str(e)}", exc_info=True)
            return None, None
    
    #-----------------------------------------------------

    async def delete(self, key: str) -> tuple[bool, str | None]:
        """Delete file from AWS S3"""
        try:
            object_key = self._build_object_key(key)
            
            async with self.session.client("s3", **self._client_params) as client:
                response = await client.delete_object(Bucket=self.bucket, Key=object_key)
            
            logger.info(f"File deleted from S3 successfully: {object_key}")
            
            return True, None
        
        except Exception as e:
            error_msg = f"Failed to delete from S3: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    #-----------------------------------------------------

    async def generate_signed_url(
        self, 
        key: str, 
        expires: int = 7200,
        content_type: str | None = None
    ) -> str | None:
        """Generate signed URL for S3 file"""
        try:
            object_key = self._build_object_key(key)
            
            async with self.session.client("s3", **self._client_params) as client:
                params = {
                    "Bucket": self.bucket,
                    "Key": object_key,
                    "ResponseContentDisposition": "inline"
                }
                
                # Add content type if provided for proper browser rendering
                if content_type:
                    params["ResponseContentType"] = content_type
                
                url = await client.generate_presigned_url(
                    "get_object",
                    Params=params,
                    ExpiresIn=expires
                )
                
                return url
        
        except Exception as e:
            logger.error(f"Failed to generate signed URL: {str(e)}", exc_info=True)
            return None

    #-----------------------------------------------------

    async def get_file_info(self, key: str) -> Dict[str, Any] | None:
        """Get file metadata from AWS S3"""
        try:
            object_key = self._build_object_key(key)
            
            async with self.session.client("s3", **self._client_params) as client:
                response = await client.head_object(Bucket=self.bucket, Key=object_key)
            
            return {
                "success": True,
                "size": response.get("ContentLength"),
                "content_type": response.get("ContentType"),
                "last_modified": response.get("LastModified"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {})
            }
        
        except Exception as e:
            logger.error(f"Failed to get file info from S3: {str(e)}", exc_info=True)
            return None

#-----------------------------------------------------------------------------

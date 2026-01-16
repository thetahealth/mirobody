#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import time
import uuid
from typing import BinaryIO, Dict, Optional, Union

from dotenv import load_dotenv
from fastapi import UploadFile

from mirobody.utils.config import safe_read_cfg

# Environment check for OSS initialization - delay initialization
def get_env_config():
    """Get environment configuration safely"""
    try:
        env = safe_read_cfg("ENV")
        cluster = safe_read_cfg("CLUSTER")
        is_aliyun = (cluster or "").upper() == "ALIYUN"
        ak = safe_read_cfg("ALI_OSS_ACCESS_KEY")
        sk = safe_read_cfg("ALI_OSS_SECRET_KEY")
        endpoint = safe_read_cfg("ALI_OSS_ENDPOINT")
        bucket = safe_read_cfg("ALI_OSS_BUCKET_NAME")
        
        
        return {
            'ENV': env,
            'IS_ALIYUN': is_aliyun,
            'AK': ak,
            'SK': sk,
            'ENDPOINT': endpoint,
            'BUCKET_NAME': bucket
        }
    except Exception:
        return {
            'ENV': None,
            'IS_ALIYUN': False,
            'AK': None,
            'SK': None,
            'ENDPOINT': None,
            'BUCKET_NAME': None
        }

load_dotenv()


class AliOSS:
    """
    Alibaba Cloud OSS file upload utility class, using AK/SK access - lazy initialization
    """

    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        bucket_name: Optional[str] = None,
        secure: bool = True,
        default_dir: str = "uploads",
    ):
        """
        Initialize Alibaba Cloud OSS client using AK/SK access
        Note: Configuration reading will be delayed until actual use

        Args:
            access_key: Alibaba Cloud access key AK, if None will be read from env var ALI_OSS_ACCESS_KEY
            secret_key: Alibaba Cloud secret key SK, if None will be read from env var ALI_OSS_SECRET_KEY
            endpoint: OSS access domain, if None will be read from env var ALI_OSS_ENDPOINT
            bucket_name: OSS bucket name, if None will be read from env var ALI_OSS_BUCKET_NAME
            secure: Whether to use HTTPS
            default_dir: Default upload directory
        """
        # Store initialization parameters for later use
        self._init_access_key = access_key
        self._init_secret_key = secret_key
        self._init_endpoint = endpoint
        self._init_bucket_name = bucket_name
        self._init_secure = secure
        self.default_dir = default_dir
        
        # Delay configuration loading until first use
        self._config_loaded = False
        self._initialized = False
        self.oss_enabled = None  # Will be determined when config is loaded
        
        # Initialize as None - will be set during lazy initialization
        self.access_key = None
        self.secret_key = None
        self.endpoint = None
        self.bucket_name = None
        self.auth = None
        self.endpoint_url = None
        self.bucket = None
        self.domain = None

    def _load_config_if_needed(self):
        """
        Lazy load configuration - only read config when actually needed
        """
        if self._config_loaded:
            return
            
        try:
            # Read configuration now, should be initialized at this point
            config = get_env_config()
            
            # Set configuration parameters (prioritize initialization parameters)
            self.access_key = self._init_access_key or config['AK']
            self.secret_key = self._init_secret_key or config['SK'] 
            self.endpoint = self._init_endpoint or config['ENDPOINT']
            self.bucket_name = self._init_bucket_name or config['BUCKET_NAME']
            
            # Check if OSS should be enabled
            self.oss_enabled = self._should_enable_oss_with_config(config)
            logging.info("🚗 OSS enabled status: " + str(self.oss_enabled))
            
            if self.oss_enabled:
                # Build endpoint URL
                protocol = "https" if self._init_secure else "http"
                self.endpoint_url = f"{protocol}://{self.endpoint}"
                
                # Validate configuration completeness
                if not all([self.access_key, self.secret_key, self.endpoint, self.bucket_name]):
                    logging.error(f"🚗 OSS configuration incomplete: AK:{bool(self.access_key)}, SK:{bool(self.secret_key)}, ENDPOINT:{bool(self.endpoint)}, BUCKET:{bool(self.bucket_name)}")
                    self.oss_enabled = False
                else:
                    logging.info(f"🚗 OSS config loaded successfully: ENDPOINT={self.endpoint}, BUCKET={self.bucket_name}")
            
            self._config_loaded = True
            
        except Exception as e:
            logging.error(f"🚗 Failed to load OSS config: {str(e)}", stack_info=True)
            self.oss_enabled = False
            self._config_loaded = True

    def _should_enable_oss_with_config(self, config: dict) -> bool:
        """
        Check if OSS should be enabled based on environment configuration

        Args:
            config: Configuration dictionary loaded from environment
            
        Returns:
            bool: True if OSS should be enabled, False otherwise
        """
        # Check if we have the required configuration
        actual_ak = self._init_access_key or config['AK']
        actual_sk = self._init_secret_key or config['SK']
        actual_endpoint = self._init_endpoint or config['ENDPOINT']
        actual_bucket = self._init_bucket_name or config['BUCKET_NAME']
        
        has_config = bool(actual_ak and actual_sk and actual_endpoint and actual_bucket)
        
        # If we have the configuration, enable OSS
        if has_config:
            logging.info("🚗 OSS enabled: found complete OSS configuration")
            return True
        else:
            logging.info(f"🚗 OSS disabled: missing configuration (AK:{bool(actual_ak)}, SK:{bool(actual_sk)}, ENDPOINT:{bool(actual_endpoint)}, BUCKET:{bool(actual_bucket)})")
            return False

    def _import_oss2(self):
        """Import oss2 with SyntaxWarning suppression"""
        try:
            import warnings

            warnings.filterwarnings("ignore", category=SyntaxWarning, module="oss2")
            import oss2

            return oss2
        except ImportError as e:
            raise ImportError(f"Failed to import oss2: {e}. Please install oss2 library.")

    def _ensure_initialized(self):
        """Ensure OSS client is initialized - with lazy config loading"""
        # Load configuration if not already loaded
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            raise RuntimeError("OSS service is disabled for this environment")

        if not self._initialized:
            try:
                # Import oss2 only when needed
                oss2 = self._import_oss2()

                # Create Auth instance using AK and SK
                self.auth = oss2.Auth(self.access_key, self.secret_key)

                # Create Bucket instance
                self.bucket = oss2.Bucket(self.auth, self.endpoint_url, self.bucket_name)

                # Default domain
                self.domain = os.getenv("ALI_OSS_DOMAIN") or f"{self.bucket_name}.{self.endpoint}"

                logging.info(f"Alibaba Cloud OSS client initialized successfully, bucket: {self.bucket_name}")
                self._initialized = True
            except Exception:
                raise

    def _get_content_type(self, filename: str) -> str:
        """
        Get content type based on filename

        Args:
            filename: File name

        Returns:
            str: Content type
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
            # Web pages
            "html": "text/html",
            "htm": "text/html",
            "css": "text/css",
            "js": "application/javascript",
            "json": "application/json",
            "xml": "application/xml",
            # Others
            "zip": "application/zip",
            "rar": "application/x-rar-compressed",
            "7z": "application/x-7z-compressed",
            "tar": "application/x-tar",
            "gz": "application/gzip",
        }

        return content_types.get(ext, "application/octet-stream")  # Default binary stream

    def upload_file(
        self,
        file_data: Union[bytes, BinaryIO, str],
        file_name: Optional[str] = None,
        directory: Optional[str] = None,
        content_type: Optional[str] = None,
        metadata: Optional[Dict] = None,
        expires: Optional[int] = 7200,
    ) -> Dict:
        """
        Upload file to OSS

        Args:
            file_data: File data, can be bytes, file object or local file path
            file_name: File name, if None will be auto-generated
            directory: Storage directory, if None will use default directory
            content_type: File content type
            metadata: File metadata

        Returns:
            Dict: Dictionary containing upload result
        """
        # Load configuration if needed before checking oss_enabled
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            logging.warning("OSS service is disabled, file upload skipped")
            return {"success": False, "error": "OSS service is disabled for this environment"}

        self._ensure_initialized()

        try:
            # Set directory
            directory = directory or self.default_dir

            # Generate random filename if not provided
            if file_name is None:
                timestamp = int(time.time())
                random_str = str(uuid.uuid4()).replace("-", "")[:8]
                file_name = f"{timestamp}_{random_str}"

            # Build complete object key (OSS path)
            object_key = f"{directory.rstrip('/')}/{file_name}"

            # Prepare headers
            headers = {}
            if content_type:
                headers["Content-Type"] = content_type
            else:
                # Auto-detect content type from filename if not provided
                detected_content_type = self._get_content_type(file_name)
                headers["Content-Type"] = detected_content_type

            # Prepare metadata
            if metadata:
                for k, v in metadata.items():
                    headers[f"x-oss-meta-{k}"] = str(v)

            # Execute different upload operations based on file_data type
            if isinstance(file_data, bytes):
                result = self.bucket.put_object(object_key, file_data, headers=headers)
            elif isinstance(file_data, bytearray):
                # Convert bytearray to bytes for OSS upload
                result = self.bucket.put_object(object_key, bytes(file_data), headers=headers)
            elif hasattr(file_data, "read"):
                result = self.bucket.put_object(object_key, file_data, headers=headers)
            elif isinstance(file_data, str) and os.path.isfile(file_data):
                result = self.bucket.put_object_from_file(object_key, file_data, headers=headers)
            else:
                raise ValueError("Unsupported file data type")

            # Build URL, generate signed URL for private bucket
            response_headers = {"response-content-disposition": "inline"}
            url = self.bucket.sign_url("GET", object_key, expires, params=response_headers)  # Use response headers parameter

            logging.info(f"File uploaded successfully: {object_key}")
            return {
                "success": True,
                "object_key": object_key,
                "url": url,
                "expires_in": expires,  # URL expiration time (seconds)
                "etag": result.etag,
                "request_id": result.request_id,
            }
        except Exception as e:
            logging.error(f"File upload failed: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}

    async def upload_fastapi_file(
        self,
        file: UploadFile,
        directory: Optional[str] = None,
        custom_filename: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> Dict:
        """
        Upload FastAPI UploadFile

        Args:
            file: FastAPI UploadFile object
            directory: Storage directory, if None uses default directory
            custom_filename: Custom filename, if None uses original filename
            metadata: File metadata

        Returns:
            Dict: Dictionary containing upload result
        """
        try:
            content = await file.read()

            # Process filename
            original_filename = file.filename
            if custom_filename:
                # Preserve original file extension
                if original_filename and "." in original_filename:
                    ext = original_filename.split(".")[-1]
                    if "." not in custom_filename:
                        custom_filename = f"{custom_filename}.{ext}"
                file_name = custom_filename
            else:
                # Generate random filename but preserve original extension
                if original_filename and "." in original_filename:
                    ext = original_filename.split(".")[-1]
                    timestamp = int(time.time())
                    random_str = str(uuid.uuid4()).replace("-", "")[:8]
                    file_name = f"{timestamp}_{random_str}.{ext}"
                else:
                    timestamp = int(time.time())
                    random_str = str(uuid.uuid4()).replace("-", "")[:8]
                    file_name = f"{timestamp}_{random_str}"

            # Upload file
            return self.upload_file(
                file_data=content,
                file_name=file_name,
                directory=directory,
                content_type=file.content_type or self._get_content_type(file_name),
                metadata=metadata,
            )
        except Exception as e:
            logging.error(f"FastAPI file upload failed: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}
        finally:
            await file.seek(0)  # Reset file pointer for re-reading

    def delete_file(self, object_key: str) -> Dict:
        """
        Delete file from OSS

        Args:
            object_key: Complete object key (path)

        Returns:
            Dict: Dictionary containing deletion result
        """
        # Load configuration if needed
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            logging.warning("OSS service is disabled, file deletion skipped")
            return {"success": False, "error": "OSS service is disabled for this environment"}

        try:
            self._ensure_initialized()
            result = self.bucket.delete_object(object_key)
            logging.info(f"File deleted successfully: {object_key}")
            return {"success": True, "request_id": result.request_id}
        except Exception as e:
            logging.error(f"File deletion failed: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}

    def get_file_info(self, object_key: str) -> Dict:
        """
        Get file information

        Args:
            object_key: Complete object key (path)

        Returns:
            Dict: Dictionary containing file information
        """
        # Load configuration if needed
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            logging.warning("OSS service is disabled, file info retrieval skipped")
            return {"success": False, "error": "OSS service is disabled for this environment"}

        try:
            self._ensure_initialized()
            file_info = self.bucket.get_object_meta(object_key)
            return {
                "success": True,
                "size": file_info.content_length,
                "content_type": file_info.content_type,
                "last_modified": file_info.last_modified,
                "etag": file_info.etag,
                "request_id": file_info.request_id,
                "metadata": {
                    k.replace("x-oss-meta-", ""): v for k, v in file_info.headers.items() if k.startswith("x-oss-meta-")
                },
            }
        except Exception as e:
            # Check if it's a NoSuchKey error by error message
            if "NoSuchKey" in str(e) or "does not exist" in str(e):
                logging.warning(f"File does not exist: {object_key}")
                return {"success": False, "error": "File does not exist"}
            logging.error(f"Failed to get file info: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}

    def list_files(self, directory: str, limit: int = 100) -> Dict:
        """
        List files in directory

        Args:
            directory: Directory path
            limit: Maximum number of results

        Returns:
            Dict: Dictionary containing file list
        """
        # Load configuration if needed
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            logging.warning("OSS service is disabled, file listing skipped")
            return {"success": False, "error": "OSS service is disabled for this environment"}

        try:
            self._ensure_initialized()
            # Import oss2 for ObjectIterator
            oss2 = self._import_oss2()

            # Ensure directory ends with '/'
            prefix = directory if directory.endswith("/") else directory + "/"

            files = []
            for obj in oss2.ObjectIterator(self.bucket, prefix=prefix, max_keys=limit):
                if not obj.key.endswith("/"):  # Exclude directories
                    # Generate signed URL
                    response_headers = {"response-content-disposition": "inline"}
                    signed_url = self.bucket.sign_url("GET", obj.key, 7200, params=response_headers)

                    files.append(
                        {
                            "key": obj.key,
                            "size": obj.size,
                            "last_modified": obj.last_modified,
                            "etag": obj.etag,
                            "url": signed_url,
                        }
                    )

            logging.info(f"File list retrieved successfully, total {len(files)} files")
            return {"success": True, "files": files, "total": len(files)}
        except Exception as e:
            logging.error(f"Failed to get file list: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}

    def generate_signed_url(self, object_key: str, expires: int = 7200) -> Dict:
        """
        Generate signed URL for file

        Args:
            object_key: Complete object key (path)
            expires: Link expiration time (seconds), default 2 hours

        Returns:
            Dict: Dictionary containing signed URL
        """
        # Load configuration if needed
        self._load_config_if_needed()
        
        if not self.oss_enabled:
            logging.warning("OSS service is disabled, signed URL generation skipped")
            return {"success": False, "error": "OSS service is disabled for this environment"}

        try:
            self._ensure_initialized()
            response_headers = {"response-content-disposition": "inline"}
            url = self.bucket.sign_url("GET", f"{self.default_dir}/{object_key}", expires, params=response_headers)
            logging.info(f"Signed URL generated successfully: {object_key}")
            return {"success": True, "signed_url": url, "expires_in": expires}
        except Exception as e:
            logging.error(f"Failed to generate signed URL: {str(e)}", stack_info=True)
            return {"success": False, "error": str(e)}

    def get_oss_status(self) -> Dict:
        """
        Get OSS service status - with lazy config loading

        Returns:
            Dict: Dictionary containing OSS status information
        """
        # Load configuration if needed
        self._load_config_if_needed()
        
        try:
            config = get_env_config()
            cloud_provider = safe_read_cfg("CLOUD_PROVIDER") if safe_read_cfg("CLOUD_PROVIDER") else "NOT_SET"
        except Exception:
            config = {'ENV': 'UNKNOWN', 'IS_ALIYUN': False}
            cloud_provider = "NOT_SET"
            
        return {
            "oss_enabled": self.oss_enabled,
            "environment": config['ENV'],
            "is_aliyun": config['IS_ALIYUN'],
            "cloud_provider": cloud_provider,
            "initialized": self._initialized if hasattr(self, "_initialized") else False,
            "config_loaded": self._config_loaded,
            "bucket_name": self.bucket_name if self.oss_enabled else None,
            "endpoint": self.endpoint if self.oss_enabled else None,
        }


# Create global OSS client instance with lazy initialization
# Configuration will be loaded only when needed
try:
    oss_client = AliOSS()
except Exception as e:
    logging.warning(f"Failed to initialize OSS client: {str(e)}", stack_info=True)
    # Create a dummy OSS client with disabled state
    oss_client = AliOSS.__new__(AliOSS)
    oss_client.oss_enabled = False
    oss_client._initialized = False
    oss_client._config_loaded = True

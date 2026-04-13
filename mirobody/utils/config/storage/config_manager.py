import logging
from typing import Dict, Optional, List

from .constants import DEFAULT_LOCAL_UPLOAD_PATH

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class StorageConfigManager:
    """Unified configuration manager for storage backends"""
    
    # Storage type priority order (cloud storage first, local as fallback)
    PRIORITY_ORDER = ["aws", "aliyun", "local"]
    
    def __init__(self):
        self._config_cache: Dict[str, Optional[Dict]] = {}
        self._available_storages: Optional[List[str]] = None
    
    #-----------------------------------------------------
    
    def detect_available_storage(self) -> List[str]:
        """
        Detect all available storage configurations
        
        Returns:
            List of available storage types in priority order
        """
        if self._available_storages is not None:
            return self._available_storages
        
        available = []
        
        # Check Local storage
        if self._has_local_config():
            available.append("local")
            logger.info("Local storage configuration detected")
        
        # Check AWS S3
        if self._has_aws_config():
            available.append("aws")
            logger.info("AWS S3 configuration detected")
        
        # Check Aliyun OSS
        if self._has_aliyun_config():
            available.append("aliyun")
            logger.info("Aliyun OSS configuration detected")
        
        # Local storage is always available as fallback
        if "local" not in available:
            available.append("local")
            logger.info("Local storage available as default fallback")
        
        # Sort by priority
        sorted_available = [
            storage for storage in self.PRIORITY_ORDER 
            if storage in available
        ]
        
        self._available_storages = sorted_available
        logger.info(f"Available storages (priority order): {', '.join(sorted_available)}")
        
        return sorted_available
    
    #-----------------------------------------------------
    
    def get_storage_config(self, storage_type: str) -> Optional[Dict]:
        """
        Get configuration for specified storage type
        
        Args:
            storage_type: Storage type ('local', 'aws', 'aliyun')
            
        Returns:
            Configuration dictionary or None if not available
        """
        if storage_type in self._config_cache:
            return self._config_cache[storage_type]
        
        config = None
        
        if storage_type == "local":
            config = self._get_local_config()
        elif storage_type == "aws":
            config = self._get_aws_config()
        elif storage_type == "aliyun":
            config = self._get_aliyun_config()
        self._config_cache[storage_type] = config
        return config
    
    #-----------------------------------------------------
    # Local Storage Configuration
    #-----------------------------------------------------
    
    def _has_local_config(self) -> bool:
        """Check if local storage configuration exists or is enabled"""
        import os
        
        try:
            # Check environment variable
            env = os.environ.get("ENV", "").lower()
            if env in ("local", "localdb"):
                return True
            
            return False
        except Exception:
            return False
    
    def _get_local_config(self) -> Dict:
        """
        Get local storage configuration
        
        Returns:
            Configuration dictionary with base_path and proxy_url
        """
        from mirobody.utils.config import safe_read_cfg
        
        try:
            base_path = safe_read_cfg("LOCAL_STORAGE_BASE_PATH") or DEFAULT_LOCAL_UPLOAD_PATH
            
            # Get proxy URL from DATA_PUBLIC_URL if available
            data_public_url = safe_read_cfg("MCP_PUBLIC_URL")
            proxy_url = f"{data_public_url.rstrip('/')}/files" if data_public_url else ""
            
            logger.info(f"Using local storage: base_path={base_path}, proxy_url={proxy_url}")
            
            return {
                "base_path": base_path,
                "prefix": "",
                "proxy_url": proxy_url
            }
            
        except Exception as e:
            logger.warning(f"Failed to load local storage config, using defaults: {str(e)}")
            # Return default config
            return {
                "base_path": base_path,
                "prefix": "",
                "proxy_url": ""
            }
    
    #-----------------------------------------------------
    # AWS S3 Configuration
    #-----------------------------------------------------
    
    def _has_aws_config(self) -> bool:
        """Check if AWS S3 configuration exists"""
        from mirobody.utils.config import safe_read_cfg
        
        try:
            # Note: safe_read_cfg auto-converts key to uppercase
            key = safe_read_cfg("s3_key")
            token = safe_read_cfg("s3_token")
            region = safe_read_cfg("s3_region")
            bucket = safe_read_cfg("s3_bucket")
            
            return bool(key and token and region and bucket)
        except Exception:
            return False
    
    def _get_aws_config(self) -> Optional[Dict]:
        """Get AWS S3 configuration"""
        from mirobody.utils.config import safe_read_cfg
        
        try:
            # Note: safe_read_cfg auto-converts key to uppercase
            key = safe_read_cfg("s3_key")
            token = safe_read_cfg("s3_token")
            region = safe_read_cfg("s3_region")
            bucket = safe_read_cfg("s3_bucket")
            prefix = safe_read_cfg("s3_prefix")
            cdn = safe_read_cfg("s3_cdn")
            
            if not all([key, token, region, bucket]):
                return None
            
            return {
                "access_key_id": key,
                "secret_access_key": token,
                "region": region,
                "bucket": bucket,
                "prefix": prefix or "",
                "cdn": cdn or "",
                "endpoint": ""
            }
        except Exception as e:
            logger.warning(f"Failed to load AWS config: {str(e)}")
            return None
    
    #-----------------------------------------------------
    # Aliyun OSS Configuration
    #-----------------------------------------------------
    
    def _has_aliyun_config(self) -> bool:
        """Check if Aliyun OSS configuration exists"""
        from mirobody.utils.config import safe_read_cfg
        
        try:
            ak = safe_read_cfg("ALI_OSS_ACCESS_KEY")
            sk = safe_read_cfg("ALI_OSS_SECRET_KEY")
            endpoint = safe_read_cfg("ALI_OSS_ENDPOINT")
            bucket = safe_read_cfg("ALI_OSS_BUCKET_NAME")
            
            return bool(ak and sk and endpoint and bucket)
        except Exception:
            return False
    
    def _get_aliyun_config(self) -> Optional[Dict]:
        """Get Aliyun OSS configuration"""
        from mirobody.utils.config import safe_read_cfg
        
        try:
            ak = safe_read_cfg("ALI_OSS_ACCESS_KEY")
            sk = safe_read_cfg("ALI_OSS_SECRET_KEY")
            endpoint = safe_read_cfg("ALI_OSS_ENDPOINT")
            bucket = safe_read_cfg("ALI_OSS_BUCKET_NAME")
            prefix = safe_read_cfg("ALI_OSS_PREFIX") or ""
            cdn = safe_read_cfg("ALI_OSS_DOMAIN") or ""
            
            if not all([ak, sk, endpoint, bucket]):
                return None
            
            return {
                "access_key_id": ak,
                "secret_access_key": sk,
                "region": "",  # OSS uses endpoint instead
                "bucket": bucket,
                "prefix": prefix,
                "cdn": cdn,
                "endpoint": endpoint
            }
        except Exception as e:
            logger.warning(f"Failed to load Aliyun config: {str(e)}")
            return None
    
    #-----------------------------------------------------

    def get_primary_storage(self) -> tuple[str, Dict]:
        """
        Get primary storage type and configuration based on priority
        
        Returns:
            Tuple of (storage_type, config_dict)
        """
        available = self.detect_available_storage()
        
        if not available:
            logger.warning("No storage backend configured, using default local storage")
            return ("local", self._get_local_config())
        
        primary_type = available[0]
        primary_config = self.get_storage_config(primary_type)
        
        logger.info(f"Primary storage selected: {primary_type}")
        
        return (primary_type, primary_config)
    
    #-----------------------------------------------------
    
    def clear_cache(self):
        """Clear configuration cache (useful for testing)"""
        self._config_cache.clear()
        self._available_storages = None

#-----------------------------------------------------------------------------


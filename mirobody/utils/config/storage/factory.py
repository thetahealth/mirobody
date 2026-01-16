import logging
from typing import Optional

from .abstract import AbstractStorage
from .config_manager import StorageConfigManager
from .local import LocalStorage
from .aws import AwsStorage
from .aliyun import AliyunStorage
from .minio import MinioStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class StorageFactory:
    """Factory class for creating storage instances based on configuration"""
    
    _instance: Optional[AbstractStorage] = None
    _storage_type: Optional[str] = None
    _config_manager: Optional[StorageConfigManager] = None
    
    #-----------------------------------------------------
    
    @classmethod
    def create_storage(
        cls, 
        config_manager: Optional[StorageConfigManager] = None,
        force_type: Optional[str] = None
    ) -> AbstractStorage:
        """
        Create storage instance based on configuration
        
        Args:
            config_manager: Configuration manager (creates new one if None)
            force_type: Force specific storage type ('aws', 'aliyun', 'minio')
            
        Returns:
            Storage instance
        """
        import os
        
        # Force local storage for local development environment
        env = os.environ.get("ENV", "").lower()
        if env in ("local", "localdb") and force_type is None:
            force_type = "local"
            logger.info(f"Local environment detected (ENV={env}), forcing local storage")
        
        # Return cached instance if exists and no force_type specified
        if cls._instance is not None and force_type is None:
            logger.info(f"Reusing cached storage instance: {cls._storage_type}")
            return cls._instance
        
        # Create config manager if not provided
        if config_manager is None:
            config_manager = StorageConfigManager()
        
        cls._config_manager = config_manager
        
        # Determine storage type
        if force_type:
            storage_type = force_type
            logger.info(f"Forced storage type: {storage_type}")
        else:
            # Auto-detect based on configuration
            storage_type, config = config_manager.get_primary_storage()
        
        # Get configuration for selected storage type
        config = config_manager.get_storage_config(storage_type)
        
        if config is None:
            logger.warning(f"No configuration found for {storage_type}, falling back to local storage")
            storage_type = "local"
            config = config_manager.get_storage_config("local")
        
        # Create storage instance
        try:
            if storage_type == "local":
                instance = LocalStorage(**config)
            elif storage_type == "aws":
                instance = AwsStorage(**config)
            elif storage_type == "aliyun":
                instance = AliyunStorage(**config)
            elif storage_type == "minio":
                instance = MinioStorage(**config)
            else:
                logger.error(f"Unknown storage type: {storage_type}, using local storage")
                instance = LocalStorage(**config_manager.get_storage_config("local"))
                storage_type = "local"
            
            # Cache instance
            cls._instance = instance
            cls._storage_type = storage_type
            
            logger.info(f"Storage instance created successfully: {storage_type}")
            
            return instance
        
        except Exception as e:
            logger.error(f"Failed to create {storage_type} storage: {str(e)}, falling back to local storage")
            
            # Fallback to local storage
            local_config = config_manager.get_storage_config("local")
            instance = LocalStorage(**local_config)
            cls._instance = instance
            cls._storage_type = "local"
            
            return instance
    
    #-----------------------------------------------------
    
    @classmethod
    def get_storage_type(cls) -> Optional[str]:
        """
        Get current storage type
        
        Returns:
            Storage type string or None if not initialized
        """
        return cls._storage_type
    
    #-----------------------------------------------------
    
    @classmethod
    def switch_storage(cls, storage_type: str) -> AbstractStorage:
        """
        Switch to different storage backend
        
        Args:
            storage_type: Storage type to switch to ('local', 'aws', 'aliyun', 'minio')
            
        Returns:
            New storage instance
        """
        logger.info(f"Switching storage from {cls._storage_type} to {storage_type}")
        
        # Clear cached instance
        cls._instance = None
        cls._storage_type = None
        
        # Create new instance with forced type
        return cls.create_storage(
            config_manager=cls._config_manager,
            force_type=storage_type
        )
    
    #-----------------------------------------------------
    
    @classmethod
    def get_instance(cls) -> Optional[AbstractStorage]:
        """
        Get current storage instance without creating new one
        
        Returns:
            Current storage instance or None if not initialized
        """
        return cls._instance
    
    #-----------------------------------------------------
    
    @classmethod
    def reset(cls):
        """Reset factory state (useful for testing)"""
        cls._instance = None
        cls._storage_type = None
        if cls._config_manager:
            cls._config_manager.clear_cache()
        cls._config_manager = None
        
        logger.info("Storage factory reset")

#-----------------------------------------------------------------------------

def get_storage_client() -> AbstractStorage:
    """
    Convenience function to get storage client
    
    Returns:
        Storage instance
    """
    instance = StorageFactory.get_instance()
    if instance is None:
        instance = StorageFactory.create_storage()
    return instance

#-----------------------------------------------------------------------------

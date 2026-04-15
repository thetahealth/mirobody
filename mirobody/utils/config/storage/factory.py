import logging

from .abstract import AbstractStorage
from .local import LocalStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

_instance: AbstractStorage | None = None

#-----------------------------------------------------------------------------

def get_storage_client() -> AbstractStorage:
    """
    Get or create storage client.
    Tries each cloud storage subclass; falls back to LocalStorage.
    """
    global _instance
    if _instance is not None:
        return _instance

    for sub in AbstractStorage.__subclasses__():
        if sub is LocalStorage:
            continue
        try:
            _instance = sub()
            logger.info(f"Storage initialized: {sub.__name__}")
            return _instance
        except Exception as e:
            logger.debug(f"{sub.__name__} not available: {e}")

    _instance = LocalStorage()
    logger.info("Falling back to LocalStorage")
    return _instance

#-----------------------------------------------------------------------------

def reset_storage():
    """Reset cached instance (useful for testing)"""
    global _instance
    _instance = None

#-----------------------------------------------------------------------------

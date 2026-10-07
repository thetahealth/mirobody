import logging

from .abstract import AbstractStorage
from .aliyun import AliyunStorage
from .aws import AwsStorage
from .local import LocalStorage

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

# The cloud backends, tried in this order. Used to be
# `AbstractStorage.__subclasses__()`, which made the candidate list a side
# effect of which modules `storage/__init__.py` happened to import first.
_CLOUD_BACKENDS: tuple[type[AbstractStorage], ...] = (AwsStorage, AliyunStorage)

_instance: AbstractStorage | None = None

#-----------------------------------------------------------------------------

def get_storage_client() -> AbstractStorage:
    """
    Get or create storage client.
    Tries each cloud storage backend; falls back to LocalStorage.
    """
    global _instance
    if _instance is not None:
        return _instance

    for backend in _CLOUD_BACKENDS:
        try:
            _instance = backend()
            logger.info(f"Storage initialized: {backend.__name__}")
            return _instance
        except Exception as e:
            # Expected for every backend a deployment does not configure: its
            # constructor raises ValueError naming the missing keys.
            logger.debug("%s not available: error_type=%s", backend.__name__, type(e).__name__)

    _instance = LocalStorage()
    logger.info("Falling back to LocalStorage")
    return _instance

#-----------------------------------------------------------------------------

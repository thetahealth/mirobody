# Register storage backends so AbstractStorage.__subclasses__() can find them
from .aws import AwsStorage as AwsStorage
from .aliyun import AliyunStorage as AliyunStorage

from .factory import get_storage_client

__all__ = ["get_storage_client"]

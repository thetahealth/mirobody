
import logging
from typing import BinaryIO

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.file_types import guess_mime

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------


def storage_failure(action: str, key: str, exc: Exception) -> str:
    """Log a failed storage call by the key's fingerprint and the error's type,
    and return the message the caller gets back. Neither carries the error's
    text: a vendor's message can quote the object key, and a key can hold the
    uploaded file's name."""
    from mirobody.utils.log import secret_fingerprint

    error_type = type(exc).__name__
    logger.error("%s failed: key=%s error_type=%s", action, secret_fingerprint(key), error_type,
                 exc_info=not is_driver_exception(exc))
    return f"{action} failed ({error_type})"


class AbstractStorage:
    """Abstract base class for all storage backends"""
    
    def __init__(
        self,
        access_key_id       : str = "",
        secret_access_key   : str = "",
        region              : str = "",
        bucket              : str = "",
        prefix              : str = "",
        cdn                 : str = "",
        endpoint            : str = ""
    ):
        self.region = (region or "").strip()
        self.access_key_id = (access_key_id or "").strip()
        self.secret_access_key = (secret_access_key or "").strip()
        self.bucket = (bucket or "").strip()
        self.prefix = (prefix or "").strip()
        self.cdn = (cdn or "").strip()
        self.endpoint = (endpoint or "").strip()

    #-----------------------------------------------------

    async def put(
        self,
        key: str,
        content: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        expires: int = 7200
    ) -> tuple[str | None, str | None]:
        """
        Upload file to storage

        Args:
            key: File key/path
            content: File content (bytes or file-like object)
            content_type: MIME type of the file
            metadata: Additional metadata
            expires: URL expiration time in seconds

        Returns:
            (url, error). If successful, error is None.
        """

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """
        Get file content and generate access URL

        Args:
            key: File key/path

        Returns:
            (content, error). Returns (None, error) on failure.
        """

    async def delete(self, key: str) -> str | None:
        """
        Delete file from storage

        Args:
            key: File key/path

        Returns:
            Error message string, or None on success.
        """

    async def generate_signed_url(
        self,
        key: str,
        expires: int = 7200,
        content_type: str | None = None
    ) -> tuple[str | None, str | None]:
        """
        Generate signed URL for file access

        Args:
            key: File key/path
            expires: URL expiration time in seconds
            content_type: MIME type for ResponseContentType header (optional)

        Returns:
            (signed_url, error). If successful, error is None.
        """

    def get_storage_type(self) -> str:
        """
        Get storage type identifier
        """
        return self.__class__.__name__.lower().removesuffix("storage")

    #-----------------------------------------------------

    def _build_object_key(self, key: str) -> str:
        """
        Build full object key with prefix.
        Handles empty prefix correctly without adding extra slashes.
        
        Args:
            key: File key/path
            
        Returns:
            Full object key with prefix (if configured)
        """
        # Clean prefix: remove quotes and whitespace, treat "", '', "null", "none" as empty
        prefix = self.prefix.strip().strip('"').strip("'") if self.prefix else ""
        if prefix.lower() in ("", "null", "none"):
            prefix = ""
        
        if prefix:
            return f"{prefix.strip('/')}/{key.lstrip('/')}"
        return key.lstrip("/")

    #-----------------------------------------------------

    @staticmethod
    def get_content_type_from_filename(filename: str) -> str:
        """MIME type for a filename: the `Content-Type` stored on the object.

        Delegates to `utils.file_types.guess_mime` so a bare container and a
        developer laptop store the same value. Reading `mimetypes` directly made
        this a property of the host's `/etc/mime.types`: `.docx` and `.pptx`
        answer `application/octet-stream` from the interpreter's built-in table
        alone, and the answer is baked into the object at PUT time.
        """
        return guess_mime(filename)

#-----------------------------------------------------------------------------

import mimetypes

from typing import Any, BinaryIO

#-----------------------------------------------------------------------------

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
        ...

    async def get(self, key: str) -> tuple[bytes | None, str | None]:
        """
        Get file content and generate access URL

        Args:
            key: File key/path

        Returns:
            (content, error). Returns (None, error) on failure.
        """
        ...

    async def delete(self, key: str) -> str | None:
        """
        Delete file from storage

        Args:
            key: File key/path

        Returns:
            Error message string, or None on success.
        """
        ...

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
        ...

    async def get_file_info(self, key: str) -> tuple[dict[str, Any] | None, str | None]:
        """
        Get file metadata information

        Args:
            key: File key/path

        Returns:
            (file_info, error). If successful, error is None.
        """
        ...

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
    def get_content_type(content: bytes | BinaryIO) -> str:
        """
        Detect content type from file content using magic bytes (pure Python, no C dependency).
        Covers common images, audio, video, archives, and documents.

        Args:
            content: File content bytes or file-like object (only the first ~2000 bytes are needed)

        Returns:
            MIME type string
        """
        if hasattr(content, "read"):
            stream = content
            pos = stream.tell()
            content = stream.read(2000)
            stream.seek(pos)
        if not content or len(content) < 4:
            return "application/octet-stream"

        h = content[:16]

        # --- Images ---
        if h[:8] == b'\x89PNG\r\n\x1a\n':
            return "image/png"
        if h[:3] == b'\xff\xd8\xff':
            return "image/jpeg"
        if h[:6] in (b'GIF87a', b'GIF89a'):
            return "image/gif"
        if h[:4] == b'RIFF' and content[8:12] == b'WEBP':
            return "image/webp"
        if h[:4] in (b'\x00\x00\x01\x00', b'\x00\x00\x02\x00'):
            return "image/vnd.microsoft.icon"
        # BMP
        if h[:2] == b'BM':
            return "image/bmp"
        # TIFF
        if h[:4] in (b'II\x2a\x00', b'MM\x00\x2a'):
            return "image/tiff"

        # --- Audio ---
        # MP3: ID3 tag or sync word
        if h[:3] == b'ID3' or h[:2] == b'\xff\xfb' or h[:2] == b'\xff\xf3' or h[:2] == b'\xff\xf2':
            return "audio/mpeg"
        # FLAC
        if h[:4] == b'fLaC':
            return "audio/flac"
        # OGG (audio/video)
        if h[:4] == b'OggS':
            return "audio/ogg"
        # AAC (ADTS frame header)
        if h[:2] == b'\xff\xf1' or h[:2] == b'\xff\xf9':
            return "audio/aac"
        # WAV
        if h[:4] == b'RIFF' and content[8:12] == b'WAVE':
            return "audio/wav"

        # --- Video / ISO BMFF (mp4, mov, m4a, etc.) ---
        if len(content) >= 12:
            ftyp = content[4:8]
            if ftyp == b'ftyp':
                brand = content[8:12]
                if brand in (b'isom', b'iso2', b'mp41', b'mp42', b'avc1', b'dash', b'M4V '):
                    return "video/mp4"
                if brand in (b'qt  ',):
                    return "video/quicktime"
                if brand in (b'M4A ', b'mp4a'):
                    return "audio/mp4"
                return "video/mp4"
        # AVI
        if h[:4] == b'RIFF' and content[8:12] == b'AVI ':
            return "video/x-msvideo"
        # FLV
        if h[:3] == b'FLV':
            return "video/x-flv"
        # MKV / WebM (EBML header)
        if h[:4] == b'\x1a\x45\xdf\xa3':
            # WebM is a subset of MKV; a rough check on doctype
            if b'webm' in content[:64]:
                return "video/webm"
            return "video/x-matroska"
        # WMV / ASF
        if h[:16] == b'\x30\x26\xb2\x75\x8e\x66\xcf\x11\xa6\xd9\x00\xaa\x00\x62\xce\x6c':
            return "video/x-ms-wmv"

        # --- Archives ---
        if h[:4] == b'PK\x03\x04':
            # ZIP-based: check for Office documents
            if len(content) >= 30:
                # Office Open XML files contain specific paths in the zip
                segment = content[:2000] if len(content) > 2000 else content
                if b'word/' in segment:
                    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                if b'xl/' in segment:
                    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if b'ppt/' in segment:
                    return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            return "application/zip"
        if h[:6] == b'Rar!\x1a\x07':
            return "application/x-rar-compressed"
        if h[:6] == b'\xfd7zXZ\x00':
            return "application/x-xz"
        if h[:6] == b'7z\xbc\xaf\x27\x1c':
            return "application/x-7z-compressed"
        if h[:2] == b'\x1f\x8b':
            return "application/gzip"
        if h[:3] == b'BZh':
            return "application/x-bzip2"

        # --- Documents ---
        if h[:5] == b'%PDF-':
            return "application/pdf"
        # MS Office legacy (DOC, XLS, PPT share the same OLE2 signature)
        if h[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
            return "application/msword"

        # --- Text (heuristic, no reliable magic bytes) ---
        try:
            head = content[:512].decode("utf-8", errors="strict")
        except (UnicodeDecodeError, ValueError):
            return "application/octet-stream"

        stripped = head.lstrip()
        if stripped.upper().startswith("<!DOCTYPE") or stripped.lower().startswith("<html"):
            return "text/html"
        if stripped[:5] == "<?xml":
            # SVG is XML-based
            if "<svg" in head[:1024] if len(content) > 512 else "<svg" in stripped:
                return "image/svg+xml"
            return "application/xml"
        if stripped[:1] in ('{', '['):
            return "application/json"
        if stripped.startswith("#!"):
            return "text/x-script"

        return "text/plain"

    @staticmethod
    def get_content_type_from_filename(filename: str) -> str:
        """
        Get content type based on file extension using the standard library.

        Args:
            filename: File name with extension

        Returns:
            MIME type string
        """
        mime, _ = mimetypes.guess_type(filename or "")
        return mime or "application/octet-stream"

#-----------------------------------------------------------------------------

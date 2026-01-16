"""
Compressed file processor

Supports decompression and processing of ZIP, RAR, 7Z, TAR.GZ and other compression formats
Includes security checks and file filtering functionality
"""

import logging
import mimetypes
import os
import shutil
import tarfile
import tempfile
import zipfile
from typing import Dict, List, Optional, Tuple

import py7zr
import rarfile

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

# Compressed file security configuration
COMPRESS_CONFIG = {
    "MAX_SIZE": 100 * 1024 * 1024,  # 100MB
    "MAX_EXTRACTED_SIZE": 500 * 1024 * 1024,  # 500MB
    "MAX_FILES": 200,
    "MAX_DEPTH": 3,
    "TIMEOUT": 300,  # 5 minutes
}

# Supported compression formats
SUPPORTED_COMPRESS_FORMATS = {
    "application/zip": [".zip"],
    "application/x-zip-compressed": [".zip"],
    "application/x-rar-compressed": [".rar"],
    "application/x-7z-compressed": [".7z"],
    "application/gzip": [".tar.gz", ".tgz"],
    "application/x-tar": [".tar"],
    "application/x-gzip": [".gz"],
}

# Supported file types (files that need processing after decompression)
SUPPORTED_FILE_TYPES = {
    "image/jpeg": [".jpg", ".jpeg"],
    "image/png": [".png"],
    "image/gif": [".gif"],
    "image/bmp": [".bmp"],
    "image/tiff": [".tiff", ".tif"],
    "application/pdf": [".pdf"],
    "audio/mpeg": [".mp3"],
    "audio/wav": [".wav"],
    "audio/x-m4a": [".m4a"],
    "text/plain": [".txt", ".csv", ".tsv"],
}


class SecurityChecker:
    """Security checker"""

    @staticmethod
    def validate_file_size(file_content: bytes, filename: str) -> bool:
        """Validate file size"""
        if len(file_content) > COMPRESS_CONFIG["MAX_SIZE"]:
            logging.warning(f"Compressed file too large: {filename}, size: {len(file_content)} bytes")
            return False
        return True

    @staticmethod
    def validate_path(file_path: str) -> bool:
        """Validate file path to prevent path traversal attacks"""
        # Check for path traversal attack patterns
        if ".." in file_path:
            logging.warning(f"Suspicious path detected: {file_path}")
            return False

        # Check for attempts to access root directory
        if file_path.startswith("/") or file_path.startswith("\\"):
            logging.warning(f"Absolute path detected: {file_path}")
            return False

        # Check filename length
        if len(os.path.basename(file_path)) > 255:
            logging.warning(f"Filename too long: {file_path}")
            return False

        return True

    @staticmethod
    def validate_extracted_size(total_size: int) -> bool:
        """Validate total size after decompression"""
        if total_size > COMPRESS_CONFIG["MAX_EXTRACTED_SIZE"]:
            logging.warning(f"Total file size after decompression too large: {total_size} bytes")
            return False
        return True

    @staticmethod
    def validate_file_count(file_count: int) -> bool:
        """Validate file count"""
        if file_count > COMPRESS_CONFIG["MAX_FILES"]:
            logging.warning(f"Too many files: {file_count}")
            return False
        return True


class FileTypeFilter:
    """File type filter"""

    @staticmethod
    def get_content_type_by_extension(filename: str) -> str:
        """Get Content-Type based on file extension"""
        _, ext = os.path.splitext(filename.lower())

        # Iterate through supported file types
        for content_type, extensions in SUPPORTED_FILE_TYPES.items():
            if ext in extensions:
                return content_type

        # Use mimetypes module as fallback
        content_type, _ = mimetypes.guess_type(filename)
        return content_type or "application/octet-stream"

    @staticmethod
    def is_supported_file(filename: str) -> bool:
        """Check if file type is supported"""
        content_type = FileTypeFilter.get_content_type_by_extension(filename)

        # Check if it's an explicitly supported type
        for supported_type in SUPPORTED_FILE_TYPES.keys():
            if content_type == supported_type:
                return True
            # Check main type matching (e.g. image/* matches image/jpeg)
            if "/" in supported_type and "/" in content_type:
                supported_main = supported_type.split("/")[0]
                content_main = content_type.split("/")[0]
                if supported_main == content_main and supported_main in [
                    "image",
                    "audio",
                ]:
                    return True

        return False

    @staticmethod
    def filter_supported_files(file_list: List[str]) -> List[str]:
        """Filter out supported file types"""
        return [f for f in file_list if FileTypeFilter.is_supported_file(f)]


class CompressedFileProcessor:
    """Compressed file processor"""

    def __init__(self):
        self.security_checker = SecurityChecker()
        self.file_filter = FileTypeFilter()

    def is_compressed_file(self, content_type: str, filename: str) -> bool:
        """Check if file is compressed"""
        if content_type in SUPPORTED_COMPRESS_FORMATS:
            return True

        # Determine by file extension
        _, ext = os.path.splitext(filename.lower())
        for extensions in SUPPORTED_COMPRESS_FORMATS.values():
            if ext in extensions:
                return True

        return False

    async def process_compressed_file(
        self, file_content: bytes, filename: str, content_type: str
    ) -> Tuple[bool, List[Dict], str]:
        """
        Process compressed file

        Args:
            file_content: File content
            filename: File name
            content_type: Content type

        Returns:
            Tuple[bool, List[Dict], str]: (success, extracted file list, error message)
        """
        language = get_req_ctx("language", "en")

        # Security check
        if not self.security_checker.validate_file_size(file_content, filename):
            return False, [], t("file_too_large", language)

        # Create temporary directory
        temp_dir = None
        try:
            temp_dir = await self._create_temp_directory()

            # Extract file
            success, extracted_files, error_msg = await self._extract_compressed_file(
                file_content, filename, content_type, temp_dir
            )
            if not success:
                return False, [], error_msg

            # Filter and validate extracted files
            valid_files = await self._filter_and_validate_files(extracted_files, temp_dir)

            # Read valid file content
            processed_files = []
            for file_info in valid_files:
                try:
                    file_data = await self._read_extracted_file(file_info, temp_dir)
                    if file_data:
                        processed_files.append(file_data)
                except Exception:
                    continue

            return True, processed_files, ""

        except Exception as e:
            logging.error(f"Compressed file processing error: {e}", stack_info=True)
            return False, [], str(e)

        finally:
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass

    async def _create_temp_directory(self) -> str:
        """Create temporary directory"""
        temp_dir = tempfile.mkdtemp(prefix="compress_extract_")
        return temp_dir

    async def _extract_compressed_file(
        self, file_content: bytes, filename: str, content_type: str, temp_dir: str
    ) -> Tuple[bool, List[str], str]:
        """Extract compressed file"""
        language = get_req_ctx("language", "en")

        try:
            # Create temporary compressed file
            temp_compress_file = os.path.join(temp_dir, filename)
            with open(temp_compress_file, "wb") as f:
                f.write(file_content)

            extracted_files = []

            # Select decompression method based on file type
            _, ext = os.path.splitext(filename.lower())

            if ext == ".zip" or content_type in [
                "application/zip",
                "application/x-zip-compressed",
            ]:
                extracted_files = await self._extract_zip(temp_compress_file, temp_dir)
            elif ext == ".rar" or content_type == "application/x-rar-compressed":
                extracted_files = await self._extract_rar(temp_compress_file, temp_dir)
            elif ext == ".7z" or content_type == "application/x-7z-compressed":
                extracted_files = await self._extract_7z(temp_compress_file, temp_dir)
            elif ext in [".tar.gz", ".tgz"] or content_type == "application/gzip":
                extracted_files = await self._extract_tar_gz(temp_compress_file, temp_dir)
            elif ext == ".tar" or content_type == "application/x-tar":
                extracted_files = await self._extract_tar(temp_compress_file, temp_dir)
            else:
                return False, [], t("unsupported_compress_format", language)

            # Delete temporary compressed file
            os.unlink(temp_compress_file)

            return True, extracted_files, ""

        except Exception as e:
            error_msg = f"Failed to extract file: {str(e)}"
            logging.error(error_msg, stack_info=True)
            return False, [], error_msg

    async def _extract_zip(self, zip_path: str, extract_dir: str) -> List[str]:
        """Extract ZIP file"""
        extracted_files = []

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Check file list
            file_list = zip_ref.namelist()

            if not self.security_checker.validate_file_count(len(file_list)):
                raise ValueError("Too many files in compressed file")

            # Calculate total size after decompression
            total_size = sum(zip_ref.getinfo(name).file_size for name in file_list)
            if not self.security_checker.validate_extracted_size(total_size):
                raise ValueError("Total file size after decompression too large")

            # Extract files
            for file_info in zip_ref.infolist():
                if file_info.is_dir():
                    continue

                # Security check
                if not self.security_checker.validate_path(file_info.filename):
                    logging.warning(f"Skipping unsafe file path: {file_info.filename}")
                    continue

                # Extract files
                try:
                    zip_ref.extract(file_info, extract_dir)
                    extracted_path = os.path.join(extract_dir, file_info.filename)
                    if os.path.isfile(extracted_path):
                        extracted_files.append(file_info.filename)
                except Exception as e:
                    logging.error(f"Failed to extract file: {file_info.filename}, error: {str(e)}", stack_info=True)
                    continue

        return extracted_files

    async def _extract_rar(self, rar_path: str, extract_dir: str) -> List[str]:
        """Extract RAR file"""
        extracted_files = []

        try:
            with rarfile.RarFile(rar_path, "r") as rar_ref:
                file_list = rar_ref.namelist()

                if not self.security_checker.validate_file_count(len(file_list)):
                    raise ValueError("Too many files in compressed file")

                # Extract files
                for file_info in rar_ref.infolist():
                    if file_info.is_dir():
                        continue

                    if not self.security_checker.validate_path(file_info.filename):
                        logging.warning(f"Skipping unsafe file path: {file_info.filename}")
                        continue

                    try:
                        rar_ref.extract(file_info, extract_dir)
                        extracted_path = os.path.join(extract_dir, file_info.filename)
                        if os.path.isfile(extracted_path):
                            extracted_files.append(file_info.filename)
                    except Exception as e:
                        logging.error(f"Failed to extract RAR file: {file_info.filename}, error: {str(e)}", stack_info=True)
                        continue
        except rarfile.NotRarFile:
            raise ValueError("Not a valid RAR file")

        return extracted_files

    async def _extract_7z(self, sevenz_path: str, extract_dir: str) -> List[str]:
        """Extract 7Z file"""
        extracted_files = []

        with py7zr.SevenZipFile(sevenz_path, mode="r") as sevenz_ref:
            file_list = sevenz_ref.getnames()

            if not self.security_checker.validate_file_count(len(file_list)):
                raise ValueError("Too many files in compressed file")

            # Extract all files
            sevenz_ref.extractall(path=extract_dir)

            # Collect extracted files
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    if file != os.path.basename(sevenz_path):  # Exclude original compressed file
                        rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
                        extracted_files.append(rel_path)

        return extracted_files

    async def _extract_tar_gz(self, tar_gz_path: str, extract_dir: str) -> List[str]:
        """Extract TAR.GZ file"""
        extracted_files = []

        with tarfile.open(tar_gz_path, "r:gz") as tar_ref:
            file_list = tar_ref.getnames()

            if not self.security_checker.validate_file_count(len(file_list)):
                raise ValueError("Too many files in compressed file")

            for member in tar_ref.getmembers():
                if member.isfile():
                    if not self.security_checker.validate_path(member.name):
                        continue

                    try:
                        tar_ref.extract(member, extract_dir)
                        extracted_files.append(member.name)
                    except Exception as e:
                        logging.error(f"Failed to extract TAR.GZ file: {member.name}, error: {str(e)}", stack_info=True)
                        continue

        return extracted_files

    async def _extract_tar(self, tar_path: str, extract_dir: str) -> List[str]:
        """Extract TAR file"""
        extracted_files = []

        with tarfile.open(tar_path, "r") as tar_ref:
            file_list = tar_ref.getnames()

            if not self.security_checker.validate_file_count(len(file_list)):
                raise ValueError("Too many files in compressed file")

            for member in tar_ref.getmembers():
                if member.isfile():
                    if not self.security_checker.validate_path(member.name):
                        continue

                    try:
                        tar_ref.extract(member, extract_dir)
                        extracted_files.append(member.name)
                    except Exception as e:
                        logging.error(f"Failed to extract TAR file: {member.name}, error: {str(e)}", stack_info=True)
                        continue

        return extracted_files

    async def _filter_and_validate_files(self, extracted_files: List[str], temp_dir: str) -> List[Dict]:
        """Filter and validate extracted files"""
        valid_files = []

        # First filter out system files and hidden files
        filtered_files = []
        for file_path in extracted_files:
            filename = os.path.basename(file_path)

            if not self._should_skip_file(filename, file_path):
                filtered_files.append(file_path)

        # Filter supported file types
        supported_files = self.file_filter.filter_supported_files(filtered_files)

        for file_path in supported_files:
            full_path = os.path.join(temp_dir, file_path)

            if not os.path.isfile(full_path):
                continue

            try:
                file_stat = os.stat(full_path)
                content_type = self.file_filter.get_content_type_by_extension(file_path)

                valid_files.append(
                    {
                        "path": file_path,
                        "full_path": full_path,
                        "size": file_stat.st_size,
                        "content_type": content_type,
                        "filename": os.path.basename(file_path),
                    }
                )
            except Exception as e:
                logging.error(f"Failed to get file information: {file_path}, error: {str(e)}", stack_info=True)
                continue

        logging.info(f"Valid files after filtering: {len(valid_files)}/{len(extracted_files)}")

        return valid_files

    def _should_skip_file(self, filename: str, file_path: str) -> bool:
        """Determine whether to skip file"""
        # Skip macOS metadata files starting with ._
        if filename.startswith("._"):
            return True

        # Skip hidden files starting with .
        if filename.startswith("."):
            return True

        # Skip files in __MACOSX directory
        if "__MACOSX" in file_path:
            return True

        # Skip other system files
        system_files = ["Thumbs.db", "Desktop.ini", ".DS_Store"]
        if filename in system_files:
            return True

        return False

    async def _read_extracted_file(self, file_info: Dict, temp_dir: str) -> Optional[Dict]:
        """Read extracted file content"""
        try:
            if not os.path.exists(file_info["full_path"]):
                return None

            # Check file size
            file_size = os.path.getsize(file_info["full_path"])
            if file_size == 0:
                logging.error(f"File is empty: {file_info['full_path']}", stack_info=True)
                return None

            logging.info(f"Reading extracted file: {file_info['filename']}, size: {file_size} bytes")

            with open(file_info["full_path"], "rb") as f:
                content = f.read()

            # Validate read content
            if not content or len(content) == 0:
                logging.error(f"Read file content is empty: {file_info['filename']}", stack_info=True)
                return None

            # For image files, check file header
            if file_info["content_type"].startswith("image/"):
                # Check file headers for common image formats
                if not self._validate_image_header(content, file_info["filename"]):
                    logging.error(f"Image file header validation failed: {file_info['filename']}", stack_info=True)
                    return None

            # For PDF files, check file header
            elif file_info["content_type"] == "application/pdf":
                if not content.startswith(b"%PDF"):
                    logging.error(f"PDF file header validation failed: {file_info['filename']}", stack_info=True)
                    return None

            logging.info(f"Successfully read extracted file: {file_info['filename']}, content length: {len(content)}")

            return {
                "content": content,
                "filename": file_info["filename"],
                "content_type": file_info["content_type"],
                "original_path": file_info["path"],
                "size": len(content),
            }
        except Exception as e:
            logging.error(f"Failed to read file content: {file_info['path']}, error: {str(e)}", stack_info=True)
            return None

    def _validate_image_header(self, content: bytes, filename: str) -> bool:
        """Validate image file header"""
        try:
            # Check file header
            if filename.lower().endswith((".jpg", ".jpeg")):
                return content.startswith(b"\xff\xd8\xff")
            elif filename.lower().endswith(".png"):
                return content.startswith(b"\x89PNG\r\n\x1a\n")
            elif filename.lower().endswith(".gif"):
                return content.startswith((b"GIF87a", b"GIF89a"))
            elif filename.lower().endswith(".bmp"):
                return content.startswith(b"BM")
            else:
                # For other formats, temporarily pass validation
                return True
        except Exception:
            return False

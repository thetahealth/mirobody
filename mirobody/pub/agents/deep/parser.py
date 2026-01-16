"""
Unified File Parser

Integrates basic parsing (Langchain) and intelligent parsing (LLM) capabilities.
Supports 10+ file types with automatic routing to the best parsing method.
"""

import logging
import os
import tempfile
import traceback
from io import BytesIO, StringIO
from pathlib import Path
from typing import BinaryIO, List, Optional, TextIO, Union

from langchain_community.document_loaders import (
    CSVLoader,
    Docx2txtLoader,
    JSONLoader,
    PyPDFLoader,
    UnstructuredPowerPointLoader,
    WebBaseLoader,
)
from langchain_core.documents import Document
from langchain_core.messages import HumanMessage, SystemMessage

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import openpyxl
except ImportError:
    openpyxl = None

# Supported file type mapping - centralized configuration
SUPPORTED_FILE_TYPES = {
    # Document types
    "pdf": "document",
    "docx": "document",
    "doc": "document",
    "csv": "document",
    "xlsx": "excel",
    "xls": "excel",
    "pptx": "presentation",
    "ppt": "presentation",
    # Image types
    "jpg": "image",
    "jpeg": "image",
    "png": "image",
    "gif": "image",
    "webp": "image",
    "bmp": "image",
    # Text/Code types
    "py": "text",
    "js": "text",
    "ts": "text",
    "jsx": "text",
    "tsx": "text",
    "java": "text",
    "c": "text",
    "cpp": "text",
    "h": "text",
    "hpp": "text",
    "cs": "text",
    "php": "text",
    "rb": "text",
    "go": "text",
    "rs": "text",
    "swift": "text",
    "kt": "text",
    "scala": "text",
    "r": "text",
    "sql": "text",
    "sh": "text",
    "bash": "text",
    "zsh": "text",
    "fish": "text",
    "ps1": "text",
    "bat": "text",
    "cmd": "text",
    "yaml": "text",
    "yml": "text",
    "toml": "text",
    "ini": "text",
    "cfg": "text",
    "conf": "text",
    "properties": "text",
    "css": "text",
    "scss": "text",
    "sass": "text",
    "less": "text",
    "vue": "text",
    "svelte": "text",
    "jinja": "text",
    "j2": "text",
    "template": "text",
    "tpl": "text",
    "hbs": "text",
    "mustache": "text",
    "ejs": "text",
    "dockerfile": "text",
    "makefile": "text",
    "gradle": "text",
    "cmake": "text",
    "requirements": "text",
    "md": "text",
    "txt": "text",
    "htm": "text",
    "html": "text",
    "json": "text",
    "xml": "text",
}

# Derived sets for quick type checking
IMAGE_EXTENSIONS: set[str] = {ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "image"}
TEXT_CODE_EXTENSIONS: set[str] = {ext for ext, type_ in SUPPORTED_FILE_TYPES.items() if type_ == "text"}

logger = logging.getLogger(__name__)

# File input type
FileInput = Union[str, BinaryIO, TextIO, BytesIO, StringIO]


class StructuredExcelLoader:
    """
    Custom Excel loader that preserves formatting information and line breaks.
    Uses pandas and openpyxl for better Excel file parsing.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def load(self) -> List[Document]:
        """Load Excel file and return Document list."""
        if not pd:
            raise ImportError(
                "pandas is required for StructuredExcelLoader. Please install it with: pip install pandas"
            )

        if not openpyxl:
            raise ImportError(
                "openpyxl is required for StructuredExcelLoader. Please install it with: pip install openpyxl"
            )

        documents: List[Document] = []

        try:
            # Read all sheets
            excel_file = pd.ExcelFile(self.file_path, engine="openpyxl")

            for sheet_name in excel_file.sheet_names:
                # Read each sheet, preserving original format
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    engine="openpyxl",
                    keep_default_na=False,  # Keep empty values as empty strings instead of NaN
                    dtype=str,  # Read all data as strings to preserve format
                )

                # Build table content, preserving formatting
                content_lines: List[str] = []

                # Add sheet name
                if len(excel_file.sheet_names) > 1:
                    content_lines.append(f"=== Sheet: {sheet_name} ===\n")

                if df.empty:
                    content_lines.append("(Empty sheet)")
                else:
                    # Add headers
                    headers = df.columns.tolist()
                    header_line = " | ".join(str(h) for h in headers)
                    content_lines.append(header_line)
                    content_lines.append("-" * len(header_line))

                    # Add data rows
                    for index, row in df.iterrows():
                        row_data: List[str] = []
                        for col in df.columns:
                            cell_value = row[col]
                            # Preserve original format, including line breaks
                            if pd.isna(cell_value) or cell_value == "":
                                row_data.append("")
                            else:
                                # Ensure line breaks are preserved
                                cell_str = str(cell_value).replace("\\n", "\n")
                                row_data.append(cell_str)

                        row_line = " | ".join(row_data)
                        content_lines.append(row_line)

                # Create document
                content = "\n".join(content_lines)

                metadata = {
                    "source": self.file_path,
                    "sheet_name": sheet_name,
                    "sheet_index": excel_file.sheet_names.index(sheet_name),
                    "total_sheets": len(excel_file.sheet_names),
                    "rows": len(df) if not df.empty else 0,
                    "columns": len(df.columns) if not df.empty else 0,
                }

                document = Document(page_content=content, metadata=metadata)
                documents.append(document)

        except Exception as e:
            # If custom parsing fails, create an error document
            error_content = f"Failed to parse Excel file: {str(e)}"
            error_metadata = {
                "source": self.file_path,
                "error": str(e),
                "loader": "StructuredExcelLoader",
            }
            document = Document(page_content=error_content, metadata=error_metadata)
            documents.append(document)

        return documents


class FileParser:
    """
    Unified File Parser with intelligent fallback strategy.

    Supported file types:
    - PDF: PyPDF (fast, local) → unified vision API (fallback for complex PDFs)
    - Image: unified vision API (auto-selects: gemini → openrouter → doubao)
    - DOCX/DOC: Docx2txt
    - Excel (XLSX/XLS): StructuredExcelLoader
    - CSV: CSVLoader
    - PowerPoint: UnstructuredPowerPointLoader
    - Text/Code: Direct read (70+ file extensions supported)
    - Webpage: WebBaseLoader

    Supported input types:
    - URL (str): Network URL or local file path
    - IO objects: BinaryIO, TextIO, BytesIO, StringIO
    
    Note: LLM-based parsing automatically selects the best available provider
    based on configured API keys (GOOGLE_API_KEY, OPENROUTER_API_KEY, VOLCENGINE_API_KEY)
    """

    def __init__(self):
        """
        Initialize file parser.

        Note: LLM-based parsing (images, PDFs) uses unified_file_extract which
        automatically selects the best available provider (gemini/openrouter/doubao)
        based on configured API keys. No need to pass llm_client.
        """
        logger.info("FileParser initialized with unified vision API support")

    async def parse_file(
        self, file_input: FileInput, file_type: str, filename: Optional[str] = None
    ) -> str:
        """
        Unified parsing entry point, automatically routes to the appropriate parsing method

        Args:
            file_input: File input, can be URL string or IO object
            file_type: File type/extension
            filename: Filename (required when file_input is an IO object)

        Returns:
            Parsed text content
        """
        try:
            # Normalize file type
            file_type = file_type.lower().lstrip(".")

            # Check if file type is supported
            if file_type not in SUPPORTED_FILE_TYPES:
                logger.warning(f"Unsupported file type: {file_type}")
                return f"Unsupported file type: {file_type}"

            # Handle image files
            if file_type in IMAGE_EXTENSIONS:
                return await self._parse_image(file_input, file_type, filename)

            # Read text/code files directly
            if file_type in TEXT_CODE_EXTENSIONS:
                return await self._parse_text_file(file_input, file_type, filename)

            # PDF special handling: Try PyPDF first (fast), fallback to LLM on failure
            if file_type == "pdf":
                try:
                    logger.info("Attempting PDF parsing with PyPDF (Langchain)")
                    content = await self._parse_document_with_langchain(
                        file_input, file_type, filename
                    )
                    # Check if content is meaningful (not just whitespace or too short)
                    if content and len(content.strip()) > 50:
                        logger.info(f"PDF parsed successfully with PyPDF, content length: {len(content)}")
                        return content
                    # Content insufficient, fall through to LLM parsing
                    logger.warning(
                        f"PyPDF parsing returned insufficient content (length: {len(content.strip())}), "
                        "falling back to LLM-based parsing"
                    )
                except Exception as e:
                    # PyPDF failed, fall through to LLM parsing
                    logger.warning(f"PyPDF parsing failed: {e}, falling back to LLM-based parsing")
                
                # Fallback to LLM-based parsing
                return await self._parse_pdf_with_llm(file_input=file_input, filename=filename)

            # Other document types use Langchain
            return await self._parse_document_with_langchain(
                file_input, file_type, filename
            )

        except Exception as e:
            logger.error(f"File parsing failed: {e}\n{traceback.format_exc()}")
            return f"File parsing failed: {str(e)}"

    # ==================== Private Parsing Methods ====================

    async def _parse_image(
        self,
        file_input: FileInput,
        image_type: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Parse image file using unified vision API.
        
        Args:
            file_input: File input (path, URL, or IO object)
            image_type: Image file extension (jpg, jpeg, png, gif, webp, bmp)
            filename: Optional filename
            
        Returns:
            Parsed image content as text
        """
        from mirobody.utils.llm.file_processors import unified_file_extract

        temp_file_path = await self._ensure_file_path(file_input, image_type, filename)
        needs_cleanup = not isinstance(file_input, str) or file_input.startswith(
            ("http://", "https://")
        )

        if not temp_file_path or not os.path.exists(temp_file_path):
            logger.warning(f"Image does not save successfully: {temp_file_path}")
            raise ValueError("Image content reading failed")

        try:
            prompt = (
                "You are a professional image recognition expert. Please fully understand the content format and meaning of the image, and convert it into clear textual content.\n\n"
                "- Your first goal is to ensure the accuracy of the original text content and clarity of the presentation format\n"
                "- When encountering non-text content, always represent it in the form of <image>image content description</image> to help understand the original information\n"
                "- When non-text content contains effective information such as data, you can create custom formats to aid understanding. For example, <chart>important data in the table</chart>, etc.\n"
                "- You must respect the original content and cannot fabricate information that does not exist\n"
                "- The final output should be a clear and concise text description of the image content, without any additional formatting or comments\n"
                "\nPlease describe the main content in this image."
            )

            img_type = "jpeg" if image_type in ["jpg", "jpeg"] else "png"
            mime_type = f"image/{img_type}"
            
            # Use unified file extract function
            content = await unified_file_extract(
                file_path=temp_file_path,
                prompt=prompt,
                content_type=mime_type,
            )

            if needs_cleanup:
                self._delete_file(temp_file_path)

            if content:
                return content
            else:
                raise ValueError("Image content reading failed")

        except Exception as e:
            if needs_cleanup:
                self._delete_file(temp_file_path)
            logger.error(f"Image parsing failed: {e}")
            raise e

    async def _parse_text_file(
        self, file_input: FileInput, file_type: str, filename: Optional[str] = None
    ) -> str:
        """Parse text file by reading content directly."""
        temp_file_path = await self._ensure_file_path(file_input, file_type, filename)
        needs_cleanup = not isinstance(file_input, str) or file_input.startswith(
            ("http://", "https://")
        )

        if not temp_file_path or not os.path.exists(temp_file_path):
            logger.warning(f"Text file does not save successfully: {temp_file_path}")
            raise ValueError("Text file content reading failed")

        try:
            with open(temp_file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

            if needs_cleanup:
                self._delete_file(temp_file_path)

            if not content:
                logger.warning(f"Text file content is empty: {temp_file_path}")
                return ""

            return content

        except Exception as e:
            if needs_cleanup:
                self._delete_file(temp_file_path)
            logger.error(f"Text file parsing failed: {e}")
            raise e

    async def _parse_document_with_langchain(
        self, file_input: FileInput, file_type: str, filename: Optional[str] = None
    ) -> str:
        """Parse document using Langchain loaders."""
        # File type to loader mapping
        LOADER_MAP = {
            "pdf": PyPDFLoader,
            "docx": Docx2txtLoader,
            "doc": Docx2txtLoader,
            "csv": CSVLoader,
            "json": JSONLoader,
            "xlsx": StructuredExcelLoader,
            "xls": StructuredExcelLoader,
            "pptx": UnstructuredPowerPointLoader,
            "ppt": UnstructuredPowerPointLoader,
        }

        temp_file_path = await self._ensure_file_path(file_input, file_type, filename)
        needs_cleanup = not isinstance(file_input, str) or file_input.startswith(
            ("http://", "https://")
        )

        if not temp_file_path or not os.path.exists(temp_file_path):
            logger.warning(f"Document does not save successfully: {temp_file_path}")
            raise ValueError("Document content reading failed")

        try:
            loader_class = LOADER_MAP.get(file_type.lower())

            if not loader_class:
                raise ValueError(f"Unsupported file type: {file_type}")

            # Special handling for different loader types
            if loader_class == JSONLoader:
                loader = loader_class(
                    file_path=temp_file_path, jq_schema=".", text_content=False
                )
            elif loader_class == StructuredExcelLoader:
                loader = loader_class(temp_file_path)
            else:
                loader = loader_class(temp_file_path)

            # Load documents
            docs: List[Document] = loader.load()

            # Extract text content
            content = "\n\n".join([doc.page_content for doc in docs])

            if needs_cleanup:
                self._delete_file(temp_file_path)

            if not content:
                logger.warning(
                    f"Document reading failed or content is empty: {temp_file_path}"
                )
                raise ValueError("Document content reading failed")

            return content

        except Exception as e:
            if needs_cleanup:
                self._delete_file(temp_file_path)
            logger.error(f"Document parsing failed: {e}\n{traceback.format_exc()}")
            raise e

    async def _parse_pdf_with_llm(
        self,
        file_input: FileInput,
        filename: Optional[str] = None,
    ) -> str:
        """
        Parse PDF file using LLM (unified vision API).
        
        Uses unified_file_extract which automatically:
        - Selects best available provider (gemini/openrouter/doubao)
        - Converts PDF to images and processes with vision model
        - Handles errors and provider fallback
        """
        from mirobody.utils.llm.file_processors import unified_file_extract

        temp_file_path = await self._ensure_file_path(file_input, "pdf", filename)
        needs_cleanup = not isinstance(file_input, str) or file_input.startswith(
            ("http://", "https://")
        )

        if not temp_file_path or not os.path.exists(temp_file_path):
            logger.warning(f"PDF does not save successfully: {temp_file_path}")
            raise ValueError("PDF file reading failed")

        try:
            # Define extraction prompt
            prompt = (
                "Please extract all text content from this PDF document. "
                "Extract the text exactly as it appears, preserving formatting, line breaks, and structure. "
                "If the document contains tables, charts, or diagrams, describe their content clearly. "
                "Do not summarize or paraphrase - extract the complete text content."
            )
            
            # Use unified file extract (auto-selects best provider)
            content = await unified_file_extract(
                file_path=temp_file_path,
                prompt=prompt,
                content_type="application/pdf"
            )

            if needs_cleanup:
                self._delete_file(temp_file_path)

            if content:
                return content
            else:
                raise ValueError("LLM PDF parsing returned empty content")

        except Exception as e:
            if needs_cleanup:
                self._delete_file(temp_file_path)
            logger.error(f"LLM-based PDF parsing failed: {e}")
            raise e

    async def parse_webpage(self, url: str) -> str:
        """Parse webpage content"""
        try:
            loader = WebBaseLoader(url)
            docs: List[Document] = []
            async for doc in loader.alazy_load():
                docs.append(doc)

            if not docs:
                logger.warning(f"No content extracted from URL: {url}")
                return f"No content could be extracted from the URL: {url}"

            # Extract and merge text content
            content = ""
            for doc in docs:
                metadata = doc.metadata  # type: dict[str, str]
                title = metadata.get("title", "Untitled")
                description = metadata.get("description", "[NULL]")
                content += (
                    f"### {title}\n\n{description}\n\n{doc.page_content}\n\n"
                )

            if not content:
                logger.warning(f"Empty content extracted from URL: {url}")
                return f"The webpage at {url} appears to be empty or could not be parsed properly."

            return content

        except Exception as e:
            logger.error(f"Webpage parsing failed: {e}\n{traceback.format_exc()}")
            return f"Failed to parse webpage: {str(e)}"

    # ==================== Helper Methods ====================

    def _get_temp_file_path(
        self, file_input: FileInput, file_type: str, filename: Optional[str] = None
    ) -> Optional[str]:
        """Create temporary file path based on input type."""
        if isinstance(file_input, str):
            return None  # URL or file path, needs download

        # IO object, create temporary file
        suffix = f".{file_type}" if not file_type.startswith(".") else file_type
        if filename and "." in filename:
            suffix = "." + filename.split(".")[-1]

        temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)

        try:
            with os.fdopen(temp_fd, "wb") as temp_file:
                if hasattr(file_input, "read"):
                    # Reset file pointer to beginning if seekable (e.g., BytesIO)
                    # This fixes the issue where BytesIO is read multiple times (PyPDF fallback to LLM)
                    if hasattr(file_input, "seek"):
                        file_input.seek(0)
                    
                    content = file_input.read()
                    if isinstance(content, str):
                        content = content.encode("utf-8")
                    temp_file.write(content)
                else:
                    raise ValueError(f"Unsupported file input type: {type(file_input)}")

            return temp_path
        except Exception as e:
            try:
                os.unlink(temp_path)
            except:
                pass
            raise e

    async def _ensure_file_path(
        self, file_input: FileInput, file_type: str, filename: Optional[str] = None
    ) -> str:
        """Ensure file path is obtained, handle URL download and IO objects."""
        if isinstance(file_input, str):
            # URL or file path
            if file_input.startswith(("http://", "https://")):
                # Download network file
                temp_file_path = await self._download_file(file_input, file_type)
                if not temp_file_path or not os.path.exists(temp_file_path):
                    raise ValueError(f"File download failed: {file_input}")
                return temp_file_path
            else:
                # Local file path
                if not os.path.exists(file_input):
                    raise ValueError(f"File not found: {file_input}")
                return file_input
        else:
            # IO object
            temp_path = self._get_temp_file_path(file_input, file_type, filename)
            if not temp_path:
                raise ValueError("Failed to create temporary file from IO object")
            return temp_path

    async def _download_file(self, url: str, file_type: str) -> str:
        """Download file from URL to temporary location."""
        import httpx
        
        suffix = f".{file_type}" if not file_type.startswith(".") else file_type
        temp_fd, temp_path = tempfile.mkstemp(suffix=suffix)
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                
                with os.fdopen(temp_fd, "wb") as temp_file:
                    temp_file.write(response.content)
                
                logger.info(f"Downloaded file from {url} to {temp_path}")
                return temp_path
                
        except Exception as e:
            try:
                os.unlink(temp_path)
            except:
                pass
            logger.error(f"Failed to download file from {url}: {e}")
            raise ValueError(f"File download failed: {e}") from e

    @staticmethod
    def _delete_file(file_path: str) -> None:
        """Safely delete file."""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.warning(f"Delete file failed: {file_path}. {e}")

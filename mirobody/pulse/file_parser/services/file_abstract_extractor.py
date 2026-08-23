"""
File Abstract Extractor Service
Extracts file summaries for different file types, with special handling for PDF files
"""

import asyncio
import hashlib
import os
import tempfile
import logging
import json
from typing import Dict, Optional

import pdfplumber
from PIL import Image
from mirobody.utils.llm import unified_file_extract
from mirobody.utils.file_types import is_document_file, is_excel_file, is_text_file
from mirobody.pulse.file_parser.services.prompts.file_abstract_prompt import FILE_ABSTRACT_PROMPT, FALLBACK_ABSTRACT_TEMPLATES
from mirobody.pulse.file_parser.services.prompts.file_original_text_prompt import FILE_ORIGINAL_TEXT_PROMPT


# Minimum stripped chars from a PDF's embedded text layer to accept it as a
# born-digital extraction (Tier 1) instead of falling back to Vision-LLM OCR.
# Scanned/image-only PDFs yield ~0 chars here.
_PDF_TEXT_LAYER_MIN_CHARS = 100


async def lookup_extracted_text(file_content: bytes) -> Optional[str]:
    """Text a previous extraction of these exact bytes produced, or None.

    The cheap half of extraction: one indexed lookup, never a model call. The
    DeepAgent workspace uses it at registration time to decide whether a file
    still needs OCR at all (see ``agent/deep/parser.FileParser.prepare``).
    """
    if not file_content:
        return None
    return await _read_original_text_cache(hashlib.sha256(file_content).hexdigest())


async def _read_original_text_cache(content_hash: str) -> Optional[str]:
    """Dedup read: SHA256 of the raw bytes -> text some earlier upload extracted.

    The cache IS ``th_files``. Every persistence path
    (``FileDbService.insert_file`` / ``update_file_processed`` /
    ``BaseFileHandler._save_original_text_to_db``) already writes
    ``content_hash`` and ``original_text`` onto the same row, so a dedicated
    hash->text table stored a second copy of the same health text and bought
    nothing: it was never normalised away, nothing ever deleted from it, and no
    foreign key tied it to the file it came from — so a user's extracted report
    text outlived the file they deleted, in a table with no ``user_id``. Reading
    through ``th_files`` (``is_del = false``) makes "delete the file, lose the
    text" true.

    The trade-off, stated plainly: an extraction only lands in the cache if it
    reaches a ``th_files`` row. Registration (``FileParser.prepare``) reads this
    cache but never OCRs to populate it; the agent's own OCR runs lazily, on
    the first ``read_file`` (``PgFilesystemBackend._lazy_extract_doc_text``),
    and is written back into this same ``th_files`` row rather than kept apart.
    That write happens only once the extraction finishes, so a file the agent
    reads before the upload pipeline's own extraction lands can still be OCR'd
    by both. That is a cost, not a correctness, difference.

    ``GLOBAL_FILE_CACHE_ENABLED: false`` forces fresh extraction. Deferred
    imports + broad except: extraction must keep working without a database.
    """
    try:
        from mirobody.utils.config import safe_read_cfg
        if str(safe_read_cfg("GLOBAL_FILE_CACHE_ENABLED", "true")).strip().lower() in ("false", "0", "no"):
            return None
        from mirobody.utils.db import execute_query
        rows = await execute_query(
            """
            SELECT decrypt_content(original_text) AS original_text
            FROM th_files
            WHERE content_hash = :hash AND is_del = false
              AND original_text IS NOT NULL AND original_text != ''
            ORDER BY updated_at DESC LIMIT 1
            """,
            params={"hash": content_hash},
        )
        text = rows[0].get("original_text") if rows else None
        return text if text and text.strip() else None
    except Exception as e:
        logging.warning(f"original-text cache read failed (hash={content_hash[:16]}...): {e}")
        return None


class FileAbstractExtractor:
    """Service for extracting file abstracts/summaries"""
    
    def __init__(self):
        self.max_abstract_length = 200  # Maximum length for abstract
    
    def _infer_file_extension(self, content_type: str, file_type: str, original_filename: str = "") -> str:
        """
        Infer file extension from content type or file type
        
        Args:
            content_type: MIME content type
            file_type: File type string
            original_filename: Original filename (optional)
            
        Returns:
            str: File extension (e.g., '.pdf', '.jpg', '.png')
        """
        # Try to get extension from original filename first
        if original_filename and "." in original_filename:
            ext = original_filename.rsplit(".", 1)[-1].lower()
            if ext in ["pdf", "jpg", "jpeg", "png", "gif", "bmp", "webp"]:
                return f".{ext}"
        
        # Map content types to extensions
        content_type_map = {
            "application/pdf": ".pdf",
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/bmp": ".bmp",
            "image/webp": ".webp",
        }
        
        if content_type and content_type in content_type_map:
            return content_type_map[content_type]
        
        # Fallback based on file_type
        if file_type == "pdf":
            return ".pdf"
        elif file_type == "image":
            return ".jpg"  # Default to jpg for generic image type
        
        return ""
    
    async def extract_file_abstract(
        self, 
        file_content: bytes, 
        file_type: str, 
        filename: str,
        content_type: str = None
    ) -> Dict[str, str]:
        """
        Extract abstract and generated filename from file content
        
        Args:
            file_content: Binary file content
            file_type: File type (pdf, image, excel, etc.)
            filename: Original filename
            content_type: MIME content type
            
        Returns:
            Dict[str, str]: Dictionary with keys:
                - file_name: Generated file name (only for PDF and images, empty for others)
                - file_abstract: File abstract (max 200 characters)
        """
        try:
            # Route to appropriate extractor based on file type
            if file_type == "pdf" or (content_type and content_type == "application/pdf"):
                return await self._extract_pdf_abstract(file_content, filename)
            elif file_type == "image" or (content_type and content_type.startswith("image/")):
                return await self._extract_image_abstract(file_content, filename)
            elif (file_type == "excel" or 
                  (content_type and ("spreadsheet" in content_type or "excel" in content_type or
                   content_type in ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]))):
                return await self._extract_excel_abstract(file_content, filename)
            else:
                return await self._extract_generic_abstract(file_content, filename, file_type)
                
        except Exception as e:
            logging.error(f"File abstract extraction failed for {filename}: {e}", stack_info=True)
            # Return a basic fallback abstract
            return self._create_fallback_abstract(filename, file_type)
    
    async def _extract_pdf_abstract(self, file_content: bytes, filename: str) -> Dict[str, str]:
        """
        Extract abstract and generate filename from PDF file (first 2 and last 2 pages only)
        
        Args:
            file_content: PDF file binary content
            filename: Original filename
            
        Returns:
            Dict[str, str]: Dictionary with file_name and file_abstract
        """
        try:
            # Create temporary file for PDF processing
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            try:
                # Read PDF and extract text from first 2 and last 2 pages
                extracted_text = ""
                pdf_document = pdfplumber.open(temp_file_path)
                total_pages = len(pdf_document.pages)
                
                # Determine which pages to extract (first 2 + last 2)
                pages_to_extract = set()
                
                # Add first 2 pages
                for i in range(min(2, total_pages)):
                    pages_to_extract.add(i)
                
                # Add last 2 pages (if different from first pages)
                if total_pages > 2:
                    for i in range(max(total_pages - 2, 2), total_pages):
                        pages_to_extract.add(i)
                
                # Extract text from selected pages
                for page_num in sorted(pages_to_extract):
                    try:
                        page = pdf_document.pages[page_num]
                        page_text = page.extract_text()
                        if page_text.strip():
                            extracted_text += f"\n[Page {page_num + 1}]\n{page_text}"
                    except Exception as e:
                        logging.warning(f"Failed to extract text from page {page_num + 1}: {e}")
                        continue
                
                pdf_document.close()
                
                # Generate abstract and filename using LLM
                if extracted_text.strip():
                    # Use LLM file extraction for better quality
                    file_extension = self._infer_file_extension("application/pdf", "pdf", filename)
                    result = await self._generate_llm_abstract_with_file(
                        temp_file_path=temp_file_path,
                        content_type="application/pdf",
                        context=f"PDF document: {filename} ({total_pages} pages)",
                        file_extension=file_extension,
                        generate_filename=True  # PDF files get generated filename
                    )
                else:
                    result = {
                        "file_name": "",
                        "file_abstract": f"PDF document ({total_pages} pages): {filename} - File uploaded successfully, but text extraction failed"
                    }
                
                # Ensure abstract is truncated
                if result.get("file_abstract"):
                    result["file_abstract"] = self._truncate_abstract(result["file_abstract"])
                
                return result
                
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"PDF abstract extraction failed: {e}", stack_info=True)
            return self._create_fallback_abstract(filename, "pdf")
    
    async def _extract_image_abstract(self, file_content: bytes, filename: str) -> Dict[str, str]:
        """
        Extract abstract and generate filename from image file
        
        Args:
            file_content: Image file binary content
            filename: Original filename
            
        Returns:
            Dict[str, str]: Dictionary with file_name and file_abstract
        """
        try:
            # Create temporary file for image processing
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            try:
                # Get basic image information
                with Image.open(temp_file_path) as img:
                    width, height = img.size
                    format_info = img.format or "Unknown"
                
                # Generate abstract and filename using LLM
                try:
                    file_extension = self._infer_file_extension("image/jpeg", "image", filename)
                    result = await self._generate_llm_abstract_with_file(
                        temp_file_path=temp_file_path,
                        content_type="image/jpeg",  # Use generic image type for LLM
                        context=f"Image file: {filename} ({width}x{height}, {format_info} format)",
                        file_extension=file_extension,
                        generate_filename=True  # Image files get generated filename
                    )
                except Exception as e:
                    logging.warning(f"Image LLM processing failed: {e}")
                    result = {
                        "file_name": "",
                        "file_abstract": f"Image file: {filename} ({width}x{height}, {format_info} format) - Image uploaded successfully and ready for viewing"
                    }
                
                # Ensure abstract is truncated
                if result.get("file_abstract"):
                    result["file_abstract"] = self._truncate_abstract(result["file_abstract"])
                
                return result
                
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"Image abstract extraction failed: {e}", stack_info=True)
            return self._create_fallback_abstract(filename, "image")
    
    async def _extract_excel_abstract(self, file_content: bytes, filename: str) -> Dict[str, str]:
        """
        Extract abstract from Excel file (no filename generation for Excel)
        
        Args:
            file_content: Excel file binary content
            filename: Original filename
            
        Returns:
            Dict[str, str]: Dictionary with empty file_name and file_abstract
        """
        try:
            import pandas as pd
            
            # Create temporary file for Excel processing
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            try:
                # Read Excel file structure
                excel_file = pd.ExcelFile(temp_file_path)
                sheet_names = excel_file.sheet_names
                total_sheets = len(sheet_names)
                
                # Read first sheet to get some basic info
                if sheet_names:
                    first_sheet = pd.read_excel(temp_file_path, sheet_name=sheet_names[0], nrows=10)
                    rows, cols = first_sheet.shape
                    column_names = list(first_sheet.columns)[:5]  # First 5 column names
                    
                    # Basic fallback abstract
                    basic_abstract = f"Excel file: {filename} ({total_sheets} sheets) - Contains columns: {', '.join(column_names)}"
                    
                    # Try to use LLM for better analysis
                    try:
                        context = f"Excel file: {filename} ({total_sheets} sheets, {rows} rows)"
                        csv_temp_path = None
                        try:
                            # Convert Excel to CSV for LLM processing
                            csv_content = first_sheet.to_csv(index=False, encoding='utf-8')
                            with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False, encoding='utf-8') as csv_temp_file:
                                csv_temp_file.write(csv_content)
                                csv_temp_path = csv_temp_file.name
                            
                            result = await self._generate_llm_abstract_with_file(
                                temp_file_path=csv_temp_path, content_type="text/csv",
                                context=context, file_extension="", generate_filename=False
                            )
                            gemini_abstract = result.get("file_abstract", "")
                        finally:
                            if csv_temp_path:
                                try:
                                    os.unlink(csv_temp_path)
                                except Exception:
                                    pass
                        
                        # Use LLM result if it's meaningful
                        if (gemini_abstract and len(gemini_abstract.strip()) > 20 and 
                            not gemini_abstract.startswith("Excel file") and
                            not gemini_abstract.startswith("File processed") and
                            "contains content" not in gemini_abstract.lower()):
                            abstract = gemini_abstract
                        else:
                            abstract = basic_abstract
                            
                    except Exception as llm_error:
                        logging.error(f"LLM analysis failed for Excel: {llm_error}")
                        abstract = basic_abstract
                else:
                    abstract = f"Excel file: {filename} - Empty document or unable to read sheets"
                
                return {
                    "file_name": "",  # Excel files don't get generated filename
                    "file_abstract": self._truncate_abstract(abstract)
                }
                
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"Excel abstract extraction failed: {filename}, {e}")
            return {"file_name": "", "file_abstract": f"Excel file: {filename} - Spreadsheet uploaded, analyzing content in background"}
    
    async def _extract_generic_abstract(self, file_content: bytes, filename: str, file_type: str) -> Dict[str, str]:
        """
        Extract abstract from generic file types (no filename generation)
        
        Args:
            file_content: File binary content
            filename: Original filename
            file_type: File type
            
        Returns:
            Dict[str, str]: Dictionary with empty file_name and file_abstract
        """
        try:
            file_size = len(file_content)
            
            # Try to extract text if it's a text-based file
            if file_type in ["text", "txt"]:
                try:
                    text_content = file_content.decode('utf-8', errors='ignore')[:3000]  # First 3000 chars
                    if text_content.strip():
                        result = await self._generate_llm_abstract_with_content(
                            text_content, 
                            f"Text file: {filename}"
                        )
                        return {
                            "file_name": "",
                            "file_abstract": self._truncate_abstract(result.get("file_abstract", ""))
                        }
                except Exception:
                    pass
            
            # Fallback to basic file info
            abstract = f"{file_type.upper()} file: {filename} ({self._format_file_size(file_size)}) - File uploaded successfully"
            return {
                "file_name": "",
                "file_abstract": self._truncate_abstract(abstract)
            }
            
        except Exception as e:
            logging.error(f"Generic abstract extraction failed: {e}", stack_info=True)
            return self._create_fallback_abstract(filename, file_type)
    
    async def _generate_llm_abstract_with_file(
        self, 
        temp_file_path: str, 
        content_type: str, 
        context: str, 
        file_extension: str = "",
        generate_filename: bool = True
    ) -> Dict[str, str]:
        """
        Generate abstract and filename using LLM file extract service (Gemini or Doubao based on environment)
        
        Args:
            temp_file_path: Path to temporary file
            content_type: MIME content type
            context: Context information
            file_extension: File extension to include in generated filename
            generate_filename: Whether to generate a new filename (True for PDF/images, False for others)
            
        Returns:
            Dict[str, str]: Dictionary with file_name and file_abstract
        """
        try:
            # Prepare prompt with file extension hint
            extension_hint = f"IMPORTANT: The file extension MUST be '{file_extension}'. Do not use any other extension." if file_extension else ""
            prompt = f"""{FILE_ABSTRACT_PROMPT}

File context: {context}
{extension_hint}

Please return strictly in JSON format, do not include any markdown code block markers or other formatting."""
            
            # Use unified file extract (auto-selects model based on environment)
            # json_mode=True because we expect JSON output for file abstract extraction
            response = await unified_file_extract(
                file_path=temp_file_path,
                prompt=prompt,
                content_type=content_type,
                json_mode=True
            )
                
            if response and response.strip():
                # Clean up response - remove markdown code blocks if present
                cleaned_response = response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]
                if cleaned_response.startswith("```"):
                    cleaned_response = cleaned_response[3:]
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]
                cleaned_response = cleaned_response.strip()
                
                # Try to parse as JSON
                try:
                    result = json.loads(cleaned_response)
                    file_name = result.get("file_name", "") if generate_filename else ""
                    file_abstract = result.get("file_abstract", "")
                    
                    # Validate and clean up
                    if file_abstract:
                        file_abstract = self._truncate_abstract(file_abstract)
                    
                    logging.info(f"✅ Abstract generation successful: file_name='{file_name}', abstract_len={len(file_abstract)}")
                    
                    return {
                        "file_name": file_name,
                        "file_abstract": file_abstract
                    }
                    
                except json.JSONDecodeError as json_error:
                    logging.warning(f"LLM returned invalid JSON, treating as plain text: {json_error}")
                    # Fallback: treat the whole response as abstract
                    abstract = self._truncate_abstract(cleaned_response)
                    return {
                        "file_name": "",
                        "file_abstract": abstract
                    }
            else:
                logging.warning(f"LLM returned empty response, using fallback")
                return {
                    "file_name": "",
                    "file_abstract": f"{context} - Contains relevant content, processed successfully"
                }
                
        except Exception as e:
            logging.warning(f"LLM abstract generation failed: {e}")
            return {
                "file_name": "",
                "file_abstract": f"{context} - Contains relevant content, processed successfully"
            }
    
    async def _generate_llm_abstract_with_content(self, content: str, context: str) -> Dict[str, str]:
        """
        Generate abstract using text content only (fallback method, no filename generation)
        
        Args:
            content: Text content to summarize
            context: Context information
            
        Returns:
            Dict[str, str]: Dictionary with empty file_name and file_abstract
        """
        try:
            # Create a temporary text file for LLM processing
            with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False, encoding='utf-8') as temp_file:
                temp_file.write(content[:3000])  # Limit content to avoid token limits
                temp_file_path = temp_file.name
            
            try:
                # Use LLM to process the text file
                result = await self._generate_llm_abstract_with_file(
                    temp_file_path=temp_file_path,
                    content_type="text/plain",
                    context=context,
                    file_extension="",
                    generate_filename=False  # Text files don't get generated filename
                )
                return result
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.warning(f"Text-based abstract generation failed: {e}")
            return {
                "file_name": "",
                "file_abstract": f"{context} - Contains relevant content, processed successfully"
            }
    
    def _create_fallback_abstract(self, filename: str, file_type: str) -> Dict[str, str]:
        """
        Create a fallback abstract when extraction fails
        
        Args:
            filename: Original filename
            file_type: File type
            
        Returns:
            Dict[str, str]: Dictionary with empty file_name and file_abstract
        """
        try:
            # Use template from FALLBACK_ABSTRACT_TEMPLATES
            template = FALLBACK_ABSTRACT_TEMPLATES.get(
                file_type, 
                FALLBACK_ABSTRACT_TEMPLATES["default"]
            )
            
            # Create abstract based on template
            if file_type == "pdf":
                abstract = template.format(filename=filename, page_count="unknown pages")
            elif file_type == "image":
                abstract = template.format(filename=filename, resolution="unknown resolution")
            elif file_type == "excel":
                abstract = template.format(filename=filename, sheet_count="unknown")
            elif file_type == "genetic":
                abstract = template.format(filename=filename, file_size="unknown size")
            elif file_type == "text":
                abstract = template.format(filename=filename, word_count="unknown")
            else:
                abstract = template.format(file_type=file_type.upper(), filename=filename)
                
        except Exception as e:
            logging.warning(f"Fallback template formatting failed: {e}")
            # Ultimate fallback
            abstract = f"{file_type.upper()} file: {filename} - File uploaded successfully and ready for viewing"
        
        return {
            "file_name": "",  # Fallback doesn't generate filename
            "file_abstract": self._truncate_abstract(abstract)
        }
    
    def _truncate_abstract(self, abstract: str) -> str:
        """
        Truncate abstract to maximum length
        
        Args:
            abstract: Original abstract
            
        Returns:
            str: Truncated abstract
        """
        if len(abstract) <= self.max_abstract_length:
            return abstract
        
        # Truncate and add ellipsis
        truncated = abstract[:self.max_abstract_length - 3] + "..."
        
        # Try to break at word boundary for better readability
        if " " in truncated:
            last_space = truncated.rfind(" ")
            if last_space > self.max_abstract_length * 0.8:  # If space is not too far back
                truncated = abstract[:last_space] + "..."
        
        return truncated
    
    def _format_file_size(self, size_bytes: int) -> str:
        """
        Format file size in human readable format
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            str: Formatted size string
        """
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}TB"
    
    async def extract_file_original_text(
        self,
        file_content: bytes,
        file_type: str,
        filename: str,
        content_type: str = None
    ) -> str:
        """
        Extract original text content from file
        
        Args:
            file_content: Binary file content
            file_type: File type (pdf, image, excel, etc.)
            filename: Original filename
            content_type: MIME content type
            
        Returns:
            str: Original text content extracted from file
        """
        try:
            logging.info(f"📄 [Original Text] Starting original text extraction: {filename}, file_type: {file_type}, content_type: {content_type}")

            # Dedup on the raw bytes before any extraction work — the expensive
            # paths below are Vision-LLM calls.
            content_hash = hashlib.sha256(file_content).hexdigest() if file_content else ""
            if content_hash:
                cached = await _read_original_text_cache(content_hash)
                if cached:
                    logging.info(f"🎯 [Original Text] Cache hit: {filename} (hash={content_hash[:16]}..., {len(cached)} chars)")
                    return cached

            # Determine file type from content_type or filename extension
            is_pdf = file_type == "pdf" or (content_type and content_type == "application/pdf")
            is_image = file_type == "image" or (content_type and content_type.startswith("image/"))
            is_excel = is_excel_file(filename, content_type)
            is_text = is_text_file(filename, content_type)
            is_document = is_document_file(filename, content_type)

            if is_pdf:
                text = await self._extract_pdf_original_text(file_content, filename)
            elif is_image:
                text = await self._extract_image_original_text(file_content, filename)
            elif is_excel:
                text = await self._extract_excel_original_text(file_content, filename)
            elif is_document:
                text = await self._extract_document_original_text(file_content, filename)
            elif is_text:
                text = await self._extract_text_original_text(file_content, filename)
            else:
                # For other file types, return empty string
                logging.info(f"📄 [Original Text] Skipping original text extraction for unsupported file: {filename}, type: {file_type}")
                return ""

            # No write-back step: the caller persists this text onto the
            # th_files row (with the same content_hash), which IS the cache.
            return text

        except Exception as e:
            logging.error(f"❌ [Original Text] Original text extraction failed for {filename}: {e}", stack_info=True)
            return ""
    
    def _extract_pdf_text_layer(self, file_content: bytes) -> str:
        """Tier-1 PDF extraction: the embedded text layer via pdfplumber.

        Free and instant for born-digital PDFs (most lab/portal exports). Returns
        "" for scanned / image-only PDFs (no extractable text layer), which then
        fall through to Vision-LLM OCR.
        """
        import io
        parts: list[str] = []
        try:
            with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                for i, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text() or ""
                    except Exception:
                        page_text = ""
                    if page_text.strip():
                        parts.append(f"[Page {i + 1}]\n{page_text}")
        except Exception as e:
            logging.warning(f"📄 [Original Text] pdfplumber text-layer extraction failed: {e}")
            return ""
        return "\n\n".join(parts)

    async def _extract_pdf_original_text(self, file_content: bytes, filename: str) -> str:
        """
        Extract original text from a PDF, two-tier:

        1. Embedded text layer (pdfplumber) — free/instant for born-digital PDFs.
        2. Vision-LLM OCR — fallback for scanned / image-only PDFs whose text
           layer is empty or too thin to be a real text layer.

        Args:
            file_content: PDF file binary content
            filename: Original filename

        Returns:
            str: Original text content
        """
        import time
        start_time = time.time()

        # Tier 1: embedded text layer. Only trust it when it yields a substantial
        # amount of text — a near-empty result means a scanned PDF, so fall back.
        # pdfplumber is synchronous and CPU-bound, so run it in a worker thread to
        # avoid blocking the event loop (and the upload WebSocket) during parsing.
        text_layer = await asyncio.to_thread(self._extract_pdf_text_layer, file_content)
        if len(text_layer.strip()) >= _PDF_TEXT_LAYER_MIN_CHARS:
            logging.info(
                f"✅ [Original Text] PDF text-layer hit: {filename}, "
                f"{len(text_layer)} chars, took {time.time() - start_time:.2f}s "
                f"(skipped Vision LLM)"
            )
            return text_layer.strip()
        logging.info(
            f"📄 [Original Text] PDF text layer thin ({len(text_layer.strip())} chars) "
            f"-> Vision LLM OCR: {filename}"
        )

        try:
            # Create temporary file for PDF processing
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name

            try:
                logging.info(f"📄 [Original Text] Extracting PDF text with Vision LLM: {filename}")

                prompt = FILE_ORIGINAL_TEXT_PROMPT

                # Use unified file extract with Vision LLM
                response = await unified_file_extract(
                    file_path=temp_file_path,
                    prompt=prompt,
                    content_type="application/pdf",
                    json_mode=False
                )

                elapsed_time = time.time() - start_time

                if response and response.strip():
                    logging.info(f"✅ [Original Text] PDF extraction successful: {filename}, extracted {len(response)} characters, took {elapsed_time:.2f}s")
                    return response.strip()
                else:
                    logging.warning(f"⚠️ [Original Text] LLM returned empty response for PDF: {filename}, took {elapsed_time:.2f}s")
                    return ""

            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass

        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.error(f"❌ [Original Text] PDF original text extraction failed: {filename}, error: {e}, took {elapsed_time:.2f}s", stack_info=True)
            return ""
    
    async def _extract_image_original_text(self, file_content: bytes, filename: str) -> str:
        """
        Extract original text from image file using Gemini/Doubao
        
        Args:
            file_content: Image file binary content
            filename: Original filename
            
        Returns:
            str: Original text content recognized from image
        """
        try:
            # Create temporary file for image processing
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            try:
                logging.info(f"📄 [Original Text] Starting image text extraction with LLM: {filename}")
                
                prompt = FILE_ORIGINAL_TEXT_PROMPT
                
                # Use unified file extract (auto-selects model based on environment)
                response = await unified_file_extract(
                    file_path=temp_file_path,
                    prompt=prompt,
                    content_type="image/jpeg",
                    json_mode=False
                )
                
                if response and response.strip():
                    logging.info(f"✅ [Original Text] Image extraction successful: {filename}, extracted {len(response)} characters")
                    return response.strip()
                else:
                    logging.info(f"ℹ️ [Original Text] LLM returned empty response for image (possibly no text): {filename}")
                    return ""
                    
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"❌ [Original Text] Image original text extraction failed: {filename}, error: {e}", stack_info=True)
            return ""

    async def _extract_document_original_text(
        self, file_content: bytes, filename: str
    ) -> str:
        """Word or PowerPoint to markdown, the same way Excel becomes a table.

        Both formats used to be accepted by `file_uploader.SUPPORTED_EXTENSIONS`
        with no handler in existence: the picker took the file, the upload ran,
        and `file_processor` answered "file not supported" at the end. This is
        the extraction that closes that, and it is deliberately plain — a lab
        report saved as .docx is text and a table, not a layout problem.

        Legacy binary `.doc`/`.ppt` are NOT handled: python-docx and
        python-pptx read the zip-based formats only. They stay out of the
        accepted set rather than failing late, which is the whole point.
        """
        import io
        import time

        start = time.time()
        ext = (filename or "").rsplit(".", 1)[-1].lower()
        parts: list[str] = []

        try:
            if ext == "docx":
                import docx

                doc = docx.Document(io.BytesIO(file_content))
                parts.append(f"# Word Document: {filename}")
                for para in doc.paragraphs:
                    text = (para.text or "").strip()
                    if text:
                        # Heading levels carry the report's own sectioning, and
                        # the indicator extractor downstream reads structure.
                        style = (para.style.name or "") if para.style else ""
                        if style.startswith("Heading"):
                            level = style.removeprefix("Heading ").strip()
                            hashes = "#" * (int(level) + 1 if level.isdigit() else 2)
                            parts.append(f"{hashes} {text}")
                        else:
                            parts.append(text)
                for i, table in enumerate(doc.tables, 1):
                    parts.append(f"## Table {i}")
                    for r, row in enumerate(table.rows):
                        cells = [(c.text or "").strip().replace("|", "/") for c in row.cells]
                        parts.append("| " + " | ".join(cells) + " |")
                        if r == 0:
                            parts.append("|" + "---|" * len(cells))

            elif ext == "pptx":
                from pptx import Presentation

                deck = Presentation(io.BytesIO(file_content))
                parts.append(f"# Presentation: {filename}")
                for n, slide in enumerate(deck.slides, 1):
                    parts.append(f"## Slide {n}")
                    for shape in slide.shapes:
                        if shape.has_text_frame:
                            for para in shape.text_frame.paragraphs:
                                text = "".join(r.text or "" for r in para.runs).strip()
                                if text:
                                    parts.append(text)
                        if getattr(shape, "has_table", False):
                            for r, row in enumerate(shape.table.rows):
                                cells = [(c.text or "").strip().replace("|", "/") for c in row.cells]
                                parts.append("| " + " | ".join(cells) + " |")
                                if r == 0:
                                    parts.append("|" + "---|" * len(cells))
            else:
                return ""

        except Exception as e:
            # Same contract as every other extractor here: a failure is a logged
            # empty string, not an exception that fails the upload.
            logging.warning(f"⚠️ [Original Text] {ext} extraction failed for {filename}: {e}")
            return ""

        text = "\n".join(parts).strip()
        if len(parts) <= 1:
            logging.info(f"ℹ️ [Original Text] {ext} file has no text: {filename}")
            return ""
        logging.info(
            f"📄 [Original Text] {ext}: {filename} -> {len(text)} chars "
            f"in {time.time() - start:.2f}s"
        )
        return text

    async def _extract_excel_original_text(self, file_content: bytes, filename: str) -> str:
        """
        Extract original text from Excel file
        
        Args:
            file_content: Excel file binary content
            filename: Original filename
            
        Returns:
            str: Original text content (formatted as markdown table)
        """
        import io
        import time
        
        start_time = time.time()
        
        try:
            import pandas as pd

            logging.info(f"📄 [Original Text] Extracting Excel text: {filename}")

            # Read ALL sheets (sheet_name=None -> {sheet: DataFrame}); a workbook
            # often has more than one sheet and reading only the first silently
            # drops the rest.
            try:
                sheets = pd.read_excel(io.BytesIO(file_content), engine="openpyxl", sheet_name=None)
            except Exception:
                # Try with xlrd for older .xls files
                try:
                    sheets = pd.read_excel(io.BytesIO(file_content), engine="xlrd", sheet_name=None)
                except Exception as e:
                    logging.warning(f"⚠️ [Original Text] Failed to read Excel with both engines: {e}")
                    return ""

            non_empty = {name: df for name, df in (sheets or {}).items() if not df.empty}
            if not non_empty:
                logging.info(f"ℹ️ [Original Text] Excel file is empty: {filename}")
                return ""

            # Convert each sheet to a markdown table, sharing a global row budget
            # so a huge workbook can't blow up the text.
            text_parts = [f"# Excel File: {filename}", f"Sheets: {len(non_empty)}", ""]
            rows_budget = 5000
            for sheet_name, df in non_empty.items():
                text_parts.append(f"## Sheet: {sheet_name}")
                text_parts.append(f"Rows: {len(df)}, Columns: {len(df.columns)}")
                headers = " | ".join(str(col) for col in df.columns)
                text_parts.append(f"| {headers} |")
                text_parts.append("|" + "|".join(["---"] * len(df.columns)) + "|")

                max_rows = min(len(df), max(rows_budget, 0))
                for idx in range(max_rows):
                    row_values = " | ".join(str(val) if pd.notna(val) else "" for val in df.iloc[idx])
                    text_parts.append(f"| {row_values} |")
                rows_budget -= max_rows
                if len(df) > max_rows:
                    text_parts.append(f"... and {len(df) - max_rows} more rows")
                text_parts.append("")
                if rows_budget <= 0:
                    text_parts.append("... (remaining sheets truncated)")
                    break

            result = "\n".join(text_parts)
            elapsed_time = time.time() - start_time

            logging.info(f"✅ [Original Text] Excel extraction successful: {filename}, {len(non_empty)} sheet(s), {len(result)} chars, took {elapsed_time:.2f}s")
            return result
            
        except ImportError:
            logging.warning(f"⚠️ [Original Text] pandas not available for Excel extraction: {filename}")
            return ""
        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.error(f"❌ [Original Text] Excel extraction failed: {filename}, error: {e}, took {elapsed_time:.2f}s")
            return ""

    async def _extract_text_original_text(self, file_content: bytes, filename: str) -> str:
        """
        Extract original text from text file (txt, md, csv, json, etc.)
        
        Args:
            file_content: Text file binary content
            filename: Original filename
            
        Returns:
            str: Original text content
        """
        import time
        
        start_time = time.time()
        
        try:
            logging.info(f"📄 [Original Text] Extracting text file: {filename}")
            
            # Try different encodings
            encodings = ["utf-8", "utf-8-sig", "gbk", "gb2312", "latin-1"]
            text = None
            
            for encoding in encodings:
                try:
                    text = file_content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if text is None:
                # Fallback: decode with errors='replace'
                text = file_content.decode("utf-8", errors="replace")
            
            # Limit text length to avoid huge content
            max_chars = 100000  # 100K chars max
            if len(text) > max_chars:
                text = text[:max_chars] + f"\n\n... (truncated, total {len(file_content)} bytes)"
            
            elapsed_time = time.time() - start_time
            logging.info(f"✅ [Original Text] Text extraction successful: {filename}, {len(text)} chars, took {elapsed_time:.2f}s")
            
            return text.strip()
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logging.error(f"❌ [Original Text] Text extraction failed: {filename}, error: {e}, took {elapsed_time:.2f}s")
            return ""
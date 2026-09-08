"""
File Abstract Extractor Service
Extracts file summaries for different file types, with special handling for PDF files
"""

import hashlib
import os
import io
import csv
import tempfile
import logging
import json

from PIL import Image
from mirobody.utils.llm import unified_file_extract
from mirobody.pulse.file_parser.services.prompts.file_abstract_prompt import FILE_ABSTRACT_PROMPT, FALLBACK_ABSTRACT_TEMPLATES
from mirobody.documents import extract as documents
from mirobody.documents.ocr import vision_ocr

logger = logging.getLogger(__name__)




async def lookup_extracted_text(file_content: bytes) -> str | None:
    """Text a previous extraction of these exact bytes produced, or None.

    The cheap half of extraction: one indexed lookup, never a model call. The
    The agent's virtual filesystem uses it at registration time to decide whether a file
    still needs OCR at all (see ``agent/filesystem/parser.FileParser.prepare``).
    """
    if not file_content:
        return None
    return await _read_original_text_cache(hashlib.sha256(file_content).hexdigest())


async def _read_original_text_cache(content_hash: str) -> str | None:
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
        logger.warning(f"original-text cache read failed (hash={content_hash[:16]}...): {e}")
        return None


class ThFilesTextCache:
    """`documents.extract.TextCache` over `th_files`: reads by content hash;
    writes are the persistence path's (every row carries `content_hash` and
    `original_text`), so `put` is a no-op here."""

    async def get(self, digest: str) -> str | None:
        return await _read_original_text_cache(digest)

    async def put(self, digest: str, text: str) -> None:
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
        if file_type == "image":
            return ".jpg"  # Default to jpg for generic image type
        
        return ""
    
    async def extract_file_abstract(
        self, 
        file_content: bytes, 
        file_type: str, 
        filename: str,
        content_type: str = None
    ) -> dict[str, str]:
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
            if file_type == "image" or (content_type and content_type.startswith("image/")):
                return await self._extract_image_abstract(file_content, filename)
            if (file_type == "excel" or 
                  (content_type and ("spreadsheet" in content_type or "excel" in content_type or
                   content_type in ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]))):
                return await self._extract_excel_abstract(file_content, filename)
            return await self._extract_generic_abstract(file_content, filename, file_type)
                
        except Exception as e:
            logger.error(f"File abstract extraction failed for {filename}: {e}", stack_info=True)
            # Return a basic fallback abstract
            return self._create_fallback_abstract(filename, file_type)
    
    async def _extract_pdf_abstract(self, file_content: bytes, filename: str) -> dict[str, str]:
        """Abstract and generated filename for a PDF, from its TEXT: the embedded
        layer for a born-digital document, the OCR of the scanned pages
        otherwise (`documents.extract.pdf_text`, cached by content hash). The
        model summarises text; it is never handed the whole file."""
        try:
            text = await documents.extract_text(filename, "application/pdf", file_content, ocr=vision_ocr, cache=ThFilesTextCache())
            page_count = text.count("--- page ")
            if not text.strip():
                return {
                    "file_name": "",
                    "file_abstract": self._truncate_abstract(f"PDF document: {filename} - File uploaded successfully, but text extraction failed"),
                }
            result = await self._generate_llm_abstract_with_content(
                text,
                f"PDF document: {filename} ({page_count} pages)",
                file_extension=self._infer_file_extension("application/pdf", "pdf", filename),
                generate_filename=True,
            )
            if result.get("file_abstract"):
                result["file_abstract"] = self._truncate_abstract(result["file_abstract"])
            return result
        except Exception as e:
            logger.error("PDF abstract extraction failed: %s", type(e).__name__)
            return self._create_fallback_abstract(filename, "pdf")

    async def _extract_image_abstract(self, file_content: bytes, filename: str) -> dict[str, str]:
        """Abstract and generated filename for an image. The OCR text (one
        cached vision call, shared with the original-text pass) is what the
        model summarises; only an image with no readable text goes to the
        vision model as an image, so a photo of a meal still gets a
        description."""
        try:
            with Image.open(io.BytesIO(file_content)) as img:
                width, height = img.size
                format_info = img.format or "Unknown"
            context = f"Image file: {filename} ({width}x{height}, {format_info} format)"
            file_extension = self._infer_file_extension("image/jpeg", "image", filename)
            text = await documents.extract_text(filename, f"image/{format_info.lower()}", file_content, ocr=vision_ocr, cache=ThFilesTextCache())
            if text.strip():
                result = await self._generate_llm_abstract_with_content(text, context, file_extension=file_extension, generate_filename=True)
            else:
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as temp_file:
                    temp_file.write(file_content)
                    temp_file_path = temp_file.name
                try:
                    result = await self._generate_llm_abstract_with_file(
                        temp_file_path=temp_file_path, content_type="image/jpeg", context=context,
                        file_extension=file_extension, generate_filename=True,
                    )
                finally:
                    try:
                        os.unlink(temp_file_path)
                    except Exception:
                        pass
            if result.get("file_abstract"):
                result["file_abstract"] = self._truncate_abstract(result["file_abstract"])
            return result
        except Exception as e:
            logger.error("Image abstract extraction failed: %s", type(e).__name__)
            return self._create_fallback_abstract(filename, "image")

    async def _extract_excel_abstract(self, file_content: bytes, filename: str) -> dict[str, str]:
        """Abstract for an Excel file (no filename generation for Excel): the
        first sheet's header and first ten rows go to the model as CSV; the
        fallback names the columns."""
        try:
            sheets = documents.xlsx_sheets(file_content)
            non_empty = [(name, rows) for name, rows in sheets if rows]
            if not non_empty:
                abstract = f"Excel file: {filename} - Empty document or unable to read sheets"
                return {"file_name": "", "file_abstract": self._truncate_abstract(abstract)}

            _name, rows = non_empty[0]
            header, body = rows[0], rows[1:11]
            column_names = [c for c in header[:5] if c]
            abstract = f"Excel file: {filename} ({len(sheets)} sheets) - Contains columns: {', '.join(column_names)}"

            csv_temp_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
                ) as csv_temp_file:
                    csv.writer(csv_temp_file).writerows([header, *body])
                    csv_temp_path = csv_temp_file.name
                result = await self._generate_llm_abstract_with_file(
                    temp_file_path=csv_temp_path, content_type="text/csv",
                    context=f"Excel file: {filename} ({len(sheets)} sheets, {len(body)} rows)",
                    file_extension="", generate_filename=False,
                )
                llm_abstract = result.get("file_abstract", "")
                # Use the model's abstract only when it is more than a restatement.
                if (llm_abstract and len(llm_abstract.strip()) > 20
                        and not llm_abstract.startswith("Excel file")
                        and not llm_abstract.startswith("File processed")
                        and "contains content" not in llm_abstract.lower()):
                    abstract = llm_abstract
            except Exception as llm_error:
                logger.error("LLM analysis failed for Excel: %s", type(llm_error).__name__)
            finally:
                if csv_temp_path:
                    try:
                        os.unlink(csv_temp_path)
                    except Exception:
                        pass

            return {"file_name": "", "file_abstract": self._truncate_abstract(abstract)}
        except Exception as e:
            logger.error("Excel abstract extraction failed: %s", type(e).__name__)
            return {"file_name": "", "file_abstract": f"Excel file: {filename} - Spreadsheet uploaded, analyzing content in background"}

    async def _extract_generic_abstract(self, file_content: bytes, filename: str, file_type: str) -> dict[str, str]:
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
            logger.error(f"Generic abstract extraction failed: {e}", stack_info=True)
            return self._create_fallback_abstract(filename, file_type)
    
    async def _generate_llm_abstract_with_file(
        self, 
        temp_file_path: str, 
        content_type: str, 
        context: str, 
        file_extension: str = "",
        generate_filename: bool = True
    ) -> dict[str, str]:
        """
        Generate abstract and filename using the LLM file extract service (provider auto-selected by available key)
        
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
                    
                    logger.info(f"Abstract generation successful: file_name='{file_name}', abstract_len={len(file_abstract)}")
                    
                    return {
                        "file_name": file_name,
                        "file_abstract": file_abstract
                    }
                    
                except json.JSONDecodeError as json_error:
                    logger.warning(f"LLM returned invalid JSON, treating as plain text: {json_error}")
                    # Fallback: treat the whole response as abstract
                    abstract = self._truncate_abstract(cleaned_response)
                    return {
                        "file_name": "",
                        "file_abstract": abstract
                    }
            else:
                logger.warning("LLM returned empty response, using fallback")
                return {
                    "file_name": "",
                    "file_abstract": f"{context} - Contains relevant content, processed successfully"
                }
                
        except Exception as e:
            logger.warning(f"LLM abstract generation failed: {e}")
            return {
                "file_name": "",
                "file_abstract": f"{context} - Contains relevant content, processed successfully"
            }
    
    async def _generate_llm_abstract_with_content(self, content: str, context: str) -> dict[str, str]:
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
            logger.warning(f"Text-based abstract generation failed: {e}")
            return {
                "file_name": "",
                "file_abstract": f"{context} - Contains relevant content, processed successfully"
            }
    
    def _create_fallback_abstract(self, filename: str, file_type: str) -> dict[str, str]:
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
            logger.warning(f"Fallback template formatting failed: {e}")
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
        """The document's text (`mirobody.documents.extract`): the PDF text layer
        page by page with only scanned pages OCR'd, images downscaled then
        OCR'd, spreadsheets and Word/PowerPoint as markdown, text decoded — cached
        by content hash through `th_files`, so the same bytes are never OCR'd
        twice. ``""`` for a kind nothing reads, or on failure (logged by type)."""
        try:
            hint = content_type or ({"pdf": "application/pdf", "image": "image/jpeg"}.get((file_type or "").lower()))
            text = await documents.extract_text(filename, hint, file_content, ocr=vision_ocr, cache=ThFilesTextCache())
            logger.info("[Original Text] extracted: file_type=%s char_count=%d", file_type, len(text))
            return text
        except Exception as e:
            logger.error("[Original Text] extraction failed: file_type=%s error_type=%s", file_type, type(e).__name__)
            return ""


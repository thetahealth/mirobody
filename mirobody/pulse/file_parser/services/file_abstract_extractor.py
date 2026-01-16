"""
File Abstract Extractor Service
Extracts file summaries for different file types, with special handling for PDF files
"""

import os
import tempfile
import logging
import json
from typing import Dict

import pdfplumber
from PIL import Image
from mirobody.utils.llm import unified_file_extract
from mirobody.pulse.file_parser.services.prompts.file_abstract_prompt import FILE_ABSTRACT_PROMPT, FALLBACK_ABSTRACT_TEMPLATES


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
            elif file_type == "genetic":
                return await self._extract_genetic_abstract(file_content, filename)
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
    
    async def _extract_genetic_abstract(self, file_content: bytes, filename: str) -> Dict[str, str]:
        """
        Extract abstract from genetic data files (no filename generation)
        Only processes first few lines to avoid performance issues with large files
        
        Args:
            file_content: Genetic file binary content
            filename: Original filename
            
        Returns:
            Dict[str, str]: Dictionary with empty file_name and file_abstract
        """
        try:
            # Decode only the first part of the file (first 2000 characters)
            content_str = file_content[:2000].decode('utf-8', errors='ignore')
            
            # Split into lines and take first 20 lines for analysis
            lines = content_str.split('\n')[:20]  # First 20 lines should be enough
            
            # Count total file size for display
            total_size = len(file_content)
            
            # Analyze the header to extract basic information
            file_info = []
            data_lines_count = 0
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                # Look for WeGene header information
                if line.startswith("# This data file generated by WeGene"):
                    file_info.append("WeGene genetic data")
                elif line.startswith("# Batch"):
                    batch_info = line.replace("# Batch: ", "")
                    file_info.append(f"Batch: {batch_info}")
                elif line.startswith("# Generated"):
                    date_info = line.replace("# Generated at ", "")
                    file_info.append(f"Generated: {date_info}")
                elif not line.startswith("#") and "\t" in line:
                    # This looks like actual genetic data
                    data_lines_count += 1
            
            # Create abstract based on extracted information
            if file_info:
                info_str = ", ".join(file_info[:2])  # First 2 pieces of info to keep it concise
                if data_lines_count > 0:
                    abstract = f"Genetic data file: {filename} - {info_str}, contains {data_lines_count}+ genetic variants ({self._format_file_size(total_size)})"
                else:
                    abstract = f"Genetic data file: {filename} - {info_str} ({self._format_file_size(total_size)})"
            else:
                # Fallback if no header info found
                abstract = f"Genetic data file: {filename} - Contains genetic test data ({self._format_file_size(total_size)})"
            
            # Try to use LLM for better analysis if we have some content
            if len(lines) > 5 and total_size < 50 * 1024 * 1024:
                try:
                    sample_content = '\n'.join(lines)
                    with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False, encoding='utf-8') as temp_file:
                        temp_file.write(sample_content)
                        temp_file_path = temp_file.name
                    
                    try:
                        result = await self._generate_llm_abstract_with_file(
                            temp_file_path=temp_file_path, content_type="text/plain",
                            context=f"Genetic data file: {filename} (first 20 lines sample)",
                            file_extension="", generate_filename=False
                        )
                        llm_abstract = result.get("file_abstract", "")
                        if llm_abstract and len(llm_abstract.strip()) > 30:
                            abstract = llm_abstract
                    finally:
                        try:
                            os.unlink(temp_file_path)
                        except Exception:
                            pass
                except Exception:
                    pass  # Continue with basic abstract
            
            return {
                "file_name": "",  # Genetic files don't get generated filename
                "file_abstract": self._truncate_abstract(abstract)
            }
                    
        except Exception as e:
            logging.error(f"Genetic file abstract extraction failed: {e}", stack_info=True)
            return self._create_fallback_abstract(filename, "genetic")
    
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
            extension_hint = f"(File extension should be: {file_extension})" if file_extension else ""
            prompt = f"""{FILE_ABSTRACT_PROMPT}

File context: {context}
{extension_hint}

Please return strictly in JSON format, do not include any markdown code block markers or other formatting."""
            
            # Use unified file extract (auto-selects model based on environment)
            response = await unified_file_extract(
                file_path=temp_file_path,
                prompt=prompt,
                content_type=content_type
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
            if file_type == "pdf" or (content_type and content_type == "application/pdf"):
                return await self._extract_pdf_original_text(file_content, filename)
            elif file_type == "image" or (content_type and content_type.startswith("image/")):
                return await self._extract_image_original_text(file_content, filename)
            else:
                # For other file types, return empty string
                logging.info(f"📄 [Original Text] Skipping original text extraction for non-PDF/image file: {filename}, type: {file_type}")
                return ""
                
        except Exception as e:
            logging.error(f"❌ [Original Text] Original text extraction failed for {filename}: {e}", stack_info=True)
            return ""
    
    async def _extract_pdf_original_text(self, file_content: bytes, filename: str) -> str:
        """
        Extract original text from PDF file
        First try direct text extraction with pdfplumber, fallback to Gemini/Doubao if failed
        
        Args:
            file_content: PDF file binary content
            filename: Original filename
            
        Returns:
            str: Original text content
        """
        try:
            # Create temporary file for PDF processing
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name
            
            try:
                # Try direct text extraction with pdfplumber first
                extracted_text = ""
                
                try:
                    logging.info(f"📄 [Original Text] Attempting direct PDF text extraction: {filename}")
                    
                    pdf_document = pdfplumber.open(temp_file_path)
                    total_pages = len(pdf_document.pages)
                    
                    # Extract text from all pages
                    for page_num in range(total_pages):
                        try:
                            page = pdf_document.pages[page_num]
                            page_text = page.extract_text()
                            if page_text.strip():
                                extracted_text += f"\n[Page {page_num + 1}]\n{page_text}"
                        except Exception as e:
                            logging.warning(f"⚠️ [Original Text] Failed to extract text from page {page_num + 1}: {e}")
                            continue
                    
                    pdf_document.close()
                    
                    # Check if we got meaningful text
                    if extracted_text.strip() and len(extracted_text.strip()) > 50:
                        logging.info(f"✅ [Original Text] Direct PDF extraction successful: {filename}, extracted {len(extracted_text)} characters")
                        return extracted_text.strip()
                    else:
                        logging.info(f"⚠️ [Original Text] Direct PDF extraction returned insufficient text: {filename}, length: {len(extracted_text)}")
                except Exception as direct_error:
                    logging.warning(f"⚠️ [Original Text] Direct PDF extraction failed: {filename}, error: {str(direct_error)}")

                # Fallback to Gemini/Doubao
                logging.info(f"🔄 [Original Text] Falling back to LLM extraction for PDF: {filename}")
                
                prompt = """Please extract and return ALL the original text content from this PDF file.
Return the complete text exactly as it appears in the document, preserving formatting where possible.
Do not summarize or modify the content - return the full original text."""
                
                # Use unified file extract (auto-selects model based on environment)
                response = await unified_file_extract(
                    file_path=temp_file_path,
                    prompt=prompt,
                    content_type="application/pdf"
                )
                
                if response and response.strip():
                    logging.info(f"✅ [Original Text] PDF extraction successful: {filename}, extracted {len(response)} characters")
                    return response.strip()
                else:
                    logging.warning(f"⚠️ [Original Text] LLM returned empty response for PDF: {filename}")
                    return ""
                    
            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass
                    
        except Exception as e:
            logging.error(f"❌ [Original Text] PDF original text extraction failed: {filename}, error: {e}", stack_info=True)
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
                
                prompt = """Please extract and return ALL text content visible in this image.
Include all text you can see, including:
- Main text content
- Labels, captions, titles
- Numbers, dates, measurements
- Any other readable text

Return the complete text exactly as it appears, preserving the order and structure where possible.
If there is no text in the image, return an empty response."""
                
                # Use unified file extract (auto-selects model based on environment)
                response = await unified_file_extract(
                    file_path=temp_file_path,
                    prompt=prompt,
                    content_type="image/jpeg"
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
"""
PDF paging processing tool

Responsible for splitting PDF files into single pages, supports parallel processing of large PDF files
Implemented using pypdf and pdfplumber for better compatibility (no C dependencies)
"""

import os
import tempfile
import logging
from typing import List, Tuple

import pdfplumber
from pypdf import PdfReader, PdfWriter


class PDFSplitter:
    """PDF paging processing tool class - implemented using pypdf and pdfplumber"""

    @staticmethod
    def split_pdf_to_pages(pdf_path: str) -> List[str]:
        """
        Split PDF file into single-page files (using pypdf)

        Args:
            pdf_path: PDF file path

        Returns:
            List[str]: List of single-page PDF file paths
        """
        page_files = []

        try:
            # Open PDF document
            reader = PdfReader(pdf_path)
            total_pages = len(reader.pages)

            logging.info(f"📄 Using pypdf to split PDF: {pdf_path}, total pages: {total_pages}")

            # Create temporary directory to store page files
            temp_dir = tempfile.mkdtemp(prefix="pdf_pages_pypdf_")

            for page_num in range(total_pages):
                try:
                    # Create new PDF writer
                    writer = PdfWriter()

                    # Add specified page
                    writer.add_page(reader.pages[page_num])

                    # Generate single-page file path
                    page_filename = f"page_{page_num + 1}.pdf"
                    page_file_path = os.path.join(temp_dir, page_filename)

                    # Save single-page PDF file
                    with open(page_file_path, "wb") as output_file:
                        writer.write(output_file)

                    page_files.append(page_file_path)
                    logging.info(f"✅ Successfully split page {page_num + 1}: {page_filename}")

                except Exception as e:
                    logging.error(f"❌ Failed to split page {page_num + 1}: {str(e)}", stack_info=True)
                    continue

            logging.info(f"📄 pypdf splitting complete: total pages {total_pages}, successfully split {len(page_files)} pages")

        except Exception as e:
            logging.error(f"❌ pypdf splitting failed: {str(e)}", stack_info=True)
            # Clean up generated files
            PDFSplitter.cleanup_page_files(page_files)
            return []

        return page_files

    @staticmethod
    def get_pdf_page_count(pdf_path: str) -> int:
        """
        Get page count of PDF file (using pypdf)

        Args:
            pdf_path: PDF file path

        Returns:
            int: Page count, returns 0 on failure
        """
        try:
            reader = PdfReader(pdf_path)
            return len(reader.pages)
        except Exception as e:
            logging.error(f"❌ Failed to get PDF page count: {str(e)}", stack_info=True)
            return 0

    @staticmethod
    def extract_page_text_preview(pdf_path: str, max_pages: int = 3) -> List[str]:
        """
        Extract text content from first few PDF pages for quick preview (using pdfplumber)

        Args:
            pdf_path: PDF file path
            max_pages: Maximum preview pages

        Returns:
            List[str]: Text content for each page
        """
        page_texts = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                preview_pages = min(max_pages, total_pages)

                for page_num in range(preview_pages):
                    try:
                        page = pdf.pages[page_num]
                        # Use pdfplumber to extract text with high quality
                        text = page.extract_text()
                        if text:
                            page_texts.append(text.strip())
                        else:
                            page_texts.append("")
                    except Exception as e:
                        logging.warning(f"⚠️ Failed to extract text from page {page_num + 1}: {str(e)}")
                        page_texts.append("")

        except Exception as e:
            logging.error(f"❌ PDF text preview failed: {str(e)}", stack_info=True)

        return page_texts

    @staticmethod
    def extract_page_text_advanced(pdf_path: str, page_num: int = 0) -> str:
        """
        Advanced text extraction (using pdfplumber advanced features)

        Args:
            pdf_path: PDF file path
            page_num: Page number (starting from 0)

        Returns:
            str: Extracted text content
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_num >= len(pdf.pages):
                    return ""

                page = pdf.pages[page_num]

                # Use pdfplumber advanced text extraction with layout preservation
                # Extract words and reconstruct text with better formatting
                words = page.extract_words(
                    keep_blank_chars=True,
                    x_tolerance=3,
                    y_tolerance=3,
                )

                if not words:
                    # Fallback to simple extraction
                    text = page.extract_text()
                    return text.strip() if text else ""

                # Group words by approximate y-position (same line)
                lines = {}
                for word in words:
                    # Round y position to group words on same line
                    y_key = round(word["top"] / 5) * 5
                    if y_key not in lines:
                        lines[y_key] = []
                    lines[y_key].append(word)

                # Sort lines by y position and words by x position
                extracted_text = []
                for y_key in sorted(lines.keys()):
                    line_words = sorted(lines[y_key], key=lambda w: w["x0"])
                    line_text = " ".join(w["text"] for w in line_words)
                    if line_text.strip():
                        extracted_text.append(line_text.strip())

                return "\n".join(extracted_text)

        except Exception as e:
            logging.error(f"❌ Advanced text extraction failed: {str(e)}", stack_info=True)
            return ""

    @staticmethod
    def analyze_pdf_structure(pdf_path: str) -> dict:
        """
        Analyze PDF structure and get more information for optimized processing

        Args:
            pdf_path: PDF file path

        Returns:
            dict: PDF structure information
        """
        try:
            # Use pypdf for metadata
            reader = PdfReader(pdf_path)

            structure_info = {
                "total_pages": len(reader.pages),
                "has_images": False,
                "has_tables": False,
                "text_density": [],
                "page_sizes": [],
                "metadata": dict(reader.metadata) if reader.metadata else {},
            }

            # Use pdfplumber for detailed page analysis
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Get page dimensions
                    width = page.width
                    height = page.height
                    structure_info["page_sizes"].append({"width": width, "height": height})

                    # Check if page has images
                    if page.images:
                        structure_info["has_images"] = True

                    # Check for tables
                    tables = page.find_tables()
                    if tables:
                        structure_info["has_tables"] = True

                    # Calculate text density
                    text = page.extract_text()
                    text_density = len(text.strip()) / (width * height) if text else 0
                    structure_info["text_density"].append(text_density)

            logging.info(f"📊 PDF structure analysis complete: {structure_info['total_pages']} pages, "
                f"contains images: {structure_info['has_images']}, contains tables: {structure_info['has_tables']}"
            )

            return structure_info

        except Exception as e:
            logging.error(f"❌ PDF structure analysis failed: {str(e)}", stack_info=True)
            return {}

    @staticmethod
    def cleanup_page_files(page_files: List[str]) -> None:
        """
        Clean up page files and temporary directory

        Args:
            page_files: List of page file paths
        """
        if not page_files:
            return

        try:
            # Get temporary directory path
            temp_dir = os.path.dirname(page_files[0]) if page_files else None

            # Delete all page files
            for page_file in page_files:
                try:
                    if os.path.exists(page_file):
                        os.unlink(page_file)
                except Exception as e:
                    logging.warning(f"⚠️ Failed to clean up page file: {page_file}, error: {str(e)}")


            # Delete temporary directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    os.rmdir(temp_dir)
                    logging.info(f"🗑️ Successfully cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    logging.warning(f"⚠️ Failed to clean up temporary directory: {temp_dir}, error: {str(e)}")

        except Exception as e:
            logging.error(f"❌ Error occurred while cleaning up page files: {str(e)}", stack_info=True)

    @staticmethod
    def filter_pages_with_indicators(page_files: List[str], max_preview_pages: int = 10) -> Tuple[List[str], List[str]]:
        """
        Filter pages that may contain indicators (optimized with pdfplumber)

        Note: This method is no longer used, changed to process all pages to avoid missing indicators

        Args:
            page_files: All page file paths
            max_preview_pages: Maximum preview pages

        Returns:
            Tuple[List[str], List[str]]: (Pages containing indicators, Pages not containing indicators)
        """
        indicator_pages = []
        non_indicator_pages = []

        # If fewer pages, process all pages directly
        if len(page_files) <= max_preview_pages:
            return page_files, []

        try:
            for page_file in page_files:
                try:
                    # Use pdfplumber to extract page text
                    page_texts = PDFSplitter.extract_page_text_preview(page_file, max_pages=1)
                    page_text = page_texts[0] if page_texts else ""

                    # If pdfplumber text extraction quality is poor, try advanced extraction
                    if len(page_text.strip()) < 50:
                        page_text = PDFSplitter.extract_page_text_advanced(page_file, 0)

                    # Determine if contains indicators
                    if PDFSplitter.is_page_likely_contains_indicators(page_text):
                        indicator_pages.append(page_file)
                    else:
                        non_indicator_pages.append(page_file)

                except Exception as e:
                    logging.warning(f"⚠️ Failed to filter page: {page_file}, error: {str(e)}")
                    # If unable to determine, default to containing indicators
                    indicator_pages.append(page_file)

            logging.info(f"📊 pdfplumber page filtering complete: {len(indicator_pages)} pages with indicators, "
                f"{len(non_indicator_pages)} pages without indicators"
            )

        except Exception as e:
            logging.error(f"❌ Page filtering failed: {str(e)}", stack_info=True)
            # If filtering fails, return all pages
            return page_files, []

        return indicator_pages, non_indicator_pages

    @staticmethod
    def get_pdf_info(pdf_path: str) -> dict:
        """
        Get basic information of PDF file

        Args:
            pdf_path: PDF file path

        Returns:
            dict: PDF information
        """
        try:
            reader = PdfReader(pdf_path)
            metadata = reader.metadata or {}

            info = {
                "page_count": len(reader.pages),
                "metadata": dict(metadata) if metadata else {},
                "is_encrypted": reader.is_encrypted,
                "is_pdf": True,  # If we can read it with pypdf, it's a valid PDF
                "file_size": os.path.getsize(pdf_path),
                "creation_date": str(metadata.get("/CreationDate", "")) if metadata else "",
                "modification_date": str(metadata.get("/ModDate", "")) if metadata else "",
                "title": str(metadata.get("/Title", "")) if metadata else "",
                "author": str(metadata.get("/Author", "")) if metadata else "",
                "subject": str(metadata.get("/Subject", "")) if metadata else "",
            }

            return info

        except Exception as e:
            logging.error(f"❌ Failed to get PDF information: {str(e)}", stack_info=True)
            return {}

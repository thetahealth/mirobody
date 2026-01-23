"""
Asynchronous file processing service
Supports async processing of file upload to S3 and content extraction, with progress updates via WebSocket
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.file_db_service import FileDbService
from mirobody.pulse.file_parser.file_processor import FileProcessor
from mirobody.pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
from mirobody.utils.distributed_websocket import get_distributed_ws_manager as get_file_progress_manager

# Global file processor instance with lazy initialization
_file_processor = None


def get_file_processor():
    """Get file processor instance with lazy initialization"""
    global _file_processor
    if _file_processor is None:
        _file_processor = FileProcessor()
    return _file_processor


class AsyncFileProcessor:
    """Asynchronous file processor"""

    @staticmethod
    async def send_progress_with_retry(
        user_id: int,
        message_id: str,
        status: str,
        progress: int = None,
        message: str = None,
        file_type: str = "file",
        filename: str = None,
        success: bool = False,
        raw: str = "",
        url_thumb: str = "",
        url_full: str = "",
        max_retries: int = 2,  # Reduce retry attempts
        retry_delay: float = 0.2,  # Reduce retry delay
    ):
        """
        Send WebSocket progress updates with retry mechanism
        """
        for attempt in range(max_retries):
            try:
                file_progress_manager = get_file_progress_manager()
                await file_progress_manager.send_progress_update(
                    user_id=user_id,
                    message_id=message_id,
                    status=status,
                    progress=progress,
                    message=message,
                    file_type=file_type,
                    filename=filename,
                    success=success,
                    raw=raw,
                    url_thumb=url_thumb,
                    url_full=url_full,
                )
                return True
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)

        return False

    @staticmethod
    async def generate_file_abstracts_async(
        files_data: List[Dict[str, Any]],
        message_id: str,
        language: str = "en"
    ):
        """
        Generate file abstracts asynchronously and update message content
        This runs in background after main file processing is completed

        Args:
            files_data: List of file data with content, filename, content_type
            message_id: Message ID to update
            language: Language for logging
        """
        try:
            await asyncio.sleep(0.5)  # Ensure main process completes
            
            try:
                abstract_extractor = FileAbstractExtractor()
            except Exception:
                return

            file_abstracts = []

            # Generate abstracts for each file
            for i, file_data in enumerate(files_data):
                try:
                    content_type = file_data["content_type"]
                    file_type = "file"
                    
                    if content_type:
                        if content_type.startswith("image/"):
                            file_type = "image"
                        elif content_type == "application/pdf":
                            file_type = "pdf"
                        elif content_type.startswith("audio/"):
                            file_type = "audio"
                        elif content_type.startswith("text/"):
                            file_type = "text"
                        elif "spreadsheet" in content_type or "excel" in content_type:
                            file_type = "excel"
                    
                    result_data = await abstract_extractor.extract_file_abstract(
                        file_content=file_data["content"], file_type=file_type,
                        filename=file_data["filename"], content_type=content_type
                    )
                    file_abstracts.append(result_data)
                    
                except Exception:
                    file_abstracts.append({"file_name": "", "file_abstract": f"{file_data['filename']} - File uploaded successfully"})

            # Update th_files with abstracts (by file_key)
            for i, file_data in enumerate(files_data):
                try:
                    file_key = file_data.get("s3_key") or file_data.get("file_key")
                    if not file_key:
                        logging.warning(f"No file_key found for file index {i}, skipping abstract update")
                        continue
                    
                    if i < len(file_abstracts):
                        abstract_result = file_abstracts[i]
                        file_abstract = abstract_result.get("file_abstract", "") if isinstance(abstract_result, dict) else ""
                        generated_file_name = abstract_result.get("file_name", "") if isinstance(abstract_result, dict) else ""
                        
                        # Determine content_type for file_name logic
                        content_type = file_data.get("content_type", "")
                        is_pdf_or_image = (
                            content_type == "application/pdf" or 
                            content_type.startswith("image/")
                        )
                        
                        # Only use generated file_name for PDF/image files
                        file_name_to_save = generated_file_name if (generated_file_name and is_pdf_or_image) else None
                        
                        await FileDbService.update_file_abstract(
                            file_key=file_key,
                            file_abstract=file_abstract,
                            file_name=file_name_to_save,
                        )
                        
                except Exception as update_error:
                    logging.warning(f"Failed to update abstract for file {file_data.get('filename', 'unknown')}: {update_error}")

        except Exception as e:
            logging.error(f"Abstract generation failed: {e}", stack_info=True)
    
    @staticmethod
    async def extract_file_original_texts_async(
        files_data: List[Dict[str, Any]],
        message_id: str,
        language: str = "en"
    ):
        """
        Extract original text from files asynchronously and update message comment
        This runs in background after file abstract generation is completed
        
        Args:
            files_data: List of file data with content, filename, content_type
            message_id: Message ID to update
            language: Language for logging
        """
        try:
            await asyncio.sleep(1.0)  # Ensure abstract generation completes
            
            try:
                from mirobody.pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
                text_extractor = FileAbstractExtractor()
                logging.info(f"✅ [Original Text Extraction] FileAbstractExtractor initialized successfully: message_id={message_id}")
            except Exception as init_error:
                logging.error(f"❌ [Original Text Extraction] Failed to initialize FileAbstractExtractor: message_id={message_id}, error={str(init_error)}", stack_info=True)
                return
            
            file_original_texts = []
            
            # Extract original text for each file
            for file_data in files_data:
                try:
                    content_type = file_data["content_type"]
                    file_type = "file"
                    
                    if content_type:
                        if content_type.startswith("image/"):
                            file_type = "image"
                        elif content_type == "application/pdf":
                            file_type = "pdf"
                        elif content_type.startswith("audio/"):
                            file_type = "audio"
                        elif content_type.startswith("text/"):
                            file_type = "text"
                        elif "spreadsheet" in content_type or "excel" in content_type:
                            file_type = "excel"
                    
                    original_text = await text_extractor.extract_file_original_text(
                        file_content=file_data["content"], file_type=file_type,
                        filename=file_data["filename"], content_type=content_type
                    )
                    
                    file_original_texts.append({
                        "filename": file_data["filename"],
                        "file_key": file_data.get("s3_key", ""),
                        "content_type": content_type,
                        "file_type": file_type,
                        "original_text": original_text,
                        "text_length": len(original_text) if original_text else 0,
                        "extracted_at": datetime.now().isoformat()
                    })
                    
                except Exception as extract_error:
                    file_original_texts.append({
                        "filename": file_data["filename"],
                        "file_key": file_data.get("s3_key", ""),
                        "content_type": file_data.get("content_type", ""),
                        "file_type": "unknown",
                        "original_text": "",
                        "text_length": 0,
                        "extracted_at": datetime.now().isoformat(),
                        "error": str(extract_error)
                    })
            
            # Update message comment field
            try:
                comment_json = json.dumps(file_original_texts, ensure_ascii=False)
                await FileParserDatabaseService.update_message_content(message_id=message_id, comment=comment_json)
            except Exception:
                pass
        
        except Exception as e:
            logging.error(f"Original text extraction failed: {e}", stack_info=True)

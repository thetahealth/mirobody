"""
File processing module for chat

All parameters are explicitly passed - no implicit context dependencies (get_req_ctx).
This ensures thread safety and testability.
"""

import asyncio
import logging

from datetime import datetime
from typing import List, Dict, Any

from ..pulse.file_parser.services.file_processing_service import process_files_async
from ..pulse.file_parser.services.async_file_processor import AsyncFileProcessor
from ..pulse.file_parser.services.database_services import FileParserDatabaseService
from ..utils.config.storage import get_storage_client

#-----------------------------------------------------------------------------

async def schedule_file_processing_tasks(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    language: str = "en"
):
    """
    Schedule file processing tasks including:
    1. File content processing (extract indicators, etc.)
    2. File abstract generation
    3. Original text extraction
    
    Args:
        files_data: List of file data dictionaries with content, filename, content_type
        user_id: User ID
        msg_id: Message ID
        language: Language code for extraction (default: "en")
    """
    if not files_data:
        return
    
    # Use asyncio.create_task for all background tasks
    asyncio.create_task(
        process_files_async(
            files_data=files_data,
            user_id=user_id,
            msg_id=msg_id,
        )
    )
    
    asyncio.create_task(
        AsyncFileProcessor.generate_file_abstracts_async(
            files_data=files_data,
            message_id=msg_id,
            language=language
        )
    )
    
    logging.info(f"Scheduled file processing tasks via asyncio.create_task: msg_id={msg_id}, files_count={len(files_data)}")

#-----------------------------------------------------------------------------

async def process_files_from_storage(
    file_list: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    session_id: str = None,
    query_user_id: str = None,
    language: str = "en"
):
    """
    Process files from storage (S3/MinIO/OSS) by downloading content and scheduling processing tasks
    
    All parameters are explicitly passed - no implicit context dependencies.
    
    Args:
        file_list: List of file dicts with file_key, file_name, file_type, file_url, file_size
        user_id: User ID
        msg_id: Message ID
        session_id: Session ID (required, should be passed explicitly from caller)
        query_user_id: Query user ID (optional, defaults to user_id)
        language: Language code for extraction (default: "en", should be passed explicitly)
    """
    try:
        if not file_list:
            logging.warning(f"Empty file_list provided for msg_id: {msg_id}")
            return
        
        # Use explicit defaults (no get_req_ctx)
        session_id = session_id or ""
        query_user_id = query_user_id or user_id
        
        files_data = []
        files_info = []
        overall_type = None
        
        for file_dict in file_list:
            try:
                file_key = file_dict.get("file_key", "")
                file_name = file_dict.get("file_name", "")
                file_type = file_dict.get("file_type", "")
                file_url = file_dict.get("file_url", "")
                file_size = file_dict.get("file_size", 0)
                
                if not file_key or not file_name:
                    logging.warning(f"Missing file_key or file_name in file dict: {file_dict}")
                    continue
                
                # Download file content from storage
                storage = get_storage_client()
                file_content, _ = await storage.get(file_key)
                
                if not file_content:
                    logging.warning(f"Failed to download file content for key: {file_key}")
                    continue
                
                # Determine file type category
                if file_type.startswith("image/"):
                    file_type_category = "image"
                elif file_type.startswith("audio/"):
                    file_type_category = "audio"
                elif file_type == "application/pdf":
                    file_type_category = "pdf"
                else:
                    file_type_category = "file"
                
                # Set overall_type (first file type or "file" if mixed)
                if overall_type is None:
                    overall_type = file_type_category
                elif overall_type != file_type_category:
                    overall_type = "file"
                
                # Append processing data
                files_data.append({
                    "content": file_content,
                    "filename": file_name,
                    "content_type": file_type,
                    "s3_key": file_key
                })
                
                # Build files_info for database
                files_info.append({
                    "filename": file_name,
                    "type": file_type_category,
                    "url_thumb": file_url,
                    "url_full": file_url,
                    "raw": "",
                    "file_size": file_size,
                    "file_key": file_key,
                })
                
            except Exception as file_error:
                logging.error(f"Failed to process file {file_dict.get('file_name', 'unknown')}: {str(file_error)}", exc_info=True)
                continue
        
        if not files_data:
            logging.warning(f"No valid files to process for msg_id: {msg_id}")
            return
        
        # Prepare message content for database
        upload_time = datetime.now()
        message_content = {
            "success": True,
            "message": f"Processing completed: {len(files_data)} files successful",
            "type": overall_type or "file",
            "url_thumb": [f["url_thumb"] for f in files_info],
            "url_full": [f["url_full"] for f in files_info],
            "message_id": msg_id,
            "files": files_info,
            "original_filenames": [f["filename"] for f in files_info],
            "file_sizes": [f["file_size"] for f in files_info],
            "upload_time": upload_time.isoformat(),
            "total_files": len(files_data),
            "successful_files": len(files_data),
            "failed_files": 0,
            "query_user_id": user_id,
            "status": "uploaded",
            "progress": 0,
            "timestamp": upload_time.isoformat(),
            "session_id": session_id
        }
        
        # Save to database
        db_result = await FileParserDatabaseService.save_file_upload_message(
            msg_id=msg_id,
            user_id=user_id,
            session_id=session_id,
            content=message_content,
            message_type=overall_type or "file",
            query_user_id=query_user_id
        )
        
        if db_result:
            logging.info(f"Files saved to database with msg_id: {msg_id}, total: {len(files_data)} files")
        
        # Schedule file processing tasks
        await schedule_file_processing_tasks(
            files_data=files_data,
            user_id=user_id,
            msg_id=msg_id,
            language=language
        )
        
        logging.info(f"Successfully scheduled processing for {len(files_data)} files with msg_id: {msg_id}")
            
    except Exception as e:
        logging.error(f"Error in process_files_from_storage: {str(e)}", exc_info=True)

#-----------------------------------------------------------------------------

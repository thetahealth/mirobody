"""
File processing module for chat(th_messages)

All parameters are explicitly passed - no implicit context dependencies (get_req_ctx).
This ensures thread safety and testability.
"""

import asyncio
import logging

from datetime import datetime
from typing import List, Dict, Any

from ..pulse.file_parser.services.file_processing_service import process_files_async
from ..pulse.file_parser.services.async_file_processor import AsyncFileProcessor
from ..pulse.file_parser.services.file_db_service import FileDbService
from ..pulse.file_parser.services.db_utils import get_mime_type
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
) -> List[Dict[str, Any]]:
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
    
    Returns:
        List of file data dicts with 'content' (bytes), 'filename', 'content_type', 's3_key'
        Returns empty list if no files or on error
    """
    try:
        if not file_list:
            logging.warning(f"Empty file_list provided for msg_id: {msg_id}")
            return []
        
        # Use explicit defaults (no get_req_ctx)
        session_id = session_id or ""
        query_user_id = query_user_id or user_id
        
        files_data = []
        files_info = []
        
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
                
                # Ensure standard MIME type - fallback to filename-based detection
                if not file_type or "/" not in file_type:
                    file_type = get_mime_type(file_name)
                
                # Download file content from storage (with timing)
                storage = get_storage_client()
                file_content, _ = await storage.get(file_key)
                
                if not file_content:
                    logging.warning(f"Failed to download file content for key: {file_key}")
                    continue
                
                logging.info(f"📥 Downloaded {file_name} from S3 for ({len(file_content)} bytes)")
                
                # Append processing data
                files_data.append({
                    "content": file_content,
                    "filename": file_name,
                    "content_type": file_type,
                    "s3_key": file_key
                })
                
                # Build files_info for database
                files_info.append({
                    "file_key": file_key,
                    "filename": file_name,
                    "file_type": file_type,
                    "file_size": file_size,
                    "url_thumb": file_url,
                    "url_full": file_url,
                    "session_id": session_id,
                    "upload_time": datetime.now().isoformat(),
                })
                
            except Exception as file_error:
                logging.error(f"Failed to process file {file_dict.get('file_name', 'unknown')}: {str(file_error)}", exc_info=True)
                continue
        
        if not files_data:
            logging.warning(f"No valid files to process for msg_id: {msg_id}")
            return []
        
        # Save files to th_files table
        inserted_ids = await FileDbService.insert_files_batch(
            user_id=user_id,
            files_info=files_info,
            scene="report",
            created_source="web_chat",
            created_source_id=msg_id,
            query_user_id=query_user_id,
        )
        
        if inserted_ids:
            logging.info(f"Files saved to th_files with msg_id: {msg_id}, inserted: {len(inserted_ids)}/{len(files_info)} files")
        
        # Schedule file processing tasks
        await schedule_file_processing_tasks(
            files_data=files_data,
            user_id=query_user_id,
            msg_id=msg_id,
            language=language
        )
        
        logging.info(f"Successfully scheduled processing for {len(files_data)} files with msg_id: {msg_id}")
        
        # Return files_data (with content) for Agent to use - avoids re-downloading
        return files_data
            
    except Exception as e:
        logging.error(f"Error in process_files_from_storage: {str(e)}", exc_info=True)
        return []

#-----------------------------------------------------------------------------

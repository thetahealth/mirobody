"""
File processing module for chat(th_messages)

All parameters are explicitly passed - no implicit context dependencies (get_req_ctx).
This ensures thread safety and testability.
"""

import asyncio
import base64
import logging

from datetime import datetime
from typing import List, Dict, Any, Optional

from ..pulse.file_parser.services.file_processing_service import process_files_async
from ..pulse.file_parser.services.file_db_service import FileDbService
from ..pulse.file_parser.services.db_utils import get_mime_type
from ..utils.config.storage import get_storage_client
from ..pub.agents.utils.cache_config import CACHE_TTL_REDIS

#-----------------------------------------------------------------------------

async def _download_single_file(
    file_dict: Dict[str, Any],
    storage: Any,
    session_id: str,
    redis_client: Optional[Any] = None
) -> Optional[Dict[str, Any]]:
    """
    Download single file from storage and return file content.

    File Retrieval Strategy:
        1. Check Redis for cached base64 content
        2. If cache hit, decode and return content directly
        3. Otherwise, download from S3/OSS, cache base64 to Redis, return content

    Args:
        file_dict: File info dictionary
        storage: Storage client
        session_id: Session ID
        redis_client: Optional Redis client for content caching

    Returns:
        Unified file dict with content + metadata fields, or None if failed
    """
    try:
        file_key = file_dict.get("file_key", "")
        file_name = file_dict.get("file_name", "")
        file_type = file_dict.get("file_type", "")
        file_url = file_dict.get("file_url", "")
        file_size = file_dict.get("file_size", 0)

        if not file_key or not file_name:
            logging.warning(f"Missing file_key or file_name in file dict: {file_dict}")
            return None

        if not file_type or "/" not in file_type:
            file_type = get_mime_type(file_name)

        file_content = None
        content_b64 = None 
        cache_key = f"file_cache:{file_key}"

        # Try Redis cache first
        if redis_client:
            try:
                cached_b64 = await redis_client.get(cache_key)
                if cached_b64:
                    if isinstance(cached_b64, bytes):
                        cached_b64 = cached_b64.decode('utf-8')
                    file_content = base64.b64decode(cached_b64)
                    content_b64 = cached_b64  # Reuse cached base64
                    logging.info(f"🎯 Cache HIT: {file_name} ({len(file_content)} bytes)")
            except Exception as e:
                logging.warning(f"Redis cache read failed for {file_name}: {e}")

        # Download from S3/OSS if not cached
        if not file_content:
            file_content, _ = await storage.get(file_key)
            if not file_content:
                logging.warning(f"Failed to download file content for key: {file_key}")
                return None

            logging.info(f"📥 Downloaded {file_name} from S3 ({len(file_content)} bytes)")

            # Encode to base64 once (used for both Redis cache and content_b64)
            content_b64 = base64.b64encode(file_content).decode('utf-8')

            # Cache to Redis
            if redis_client:
                try:
                    await redis_client.set(cache_key, content_b64, ex=CACHE_TTL_REDIS)
                    logging.info(f"💾 Cached to Redis: {file_name}")
                except Exception as e:
                    logging.warning(f"Redis cache write failed for {file_name}: {e}")

        # Unified structure: file_info with content fields added
        # Used for both DB storage (insert_files_batch) and Agent processing
        return {
            # Identity fields
            "file_key": file_key,
            "file_name": file_name,
            "original_filename": file_name,  # unchanged for track

            # Content fields (for Agent workspace)
            "content_bytes": file_content,   # bytes - for background processing
            "content_b64": content_b64,      # str - pre-encoded, avoids re-encoding
            "content_type": file_type,

            # Metadata fields (for DB storage)
            "file_type": file_type,
            "file_size": file_size,
            "url_thumb": file_url,
            "url_full": file_url,
            "session_id": session_id,
            "upload_time": datetime.now().isoformat(),
        }

    except Exception as file_error:
        logging.error(
            f"Failed to process file {file_dict.get('file_name', 'unknown')}: {str(file_error)}",
            exc_info=True
        )
        return None

#-----------------------------------------------------------------------------

async def schedule_file_processing_tasks(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
    language: str = "en"
):
    """
    Schedule file processing tasks including:
    1. Original text extraction (immediate)
    2. Indicator extraction (async)
    3. Abstract generation (async)
    
    Args:
        files_data: List of file data dictionaries with content, filename, content_type
        user_id: User ID
        msg_id: Message ID
        language: Language code for extraction (default: "en")
    """
    if not files_data:
        return
    
    # Use asyncio.create_task for background processing
    # Note: Abstract extraction is now handled inside handler's async task
    asyncio.create_task(
        process_files_async(
            files_data=files_data,
            user_id=user_id,
            msg_id=msg_id,
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
    language: str = "en",
    redis_client: Optional[Any] = None
) -> List[Dict[str, Any]]:
    """
    Process files from storage with concurrent downloads for better performance.
    
    All parameters are explicitly passed - no implicit context dependencies.
    Supports Redis caching for improved performance.
    
    Args:
        file_list: List of file dicts with file_key, file_name, file_type, file_url, file_size
        user_id: User ID
        msg_id: Message ID
        session_id: Session ID (required, should be passed explicitly from caller)
        query_user_id: Query user ID (optional, defaults to user_id)
        language: Language code for extraction (default: "en", should be passed explicitly)
        redis_client: Optional Redis client for caching (default None, backward compatible)
    
    Returns:
        List of unified file_info dicts containing:
        - content (bytes), content_b64 (str) - for Agent processing
        - file_key, file_name, file_type, file_size, etc. - for DB storage
        Returns empty list if no files or on error
    """
    try:
        if not file_list:
            logging.warning(f"Empty file_list provided for msg_id: {msg_id}")
            return []
        
        # Use explicit defaults (no get_req_ctx)
        session_id = session_id or ""
        query_user_id = query_user_id or user_id
        
        # Get storage client (reuse for all downloads)
        storage = get_storage_client()
        
        # 🚀 Concurrent download: Create tasks for all files
        download_tasks = [
            _download_single_file(file_dict, storage, session_id, redis_client)
            for file_dict in file_list
        ]
        
        # Execute all downloads concurrently
        results = await asyncio.gather(*download_tasks, return_exceptions=True)

        # Collect successful results (unified file_info with content)
        files_info = []
        for result in results:
            if result and isinstance(result, dict):
                files_info.append(result)
            elif isinstance(result, Exception):
                logging.error(f"Download task failed with exception: {result}")

        if not files_info:
            logging.warning(f"No valid files to process for msg_id: {msg_id}")
            return []

        logging.info(
            f"✅ Concurrent download completed: {len(files_info)}/{len(file_list)} files successful"
        )

        # Save files to th_files table (uses same unified structure)
        inserted_ids = await FileDbService.insert_files_batch(
            user_id=user_id,
            files_info=files_info,
            scene="report",
            created_source="web_chat",
            created_source_id=msg_id,
            query_user_id=query_user_id,
        )

        if inserted_ids:
            logging.info(
                f"Files saved to th_files with msg_id: {msg_id}, "
                f"inserted: {len(inserted_ids)}/{len(files_info)} files"
            )

        # Schedule file processing tasks (uses same unified structure)
        await schedule_file_processing_tasks(
            files_data=files_info,
            user_id=query_user_id,
            msg_id=msg_id,
            language=language
        )

        logging.info(f"Successfully scheduled processing for {len(files_info)} files with msg_id: {msg_id}")

        # Return unified files_info for Agent to use (includes content + metadata)
        return files_info
            
    except Exception as e:
        logging.error(f"Error in process_files_from_storage: {str(e)}", exc_info=True)
        return []

#-----------------------------------------------------------------------------

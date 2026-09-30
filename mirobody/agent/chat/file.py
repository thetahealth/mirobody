"""
File processing module for chat(th_messages)

All parameters are explicitly passed - no implicit context dependencies (get_req_ctx).
This ensures thread safety and testability.
"""

import asyncio
import logging

from datetime import datetime
from typing import Any

from mirobody.collect import process_files_async
from mirobody.collect import SOURCE_ASK, FileDbService
from mirobody.utils.file_types import guess_mime
from mirobody.utils.config.storage import get_storage_client
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

async def _download_single_file(file_dict: dict[str, Any], storage: Any, session_id: str) -> dict[str, Any] | None:
    """One attachment's bytes from storage, with the metadata its th_files row
    and the extraction need. None when it cannot be read.

    There was a Redis copy of every attachment's bytes, base64, for an hour,
    keyed by file_key alone: a health document held in a second store, shared
    across accounts, to save one fetch when the same key was sent again.
    """
    file_key = file_dict.get("file_key", "")
    file_name = file_dict.get("file_name", "")
    if not file_key or not file_name:
        logger.warning("attachment without a key or a name skipped")
        return None
    file_type = file_dict.get("file_type", "")
    if not file_type or "/" not in file_type:
        file_type = guess_mime(file_name)
    file_url = file_dict.get("file_url", "")
    try:
        content, _ = await storage.get(file_key)
    except Exception as e:
        logger.warning("attachment download failed: error_type=%s", type(e).__name__)
        return None
    if not content:
        logger.warning("attachment download returned nothing")
        return None
    return {
        "file_key": file_key,
        "file_name": file_name,
        "original_filename": file_name,
        "content_bytes": content,
        "content_type": file_type,
        "file_type": file_type,
        "file_size": file_dict.get("file_size", 0),
        "url_thumb": file_url,
        "url_full": file_url,
        "session_id": session_id,
        "upload_time": datetime.now().isoformat(),
    }

#-----------------------------------------------------------------------------

def _detect_file_scene(fi: dict[str, Any]) -> str:
    """Classify one chat attachment before it can enter the Agent file mounts."""
    from mirobody.collect import GeneticHandler

    name = (fi.get("file_name") or "").lower()
    ctype = fi.get("content_type") or fi.get("file_type") or ""
    content = fi.get("content_bytes") or b""
    # The archive reader validates the trailer, member count and CRC. A prefix
    # of a valid gzip/zip can look invalid and would let it into /uploads/.
    probe = content if content.startswith((b"\x1f\x8b", b"PK\x03\x04")) else content[: GeneticHandler.SNIFF_BYTES]
    if GeneticHandler.is_genetic_name(name) or GeneticHandler.is_genetic_content(probe, ctype):
        return "genetic"
    if name.endswith((".xlsx", ".xls")):
        return "excel"
    if name.endswith(".csv"):
        return "csv"
    return "report"


async def process_files_from_storage(
    file_list: list[dict[str, Any]],
    user_id: str,
    msg_id: str,
    session_id: str = "",
    query_user_id: str = "",
    source: str = SOURCE_ASK,
) -> int:
    """File stored uploads in th_files and start their extraction; returns
    how many were filed.

    `user_id` is the uploader, `query_user_id` the record the readings land in
    (the care-circle target when asking on someone's behalf). A key whose file
    belongs to another account is dropped before it is fetched. `source` is
    `SOURCE_ASK` for a turn's attachments, `SOURCE_DATA` for `POST
    /files/upload?file=true`.
    """
    if not file_list:
        return 0
    query_user_id = query_user_id or user_id
    try:
        foreign = await FileDbService.keys_held_by_others(
            [str(f.get("file_key")) for f in file_list if f.get("file_key")], user_id)
        if foreign:
            logger.warning("attachments refused, held by another account: count=%d", len(foreign))
        storage = get_storage_client()
        results = await asyncio.gather(*(
            _download_single_file(f, storage, session_id)
            for f in file_list if str(f.get("file_key")) not in foreign
        ))
        files_info = [r for r in results if r]
        if not files_info:
            return 0

        scenes = await asyncio.gather(*(asyncio.to_thread(_detect_file_scene, fi) for fi in files_info))
        await FileDbService.insert_files_batch(
            user_id=user_id,
            files_info=files_info,
            scene="report",
            scenes_by_key={str(fi["file_key"]): scene for fi, scene in zip(files_info, scenes, strict=True)},
            created_source=source,
            created_source_id=msg_id,
            query_user_id=query_user_id,
        )
        spawn(process_files_async(files_data=files_info, user_id=query_user_id, msg_id=msg_id))
        return len(files_info)
    except Exception as e:
        logger.error("attachment filing failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return 0

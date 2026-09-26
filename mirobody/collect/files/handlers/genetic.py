from __future__ import annotations

import asyncio
import io
import logging
from typing import Any
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language
from mirobody.collect.files.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.collect.files.services import genotype_format
from mirobody.collect.files.services.genetic_processor import process_genetic_file
# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)

class GeneticHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "genetic"
        
    #: Maximum plain-text header read; wrapped uploads need full validation.
    SNIFF_BYTES = genotype_format.SNIFF_BYTES

    #: MIME is a coarse prefilter. The format reader checks the actual content.
    CONTENT_TYPES = frozenset({
        "text/plain", "text/csv", "text/tab-separated-values", "application/csv", "application/vnd.ms-excel",
        "text/vcf", "application/vcf", "application/octet-stream", "application/gzip", "application/x-gzip",
        "application/zip", "application/x-zip-compressed",
    })

    @staticmethod
    def _may_be_text(content_type: str | None) -> bool:
        return not content_type or content_type in GeneticHandler.CONTENT_TYPES

    @staticmethod
    def is_genetic_content(head: Any, content_type: str | None) -> bool:
        """Genetic-file check on an already-read header (bytes or str).

        Wrapped data requires the complete bytes so the archive can be checked
        for unsafe or multiple members.
        """
        if not GeneticHandler._may_be_text(content_type):
            return False
        return genotype_format.sniff(head) is not None

    @staticmethod
    async def is_genetic_file(file: UploadFile) -> bool:
        """Check if file is a genetic data file"""
        try:
            if not GeneticHandler._may_be_text(file.content_type):
                return False

            content = getattr(file, "content", None)
            stream = io.BytesIO(content) if isinstance(content, bytes | bytearray) else file.file
            return await asyncio.to_thread(genotype_format.sniff_stream, stream) is not None
        except Exception:
            return False
        finally:
            await file.seek(0)

    # Genetic file handling is quite different:
    # 1. It uploads to OSS/S3 first, then saves to temp for background processing.
    # 2. It calculates file size differently (seek end).
    # 3. It returns immediately after spawning background task (genetic data parsing happens async).
    
    async def process(self, ctx: FileProcessingContext) -> dict[str, Any]:
        # Override process completely because the flow is very different
        file_key = None
        try:
            language = request_language()
            
            # Generate file_key using base class method
            file_key = self._get_unique_filename(ctx)

            # Get file size
            await ctx.file.seek(0, 2)
            file_size = ctx.file.tell()
            await ctx.file.seek(0)

            if ctx.progress_callback:
                await ctx.progress_callback(30, localize("uploading_file", language, "file_processor"))

            # Upload file to OSS/S3 storage (same as other file types)
            full_url = await self._handle_upload(ctx, file_key, language)
            logger.info("genotype upload stored", extra={"size_bytes": file_size})

            if ctx.progress_callback:
                await ctx.progress_callback(40, localize("genetic_file_saving", language, "load_genetic_data"))

            # Save file to temporary path for background processing
            temp_file_path, _ = await self.temp_manager.save_upload_file_to_temp(ctx.file)

            if ctx.progress_callback:
                 await ctx.progress_callback(50, localize("genetic_file_processing_background", language, "load_genetic_data"))

            # Simple file abstract for genetic files (no LLM extraction needed)
            file_name = ctx.filename
            file_abstract = localize("genetic_file_abstract", language, "load_genetic_data", filename=ctx.filename)

            response = {
                "success": True,
                "message": localize("genetic_file_received", language, "load_genetic_data"),
                "type": "genetic",
                "filename": ctx.filename,
                "file_size": file_size,
                "url_thumb": full_url or ctx.filename,
                "full_url": full_url or ctx.filename,
                "raw": localize("genetic_file_processing_background", language, "load_genetic_data"),
                "file_abstract": file_abstract,
                "file_name": file_name,
                "message_id": ctx.message_id,
                "file_key": file_key,
            }

            # Spawn background task
            # The target person owns the active set even when a carer uploads.
            spawn(
                process_genetic_file(
                    user_id=ctx.user_id,  # Uploader ID (for WebSocket notifications)
                    target_user_id=ctx.target_user_id,
                    temp_file_path=str(temp_file_path),
                    message_id=ctx.message_id,
                    language=language,
                    original_filename=ctx.filename,
                    original_file_size=file_size,
                    source_table="th_files",
                    source_table_id=file_key,  # Use file_key as source_table_id for th_files
                    file_key=file_key,
                    full_url=full_url,
                    file_abstract=file_abstract,
                )
            )
            
            return response

        except Exception as e:
             return await self._handle_error(ctx, e, file_key)

    async def _process_content(self, *args, **kwargs):
        pass # Not used due to override

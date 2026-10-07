from __future__ import annotations

from mirobody.collect.files.services.conversation_summary import update_message_content
from mirobody.collect.files.services.report_date import resolve_report_date
import abc
import hashlib
import logging
import uuid
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.collect.files.errors import failure_reason
from mirobody.documents.extract import PartialText
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)

# Import services type hints (avoid circular imports if possible, or use Any)
# In a real scenario, we might use Protocol or specific imports if avoiding circular deps.
# For now we assume services are passed in and duck-typed or we use Any.

@dataclass
class FileProcessingContext:
    file: UploadFile
    user_id: str
    message_id: str | None
    query: str = ""
    query_user_id: str = ""
    progress_callback: Callable[[int, str], None] | None = None
    file_key: str | None = None
    skip_upload_oss: bool = False
    original_filename: str | None = None
    #: Why text extraction produced nothing, when it raised: set by
    #: `_extract_original_text`, read by `process`. A report photo with no
    #: vision provider used to pass through here as a success with empty text.
    extraction_error: str = ""
    
    @property
    def target_user_id(self) -> str:
        return self.query_user_id if self.query_user_id else self.user_id
    
    @property
    def filename(self) -> str:
        return self.original_filename or self.file.filename
        
    @property
    def content_type(self) -> str:
        return self.file.content_type


def _no_answer_reason() -> str:
    """Why no model answered an extraction: no text entry is routable (the
    sentence names the key to set), or the routed one's call failed. ONE route
    is tried, never a second: config.llm.yaml states it ("selection happens
    once; a failed call is reported, never retried elsewhere")."""
    from mirobody.utils.config.llm import no_provider_message, resolve_route

    route = resolve_route("text")
    if route is None:
        return no_provider_message("text")
    return (f"indicator extraction failed: {route.alias} ({route.model}) returned an error, and a failed call "
            "is not retried on another provider (see the server log for the provider's message); re-upload "
            "after fixing it, or point UTILS_TEXT_MODEL elsewhere")


class BaseFileHandler(abc.ABC):
    def __init__(
        self, 
        uploader=None, 
        temp_manager=None, 
        indicator_extractor=None,
        abstract_extractor=None
    ):
        self.uploader = uploader
        self.temp_manager = temp_manager
        self.indicator_extractor = indicator_extractor
        self.abstract_extractor = abstract_extractor

    async def process(self, ctx: FileProcessingContext) -> dict[str, Any]:
        """Template method for file processing"""
        unique_filename = None  # Track file_key even if processing fails
        try:
            language = request_language()
            
            await ctx.file.seek(0)
            if not await ctx.file.read(1):
                raise ValueError(localize("file_empty", language, "temp_file_manager"))

            # 1. Generate unique filename if needed
            unique_filename = self._get_unique_filename(ctx)
            
            # 2. Upload or Get URL (Common step, but can be overridden or skipped by subclasses)
            full_url = await self._handle_upload(ctx, unique_filename, language)

            # 3. The text, stored on the row at once, and the abstract
            result_data = await self._process_content(ctx, unique_filename, language)

            # 4.5. Auto-start background indicator extraction for any handler that returns original_text
            original_text = result_data.get("original_text")
            if not (original_text and original_text.strip()) and ctx.extraction_error:
                # The file is stored, but nothing could be read out of it and
                # we know why. Reporting success here rendered "indicators:
                # none" over a working-looking upload (#68); the failure and
                # its reason belong on the upload itself.
                raise RuntimeError(f"could not read the document: {ctx.extraction_error}")
            if original_text and original_text.strip() and self.indicator_extractor:
                self._start_background_indicator_extraction(
                    original_text=original_text,
                    user_id=int(ctx.target_user_id),
                    file_name=ctx.filename,
                    file_key=unique_filename,
                    message_id=ctx.message_id,
                )

            # 5. Construct final response
            return self._build_response(ctx, result_data, unique_filename, full_url, language)
            
        except Exception as e:
            return await self._handle_error(ctx, e, unique_filename)

    def _get_unique_filename(self, ctx: FileProcessingContext) -> str:
        if ctx.file_key:
            return ctx.file_key
        
        # Default unique filename generation with web_uploads prefix
        extension = ctx.file.filename.split('.')[-1].lower() if '.' in ctx.file.filename else "bin"
        return f"web_uploads/{str(uuid.uuid4())}.{extension}"

    async def _handle_upload(self, ctx: FileProcessingContext, unique_filename: str, language: str) -> str:
        """The stored file's URL: uploaded here, or signed afresh when the
        caller stored the file already (`skip_upload_oss`). A failed upload
        raises: answered with "", the upload reported success over a file that
        was never stored."""
        await self._progress(ctx, 35, "uploading_file", language)
        if not ctx.skip_upload_oss:
            full_url = await self.uploader.upload_file_and_get_url(ctx.file, unique_filename, ctx.content_type)
            logger.info("upload stored: file_key=%s", unique_filename)
            return full_url

        from mirobody.utils.config.storage import get_storage_client

        # The file is stored; a URL that cannot be signed now is signed again
        # when the file list is read (`drive_listing.regenerate_file_url`).
        try:
            full_url, err = await get_storage_client().generate_signed_url(unique_filename, content_type=ctx.content_type)
        except Exception as e:
            logger.warning("signing a stored file's url failed: file_key=%s error_type=%s", unique_filename,
                           type(e).__name__, exc_info=not is_driver_exception(e))
            return ""
        if err:
            logger.warning("signing a stored file's url failed: file_key=%s", unique_filename)
        return full_url or ""

    async def _extract_original_text(self, ctx: FileProcessingContext) -> tuple[str | None, str | None]:
        """
        Read the upload's bytes and extract original text.

        SHA256 dedup lives inside
        ``FileAbstractExtractor.extract_file_original_text`` (one cache for
        every extraction consumer, not a per-handler copy). The hash is
        computed here because callers persist it on the ``th_files`` row,
        which is what makes the next upload of the same bytes a cache hit.

        Returns:
            Tuple of (original_text, content_hash), or (None, None) on empty
            content / extraction failure. A text with pages missing
            (`PartialText`) comes without its hash: stored under it, it would
            be what the next upload of these bytes reads instead of the pages.
        """
        try:
            await ctx.file.seek(0)
            file_content = await ctx.file.read()

            if not file_content:
                logger.warning(f"[BaseFileHandler] Empty file content: {ctx.message_id}")
                return None, None

            content_hash = hashlib.sha256(file_content).hexdigest()

            original_text = await self.abstract_extractor.extract_file_original_text(
                file_content=file_content,
                file_type=self.get_type_name(),
                filename=ctx.filename,
                content_type=ctx.content_type,
            )
            if isinstance(original_text, PartialText):
                logger.warning("text extracted with pages missing: message_id=%s missing_page_count=%d",
                               ctx.message_id, len(original_text.missing_pages))
                return original_text, None
            return original_text, content_hash

        except Exception as e:
            logger.error(
                f"[BaseFileHandler] Failed to extract original text for {ctx.filename}: {e}",
                exc_info=True
            )
            ctx.extraction_error = f"{type(e).__name__}: {e}"
            return None, None

    async def _abstract(self, ctx: FileProcessingContext, original_text: str | None, language: str) -> tuple[str, str]:
        """`(abstract, file_name)` for the upload: read off its text by the text
        model, or for an image with no text, off the image by the vision model;
        the fallback sentence, under the upload's own name, when neither answered."""
        file_abstract, file_name = "", ctx.filename
        try:
            if original_text and original_text.strip():
                file_abstract, file_name = await self.abstract_extractor.abstract_from_text(
                    original_text, ctx.filename, language)
            elif self.get_type_name() == "image" and not ctx.extraction_error:
                await ctx.file.seek(0)
                file_abstract, file_name = await self.abstract_extractor.describe_image(
                    bytes(await ctx.file.read()), ctx.filename)
        except Exception as e:
            logger.warning("abstract failed: message_id=%s error_type=%s", ctx.message_id, type(e).__name__,
                           exc_info=not is_driver_exception(e))
        if not file_abstract:
            return self.abstract_extractor.fallback_abstract(ctx.filename, self.get_type_name()), ctx.filename
        return file_abstract, file_name

    async def _progress(self, ctx: FileProcessingContext, percent: int, key: str, language: str) -> None:
        if ctx.progress_callback:
            await ctx.progress_callback(percent, localize(key, language, "file_processor"))

    async def _process_content(self, ctx: FileProcessingContext, unique_filename: str, language: str) -> dict[str, Any]:
        """The upload's text and its abstract, the same for every kind of
        document: a PDF, a photo, a workbook, a Word file or a text. The text
        is written to the row as soon as it is read: a chat attachment's row
        exists before processing starts, and the agent reads the text there.
        Indicator extraction starts from `original_text` in `process`."""
        await self._progress(ctx, 55, "extracting_content", language)
        original_text, content_hash = await self._extract_original_text(ctx)
        if original_text:
            await self._save_original_text_to_db(
                file_key=unique_filename,
                original_text=original_text,
                text_length=len(original_text),
                content_hash=content_hash or "",
            )
        await self._progress(ctx, 70, "extracting_abstract", language)
        file_abstract, file_name = await self._abstract(ctx, original_text, language)
        await self._progress(ctx, 90, f"{self.get_type_name()}_processing_success", language)
        return {
            "raw": original_text or "",
            "file_abstract": file_abstract,
            "file_name": file_name,
            "original_text": original_text or "",
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash or "",
        }

    @abc.abstractmethod
    def get_type_name(self) -> str:
        pass

    def _build_response(
        self, 
        ctx: FileProcessingContext, 
        result_data: dict[str, Any], 
        unique_filename: str, 
        full_url: str, 
        language: str
    ) -> dict[str, Any]:
        
        response = {
            "success": True,
            "message": localize(f"{self.get_type_name()}_processing_success", language, "file_processor"),
            "type": self.get_type_name(),
            "filename": ctx.filename,
            "full_url": full_url,
            "message_id": ctx.message_id,
            "file_key": unique_filename,
        }
        
        # Merge specific result data
        response.update(result_data)
        
        # Optional: add url_thumb if same as full_url
        if "url_thumb" not in response and full_url:
            response["url_thumb"] = full_url
            
        return response

    async def _handle_error(self, ctx: FileProcessingContext, e: Exception, file_key: str | None = None) -> dict[str, Any]:
        language = request_language()
        error_msg = str(e)
        logger.error(f"File processing failed: {ctx.filename}, file_key: {file_key}, error: {error_msg}", exc_info=True)

        if ctx.message_id:
            try:
                await update_message_content(
                    message_id=ctx.message_id,
                    content=f"❌ {localize('file_upload_failed', language, 'file_processor')}\n\n{localize('error', language, 'file_processor')}: {error_msg}",
                    reasoning=f"Error occurred during file processing: {error_msg}",
                )
            except Exception as update_error:
                logger.error(f"Failed to update message status: {str(update_error)}", stack_info=True)

        user_message = localize(f"{self.get_type_name()}_processing_failed", language, "file_processor")
        if not user_message:
             user_message = localize('file_upload_failed', language, 'file_processor')
        # The reason travels with the message: "Image processing failed" alone
        # sent the reporter of #68 into the server logs for a cause that was
        # one sentence long ("no vision provider: set one of these keys").
        if error_msg:
            user_message = f"{user_message}: {error_msg}"

        # Some specialized error handling for JSON parsing if needed
        if "JSON parsing failed" in error_msg:
             user_message = localize("json_parsing_failed", language, "file_processor") or "File processing failed: Invalid response format"

        return {
            "success": False,
            "message": user_message,
            "status": "error",
            "message_id": ctx.message_id,
            "filename": ctx.filename,
            "error": error_msg,
            "type": self.get_type_name(),
            "raw": f"Processing failed: {ctx.filename}",
            "file_key": file_key or "",  # Include file_key even on failure
        }

    # ── Shared indicator extraction methods (used by pdf, image, text, etc.) ──

    @staticmethod
    def _indicator_extraction_enabled() -> bool:
        """`ENABLE_INDICATOR_EXTRACTION`: 0 skips the LLM extraction pass.

        config.yaml and docs/file-processing.md have both documented this switch
        for a long time and NOTHING read it, so a deployment that set it to 0 (
        to stop paying for an extraction call on a bulk import, say, or to keep
        a document store text-only) got extraction anyway. It is the single
        most expensive step in the upload path (one LLM call per file), which
        makes a silently-ignored off switch an unbudgeted bill rather than a
        cosmetic defect.

        Defaults to enabled, which is what every existing deployment already
        gets. The file itself, its text and its search index are unaffected;
        only the indicator rows are not produced.
        """
        from mirobody.utils.config import safe_read_cfg

        raw = safe_read_cfg("ENABLE_INDICATOR_EXTRACTION", "1").strip().lower()
        return raw not in ("0", "false", "no", "off")

    def _start_background_indicator_extraction(
        self,
        original_text: str,
        user_id: int,
        file_name: str,
        file_key: str,
        message_id: str | None = None,
    ):
        """Start indicator extraction in the background.

        `message_id` is the upload session the file arrived in; it is how the
        task's two progress events find the client's WebSocket (see
        `_push_upload_event`). A chat-channel upload has no such session and
        passes None: the agent asks about the date instead.
        """
        if not self._indicator_extraction_enabled():
            logger.info(
                f"⏭️  {self.get_type_name()} upload completed, indicator extraction "
                f"skipped (ENABLE_INDICATOR_EXTRACTION=0): {file_key}"
            )
            return

        # `spawn` holds the task: the handler is discarded once `process`
        # returns, and a task only it referenced could be collected mid-run.
        spawn(
            self._async_extract_indicators(
                original_text=original_text,
                user_id=user_id,
                file_name=file_name,
                file_key=file_key,
                message_id=message_id,
            ),
            name="indicator-extraction",
        )
        logger.info(
            f"{self.get_type_name()} upload completed, "
            f"background indicator extraction started: {file_key}"
        )

    @staticmethod
    async def _push_upload_event(message_id: str | None, event: dict[str, Any]) -> None:
        """Tell the client that uploaded this file what extraction found.

        The upload socket is per user and outlives the upload (it heartbeats),
        so a background task can still reach the page that sent the file,
        which is what lets the Data page ask "which date?" the moment the
        answer is known instead of on the next reload. Silent when the socket
        is gone or the upload had no session (chat channel): the file row
        carries the same facts and the page reads them on its next visit.
        """
        if not message_id:
            return
        try:
            from mirobody.collect.files.file_upload_manager import get_websocket_file_upload_manager

            manager = get_websocket_file_upload_manager()
            session = manager.upload_sessions.get(message_id) or {}
            payload = {**event, "messageId": message_id, "sessionId": session.get("session_id", "")}
            await manager.send_message_by_message_id(message_id, payload)
        except Exception as e:
            logger.debug(f"upload event {event.get('type')} not delivered for {message_id}: {e}")

    async def _async_extract_indicators(
        self,
        original_text: str,
        user_id: int,
        file_name: str,
        file_key: str,
        message_id: str | None = None,
    ) -> None:
        """Background task: extract indicators from text and update th_files.

        Two steps, two events. The date is probed FIRST (one small model call,
        a few seconds) and announced as `report_date_detected`, so the Data
        page can ask about a missing date while the 15-25 s indicator
        extraction is still running; `extraction_completed` follows with the
        count stored and the date the readings were actually filed under.
        """
        from mirobody.collect.files.services.content_formatter import ContentFormatter
        from mirobody.collect.files.services.file_db_service import FileDbService

        filed = None
        formatted_raw = original_text
        failed_reason = ""
        try:
            probed = await self.indicator_extractor.probe_report_date(original_text)
            probe_dt, probe_source = await resolve_report_date(str(user_id), probed)
            probe_report = {"report_date": probe_dt.strftime("%Y-%m-%d %H:%M:%S"), "date_source": probe_source}
            # On the file row now, not after extraction: the bar's answer
            # (set_file_report_date) reads and writes this row, and the
            # readings that land later look here for a manual date.
            await FileDbService.rows_ready(message_id)
            await FileDbService.update_file_content(file_key, probe_report)
            await self._push_upload_event(message_id, {
                "type": "report_date_detected", "file_key": file_key, "file_name": file_name, **probe_report,
            })

            filed = await self.indicator_extractor.extract_indicators_from_text(
                original_text, user_id, file_key, report_date=(probe_dt, probe_source))
            # Three kinds of "zero indicators", told apart (#68). The file is
            # stored either way, but the UI must not render 1 and 2 like 3.
            #   1. no provider can do structured extraction: configuration
            #   2. a provider was selected and its call failed: see the log
            #   3. a model read the document and found none: normal
            if filed.answer is None:
                failed_reason = _no_answer_reason()
            elif filed.indicators:
                formatted_raw = ContentFormatter.format_parsed_content(
                    file_results=[{"type": self.get_type_name(), "raw": original_text}],
                    file_names=[file_key],
                    llm_responses=[filed.answer],
                    indicators_list=[filed.indicators],
                )
        except Exception as e:
            # The fourth kind, and the one that got away: extraction RAISED
            # (a write that stored nothing, the bytearray bug #B-1) and the
            # row was written `status: completed` over an empty list.
            failed_reason = f"indicator extraction failed: {failure_reason(e)}"
            logger.error("indicator extraction failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                         exc_info=not is_driver_exception(e))

        stored = filed.stored if filed else 0
        report = filed.report if filed else None
        await FileDbService.rows_ready(message_id)
        await self._update_file_indicators(
            file_key=file_key,
            formatted_raw=formatted_raw,
            indicators_count=stored,
            failed_reason=failed_reason,
            report=report,
        )
        await self._push_upload_event(message_id, {
            "type": "extraction_completed", "file_key": file_key, "file_name": file_name,
            "indicators_count": stored,
            "failed": bool(failed_reason),
            **(report or {}),
        })
        logger.info("indicator extraction finished: file_key=%s stored_count=%d failed=%s", file_key, stored,
                    bool(failed_reason))

    async def _save_original_text_to_db(
        self,
        file_key: str,
        original_text: str,
        text_length: int,
        content_hash: str,
    ):
        """Save original_text to th_files table by file_key."""
        try:
            from mirobody.utils.db import execute_query

            sql = """
                UPDATE th_files
                SET original_text = encrypt_content(:original_text),
                    text_length = :text_length,
                    content_hash = :content_hash,
                    updated_at = NOW()
                WHERE file_key = :file_key
            """

            await execute_query(
                sql,
                params={
                    "file_key": file_key,
                    "original_text": original_text,
                    "text_length": text_length,
                    "content_hash": content_hash,
                },
            )

        except Exception as e:
            logger.warning(f"Failed to save original text to th_files: {file_key}, error: {e}")

    async def _update_file_indicators(
        self,
        file_key: str,
        formatted_raw: str,
        indicators_count: int,
        failed_reason: str = "",
        report: dict[str, str] | None = None,
    ):
        """Update th_files with indicator extraction results.

        `failed_reason` marks the file `status: failed` (the files API maps it
        to `upload_status: "failed"`), so an extraction that could not run:
        e.g. zero LLM keys: is not presented as a processed file. A later
        successful extraction on the same file_key writes `completed`, which
        clears an earlier failure.

        `report` (`report_date` + `date_source`) is the date the readings were
        filed under and whether it was read off the document or is the upload
        time standing in. Extraction runs after the upload has already
        completed, so the file row is the only place the UI can learn this
        from, it is what the Data page reads to ask "which date?" (#53).
        """
        try:
            from mirobody.collect.files.services.file_db_service import FileDbService

            updates = {
                "raw": formatted_raw,
                "indicators_count": indicators_count,
                "processed": True,
                "status": "failed" if failed_reason else "completed",
            }
            if failed_reason:
                updates["error"] = failed_reason
            if report:
                updates.update(report)

            await FileDbService.update_file_content(
                file_key=file_key,
                updates=updates,
            )

            logger.info(f"Updated th_files indicators: {file_key}")

        except Exception as e:
            logger.warning(f"Failed to update file indicators: {e}")


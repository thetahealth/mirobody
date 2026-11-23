"""An Apple Watch ECG CSV, recognised by its header and read without a model.

`electrocardiograms/ecg_YYYY-MM-DD.csv` inside an Apple Health export: a
small header, then about 15,360 microvolt samples
(`kernel.decoders.apple_ecg`). `FileHandlerFactory` routes a file here by
content, before `documents.detect.kind` ever sees it — the same precedent
`GeneticHandler` sets for a format recognised by bytes rather than extension
— so the file never reaches `IndicatorExtractor._llm_extract` (issue #334).

`process` replaces the base template entirely rather than overriding one of
its steps: every one of the base path's three model calls (the file
summary, the report-date probe, the indicator extraction) would otherwise
see the waveform, and this handler needs none of them — the one reading a
recording holds is read off its header, not guessed by a model.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import UploadFile

from mirobody.collect.files.errors import UploadError
from mirobody.collect.files.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.collect.files.services.apple_ecg_store import save_ecg_reading
from mirobody.kernel.decoders import apple_ecg
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language

#: Bytes read to sniff the format: comfortably past the eight metadata rows
#: and the lead/unit pair that follow them, nowhere near the first sample.
SNIFF_BYTES = 4096


class AppleEcgHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "apple_ecg"

    @staticmethod
    async def is_apple_ecg_file(file: UploadFile) -> bool:
        """A content sniff, ahead of `documents.detect.kind`: an ordinary CSV
        never opens with a literal `Name` row, so this never claims one."""
        try:
            await file.seek(0)
            head = await file.read(SNIFF_BYTES)
            return apple_ecg.sniff(head.decode("utf-8", errors="replace"))
        except Exception:
            return False
        finally:
            await file.seek(0)

    async def process(self, ctx: FileProcessingContext) -> dict[str, Any]:
        unique_filename = None
        try:
            language = request_language()
            await ctx.file.seek(0)
            content = await ctx.file.read()
            if not content:
                raise UploadError.phrase("file_empty", "temp_file_manager")

            unique_filename = self._get_unique_filename(ctx)
            await self._progress(ctx, 35, "uploading_file", language)
            full_url = await self._handle_upload(ctx, unique_filename, language)

            await self._progress(ctx, 60, "extracting_content", language)
            text = content.decode("utf-8", errors="replace")
            reading = apple_ecg.parse(text)
            if reading is None:
                raise UploadError.phrase("apple_ecg_not_readable")

            header_text = apple_ecg.header_text(text)
            content_hash = hashlib.sha256(content).hexdigest()
            await self._save_original_text_to_db(
                file_key=unique_filename, original_text=header_text,
                text_length=len(header_text), content_hash=content_hash,
            )

            report = await save_ecg_reading(ctx.target_user_id, reading, unique_filename)
            # `extracting: False` below tells the generic upload path
            # (`FileDbService.update_file_processed`) to mark the file
            # `completed` at once, but that call never touches
            # `indicators_count` — only the background extraction task does,
            # which this handler has none of, so it is set here instead.
            await self._update_file_indicators(
                file_key=unique_filename, formatted_raw=header_text, indicators_count=report.inserted,
            )

            await self._progress(ctx, 90, "apple_ecg_processing_success", language)
            file_abstract = localize(
                "apple_ecg_abstract", language, "file_processor",
                classification=reading.classification, date=reading.recorded_at.strftime("%Y-%m-%d %H:%M"),
            )
            result_data = {
                "raw": header_text,
                "original_text": header_text,
                "text_length": len(header_text),
                "content_hash": content_hash,
                "file_abstract": file_abstract,
                "file_name": ctx.filename,
                "indicators_count": report.inserted,
                "extracting": False,
            }
            return self._build_response(ctx, result_data, unique_filename, full_url, language)
        except Exception as e:
            return await self._handle_error(ctx, e, unique_filename)


__all__ = ["AppleEcgHandler"]

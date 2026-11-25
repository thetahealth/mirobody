"""A file that leaves no reading says so (#338).

Reproduced on 1.6.2 with an OpenAI-compatible stand-in for a local vision
model that does not read images: every answer was "The image shows a sheet of
paper with some printed text. I cannot make out the details." `mirobody doctor
--probe` failed the vision probe, and an uploaded report photo was filed
已处理 with 0 readings, 日期待确认 on the upload day, and nothing under 指标:
in the file list it could not be told from a file whose readings were stored.
The stand-in's answer was also cached under the photo's hash, so the same photo
read the same way after the model was replaced.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

pytest.importorskip("sqlalchemy")

from mirobody.collect.files.handlers import base

STAND_IN = "The image shows a sheet of paper with some printed text. I cannot make out the details."
REPORT = "总胆固醇 | 203 | mg/dL\nLDL-C | 128 | mg/dL"


class Image(base.BaseFileHandler):
    def get_type_name(self) -> str:
        return "image"


class Pdf(base.BaseFileHandler):
    def get_type_name(self) -> str:
        return "pdf"


@pytest.mark.parametrize(("handler", "text", "numberless"), [
    (Image, STAND_IN, True),
    (Image, REPORT, False),
    # NO_TEXT came back "": a photo of a meal, described by the vision model instead.
    (Image, "", False),
    # A note in words alone is no image's transcription to doubt.
    (Pdf, STAND_IN, False),
    # Full-width digits are digits.
    (Image, "血糖 ５.４", False),
])
def test_an_image_whose_text_holds_no_number_is_told_apart(handler, text, numberless):
    assert base.numberless_image_text(handler().get_type_name(), text) is numberless


class _Upload:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.filename = "photo.jpg"
        self.content_type = "image/jpeg"

    async def seek(self, offset: int) -> None:
        return None

    async def read(self, size: int = -1) -> bytes:
        return self.data


class _Text:
    def __init__(self, text: str) -> None:
        self.text = text

    async def extract_file_original_text(self, **kwargs) -> str:
        return self.text


@pytest.mark.parametrize(("text", "cached"), [(STAND_IN, False), (REPORT, True)])
async def test_a_numberless_image_text_is_not_stored_under_the_photos_hash(text, cached):
    """The row's `content_hash` is the cache key every later read of the same
    bytes looks up (`file_abstract_extractor._read_original_text_cache`): a
    re-upload and a reprocess both took the stand-in's answer from it."""
    handler = Image(abstract_extractor=_Text(text))
    ctx = base.FileProcessingContext(file=_Upload(b"\xff\xd8 jpeg bytes"), user_id="1", message_id="m1")
    original_text, content_hash = await handler._extract_original_text(ctx)
    assert original_text == text
    assert bool(content_hash) is cached
    assert ctx.numberless_image is not cached


class _NoneFound:
    """A model that read the text and found no reading."""

    async def probe_report_date(self, text: str) -> str:
        return ""

    async def extract_indicators_from_text(self, text, user_id, file_key, report_date=None, message_id=None):
        from mirobody.collect.files.services.indicator_extractor import Filed

        return Filed([], {"indicators": [], "content_info": {}}, 0, None)


@pytest.fixture
def extraction(monkeypatch):
    """`_async_extract_indicators` with the database and the router stubbed;
    returns what was written to the file row, and to the row before it."""
    from mirobody.collect.files.services import file_db_service

    async def nothing(*args, **kwargs):
        return None

    async def not_gone(*args, **kwargs):
        return False

    async def upload_day(user_id, probed, *, message_id=None):
        return datetime(2026, 11, 23, tzinfo=UTC), "upload_time"

    early: dict = {}

    async def update_file_content(file_key, updates):
        early.update(updates)

    monkeypatch.setattr(base, "wait_until_loaded", nothing)
    monkeypatch.setattr(base, "resolve_report_date", upload_day)
    monkeypatch.setattr(file_db_service.FileDbService, "rows_ready", staticmethod(nothing))
    monkeypatch.setattr(file_db_service.FileDbService, "gone", staticmethod(not_gone))
    monkeypatch.setattr(file_db_service.FileDbService, "update_file_content", staticmethod(update_file_content))
    written: dict = {}

    def run(**kwargs):
        handler = Image(indicator_extractor=_NoneFound())

        async def update(**fields):
            written.update(fields)

        monkeypatch.setattr(handler, "_update_file_indicators", update)
        monkeypatch.setattr(handler, "_push_upload_event", nothing)
        return handler._async_extract_indicators(user_id=1, file_name="photo.jpg", file_key="web_uploads/p.jpg",
                                                 **kwargs)

    return run, written, early


async def test_a_photo_that_left_no_reading_says_why_and_asks_no_date(extraction):
    from mirobody.collect.files.errors import say

    run, written, early = extraction
    await run(original_text=STAND_IN, numberless_image=True)
    assert early["date_source"] == "upload_time"  # the probe's, while extraction ran
    assert written["indicators_count"] == 0 and not written["failed_reason"]
    # Nothing was filed under the probe's date: no 日期待确认 over no readings.
    assert written["report"] == {"report_date": "", "date_source": ""}
    assert written["notice_phrases"] == [{"key": "no_readings_found"}, {"key": "image_text_has_no_numbers"}]
    said = say(written["notice_phrases"], "zh")
    assert said.startswith("这个文件里没有读出指标：视觉模型给出的这张图片的文字里没有任何数字")
    assert "mirobody doctor --probe" in said


async def test_a_document_read_with_no_reading_is_not_blamed_on_the_vision_model(extraction):
    run, written, _ = extraction
    await run(original_text="出院小结 2024-04-02 建议低盐饮食")
    assert written["notice_phrases"] == [{"key": "no_readings_found"}]


@pytest.mark.parametrize("language", ["en", "zh", "fr", "ja", "es"])
def test_every_notice_phrase_is_said_in_every_bundle_language(language):
    from mirobody.utils.i18n import localize

    for key in ("no_readings_found", "image_text_has_no_numbers"):
        assert localize(key, language, "file_processor") != key


async def test_the_file_list_tells_a_file_with_no_reading_apart(monkeypatch):
    """`processed: true` is all the status cell read, and `indicators_count`
    is 0 for a genotype file too, which loads variants, not readings."""
    from mirobody.collect.files.services import file_db_service
    from mirobody.utils.req_ctx import set_req_ctx

    def row(n: int, scene: str, content: dict) -> dict:
        return {"id": n, "user_id": 1, "query_user_id": 1, "file_name": f"f{n}", "file_type": "image/jpeg",
                "file_key": f"web_uploads/{n}", "scene": scene, "created_source": "web", "created_source_id": "",
                "file_content": json.dumps(content), "created_at": datetime(2026, 11, 23, tzinfo=UTC),
                "updated_at": datetime(2026, 11, 23, tzinfo=UTC)}

    photo = {"status": "completed", "processed": True, "indicators_count": 0,
             "notice_phrases": [{"key": "no_readings_found"}, {"key": "image_text_has_no_numbers"}]}
    rows = [row(1, "report", photo),
            row(2, "genetic", {"status": "completed", "processed": True, "type": "genetic"}),
            row(3, "report", {"status": "completed", "processed": True, "indicators_count": 12}),
            row(4, "report", {"status": "failed", "processed": True, "indicators_count": 0,
                              "notice_phrases": [{"key": "no_readings_found"}],
                              "error_phrases": [{"key": "indicator_extraction_failed"}]})]

    async def query(query, params=None):
        return [{"total": len(rows)}] if "COUNT(1)" in query else rows

    monkeypatch.setattr(file_db_service, "execute_query", query)
    with set_req_ctx({"language": "ja"}):
        listed = (await file_db_service.FileDbService.get_files_paginated("1"))["files"]
    assert [f["no_readings"] for f in listed] == [True, False, False, False]
    assert listed[0]["notice"].startswith("このファイルから指標は読み取れませんでした")
    assert [f["notice"] for f in listed[1:]] == ["", "", ""]

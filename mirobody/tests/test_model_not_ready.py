"""An upload that needs a local model the router has not loaded says so (#275).

Measured on a 1.6.0-fix.2 stack whose llama.cpp router could not finish
downloading the text model: a text-layer PDF waited 307 s on the summary, was
filed 已处理 with no readings, waited as long again on the readings and then
failed with "returned an error". With the OCR model unable to load, a scan
failed with "InternalServerError".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("openai")

from mirobody.utils.config import served

LOCAL = SimpleNamespace(base_url_env="LOCAL_BASE_URL", base_url="http://llama:8080/v1", model="minicpm5-2b",
                        alias="local-utils", llm_type="openai")
CLOUD = SimpleNamespace(base_url_env="", base_url="https://api.example/v1", model="cloud-model",
                        alias="cloud", llm_type="openai")


@pytest.fixture
def router(monkeypatch):
    """What the router reports, one answer per question; the last repeats."""
    answers: list[str | None] = []
    asked: list[str] = []

    def status(base_url, model):
        asked.append(model)
        return answers[min(len(asked), len(answers)) - 1] if answers else None

    monkeypatch.setattr(served, "served_status", status)
    monkeypatch.setattr(served, "_POLL_SEC", 0)
    monkeypatch.setattr(served, "_gave_up", {})
    return SimpleNamespace(answers=answers, asked=asked)


async def test_a_load_that_finishes_is_waited_out(router):
    router.answers[:] = ["loading", "loading", "loaded"]
    await served.wait_until_loaded(LOCAL, wait=5)
    assert len(router.asked) == 3


async def test_a_model_still_loading_is_named_with_the_fix(router):
    router.answers[:] = ["loading"]
    with pytest.raises(served.ModelNotReady) as caught:
        await served.wait_until_loaded(LOCAL, wait=0)
    assert "minicpm5-2b is still downloading or loading" in str(caught.value)
    assert "upload the file again" in str(caught.value)


async def test_once_given_up_the_next_caller_is_told_at_once(router, monkeypatch):
    """A scan's text and tables passes, and each further page, waited the
    whole window in turn: 240 s for one page on a local stack."""
    router.answers[:] = ["loading"]
    with pytest.raises(served.ModelNotReady):
        await served.wait_until_loaded(LOCAL, wait=0)
    asked = len(router.asked)
    with pytest.raises(served.ModelNotReady):
        await served.wait_until_loaded(LOCAL, wait=60)
    assert len(router.asked) == asked + 1, "polled again instead of answering at once"
    router.answers[:] = ["loaded"]
    await served.wait_until_loaded(LOCAL, wait=60)
    assert served._gave_up == {}


async def test_a_cloud_route_is_never_asked(router):
    await served.wait_until_loaded(CLOUD, wait=0)
    await served.wait_until_loaded(None, wait=0)
    assert router.asked == []


def test_a_router_that_failed_to_load_is_told_apart_from_other_errors():
    class Status500(Exception):
        status_code = 500

    loaded_failed = Status500("Error code: 500 - {'message': 'model name=glm-ocr failed to load'}")
    assert served.failed_to_load(LOCAL, loaded_failed)
    assert not served.failed_to_load(CLOUD, loaded_failed)
    assert not served.failed_to_load(LOCAL, Status500("Error code: 500 - something else"))


def test_the_upload_shows_the_reason_not_the_type():
    from mirobody.collect.files.errors import failure_reason

    assert "still downloading" in failure_reason(served.ModelNotReady("glm-ocr"))
    assert "failed to load" in failure_reason(served.ModelNotReady("glm-ocr", failed=True))


@pytest.mark.parametrize(("language", "loading", "failed"), [
    ("zh-CN", "本地模型 glm-ocr 还在下载或加载中", "本地模型 glm-ocr 加载失败"),
    ("ja", "ローカルモデル glm-ocr はまだダウンロード中", "ローカルモデル glm-ocr の読み込みに失敗しました"),
])
def test_a_reason_is_said_in_the_language_it_is_read_in(language, loading, failed):
    """The file list read "图片处理失败: ... the local model glm-ocr failed to
    load ..." under a Chinese page. A reason is kept as phrases, which carry no
    language, and said in the reader's."""
    from mirobody.collect.files.errors import reason_phrases, say
    from mirobody.utils.llm import ImageNotRead, NoResponse

    assert say(reason_phrases(served.ModelNotReady("glm-ocr")), language).startswith(loading)
    assert say(reason_phrases(served.ModelNotReady("glm-ocr", failed=True)), language).startswith(failed)
    assert "UTILS_VISION_MODEL" in say(reason_phrases(ImageNotRead()), language)
    assert say(reason_phrases(ImageNotRead()), language) != str(ImageNotRead())
    assert say(reason_phrases(NoResponse("m")), language) != str(NoResponse("m"))
    assert str(served.ModelNotReady("glm-ocr")).startswith("the local model glm-ocr"), "logs and APIs read English"


def test_a_reason_without_a_translation_is_said_as_written():
    from mirobody.collect.files.errors import UploadError, reason_phrases, say, stored_reason

    assert say([{"key": "document_not_read"}, *reason_phrases(KeyError("x"))], "zh") == "无法读取文档：KeyError"
    assert reason_phrases(UploadError("No vision model available: set a key")) == [
        {"text": "No vision model available: set a key"}]
    old = {"error": "PDF processing failed: InternalServerError"}
    assert stored_reason(old, "zh") == old["error"]
    assert stored_reason({**old, "error_phrases": ["not a phrase"]}, "zh") == old["error"]
    assert stored_reason({**old, "error_phrases": [{"key": "document_not_read"}]}, "ja") == (
        "ドキュメントを読み取れませんでした")


def test_every_new_reason_is_in_every_language_with_the_same_placeholders():
    import json
    import string
    from pathlib import Path

    import mirobody.utils.i18n as i18n

    bundle = json.loads((Path(i18n.__file__).with_name("locales") / "file_processor.json").read_text("utf-8"))
    keys = ["with_reason", "document_not_read", "indicator_extraction_failed", "model_still_loading",
            "model_failed_to_load", "model_returned_error", "image_not_read", "empty_response"]
    for key in keys:
        fields = {name for _, name, _, _ in string.Formatter().parse(bundle[key]["en"]) if name}
        for language in ("en", "zh", "fr", "ja", "es"):
            text = bundle[key][language]
            assert {name for _, name, _, _ in string.Formatter().parse(text) if name} == fields, (key, language)


def test_an_upload_message_states_the_pages_language():
    pytest.importorskip("fastapi")
    from mirobody.server.routers.file_router import _stated_language

    ctx = {"trace_id": "t", "language": "zh-CN"}
    assert _stated_language(ctx, {"language": "ja"}) == {"trace_id": "t", "language": "ja"}
    assert _stated_language(ctx, {}) is ctx
    assert _stated_language(ctx, {"language": " "}) is ctx
    assert _stated_language(ctx, {"language": ["ja"]}) is ctx


async def test_vision_waits_then_names_the_model(monkeypatch, router):
    from mirobody.utils.llm.file_processors import dispatch

    router.answers[:] = ["loading"]
    monkeypatch.setattr(served, "LOAD_WAIT_SEC", 0)
    monkeypatch.setattr(dispatch, "vision_route", lambda provider=None, model=None: LOCAL)
    with pytest.raises(served.ModelNotReady, match="still downloading"):
        await dispatch.vision_extract(b"png", "image/png", "Text Recognition:")


async def test_vision_names_a_model_that_failed_to_load(monkeypatch, router):
    from mirobody.utils.llm.file_processors import dispatch

    class Status500(Exception):
        status_code = 500

    async def refused(*args, **kwargs):
        raise Status500("model name=glm-ocr failed to load")

    router.answers[:] = ["unloaded"]
    monkeypatch.setattr(dispatch, "vision_route", lambda provider=None, model=None: LOCAL)
    monkeypatch.setattr(dispatch, "model_ready", lambda image, mime: image)
    monkeypatch.setattr(dispatch, "image_extract", refused)
    with pytest.raises(served.ModelNotReady, match="failed to load"):
        await dispatch.vision_extract(b"png", "image/png", "Text Recognition:")


def _handler():
    pytest.importorskip("sqlalchemy")
    from mirobody.collect.files.handlers import base

    class Handler(base.BaseFileHandler):
        def get_type_name(self) -> str:
            return "pdf"

    return base, Handler


async def test_extraction_fails_with_the_reason_instead_of_waiting(monkeypatch, router):
    base, Handler = _handler()
    from mirobody.collect.files.services import file_db_service

    router.answers[:] = ["loading"]
    monkeypatch.setattr(served, "LOAD_WAIT_SEC", 0)
    monkeypatch.setattr(base, "resolve_route", lambda surface: LOCAL)

    async def nothing(*args, **kwargs):
        return None

    monkeypatch.setattr(file_db_service.FileDbService, "rows_ready", staticmethod(nothing))
    written = {}

    class NeverCalled:
        async def probe_report_date(self, text):
            raise AssertionError("asked a model the router is still loading")

    handler = Handler(indicator_extractor=NeverCalled())

    async def update(**kwargs):
        written.update(kwargs)

    monkeypatch.setattr(handler, "_update_file_indicators", update)
    monkeypatch.setattr(handler, "_push_upload_event", nothing)
    await handler._async_extract_indicators(original_text="Glucose 5.2 mmol/L", user_id=1, file_name="r.pdf",
                                            file_key="web_uploads/r.pdf")
    assert written["failed_reason"] == ("indicator extraction failed: the local model minicpm5-2b is still "
                                        "downloading or loading; upload the file again once the setup page "
                                        "shows it ready")
    assert written["indicators_count"] == 0

    from mirobody.collect.files.errors import say
    from mirobody.utils.req_ctx import set_req_ctx

    with set_req_ctx({"language": "zh-CN"}):
        await handler._async_extract_indicators(original_text="Glucose 5.2 mmol/L", user_id=1, file_name="r.pdf",
                                                file_key="web_uploads/r.pdf")
    # Stored without a language: the Japanese page reading the row later is
    # answered in Japanese although the upload asked in Chinese.
    assert written["failed_phrases"] == [{"key": "indicator_extraction_failed"},
                                         {"key": "model_still_loading", "params": {"model": "minicpm5-2b"}}]
    assert say(written["failed_phrases"], "ja").startswith("指標の抽出に失敗しました：ローカルモデル minicpm5-2b")


async def test_the_summary_is_skipped_while_the_text_model_loads(monkeypatch, router):
    base, Handler = _handler()
    router.answers[:] = ["loading"]
    monkeypatch.setattr(base, "resolve_route", lambda surface: LOCAL)

    class Abstracts:
        async def abstract_from_text(self, *args):
            raise AssertionError("waited on a model the router is still loading")

        @staticmethod
        def fallback_abstract(filename, file_type):
            return f"fallback for {filename}"

    handler = Handler(abstract_extractor=Abstracts())
    ctx = SimpleNamespace(filename="r.pdf", message_id="m1", extraction_error=None)
    assert await handler._abstract(ctx, "Glucose 5.2 mmol/L", "zh") == ("fallback for r.pdf", "r.pdf")


async def test_an_unreadable_document_files_its_reason_as_phrases():
    """The upload-time failure: OCR could not load its model, so the row's
    reason is "could not read the document" and why, as phrases."""
    from mirobody.collect.files.errors import UploadError, reason_phrases, say

    base, Handler = _handler()
    phrases = [{"key": "document_not_read"}, *reason_phrases(served.ModelNotReady("glm-ocr", failed=True))]
    ctx = SimpleNamespace(filename="r.png", message_id="m1")
    answer = await Handler()._handle_error(ctx, UploadError(say(phrases), phrases), "web_uploads/r.png")
    assert answer["error_phrases"] == [{"key": "document_not_read"},
                                       {"key": "model_failed_to_load", "params": {"model": "glm-ocr"}}]
    assert answer["message"].startswith("PDF processing failed: could not read the document: the local model")

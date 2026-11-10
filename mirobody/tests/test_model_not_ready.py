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
    assert "minicpm5-2b is still downloading or loading" in written["failed_reason"]
    assert written["indicators_count"] == 0


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

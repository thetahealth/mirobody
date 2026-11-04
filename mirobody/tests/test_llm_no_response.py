"""A completion of `None` is "no model answered", not an AttributeError.

The OpenAI client returns `None`, without raising, when an endpoint answers
HTTP 200 with a JSON `null` body. On 1.5.10 a 587 s call did that during a
provider's 503 outage and the `response.choices` access raised
`AttributeError`, so the log named a line in this repo instead of the model
that did not answer (see #239). The three OpenAI-compatible surfaces each met
it: structured output and text completion return their None, and vision, whose
contract is that a failed request raises, raises `NoResponse`, whose message
is what the uploader is told.
"""

from __future__ import annotations

import logging

import pytest

# The model-client factory arrives with the `[parse]` extra (`openai`, the PDF
# library), so CI's minimal job — `pip install -e '.[test]'` — would abort
# COLLECTION here rather than skip. Decided at import time, which is the only
# point early enough (see conftest.py).
try:
    from mirobody.utils.config.llm import RouteSpec
    from mirobody.utils.llm import NoResponse, clients, utils
    from mirobody.utils.llm.file_processors import backends_openai
    from mirobody.utils.llm.file_processors.media import VisionImage
except ImportError as missing:  # pragma: no cover - the minimal install
    pytest.skip(f"needs the [parse] extra: {missing}", allow_module_level=True)

_SPEC = RouteSpec(
    alias="test", model="test-model", api_key_env="", base_url="http://127.0.0.1:9"
)
_MESSAGES = [{"role": "user", "content": '{"hello": "kitty"}'}]
_SCHEMA = {
    "type": "json_schema",
    "json_schema": {"name": "answer", "schema": {"type": "object"}},
}


class _Completions:
    async def create(self, **_kwargs):
        return None


class _Chat:
    completions = _Completions()


class _Client:
    chat = _Chat()


@pytest.fixture
def empty_endpoint(monkeypatch):
    monkeypatch.setattr(utils, "_route", lambda *_args, **_kwargs: _SPEC)
    monkeypatch.setattr(clients.client_manager, "for_spec", lambda _spec: _Client())


async def test_structured_output_returns_no_answer(empty_endpoint, caplog):
    with caplog.at_level(logging.WARNING, logger="mirobody.utils.llm.utils"):
        answer = await utils.async_get_structured_output(_MESSAGES, _SCHEMA)

    assert answer is None
    # Without the guard the None is caught by the broad handler below it and
    # still returns None, so the return value alone proves nothing: the log did
    # name an AttributeError in this file rather than a model that did not
    # answer, which is what sent a 1.5.10 report looking for a bug here.
    assert "structured output: no response" in caplog.text
    assert "AttributeError" not in caplog.text


async def test_text_completion_returns_no_answer(empty_endpoint, caplog):
    with caplog.at_level(logging.WARNING, logger="mirobody.utils.llm.utils"):
        answer = await utils.async_get_text_completion(_MESSAGES)

    assert answer is None
    assert "text completion: no response" in caplog.text
    assert "AttributeError" not in caplog.text


async def test_vision_raises_no_response(empty_endpoint):
    image = VisionImage(data="", mime="image/png")
    with pytest.raises(NoResponse, match="test-model sent back an empty response"):
        await backends_openai.image_extract(_SPEC, image, "read it", json_mode=False)


def test_the_uploader_is_told_why():
    try:
        from mirobody.collect.files.errors import failure_reason
    except ImportError as missing:  # pragma: no cover - an install without [app]
        pytest.skip(f"needs the [app] extra: {missing}")
    # Any other exception shows only its type name; this one says what to do.
    assert failure_reason(NoResponse("deepseek-flash")).startswith("deepseek-flash sent back an empty response")

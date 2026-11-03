"""A completion of `None` is "no model answered", not an AttributeError.

`async_get_structured_output` documents `None` as a normal return — no route,
a failed call, a refusal — and its own callers already branch on it. A
compatibility layer can produce `None` without raising when a provider's 503
arrives late; on 1.5.10 a 587 s call ended that way and the `response.choices`
access raised `AttributeError`, so the log named a line in this repo instead of
the model that did not answer (see #239).
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
    from mirobody.utils.llm import clients, utils
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


async def test_a_none_completion_returns_no_answer(monkeypatch, caplog):
    monkeypatch.setattr(utils, "_route", lambda *_args, **_kwargs: _SPEC)
    monkeypatch.setattr(clients.client_manager, "for_spec", lambda _spec: _Client())

    with caplog.at_level(logging.WARNING, logger="mirobody.utils.llm.utils"):
        answer = await utils.async_get_structured_output(_MESSAGES, _SCHEMA)

    assert answer is None
    # Without the guard the None is caught by the broad handler below it and
    # still returns None, so the return value alone proves nothing: the log did
    # name an AttributeError in this file rather than a model that did not
    # answer, which is what sent a 1.5.10 report looking for a bug here.
    assert "structured output: no response" in caplog.text
    assert "AttributeError" not in caplog.text

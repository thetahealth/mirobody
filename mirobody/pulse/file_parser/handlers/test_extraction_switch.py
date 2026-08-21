"""`ENABLE_INDICATOR_EXTRACTION` actually switches extraction off.

It was documented in config.yaml AND docs/file-processing.md and read by no
code, so setting it to 0 changed nothing. Indicator extraction is one LLM call
per uploaded file — the most expensive step in the upload path — which makes a
silently-ignored off switch a bill rather than a cosmetic bug. These tests are
the reason it cannot go quiet again.
"""

from __future__ import annotations

import pytest

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler


class _Handler(BaseFileHandler):
    """BaseFileHandler is abstract; the two abstract methods are irrelevant here."""

    def get_type_name(self) -> str:
        return "pdf"

    async def _process_content(self, *a, **kw):
        raise AssertionError("not exercised")


@pytest.mark.parametrize(
    "raw, enabled",
    [
        ("1", True),
        ("", True),            # unset -> on, which is what every deployment has today
        ("true", True),
        ("yes", True),
        ("2", True),           # anything not recognisably "off" stays on
        ("0", False),
        ("false", False),
        ("FALSE", False),      # case-insensitive
        ("no", False),
        ("off", False),
        ("  0  ", False),      # YAML round-trips can leave whitespace
    ],
)
def test_switch_parsing(monkeypatch, raw, enabled):
    monkeypatch.setattr(
        "mirobody.utils.config.safe_read_cfg",
        lambda key, default="": raw if key == "ENABLE_INDICATOR_EXTRACTION" else default,
    )
    assert BaseFileHandler._indicator_extraction_enabled() is enabled


def test_default_is_on_when_no_config_is_loaded():
    """A library caller with no Config initialised must not lose extraction."""
    assert BaseFileHandler._indicator_extraction_enabled() is True


def test_disabled_schedules_no_task(monkeypatch):
    """The gate is at the SCHEDULING site, so nothing is spawned at all —
    the LLM call is not made and then discarded."""
    import asyncio

    monkeypatch.setattr(
        "mirobody.utils.config.safe_read_cfg",
        lambda key, default="": "0" if key == "ENABLE_INDICATOR_EXTRACTION" else default,
    )

    def explode(*a, **kw):
        raise AssertionError("a background extraction task was created while disabled")

    monkeypatch.setattr(asyncio, "create_task", explode)

    handler = _Handler.__new__(_Handler)
    handler._background_tasks = set()

    handler._start_background_indicator_extraction(
        original_text="glucose 5.6 mmol/L", user_id=1, file_name="labs.pdf", file_key="k",
    )
    assert handler._background_tasks == set()


def test_enabled_schedules_the_task(monkeypatch):
    import asyncio

    monkeypatch.setattr(
        "mirobody.utils.config.safe_read_cfg",
        lambda key, default="": "1" if key == "ENABLE_INDICATOR_EXTRACTION" else default,
    )

    created: list[object] = []

    class FakeTask:
        def add_done_callback(self, cb):
            pass

    def fake_create_task(coro, *a, **kw):
        coro.close()                     # do not actually run the extraction
        created.append(coro)
        return FakeTask()

    monkeypatch.setattr(asyncio, "create_task", fake_create_task)

    handler = _Handler.__new__(_Handler)
    handler._background_tasks = set()

    handler._start_background_indicator_extraction(
        original_text="glucose 5.6 mmol/L", user_id=1, file_name="labs.pdf", file_key="k",
    )
    assert len(created) == 1

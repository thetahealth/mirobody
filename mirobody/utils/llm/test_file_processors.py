"""Contract tests for the vision-extraction layer.

Written BEFORE this module was split into a package, so the split has
something to be checked against: these cover the provider selection rules, the
model/json_mode resolution, and the pure merge/clean helpers — everything the
callers depend on that does not require a live model.

They are also the first tests this module has ever had. It reached 842 lines
and four providers with none.
"""

from __future__ import annotations

import json

import pytest

from . import file_processors as fp
from .file_processors import dispatch

# Provider selection and dispatch are patched on `dispatch`, not on the package:
# monkeypatch has to replace a name where it is LOOKED UP, and both
# `safe_read_cfg` and `PROVIDER_HANDLERS` are resolved inside that module. The
# assertions below are unchanged from when this file tested the single
# pre-split module — only the patch target moved, which is what makes them a
# valid before/after check on the split.


#--- pure helpers ------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ('```json\n{"a": 1}\n```', '{"a": 1}'),
        ('```\n{"a": 1}\n```', '{"a": 1}'),
        ('{"a": 1}', '{"a": 1}'),
        ("  spaced  ", "spaced"),
        ("", ""),
    ],
)
def test_clean_json_response_strips_fences(raw, expected):
    assert fp.clean_json_response(raw) == expected


def test_build_prompt_with_schema_without_schema_still_asks_for_json():
    """Providers without native schema support are steered by prose alone."""
    out = fp._build_prompt_with_schema("extract", None)
    assert out.startswith("extract")
    assert "JSON" in out


def test_build_prompt_with_schema_embeds_a_dict_schema():
    out = fp._build_prompt_with_schema("extract", {"type": "object"})
    assert "extract" in out and '"type": "object"' in out


def test_build_prompt_with_schema_survives_an_unserialisable_schema():
    """A schema we cannot serialise must degrade to the plain ask, never raise —
    it runs inside an extraction the caller expects to complete."""

    class Bad:
        def to_dict(self):
            raise RuntimeError("nope")

    out = fp._build_prompt_with_schema("extract", Bad())
    assert out.startswith("extract") and "JSON" in out


#--- merging -----------------------------------------------------------------


def test_merge_json_results_unions_keys():
    out = json.loads(fp._merge_json_results(['{"a": 1}', '{"b": 2}']))
    assert out == {"a": 1, "b": 2}


def test_merge_json_results_concatenates_lists():
    """Page 2's rows must be appended to page 1's, not replace them — this is
    the whole point of multi-page extraction."""
    out = json.loads(fp._merge_json_results(['{"rows": [1]}', '{"rows": [2]}']))
    assert out == {"rows": [1, 2]}


def test_merge_json_results_fills_gaps_in_nested_dicts_without_overwriting():
    out = json.loads(
        fp._merge_json_results(['{"p": {"a": 1, "b": ""}}', '{"p": {"b": 2, "c": 3}}'])
    )
    assert out == {"p": {"a": 1, "b": 2, "c": 3}}


def test_merge_json_results_first_non_empty_scalar_wins():
    assert json.loads(fp._merge_json_results(['{"a": ""}', '{"a": 5}'])) == {"a": 5}
    assert json.loads(fp._merge_json_results(['{"a": 1}', '{"a": 9}'])) == {"a": 1}


def test_merge_json_results_skips_unparseable_pages_rather_than_failing():
    out = json.loads(fp._merge_json_results(['{"a": 1}', "not json", '{"b": 2}']))
    assert out == {"a": 1, "b": 2}


def test_merge_page_results_empty_input_is_empty_output():
    assert fp._merge_page_results([], json_mode=False) == ""
    assert fp._merge_page_results([{"error": "boom", "page": 1}], json_mode=False) == ""


def test_merge_page_results_single_page_returns_it_directly():
    assert fp._merge_page_results([{"page": 1, "content": "hello"}], json_mode=False) == "hello"


def test_merge_page_results_single_page_json_is_defenced():
    out = fp._merge_page_results([{"page": 1, "content": '```json\n{"a":1}\n```'}], json_mode=True)
    assert out == '{"a":1}'


def test_merge_page_results_text_mode_labels_pages():
    out = fp._merge_page_results(
        [{"page": 1, "content": "one"}, {"page": 2, "content": "two"}], json_mode=False
    )
    assert "[Page 1]" in out and "[Page 2]" in out and "one" in out and "two" in out


def test_merge_page_results_json_mode_merges_instead_of_labelling():
    out = fp._merge_page_results(
        [{"page": 1, "content": '{"rows": [1]}'}, {"page": 2, "content": '{"rows": [2]}'}],
        json_mode=True,
    )
    assert json.loads(out) == {"rows": [1, 2]}


#--- provider selection ------------------------------------------------------


@pytest.fixture
def keys(monkeypatch):
    """Control which provider API keys appear configured.

    Mirrors the real `safe_read_cfg`, INCLUDING its default: an unset key
    returns the caller's default, which is how `unified_file_extract` gets a
    provider's default model when no `<PROVIDER>_VISION_MODEL` override exists.
    """
    configured: dict[str, str] = {}

    def fake(key, default=""):
        return configured.get(key, default)

    monkeypatch.setattr(dispatch, "safe_read_cfg", fake)
    return configured


def test_provider_priority_is_gemini_first(keys):
    keys.update({"GOOGLE_API_KEY": "k", "OPENROUTER_API_KEY": "k"})
    assert fp.VisionProviderConfig.get_available_provider()["name"] == "gemini"


def test_provider_falls_through_to_the_next_configured_key(keys):
    keys["DASHSCOPE_API_KEY"] = "k"
    assert fp.VisionProviderConfig.get_available_provider()["name"] == "qwen"


def test_no_keys_means_no_provider(keys):
    assert fp.VisionProviderConfig.get_available_provider() is None


def test_provider_status_reports_every_provider(keys):
    keys["GOOGLE_API_KEY"] = "k"
    status = fp.VisionProviderConfig.get_provider_status()
    assert status["gemini"] is True
    assert set(status) == {"gemini", "openrouter", "qwen", "doubao"}
    assert fp.VisionProviderConfig.list_available_providers() == ["gemini"]


#--- dispatch ----------------------------------------------------------------


@pytest.fixture
def spy(monkeypatch):
    """Replace every provider handler with a recorder."""
    calls: list[dict] = []

    async def handler(**kwargs):
        calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(
        dispatch, "PROVIDER_HANDLERS", {n: handler for n in dispatch.PROVIDER_HANDLERS}
    )
    return calls


async def test_unified_extract_routes_to_the_available_provider(keys, spy):
    keys["DASHSCOPE_API_KEY"] = "k"
    assert await fp.unified_file_extract("/f.png", "p") == "ok"
    assert spy[0]["model"] == "qwen3-vl-flash", "should use the provider's default model"


async def test_an_explicit_provider_overrides_priority(keys, spy):
    keys.update({"GOOGLE_API_KEY": "k", "VOLCENGINE_API_KEY": "k"})
    await fp.unified_file_extract("/f.png", "p", provider="doubao")
    assert spy[0]["model"] == "doubao-seed-1-6-vision-250815"


async def test_an_explicit_model_overrides_the_default(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"
    await fp.unified_file_extract("/f.png", "p", model="gemini-x")
    assert spy[0]["model"] == "gemini-x"


async def test_no_configured_provider_raises_with_the_status_in_the_message(keys, spy):
    with pytest.raises(ValueError, match="No vision provider available"):
        await fp.unified_file_extract("/f.png", "p")


async def test_an_unknown_provider_name_raises(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"
    with pytest.raises(ValueError, match="Unknown provider"):
        await fp.unified_file_extract("/f.png", "p", provider="nope")


async def test_naming_a_provider_whose_key_is_missing_raises(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"
    with pytest.raises(ValueError, match="API key not configured"):
        await fp.unified_file_extract("/f.png", "p", provider="doubao")


async def test_json_mode_is_inferred_from_a_response_schema(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"

    class Cfg:
        response_schema = {"type": "object"}
        response_mime_type = None

    await fp.unified_file_extract("/f.png", "p", config=Cfg())
    assert spy[0]["json_mode"] is True
    assert spy[0]["response_schema"] == {"type": "object"}


async def test_json_mode_is_inferred_from_the_mime_type(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"

    class Cfg:
        response_schema = None
        response_mime_type = "application/json"

    await fp.unified_file_extract("/f.png", "p", config=Cfg())
    assert spy[0]["json_mode"] is True


async def test_json_mode_defaults_off_without_a_config(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"
    await fp.unified_file_extract("/f.png", "p")
    assert spy[0]["json_mode"] is False


async def test_an_explicit_json_mode_beats_inference(keys, spy):
    keys["GOOGLE_API_KEY"] = "k"

    class Cfg:
        response_schema = {"type": "object"}
        response_mime_type = None

    await fp.unified_file_extract("/f.png", "p", config=Cfg(), json_mode=False)
    assert spy[0]["json_mode"] is False

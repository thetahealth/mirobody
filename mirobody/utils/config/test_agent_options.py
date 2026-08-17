"""The `PROMPTS_<AGENT>` / `PROVIDERS_<AGENT>` parsing rules.

Both used to be inline in `Config.get_options_for_agent`, so exercising them
meant building a Config. They take a reader now, and a dict is a reader.
"""

from __future__ import annotations

from .agent_options import load_prompt_templates, parse_providers


class Reader:
    """The two-method slice of Config these functions use."""

    def __init__(self, values: dict):
        self._values = values

    def get(self, key: str, default=None):
        return self._values.get(key, default)


#--- prompts -----------------------------------------------------------------


def test_a_shipped_prompt_resolves_to_its_text_keyed_by_basename():
    """`agent/prompts/deep.jinja` is inside the package, so it resolves even
    when the process was not started from the source tree."""
    out = load_prompt_templates(Reader({"PROMPTS_X": ["agent/prompts/deep.jinja"]}), "X")
    assert list(out) == ["deep"]
    assert len(out["deep"]) > 100, "expected the template body, not the path"


def test_at_suffix_overrides_the_key():
    out = load_prompt_templates(Reader({"PROMPTS_X": ["agent/prompts/deep.jinja@main"]}), "X")
    assert list(out) == ["main"]


def test_an_unresolvable_path_keeps_the_string_as_the_value():
    """Historical behaviour, and load-bearing: it is how a deployment passes a
    literal prompt where a path is expected. Such entries have no name, so they
    are numbered."""
    out = load_prompt_templates(Reader({"PROMPTS_X": ["just some prompt text"]}), "X")
    assert out == {"Prompt_1": "just some prompt text"}


def test_unnamed_entries_number_upwards():
    out = load_prompt_templates(Reader({"PROMPTS_X": ["one", "two"]}), "X")
    assert out == {"Prompt_1": "one", "Prompt_2": "two"}


def test_a_json_string_is_accepted_because_env_vars_cannot_hold_lists():
    out = load_prompt_templates(Reader({"PROMPTS_X": '["agent/prompts/deep.jinja"]'}), "X")
    assert list(out) == ["deep"]


def test_a_bare_string_is_one_entry():
    out = load_prompt_templates(Reader({"PROMPTS_X": "agent/prompts/deep.jinja"}), "X")
    assert list(out) == ["deep"]


def test_missing_key_and_non_strings_yield_nothing_extra():
    assert load_prompt_templates(Reader({}), "X") == {}
    assert load_prompt_templates(Reader({"PROMPTS_X": [None, 42]}), "X") == {}


#--- providers ---------------------------------------------------------------


def test_a_mapping_is_taken_as_is():
    cfg = {"gemini-2.5-flash": {"model": "gemini-2.5-flash"}}
    assert parse_providers(Reader({"PROVIDERS_X": cfg}), "X") == cfg


def test_a_list_is_keyed_by_its_provider_field_which_is_then_removed():
    out = parse_providers(
        Reader({"PROVIDERS_X": [{"provider": "gpt", "model": "gpt-5-nano"}]}), "X"
    )
    assert out == {"gpt": {"model": "gpt-5-nano"}}


def test_the_provider_name_is_stripped():
    out = parse_providers(Reader({"PROVIDERS_X": [{"provider": "  gpt  ", "a": 1}]}), "X")
    assert out == {"gpt": {"a": 1}}


def test_list_entries_without_a_usable_provider_name_are_skipped():
    out = parse_providers(
        Reader({"PROVIDERS_X": [{"model": "m"}, {"provider": ""}, {"provider": 42}]}), "X"
    )
    assert out == {}


def test_parsing_a_list_does_not_mutate_the_caller_s_config():
    """The original `del item["provider"]` edited the config dict in place, so
    the same value parsed twice lost its name the second time."""
    raw = [{"provider": "gpt", "model": "m"}]
    parse_providers(Reader({"PROVIDERS_X": raw}), "X")
    assert raw == [{"provider": "gpt", "model": "m"}]
    assert parse_providers(Reader({"PROVIDERS_X": raw}), "X") == {"gpt": {"model": "m"}}


def test_a_json_string_of_either_shape_is_accepted():
    assert parse_providers(Reader({"PROVIDERS_X": '{"a": {"model": "m"}}'}), "X") == {
        "a": {"model": "m"}
    }
    assert parse_providers(Reader({"PROVIDERS_X": '[{"provider": "b", "k": 1}]'}), "X") == {
        "b": {"k": 1}
    }


def test_unparseable_or_missing_yields_empty():
    assert parse_providers(Reader({}), "X") == {}
    assert parse_providers(Reader({"PROVIDERS_X": "not json"}), "X") == {}

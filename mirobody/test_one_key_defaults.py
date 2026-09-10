"""ONE key must run the whole shipped project — any of the five, from config alone.

The promise `config.llm.yaml` makes: put ONE of OPENROUTER_API_KEY,
DASHSCOPE_API_KEY, GOOGLE_API_KEY, OPENAI_API_KEY or DEEPSEEK_API_KEY in .env
and chat, vision/file parsing, text extraction and (where the vendor serves
one) embeddings all work, with zero further configuration — and every one of
those decisions is written in that file, where a user can read and change it.

Every piece of that is a default that can drift: the table at the top of the
file is prose, an entry in a route list can lose its `supports_image`, an
entry the chat default names can disappear. Each is pinned here, against the
SHIPPED files — the thing a self-hoster gets — loaded the way the server loads
them (config.yaml and its INCLUDE list).

Issue #68 is the failure these guard: `DEEPSEEK_API_KEY` was declared in the
config, present in one of five selection tables, and a deployment holding only
that key uploaded three documents into silence.
"""

from __future__ import annotations

import os
import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_LLM_YAML = _ROOT / "config.llm.yaml"

ONE_KEY = ("OPENROUTER_API_KEY", "DASHSCOPE_API_KEY", "GOOGLE_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY")


@pytest.fixture
def shipped(monkeypatch):
    """The shipped configuration as the process-wide Config, restored after."""
    import mirobody.utils.config.config as cfg_mod
    from mirobody.utils.config.config import Config

    monkeypatch.setattr(cfg_mod, "_global_config", cfg_mod._global_config)
    return Config(yaml_filenames=str(_ROOT / "config.yaml"))


@pytest.fixture
def only_env(monkeypatch, shipped):
    """Key lookups see only os.environ, so each test controls exactly which
    keys exist; the route lists and entries still come from the shipped files."""
    import mirobody.utils.config as cfg

    def fake(key, default=""):
        return os.environ.get(key, default)

    monkeypatch.setattr(cfg, "safe_read_cfg", fake)
    for key in (*ONE_KEY, "GEMINI_API_KEY", "GOOGLE_GENERATIVE_AI_API_KEY",
                "UTILS_VISION_MODEL", "UTILS_TEXT_MODEL", "UTILS_EMBEDDING_MODEL"):
        monkeypatch.delenv(key, raising=False)
    return shipped


def _table_rows() -> list[dict[str, str]]:
    """The one-key table at the top of config.llm.yaml: key, chat entry,
    utility entry (vision + text), embedding entry — one row per key, as a
    reader sees it."""
    rows = []
    for line in _LLM_YAML.read_text(encoding="utf-8").splitlines():
        # the table rows, not the "where to get one" lines (`KEY -> https://…`)
        m = re.match(r"#\s+([A-Z_]+_API_KEY)\s+(?!->)(\S+)\s+(\S+)\s+(\S+)", line)
        if m:
            rows.append(dict(zip(("key", "chat", "utils", "embedding"), m.groups(), strict=True)))
    return rows


#--- the table a reader sees is the configuration the code reads ----------------


def test_the_table_names_every_one_key_path_once():
    rows = _table_rows()
    assert [r["key"] for r in rows] == list(ONE_KEY), "the table must have exactly one row per one-key credential, in this order"


def test_each_table_row_matches_the_entries_and_routes(shipped):
    """Prose drifts; this holds the table to the YAML below it."""
    from mirobody.utils.config.llm import model_entries, route_value

    entries = model_entries()
    vision, text, embedding = route_value("vision"), route_value("text"), route_value("embedding")
    for row in _table_rows():
        key = row["key"]
        for column in ("chat", "utils", "embedding"):
            alias = row[column]
            if alias == "—":
                continue
            assert alias in entries, f"{key}: table names {alias!r} for {column}, which is not a MODELS entry"
            assert entries[alias].get("api_key") == key, f"{alias} reads {entries[alias].get('api_key')}, the table puts it under {key}"
        assert entries[row["chat"]].get("chat", True) is not False, f"{row['chat']} is utility-only, it cannot be the chat column"
        utils = row["utils"]
        assert utils in vision and utils in text, f"{utils} must be in both UTILS_VISION_MODEL {vision} and UTILS_TEXT_MODEL {text}"
        assert entries[utils].get("supports_image") is True, f"{utils}: a utility entry reads report photos, it must be multimodal"
        assert entries[utils].get("chat") is False, f"{utils}: utility entries stay out of the chat picker"
        if row["embedding"] != "—":
            assert row["embedding"] in embedding, f"{row['embedding']}: not in UTILS_EMBEDDING_MODEL {embedding}"
            assert entries[row["embedding"]].get("embedding"), f"{row['embedding']}: an embedding entry names its vector-column family"


#--- each key, alone, routes every surface ---------------------------------------


@pytest.mark.parametrize("key,alias_key", [
    ("OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
    ("DASHSCOPE_API_KEY", "DASHSCOPE_API_KEY"),
    ("GOOGLE_API_KEY", "GOOGLE_API_KEY"),
    ("GEMINI_API_KEY", "GOOGLE_API_KEY"),   # Google's own name for the same key
    ("OPENAI_API_KEY", "OPENAI_API_KEY"),
    ("DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"),
])
def test_one_key_alone_routes_chat_vision_and_text(only_env, monkeypatch, key, alias_key):
    from mirobody.utils.config.llm import chat_default, model_entries, resolve_route

    monkeypatch.setenv(key, "sk-x")
    default = chat_default()
    assert default and model_entries()[default].get("api_key") == alias_key, f"chat default {default!r} does not read {alias_key}"
    for surface in ("vision", "text"):
        spec = resolve_route(surface)
        assert spec is not None, f"{surface}: nothing routable with {key} alone"
        assert spec.api_key_env == alias_key, f"{surface} chose {spec.alias}, which reads {spec.api_key_env}, not {alias_key}"
        if surface == "vision":
            assert spec.supports_image is True, f"vision chose {spec.alias}, which does not declare supports_image: true"


def test_embedding_follows_the_key_and_is_honest_about_deepseek(only_env, monkeypatch):
    from mirobody.utils.embedding import resolve_embedding_provider

    assert resolve_embedding_provider() == "", "zero keys must not pretend to be openrouter"
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-d")
    assert resolve_embedding_provider() == "", "DeepSeek serves no embedding model — say so, do not borrow another gateway"
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    assert resolve_embedding_provider() == "qwen"
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-y")  # first in the list wins
    assert resolve_embedding_provider() == "openrouter"
    monkeypatch.setenv("UTILS_EMBEDDING_MODEL", "gemini-embed")  # a same-named env var pins one entry…
    assert resolve_embedding_provider() == "", "a pinned entry without its key selects nothing — no borrowing"
    monkeypatch.setenv("GOOGLE_API_KEY", "sk-g")
    assert resolve_embedding_provider() == "gemini"
    monkeypatch.setenv("UTILS_EMBEDDING_MODEL", "qwen")  # …or a vector-column family, the value 1.4.0 overlays carry
    assert resolve_embedding_provider() == "qwen"


def test_zero_keys_names_the_fix(only_env):
    from mirobody.utils.config.llm import chat_default, no_provider_message, resolve_route

    assert chat_default() is None and resolve_route("vision") is None
    message = no_provider_message("vision")
    assert "UTILS_VISION_MODEL" in message and ".env" in message
    for key in ONE_KEY:
        assert key in message, f"the fix must name {key}"


#--- the entries are honest about what they can do -------------------------------


def test_every_vision_candidate_declares_image_support(shipped):
    """The capability #68 was missing, as data: an entry the vision route
    lists must say it reads images, so a text-only model is never asked to."""
    from mirobody.utils.config.llm import RouteSpec, route_candidates

    for candidate in route_candidates("vision"):
        assert isinstance(candidate, RouteSpec), f"{candidate}: UTILS_VISION_MODEL names something that is not an entry"
        assert candidate.supports_image is True, f"{candidate.alias}: in UTILS_VISION_MODEL without supports_image: true"


def test_utility_only_entries_stay_out_of_the_chat_picker(shipped):
    from mirobody.utils.config.llm import chat_entries, model_entries

    hidden = {n for n, e in model_entries().items() if (e or {}).get("chat") is False}
    assert hidden, "the shipped file has utility-only entries (thinking off, chat: false)"
    assert not (hidden & set(chat_entries())), "a chat: false entry reached the picker"


def test_every_embedding_name_is_fully_wired(shipped):
    from mirobody.indicator.fhir.common import DIM_EMBEDDING_COLUMN, FHIR_EMBEDDING_COLUMN
    from mirobody.utils.config.llm import model_entries, route_value
    from mirobody.utils.embedding import EMBEDDING_PROVIDERS, embedding_model_id

    schema = (_ROOT / "mirobody" / "schema" / "01_basedata.sql").read_text(encoding="utf-8")
    names = route_value("embedding")
    assert isinstance(names, list) and names, "UTILS_EMBEDDING_MODEL ships as a list of entry names"
    for name in names:
        entry = model_entries().get(name) or {}
        family = entry.get("embedding")
        assert family in EMBEDDING_PROVIDERS, f"{name}: `embedding: {family}` names no API factory"
        assert embedding_model_id(family) == entry.get("model"), f"{name}: the family's model id is not this entry's"
        for table_map in (FHIR_EMBEDDING_COLUMN, DIM_EMBEDDING_COLUMN):
            column = table_map.get(family)
            assert column and column in schema, f"{family}: column {column!r} missing from the map or from schema/01_basedata.sql"


def test_python_holds_no_model_defaults():
    """The owner's rule (#52, restated 2026-09-10): a model id is configuration.
    The package may mention ids in comments about the past; it may not have a
    code path that selects one. Everything that used to — the registry, the
    vision table, the embedding table — reads config.llm.yaml now."""
    from mirobody.utils import embedding
    from mirobody.utils.config import llm

    assert not hasattr(llm, "PROVIDERS") and not hasattr(llm, "MODELS"), "a Python registry of models is back"
    assert not hasattr(llm, "SURFACE_PRIORITY"), "a Python priority table is back"
    assert not hasattr(embedding, "EMBEDDING_MODEL_IDS"), "embedding model ids belong to the MODELS entries in config.llm.yaml"


#--- the README, and the redirect rule --------------------------------------------


def test_every_live_readme_hands_out_the_one_key_path():
    """The README's job is to hand a visitor a working path: one key in .env,
    with the actual place to get it, and the file where every model decision
    lives. The two frozen editions under `archived/` are not checked here."""
    for name in ("README.md", "README.zh-CN.md"):
        text = (_ROOT / name).read_text(encoding="utf-8")
        assert "openrouter.ai/keys" in text, f"{name}: no OpenRouter key link"
        assert "platform.openai.com/api-keys" in text, f"{name}: no OpenAI key link"
        assert ".env" in text, f"{name}: does not say the key goes in .env"
        assert "api_key: OPENROUTER_API_KEY" in text, f"{name}: does not show that config.llm.yaml names the variable, not the secret"
        assert "config.llm.yaml" in text, f"{name}: does not point at config.llm.yaml"
        assert "OPENAI_API_KEY" in text and "OPENROUTER_API_KEY" in text, f"{name}: does not name both key variables"


def test_a_self_hosted_base_url_override_is_honored(shipped, monkeypatch):
    """The README's self-hosting line — serve the same embedding model behind
    any OpenAI-compatible endpoint and point `OPENROUTER_BASE_URL` at it —
    must be a mechanism, not prose, on every surface that reads the key."""
    from mirobody.utils.config.llm import LLMProvider, resolve_named
    from mirobody.utils.llm.clients import AIClientManager

    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or")
    monkeypatch.setenv("OPENROUTER_BASE_URL", "http://vllm.internal:8000/v1")
    assert shipped.get_llm(LLMProvider.OPENROUTER).base_url == "http://vllm.internal:8000/v1"
    spec = resolve_named("openrouter-utils")
    assert spec.base_url == "http://vllm.internal:8000/v1", "a MODELS entry reading the key follows the redirect"
    assert str(AIClientManager().for_spec(spec).base_url).rstrip("/") == "http://vllm.internal:8000/v1"

    monkeypatch.delenv("OPENROUTER_BASE_URL")
    shipped._llms.clear()
    assert shipped.get_llm(LLMProvider.OPENROUTER).base_url == "https://openrouter.ai/api/v1", "unset override must fall back to the entry's endpoint"
    assert resolve_named("openrouter-utils").base_url == "https://openrouter.ai/api/v1"


def test_retired_per_key_model_overrides_are_named(shipped, monkeypatch):
    """`<PREFIX>_MODEL` / `_VISION_MODEL` / `_EMBEDDING_MODEL` chose a model per
    KEY; a model belongs to an entry now. Silently ignoring them would leave a
    deployment on the shipped default with nothing to point at."""
    from mirobody.utils.config.llm import retired_model_keys

    monkeypatch.setenv("GEMINI_VISION_MODEL", "gemini-3.5-flash")
    monkeypatch.setenv("OPENROUTER_MODEL", "x")
    assert {"GEMINI_VISION_MODEL", "OPENROUTER_MODEL"} <= set(retired_model_keys())
    assert "UTILS_VISION_MODEL" not in retired_model_keys()
